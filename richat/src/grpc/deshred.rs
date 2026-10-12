//! Aggregation of `SubscribeDeshred` streams from several upstream sources.
//!
//! Deshred transactions are emitted at shred reassembly, before execution, so
//! they carry no `TransactionStatusMeta` and have no commitment level. That
//! makes this path much simpler than [`crate::channel`]: there is nothing to
//! replay, nothing to store and no per-commitment view. A broadcast plus a
//! first-seen dedup is sufficient.

use {
    futures::stream::StreamExt,
    prost::Message as _,
    richat_client::grpc::ConfigGrpcClient,
    richat_proto::geyser::{
        SubscribeDeshredRequest, SubscribeRequestFilterDeshredTransactions,
        SubscribeRequestFilterSlots, SubscribeUpdateDeshred, SubscribeUpdateDeshredTransaction,
        SubscribeUpdateDeshredTransactionInfo, SubscribeUpdatePing, SubscribeUpdatePong,
        SubscribeUpdateSlot, subscribe_update_deshred::UpdateOneof,
    },
    std::{
        collections::{BTreeMap, HashMap, HashSet},
        pin::Pin,
        sync::{Arc, Mutex},
        task::{Context, Poll},
        time::Duration,
    },
    tokio::sync::{broadcast, mpsc},
    tokio_util::sync::CancellationToken,
    tracing::{info, warn},
};

/// Length of a Solana transaction signature.
const SIGNATURE_LEN: usize = 64;

/// Length of a Solana public key.
const PUBKEY_LEN: usize = 32;

/// What the relay fans out: either a deduplicated transaction or a slot update.
///
/// Slot updates are forwarded because `SubscribeDeshredRequest` accepts slot
/// filters; dropping them would silently ignore a filter the client asked for.
// Transactions outnumber slot updates by roughly three orders of magnitude, so
// boxing the large variant would add an indirection to the hot path to save
// bytes on the rare one. Both are already behind an `Arc` for the broadcast.
#[allow(clippy::large_enum_variant)]
#[derive(Debug)]
pub enum DeshredUpdate {
    Transaction(DeshredItem),
    Slot(SubscribeUpdateSlot),
}

impl DeshredUpdate {
    /// Emission time reported by the upstream node, preserved so that clients
    /// measuring propagation latency see the node's clock, not the relay's.
    const fn created_at(&self) -> Option<prost_types::Timestamp> {
        match self {
            Self::Transaction(item) => item.created_at,
            Self::Slot(_) => None,
        }
    }
}

/// One deduplicated deshred transaction, ready to be filtered per subscriber.
#[derive(Debug)]
pub struct DeshredItem {
    pub slot: u64,
    pub is_vote: bool,
    pub created_at: Option<prost_types::Timestamp>,
    pub transaction: SubscribeUpdateDeshredTransactionInfo,
    /// Every account key referenced by the transaction, including loaded
    /// address-lookup-table addresses. Precomputed once here so that each
    /// subscriber's filter does not walk the message again.
    pub accounts: HashSet<Vec<u8>>,
}

impl DeshredItem {
    fn new(
        slot: u64,
        transaction: SubscribeUpdateDeshredTransactionInfo,
        created_at: Option<prost_types::Timestamp>,
    ) -> Self {
        let mut accounts = HashSet::new();
        if let Some(tx) = transaction.transaction.as_ref()
            && let Some(message) = tx.message.as_ref()
        {
            for key in &message.account_keys {
                accounts.insert(key.clone());
            }
        }
        for key in &transaction.loaded_writable_addresses {
            accounts.insert(key.clone());
        }
        for key in &transaction.loaded_readonly_addresses {
            accounts.insert(key.clone());
        }

        Self {
            slot,
            is_vote: transaction.is_vote,
            created_at,
            transaction,
            accounts,
        }
    }
}

/// First-seen dedup across sources, keyed by signature within a slot.
///
/// Bounded by slot count rather than by time: once more than `retain_slots`
/// slots are tracked the oldest is dropped. A late duplicate from a slot that
/// has already aged out is re-emitted rather than dropped, which is the safe
/// direction — a duplicate is recoverable by the client, a missing
/// transaction is not.
#[derive(Debug)]
struct Dedup {
    slots: BTreeMap<u64, DedupSlot>,
    retain_slots: usize,
}

#[derive(Debug, Default)]
struct DedupSlot {
    signatures: HashSet<[u8; SIGNATURE_LEN]>,
    statuses: HashSet<i32>,
}

impl Dedup {
    fn new(retain_slots: usize) -> Self {
        Self {
            slots: BTreeMap::new(),
            retain_slots: retain_slots.max(1),
        }
    }

    /// Returns `true` if this is the first time the signature is seen for the slot.
    fn insert_signature(&mut self, slot: u64, signature: &[u8]) -> bool {
        // A signature that is not 64 bytes cannot have come from a valid
        // transaction; treat it as fresh rather than silently collapsing all
        // such messages onto one key.
        let Ok(signature) = <[u8; SIGNATURE_LEN]>::try_from(signature) else {
            return true;
        };
        let fresh = self
            .slots
            .entry(slot)
            .or_default()
            .signatures
            .insert(signature);
        self.trim();
        fresh
    }

    /// Returns `true` if this `(slot, status)` has not been seen yet.
    ///
    /// Slot updates are idempotent status transitions, but every source emits
    /// them, so without this N sources produce N copies of each one.
    fn insert_slot_status(&mut self, slot: u64, status: i32) -> bool {
        let fresh = self.slots.entry(slot).or_default().statuses.insert(status);
        self.trim();
        fresh
    }

    fn trim(&mut self) {
        while self.slots.len() > self.retain_slots {
            let oldest = *self
                .slots
                .keys()
                .next()
                .expect("non-empty: len checked above");
            self.slots.remove(&oldest);
        }
    }
}

/// A single client-supplied filter, resolved from the protobuf request.
#[derive(Debug, Default)]
struct DeshredFilter {
    vote: Option<bool>,
    account_include: HashSet<Vec<u8>>,
    account_exclude: HashSet<Vec<u8>>,
    account_required: HashSet<Vec<u8>>,
}

impl DeshredFilter {
    fn new(config: &SubscribeRequestFilterDeshredTransactions) -> Result<Self, String> {
        fn parse(keys: &[String], field: &str) -> Result<HashSet<Vec<u8>>, String> {
            keys.iter()
                .map(|key| {
                    let decoded = bs58::decode(key)
                        .into_vec()
                        .map_err(|_| format!("invalid pubkey in `{field}`: {key}"))?;
                    if decoded.len() != PUBKEY_LEN {
                        return Err(format!(
                            "invalid pubkey length in `{field}`: {key} ({} bytes, expected {PUBKEY_LEN})",
                            decoded.len()
                        ));
                    }
                    Ok(decoded)
                })
                .collect()
        }

        Ok(Self {
            vote: config.vote,
            account_include: parse(&config.account_include, "account_include")?,
            account_exclude: parse(&config.account_exclude, "account_exclude")?,
            account_required: parse(&config.account_required, "account_required")?,
        })
    }

    /// Matches Yellowstone's transaction-filter semantics: `exclude` rejects,
    /// `include` requires at least one, `required` requires all.
    fn matches(&self, item: &DeshredItem) -> bool {
        if let Some(vote) = self.vote
            && vote != item.is_vote
        {
            return false;
        }
        // Iterate the transaction's key set (tens of keys), never the
        // client-supplied filter set, which is attacker-sized.
        if !self.account_exclude.is_empty()
            && item
                .accounts
                .iter()
                .any(|key| self.account_exclude.contains(key))
        {
            return false;
        }
        if !self.account_include.is_empty()
            && !item
                .accounts
                .iter()
                .any(|key| self.account_include.contains(key))
        {
            return false;
        }
        if self.account_required.len() > item.accounts.len()
            || !self
                .account_required
                .iter()
                .all(|key| item.accounts.contains(key))
        {
            return false;
        }
        true
    }
}

/// The set of filters one subscriber asked for.
#[derive(Debug, Default)]
pub struct DeshredFilters {
    transactions: HashMap<String, DeshredFilter>,
    slots: Vec<String>,
}

impl DeshredFilters {
    pub fn new(request: &SubscribeDeshredRequest, limit: usize) -> Result<Self, String> {
        if request.deshred_transactions.len() > limit {
            return Err(format!(
                "max amount of deshred_transactions filters reached, only {limit} allowed"
            ));
        }
        if request.slots.len() > limit {
            return Err(format!(
                "max amount of slots filters reached, only {limit} allowed"
            ));
        }

        let transactions = request
            .deshred_transactions
            .iter()
            .map(|(name, config)| DeshredFilter::new(config).map(|filter| (name.clone(), filter)))
            .collect::<Result<HashMap<_, _>, _>>()?;

        Ok(Self {
            transactions,
            slots: request.slots.keys().cloned().collect(),
        })
    }

    pub fn is_empty(&self) -> bool {
        self.transactions.is_empty() && self.slots.is_empty()
    }

    /// Pong reply to a client keepalive.
    pub fn encode_pong(id: i32) -> Vec<u8> {
        SubscribeUpdateDeshred {
            filters: Vec::new(),
            update_oneof: Some(UpdateOneof::Pong(SubscribeUpdatePong { id })),
            created_at: None,
        }
        .encode_to_vec()
    }

    /// Server-side keepalive for an otherwise idle stream.
    pub fn encode_ping() -> Vec<u8> {
        SubscribeUpdateDeshred {
            filters: Vec::new(),
            update_oneof: Some(UpdateOneof::Ping(SubscribeUpdatePing {})),
            created_at: None,
        }
        .encode_to_vec()
    }

    /// Encodes the item for this subscriber, or `None` if nothing matched.
    ///
    /// The `filters` field names the matching filters, so it is
    /// subscriber-specific and the message cannot be encoded once and shared.
    pub fn encode(&self, update: &DeshredUpdate) -> Option<Vec<u8>> {
        let (filters, update_oneof) = match update {
            DeshredUpdate::Transaction(item) => {
                let filters = self
                    .transactions
                    .iter()
                    .filter(|(_, filter)| filter.matches(item))
                    .map(|(name, _)| name.clone())
                    .collect::<Vec<_>>();
                (
                    filters,
                    UpdateOneof::DeshredTransaction(SubscribeUpdateDeshredTransaction {
                        transaction: Some(item.transaction.clone()),
                        slot: item.slot,
                    }),
                )
            }
            // Slot filters carry no criteria that apply before execution, so
            // every slot filter the client installed matches every update.
            DeshredUpdate::Slot(slot) => (self.slots.clone(), UpdateOneof::Slot(slot.clone())),
        };
        if filters.is_empty() {
            return None;
        }

        let encoded = SubscribeUpdateDeshred {
            filters,
            update_oneof: Some(update_oneof),
            created_at: update.created_at(),
        };
        Some(encoded.encode_to_vec())
    }
}

/// A [`Stream`] over the per-subscriber channel.
///
/// Hand-rolled rather than pulling in `tokio-stream` for one adapter.
#[derive(Debug)]
pub struct DeshredStream {
    rx: mpsc::Receiver<Result<Vec<u8>, tonic::Status>>,
}

impl DeshredStream {
    pub const fn new(rx: mpsc::Receiver<Result<Vec<u8>, tonic::Status>>) -> Self {
        Self { rx }
    }
}

impl futures::stream::Stream for DeshredStream {
    type Item = Result<Vec<u8>, tonic::Status>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

/// Fan-out point for deduplicated deshred transactions.
#[derive(Debug, Clone)]
pub struct DeshredRelay {
    tx: broadcast::Sender<Arc<DeshredUpdate>>,
}

impl DeshredRelay {
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity.max(1));
        Self { tx }
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<DeshredUpdate>> {
        self.tx.subscribe()
    }

    /// Connects to every source and forwards deduplicated transactions.
    ///
    /// Sources are independent: one failing or finishing never terminates the
    /// others, it reconnects on its own.
    pub fn spawn(
        &self,
        endpoints: Vec<ConfigGrpcClient>,
        retain_slots: usize,
        reconnect_interval: Duration,
        shutdown: CancellationToken,
    ) {
        let dedup = Arc::new(Mutex::new(Dedup::new(retain_slots)));

        for endpoint in endpoints {
            let tx = self.tx.clone();
            let dedup = Arc::clone(&dedup);
            let shutdown = shutdown.clone();

            tokio::spawn(async move {
                let name = endpoint.endpoint.clone();
                loop {
                    if shutdown.is_cancelled() {
                        break;
                    }
                    if let Err(error) =
                        Self::run_source(&endpoint, &tx, &dedup, shutdown.clone()).await
                    {
                        warn!(
                            endpoint = name,
                            ?error,
                            "deshred source failed, reconnecting"
                        );
                    } else {
                        warn!(endpoint = name, "deshred source finished, reconnecting");
                    }
                    tokio::select! {
                        () = shutdown.cancelled() => break,
                        () = tokio::time::sleep(reconnect_interval) => {}
                    }
                }
            });
        }
    }

    async fn run_source(
        endpoint: &ConfigGrpcClient,
        tx: &broadcast::Sender<Arc<DeshredUpdate>>,
        dedup: &Mutex<Dedup>,
        shutdown: CancellationToken,
    ) -> anyhow::Result<()> {
        // Use the full client config rather than a bare Endpoint so that
        // x-token, TLS and TCP/HTTP2 keepalive apply. Without keepalive a
        // connection dropped without RST never errors and the source hangs.
        let mut client = endpoint.clone().connect().await?;

        // Subscribe to everything; per-client filtering happens on the way out.
        let request = SubscribeDeshredRequest {
            deshred_transactions: [(
                String::new(),
                SubscribeRequestFilterDeshredTransactions::default(),
            )]
            .into_iter()
            .collect(),
            slots: [(String::new(), SubscribeRequestFilterSlots::default())]
                .into_iter()
                .collect(),
            ping: None,
        };

        let mut stream = client.subscribe_deshred_once(request).await?;
        info!(endpoint = endpoint.endpoint, "deshred source connected");

        loop {
            let message = tokio::select! {
                () = shutdown.cancelled() => return Ok(()),
                message = stream.next() => match message {
                    Some(message) => message?,
                    None => return Ok(()),
                },
            };

            let update = SubscribeUpdateDeshred::decode(message.as_ref())?;
            let deshred = match update.update_oneof {
                Some(UpdateOneof::DeshredTransaction(deshred)) => deshred,
                Some(UpdateOneof::Slot(slot)) => {
                    let fresh = dedup
                        .lock()
                        .expect("deshred dedup mutex poisoned")
                        .insert_slot_status(slot.slot, slot.status);
                    if fresh {
                        let _ = tx.send(Arc::new(DeshredUpdate::Slot(slot)));
                    }
                    continue;
                }
                _ => continue,
            };
            let Some(transaction) = deshred.transaction else {
                continue;
            };

            let fresh = dedup
                .lock()
                .expect("deshred dedup mutex poisoned")
                .insert_signature(deshred.slot, transaction.signature.as_slice());
            if !fresh {
                continue;
            }

            // A send error only means nobody is subscribed right now.
            let _ = tx.send(Arc::new(DeshredUpdate::Transaction(DeshredItem::new(
                deshred.slot,
                transaction,
                update.created_at,
            ))));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(seed: u8) -> [u8; SIGNATURE_LEN] {
        [seed; SIGNATURE_LEN]
    }

    fn item(slot: u64, is_vote: bool) -> DeshredItem {
        DeshredItem::new(slot, info(&sig(1), is_vote), None)
    }

    fn info(signature: &[u8], is_vote: bool) -> SubscribeUpdateDeshredTransactionInfo {
        SubscribeUpdateDeshredTransactionInfo {
            signature: signature.to_vec(),
            is_vote,
            transaction: None,
            loaded_writable_addresses: vec![vec![1; 32]],
            loaded_readonly_addresses: vec![vec![2; 32]],
            completed_data_set_starting_shred_index: 0,
            completed_data_set_ending_shred_index_exclusive: 0,
        }
    }

    #[test]
    fn dedup_is_first_seen_per_slot() {
        let mut dedup = Dedup::new(4);
        assert!(dedup.insert_signature(1, &sig(1)));
        assert!(
            !dedup.insert_signature(1, &sig(1)),
            "same slot+sig is a duplicate"
        );
        assert!(
            dedup.insert_signature(2, &sig(1)),
            "same sig in another slot is new"
        );
    }

    #[test]
    fn dedup_evicts_oldest_slots() {
        let mut dedup = Dedup::new(2);
        dedup.insert_signature(1, &sig(1));
        dedup.insert_signature(2, &sig(2));
        dedup.insert_signature(3, &sig(3));
        assert_eq!(dedup.slots.len(), 2);
        assert!(
            dedup.insert_signature(1, &sig(1)),
            "slot 1 aged out, so it is seen as new again"
        );
    }

    #[test]
    fn filter_account_semantics() {
        let item = item(1, false);
        let writable = bs58::encode(vec![1; 32]).into_string();
        let readonly = bs58::encode(vec![2; 32]).into_string();
        let absent = bs58::encode(vec![9; 32]).into_string();

        let build = |config: SubscribeRequestFilterDeshredTransactions| {
            DeshredFilter::new(&config).expect("valid filter")
        };

        assert!(build(SubscribeRequestFilterDeshredTransactions::default()).matches(&item));

        assert!(
            build(SubscribeRequestFilterDeshredTransactions {
                account_include: vec![writable.clone()],
                ..Default::default()
            })
            .matches(&item)
        );
        assert!(
            !build(SubscribeRequestFilterDeshredTransactions {
                account_include: vec![absent.clone()],
                ..Default::default()
            })
            .matches(&item),
            "include requires at least one match"
        );
        assert!(
            !build(SubscribeRequestFilterDeshredTransactions {
                account_exclude: vec![writable.clone()],
                ..Default::default()
            })
            .matches(&item),
            "exclude rejects"
        );
        assert!(
            !build(SubscribeRequestFilterDeshredTransactions {
                account_required: vec![writable.clone(), absent],
                ..Default::default()
            })
            .matches(&item),
            "required needs all"
        );
        assert!(
            build(SubscribeRequestFilterDeshredTransactions {
                account_required: vec![writable, readonly],
                ..Default::default()
            })
            .matches(&item),
            "loaded addresses count as account keys"
        );
    }

    #[test]
    fn filter_vote_flag() {
        let vote = item(1, true);
        let non_vote = item(1, false);
        let filter = DeshredFilter::new(&SubscribeRequestFilterDeshredTransactions {
            vote: Some(false),
            ..Default::default()
        })
        .expect("valid filter");
        assert!(!filter.matches(&vote));
        assert!(filter.matches(&non_vote));
    }

    #[test]
    fn encode_names_only_matching_filters() {
        let item = item(7, false);
        let present = bs58::encode(vec![1; 32]).into_string();
        let absent = bs58::encode(vec![9; 32]).into_string();

        let request = SubscribeDeshredRequest {
            deshred_transactions: [
                (
                    "hit".to_owned(),
                    SubscribeRequestFilterDeshredTransactions {
                        account_include: vec![present],
                        ..Default::default()
                    },
                ),
                (
                    "miss".to_owned(),
                    SubscribeRequestFilterDeshredTransactions {
                        account_include: vec![absent],
                        ..Default::default()
                    },
                ),
            ]
            .into_iter()
            .collect(),
            slots: HashMap::new(),
            ping: None,
        };
        let filters = DeshredFilters::new(&request, 10).expect("valid filters");

        let encoded = filters
            .encode(&DeshredUpdate::Transaction(item))
            .expect("one filter matched");
        let update = SubscribeUpdateDeshred::decode(encoded.as_slice()).expect("valid protobuf");
        assert_eq!(update.filters, vec!["hit".to_owned()]);

        match update.update_oneof {
            Some(UpdateOneof::DeshredTransaction(tx)) => assert_eq!(tx.slot, 7),
            other => panic!("unexpected update: {other:?}"),
        }
    }

    #[test]
    fn encode_returns_none_when_nothing_matches() {
        let item = item(1, false);
        let request = SubscribeDeshredRequest {
            deshred_transactions: [(
                "miss".to_owned(),
                SubscribeRequestFilterDeshredTransactions {
                    account_include: vec![bs58::encode(vec![9; 32]).into_string()],
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            slots: HashMap::new(),
            ping: None,
        };
        let filters = DeshredFilters::new(&request, 10).expect("valid filters");
        assert!(filters.encode(&DeshredUpdate::Transaction(item)).is_none());
    }

    #[test]
    fn slot_updates_follow_slot_filters() {
        let slot = DeshredUpdate::Slot(SubscribeUpdateSlot {
            slot: 42,
            parent: None,
            status: 0,
            dead_error: None,
        });

        let with_slots = DeshredFilters::new(
            &SubscribeDeshredRequest {
                deshred_transactions: HashMap::new(),
                slots: [("s".to_owned(), SubscribeRequestFilterSlots::default())]
                    .into_iter()
                    .collect(),
                ping: None,
            },
            10,
        )
        .expect("valid filters");

        let encoded = with_slots.encode(&slot).expect("slot filter matched");
        let update = SubscribeUpdateDeshred::decode(encoded.as_slice()).expect("valid protobuf");
        assert_eq!(update.filters, vec!["s".to_owned()]);
        match update.update_oneof {
            Some(UpdateOneof::Slot(got)) => assert_eq!(got.slot, 42),
            other => panic!("unexpected update: {other:?}"),
        }

        // A subscriber that asked only for transactions must not receive slots.
        let txs_only = DeshredFilters::new(
            &SubscribeDeshredRequest {
                deshred_transactions: [(
                    "t".to_owned(),
                    SubscribeRequestFilterDeshredTransactions::default(),
                )]
                .into_iter()
                .collect(),
                slots: HashMap::new(),
                ping: None,
            },
            10,
        )
        .expect("valid filters");
        assert!(txs_only.encode(&slot).is_none());
    }

    #[test]
    fn dedup_collapses_slot_statuses_across_sources() {
        let mut dedup = Dedup::new(4);
        assert!(dedup.insert_slot_status(10, 0), "first source wins");
        assert!(
            !dedup.insert_slot_status(10, 0),
            "a second source repeating the same status is dropped"
        );
        assert!(
            dedup.insert_slot_status(10, 1),
            "a different status for the same slot is new"
        );
    }

    #[test]
    fn dedup_passes_through_malformed_signatures() {
        let mut dedup = Dedup::new(4);
        // Not 64 bytes: cannot be a real signature, so it is never collapsed
        // onto a shared key — emitting a duplicate beats dropping a real
        // transaction that happens to hash alongside it.
        assert!(dedup.insert_signature(1, b"short"));
        assert!(dedup.insert_signature(1, b"short"));
    }

    #[test]
    fn filters_reject_wrong_length_pubkey() {
        let request = SubscribeDeshredRequest {
            deshred_transactions: [(
                "bad".to_owned(),
                SubscribeRequestFilterDeshredTransactions {
                    // valid base58, but 31 bytes
                    account_include: vec![bs58::encode(vec![1; 31]).into_string()],
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            slots: HashMap::new(),
            ping: None,
        };
        assert!(
            DeshredFilters::new(&request, 10).is_err(),
            "a short pubkey would silently never match"
        );
    }

    #[test]
    fn created_at_is_taken_from_upstream() {
        let upstream = prost_types::Timestamp {
            seconds: 1_700_000_000,
            nanos: 0,
        };
        let item = DeshredItem::new(1, info(&sig(1), false), Some(upstream));
        let request = SubscribeDeshredRequest {
            deshred_transactions: [(
                "all".to_owned(),
                SubscribeRequestFilterDeshredTransactions::default(),
            )]
            .into_iter()
            .collect(),
            slots: HashMap::new(),
            ping: None,
        };
        let filters = DeshredFilters::new(&request, 10).expect("valid filters");
        let encoded = filters
            .encode(&DeshredUpdate::Transaction(item))
            .expect("matched");
        let update = SubscribeUpdateDeshred::decode(encoded.as_slice()).expect("valid protobuf");
        assert_eq!(
            update.created_at,
            Some(upstream),
            "relay must not restamp the node's emission time"
        );
    }

    #[test]
    fn filters_reject_invalid_pubkey() {
        let request = SubscribeDeshredRequest {
            deshred_transactions: [(
                "bad".to_owned(),
                SubscribeRequestFilterDeshredTransactions {
                    account_include: vec!["not-base58-0OIl".to_owned()],
                    ..Default::default()
                },
            )]
            .into_iter()
            .collect(),
            slots: HashMap::new(),
            ping: None,
        };
        assert!(DeshredFilters::new(&request, 10).is_err());
    }

    #[test]
    fn filters_respect_the_limit() {
        let request = SubscribeDeshredRequest {
            deshred_transactions: (0..5)
                .map(|i| {
                    (
                        i.to_string(),
                        SubscribeRequestFilterDeshredTransactions::default(),
                    )
                })
                .collect(),
            slots: HashMap::new(),
            ping: None,
        };
        assert!(DeshredFilters::new(&request, 2).is_err());
        assert!(DeshredFilters::new(&request, 5).is_ok());
    }
}
