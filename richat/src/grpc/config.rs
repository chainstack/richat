use {
    crate::config::ConfigAppsWorkers,
    richat_client::grpc::ConfigGrpcClient,
    richat_filter::config::ConfigLimits as ConfigFilterLimits,
    richat_shared::{
        config::{
            deserialize_affinity, deserialize_humansize_usize, deserialize_maybe_num_str,
            deserialize_num_str, deserialize_x_tokens_set,
        },
        transports::grpc::ConfigGrpcServer as ConfigAppGrpcServer,
    },
    serde::Deserialize,
    std::{collections::HashSet, time::Duration},
};

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpc {
    pub server: ConfigAppGrpcServer,
    pub workers: ConfigAppsGrpcWorkers,
    pub stream: ConfigAppsGrpcStream,
    pub unary: ConfigAppsGrpcUnary,
    pub filter_limits: ConfigFilterLimits,
    #[serde(deserialize_with = "deserialize_x_tokens_set")]
    pub x_tokens: HashSet<Vec<u8>>,
    pub deshred: Option<ConfigAppsGrpcDeshred>,
}

/// Aggregation of `SubscribeDeshred` streams from upstream nodes.
///
/// Separate from `channel.sources` because deshred is a different RPC with its
/// own message family: it is pre-execution, so it has no commitment level and
/// never enters the replay channel.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcDeshred {
    /// Dragon's Mouth upstreams to aggregate `SubscribeDeshred` from.
    ///
    /// Full client configs rather than bare URLs, so they get the same
    /// x-token, TLS and keepalive handling as `channel.sources`. Without TCP
    /// keepalive a silently dropped connection never surfaces as an error and
    /// the source hangs instead of reconnecting.
    pub endpoints: Vec<ConfigGrpcClient>,
    /// Slots of dedup history kept. At ~2.5 slots/s the default is ~7 minutes.
    #[serde(deserialize_with = "deserialize_num_str")]
    pub dedup_slots: usize,
    /// Fan-out buffer. A subscriber that falls this far behind is dropped with
    /// `data_loss("lagged")`, as on the main stream.
    #[serde(deserialize_with = "deserialize_num_str")]
    pub broadcast_capacity: usize,
    /// Per-subscriber queue depth.
    #[serde(deserialize_with = "deserialize_num_str")]
    pub client_channel_capacity: usize,
    /// Maximum filters one subscriber may install.
    #[serde(deserialize_with = "deserialize_num_str")]
    pub filters_max: usize,
    #[serde(with = "humantime_serde")]
    pub reconnect_interval: Duration,
    /// Interval for server-side pings on an otherwise idle stream.
    #[serde(with = "humantime_serde")]
    pub ping_interval: Duration,
}

impl Default for ConfigAppsGrpcDeshred {
    fn default() -> Self {
        Self {
            endpoints: Vec::new(),
            dedup_slots: 1_024,
            broadcast_capacity: 65_536,
            client_channel_capacity: 16_384,
            filters_max: 10,
            reconnect_interval: Duration::from_secs(1),
            ping_interval: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcWorkers {
    #[serde(flatten)]
    pub threads: ConfigAppsWorkers,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub messages_cached_max: usize,
    #[serde(default, deserialize_with = "deserialize_maybe_num_str")]
    pub ticks_without_messages_max: Option<usize>,
}

impl Default for ConfigAppsGrpcWorkers {
    fn default() -> Self {
        Self {
            threads: ConfigAppsWorkers::default(),
            messages_cached_max: 1_024,
            ticks_without_messages_max: None,
        }
    }
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcStream {
    #[serde(deserialize_with = "deserialize_humansize_usize")]
    pub messages_len_max: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub messages_max_per_tick: usize,
    #[serde(deserialize_with = "deserialize_humansize_usize")]
    pub messages_replay_len_max: usize,
    #[serde(with = "humantime_serde")]
    pub ping_interval: Duration,
}

impl Default for ConfigAppsGrpcStream {
    fn default() -> Self {
        Self {
            messages_len_max: 16 * 1024 * 1024,
            messages_max_per_tick: 100,
            messages_replay_len_max: 256 * 1024 * 1024,
            ping_interval: Duration::from_secs(15),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigAppsGrpcUnary {
    pub enabled: bool,
    #[serde(deserialize_with = "deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub requests_queue_size: usize,
}

impl Default for ConfigAppsGrpcUnary {
    fn default() -> Self {
        Self {
            enabled: true,
            affinity: None,
            requests_queue_size: 100,
        }
    }
}
