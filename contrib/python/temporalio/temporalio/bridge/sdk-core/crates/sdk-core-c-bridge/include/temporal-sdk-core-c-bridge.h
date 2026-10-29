#pragma once

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>

/* Forward declarations for structs referenced in callback typedefs */
struct TemporalCoreCustomMetricMeter;
struct TemporalCoreCustomSlotSupplierCallbacks;


typedef enum TemporalCoreClientGrpcCompression {
  TemporalCoreClientGrpcCompression_Gzip = 0,
  TemporalCoreClientGrpcCompression_None = 1,
} TemporalCoreClientGrpcCompression;

typedef enum TemporalCoreRpcService {
  TemporalCoreRpcService_Workflow = 1,
  TemporalCoreRpcService_Operator,
  TemporalCoreRpcService_Cloud,
  TemporalCoreRpcService_Test,
  TemporalCoreRpcService_Health,
} TemporalCoreRpcService;

typedef enum TemporalCoreMetricAttributeValueType {
  TemporalCoreMetricAttributeValueType_String = 1,
  TemporalCoreMetricAttributeValueType_Int,
  TemporalCoreMetricAttributeValueType_Float,
  TemporalCoreMetricAttributeValueType_Bool,
} TemporalCoreMetricAttributeValueType;

typedef enum TemporalCoreMetricKind {
  TemporalCoreMetricKind_CounterInteger = 1,
  TemporalCoreMetricKind_HistogramInteger,
  TemporalCoreMetricKind_HistogramFloat,
  TemporalCoreMetricKind_HistogramDuration,
  TemporalCoreMetricKind_GaugeInteger,
  TemporalCoreMetricKind_GaugeFloat,
  TemporalCoreMetricKind_UpDownCounterInteger,
} TemporalCoreMetricKind;

typedef enum TemporalCoreForwardedLogLevel {
  TemporalCoreForwardedLogLevel_Trace = 0,
  TemporalCoreForwardedLogLevel_Debug,
  TemporalCoreForwardedLogLevel_Info,
  TemporalCoreForwardedLogLevel_Warn,
  TemporalCoreForwardedLogLevel_Error,
} TemporalCoreForwardedLogLevel;

typedef enum TemporalCoreOpenTelemetryMetricTemporality {
  TemporalCoreOpenTelemetryMetricTemporality_Cumulative = 1,
  TemporalCoreOpenTelemetryMetricTemporality_Delta,
} TemporalCoreOpenTelemetryMetricTemporality;

typedef enum TemporalCoreOpenTelemetryProtocol {
  TemporalCoreOpenTelemetryProtocol_Grpc = 1,
  TemporalCoreOpenTelemetryProtocol_Http,
} TemporalCoreOpenTelemetryProtocol;

typedef enum TemporalCoreRuntimeType {
  TemporalCoreRuntimeType_Unspecified = 0,
  TemporalCoreRuntimeType_Jvm = 1,
  TemporalCoreRuntimeType_Cpython = 2,
  TemporalCoreRuntimeType_Node = 3,
  TemporalCoreRuntimeType_Bun = 4,
  TemporalCoreRuntimeType_Cruby = 5,
  TemporalCoreRuntimeType_Go = 6,
  TemporalCoreRuntimeType_DotnetFramework = 7,
  TemporalCoreRuntimeType_DotnetCore = 8,
  TemporalCoreRuntimeType_Native = 9,
  TemporalCoreRuntimeType_Roadrunner = 10,
} TemporalCoreRuntimeType;

typedef enum TemporalCoreSlotKindType {
  TemporalCoreSlotKindType_WorkflowSlotKindType,
  TemporalCoreSlotKindType_ActivitySlotKindType,
  TemporalCoreSlotKindType_LocalActivitySlotKindType,
  TemporalCoreSlotKindType_NexusSlotKindType,
} TemporalCoreSlotKindType;

typedef struct TemporalCoreCancellationToken TemporalCoreCancellationToken;

/**
 * Representation of gRPC request for the callback.
 *
 * Note, temporal_core_client_grpc_override_request_respond is effectively the "free" call for
 * each request. Each request _must_ call that and the request can no longer be valid after that
 * call.
 */
typedef struct TemporalCoreClientGrpcOverrideRequest TemporalCoreClientGrpcOverrideRequest;

typedef struct TemporalCoreConnection TemporalCoreConnection;

typedef struct TemporalCoreEphemeralServer TemporalCoreEphemeralServer;

typedef struct TemporalCoreForwardedLog TemporalCoreForwardedLog;

typedef struct TemporalCoreMetric TemporalCoreMetric;

typedef struct TemporalCoreMetricAttributes TemporalCoreMetricAttributes;

typedef struct TemporalCoreMetricMeter TemporalCoreMetricMeter;

typedef struct TemporalCoreRandom TemporalCoreRandom;

typedef struct TemporalCoreRuntime TemporalCoreRuntime;

typedef struct TemporalCoreSlotReserveCompletionCtx TemporalCoreSlotReserveCompletionCtx;

typedef struct TemporalCoreWorker TemporalCoreWorker;

typedef struct TemporalCoreWorkerReplayPusher TemporalCoreWorkerReplayPusher;

typedef struct TemporalCoreByteArrayRef {
  const uint8_t *data;
  size_t size;
} TemporalCoreByteArrayRef;

typedef struct TemporalCoreByteArrayRefArray {
  const struct TemporalCoreByteArrayRef *data;
  size_t size;
} TemporalCoreByteArrayRefArray;

/**
 * Each ByteArrayRef is `<key>\n<value>`.
 * Keys cannot contain a newline.
 */
typedef struct TemporalCoreByteArrayRefArray TemporalCoreMetadataRef;

typedef struct TemporalCoreClientTlsOptions {
  struct TemporalCoreByteArrayRef server_root_ca_cert;
  struct TemporalCoreByteArrayRef domain;
  struct TemporalCoreByteArrayRef client_cert;
  struct TemporalCoreByteArrayRef client_private_key;
} TemporalCoreClientTlsOptions;

typedef struct TemporalCoreClientRetryOptions {
  uint64_t initial_interval_millis;
  double randomization_factor;
  double multiplier;
  uint64_t max_interval_millis;
  uint64_t max_elapsed_time_millis;
  uintptr_t max_retries;
} TemporalCoreClientRetryOptions;

typedef struct TemporalCoreClientKeepAliveOptions {
  uint64_t interval_millis;
  uint64_t timeout_millis;
} TemporalCoreClientKeepAliveOptions;

typedef struct TemporalCoreClientHttpConnectProxyOptions {
  struct TemporalCoreByteArrayRef target_host;
  struct TemporalCoreByteArrayRef username;
  struct TemporalCoreByteArrayRef password;
} TemporalCoreClientHttpConnectProxyOptions;

/**
 * Callback that is invoked for every gRPC call if set on the client options.
 *
 * Note, temporal_core_client_grpc_override_request_respond is effectively the "free" call for
 * each request. Each request _must_ call that and the request can no longer be valid after that
 * call. However, all of that work and the respond call may be done well after this callback
 * returns. No data lifetime is related to the callback invocation itself.
 *
 * Implementers should return as soon as possible and perform the network request in the
 * background.
 */
typedef void (*TemporalCoreClientGrpcOverrideCallback)(struct TemporalCoreClientGrpcOverrideRequest *request,
                                                       void *user_data);

typedef struct TemporalCoreClientDnsLoadBalancingOptions {
  /**
   * How often, in milliseconds, to re-resolve DNS.
   */
  uint64_t resolution_interval_millis;
} TemporalCoreClientDnsLoadBalancingOptions;

typedef struct TemporalCoreConnectionOptions {
  struct TemporalCoreByteArrayRef target_url;
  struct TemporalCoreByteArrayRef client_name;
  struct TemporalCoreByteArrayRef client_version;
  TemporalCoreMetadataRef metadata;
  TemporalCoreMetadataRef binary_metadata;
  struct TemporalCoreByteArrayRef api_key;
  struct TemporalCoreByteArrayRef identity;
  const struct TemporalCoreClientTlsOptions *tls_options;
  const struct TemporalCoreClientRetryOptions *retry_options;
  const struct TemporalCoreClientKeepAliveOptions *keep_alive_options;
  const struct TemporalCoreClientHttpConnectProxyOptions *http_connect_proxy_options;
  /**
   * If this is set, all gRPC calls go through it and no connection is made to server. The client
   * connection call usually calls this for "GetSystemInfo" before the connect is complete. See
   * the callback documentation for more important information about usage and data lifetimes.
   *
   * When a callback is set, target_url is not used to connect, but it must be set to a valid URL
   * anyways in case it is used for logging or other reasons. Similarly, other connect-specific
   * fields like tls_options, keep_alive_options, and http_connect_proxy_options will be
   * completely ignored if a callback is set.
   */
  TemporalCoreClientGrpcOverrideCallback grpc_override_callback;
  /**
   * Optional user data passed to each callback call.
   */
  void *grpc_override_callback_user_data;
  /**
   * If non-null, DNS-based load balancing is enabled. When the target URL resolves to multiple
   * addresses, requests are distributed across them and the address list is periodically
   * refreshed. If null, DNS load balancing is disabled. Ignored (forced off) when
   * http_connect_proxy_options is also set.
   */
  const struct TemporalCoreClientDnsLoadBalancingOptions *dns_load_balancing_options;
  /**
   * Selects transport-level gRPC compression. The zero value enables gzip, which is the
   * default. Ignored when grpc_override_callback is set, since that transport cannot decode
   * compressed request bodies.
   */
  enum TemporalCoreClientGrpcCompression grpc_compression;
  /**
   * Warning threshold (bytes) for the size of an outbound payload-bearing field.
   * Over-threshold fields are logged but still sent to server. 0 disables the warning.
   * NOTE: Experimental
   */
  uint64_t payloads_warn_size;
  /**
   * Warning threshold (bytes) for outbound memo size. Over-threshold memos are logged but still
   * sent to server. 0 disables the warning.
   * NOTE: Experimental
   */
  uint64_t memo_warn_size;
} TemporalCoreConnectionOptions;

typedef struct TemporalCoreByteArray {
  const uint8_t *data;
  size_t size;
  /**
   * For internal use only.
   */
  size_t cap;
  /**
   * For internal use only.
   */
  bool disable_free;
} TemporalCoreByteArray;

/**
 * If success or fail are not null, they must be manually freed when done.
 */
typedef void (*TemporalCoreClientConnectCallback)(void *user_data,
                                                  struct TemporalCoreConnection *success,
                                                  const struct TemporalCoreByteArray *fail);

/**
 * Response provided to temporal_core_client_grpc_override_request_respond. All values referenced
 * inside here must live until that call returns.
 */
typedef struct TemporalCoreClientGrpcOverrideResponse {
  /**
   * Numeric gRPC status code, see <https://grpc.io/docs/guides/status-codes/>. 0 is success, non-0
   * is failure.
   */
  int32_t status_code;
  /**
   * Headers for the response if any. Note, this is meant for user-defined metadata/headers, and
   * not the gRPC system headers (like :status or content-type).
   */
  TemporalCoreMetadataRef headers;
  /**
   * Protobuf bytes for a successful response. Ignored if status_code is non-0.
   */
  struct TemporalCoreByteArrayRef success_proto;
  /**
   * UTF-8 failure message. Ignored if status_code is 0.
   */
  struct TemporalCoreByteArrayRef fail_message;
  /**
   * Optional details for the gRPC failure. If non-empty, this should be a protobuf-serialized
   * google.rpc.Status. Ignored if status_code is 0.
   */
  struct TemporalCoreByteArrayRef fail_details;
} TemporalCoreClientGrpcOverrideResponse;

typedef struct TemporalCoreRpcCallOptions {
  enum TemporalCoreRpcService service;
  struct TemporalCoreByteArrayRef rpc;
  struct TemporalCoreByteArrayRef req;
  bool retry;
  TemporalCoreMetadataRef metadata;
  TemporalCoreMetadataRef binary_metadata;
  /**
   * 0 means no timeout
   */
  uint32_t timeout_millis;
  const struct TemporalCoreCancellationToken *cancellation_token;
} TemporalCoreRpcCallOptions;

/**
 * If success or failure byte arrays inside fail are not null, they must be
 * manually freed when done. Either success or failure_message are always
 * present. Status code may still be 0 with a failure message. Failure details
 * represent a protobuf gRPC status message.
 */
typedef void (*TemporalCoreClientRpcCallCallback)(void *user_data,
                                                  const struct TemporalCoreByteArray *success,
                                                  uint32_t status_code,
                                                  const struct TemporalCoreByteArray *failure_message,
                                                  const struct TemporalCoreByteArray *failure_details);

/**
 * OrFail result for client config loading operations.
 * Either success or fail will be null, but never both.
 * If success is not null, it contains JSON-serialized client configuration data.
 * If fail is not null, it contains UTF-8 encoded error message.
 * The returned ByteArrays must be freed by the caller.
 */
typedef struct TemporalCoreClientEnvConfigOrFail {
  const struct TemporalCoreByteArray *success;
  const struct TemporalCoreByteArray *fail;
} TemporalCoreClientEnvConfigOrFail;

/**
 * Options for loading client configuration.
 */
typedef struct TemporalCoreClientEnvConfigLoadOptions {
  struct TemporalCoreByteArrayRef path;
  struct TemporalCoreByteArrayRef data;
  bool config_file_strict;
  struct TemporalCoreByteArrayRef env_vars;
} TemporalCoreClientEnvConfigLoadOptions;

/**
 * OrFail result for client config profile loading operations.
 * Either success or fail will be null, but never both.
 * If success is not null, it contains JSON-serialized client configuration profile data.
 * If fail is not null, it contains UTF-8 encoded error message.
 * The returned ByteArrays must be freed by the caller.
 */
typedef struct TemporalCoreClientEnvConfigProfileOrFail {
  const struct TemporalCoreByteArray *success;
  const struct TemporalCoreByteArray *fail;
} TemporalCoreClientEnvConfigProfileOrFail;

/**
 * Options for loading a specific client configuration profile.
 */
typedef struct TemporalCoreClientEnvConfigProfileLoadOptions {
  struct TemporalCoreByteArrayRef profile;
  struct TemporalCoreByteArrayRef path;
  struct TemporalCoreByteArrayRef data;
  bool disable_file;
  bool disable_env;
  bool config_file_strict;
  struct TemporalCoreByteArrayRef env_vars;
} TemporalCoreClientEnvConfigProfileLoadOptions;

typedef union TemporalCoreMetricAttributeValue {
  struct TemporalCoreByteArrayRef string_value;
  int64_t int_value;
  double float_value;
  bool bool_value;
} TemporalCoreMetricAttributeValue;

typedef struct TemporalCoreMetricAttribute {
  struct TemporalCoreByteArrayRef key;
  union TemporalCoreMetricAttributeValue value;
  enum TemporalCoreMetricAttributeValueType value_type;
} TemporalCoreMetricAttribute;

typedef struct TemporalCoreMetricOptions {
  struct TemporalCoreByteArrayRef name;
  struct TemporalCoreByteArrayRef description;
  struct TemporalCoreByteArrayRef unit;
  enum TemporalCoreMetricKind kind;
} TemporalCoreMetricOptions;

/**
 * If fail is not null, it must be manually freed when done. Runtime is always
 * present, but it should never be used if fail is present, only freed after
 * fail is freed using it.
 */
typedef struct TemporalCoreRuntimeOrFail {
  struct TemporalCoreRuntime *runtime;
  const struct TemporalCoreByteArray *fail;
} TemporalCoreRuntimeOrFail;

/**
 * Operations on the log can only occur within the callback, it is freed
 * immediately thereafter.
 */
typedef void (*TemporalCoreForwardedLogCallback)(enum TemporalCoreForwardedLogLevel level,
                                                 const struct TemporalCoreForwardedLog *log);

typedef struct TemporalCoreLoggingOptions {
  struct TemporalCoreByteArrayRef filter;
  /**
   * This callback is expected to work for the life of the runtime.
   */
  TemporalCoreForwardedLogCallback forward_to;
} TemporalCoreLoggingOptions;

/**
 * Data is `<key1>\n<value1>\n<key2>\n<value2>`.
 * Keys and values cannot contain a newline within.
 */
typedef struct TemporalCoreByteArrayRef TemporalCoreNewlineDelimitedMapRef;

typedef struct TemporalCoreOpenTelemetryOptions {
  struct TemporalCoreByteArrayRef url;
  TemporalCoreNewlineDelimitedMapRef headers;
  uint32_t metric_periodicity_millis;
  enum TemporalCoreOpenTelemetryMetricTemporality metric_temporality;
  bool durations_as_seconds;
  enum TemporalCoreOpenTelemetryProtocol protocol;
  /**
   * Histogram bucket overrides in form of
   * `<metric1>\n<float>,<float>,<float>\n<metric2>\n<float>,<float>,<float>`
   */
  TemporalCoreNewlineDelimitedMapRef histogram_bucket_overrides;
} TemporalCoreOpenTelemetryOptions;

typedef struct TemporalCorePrometheusOptions {
  struct TemporalCoreByteArrayRef bind_address;
  bool counters_total_suffix;
  bool unit_suffix;
  bool durations_as_seconds;
  /**
   * Histogram bucket overrides in form of
   * `<metric1>\n<float>,<float>,<float>\n<metric2>\n<float>,<float>,<float>`
   */
  TemporalCoreNewlineDelimitedMapRef histogram_bucket_overrides;
} TemporalCorePrometheusOptions;

typedef const void *(*TemporalCoreCustomMetricMeterMetricNewCallback)(struct TemporalCoreByteArrayRef name,
                                                                      struct TemporalCoreByteArrayRef description,
                                                                      struct TemporalCoreByteArrayRef unit,
                                                                      enum TemporalCoreMetricKind kind);

typedef void (*TemporalCoreCustomMetricMeterMetricFreeCallback)(const void *metric);

typedef void (*TemporalCoreCustomMetricMeterMetricRecordIntegerCallback)(const void *metric,
                                                                         uint64_t value,
                                                                         const void *attributes);

typedef void (*TemporalCoreCustomMetricMeterMetricRecordFloatCallback)(const void *metric,
                                                                       double value,
                                                                       const void *attributes);

typedef void (*TemporalCoreCustomMetricMeterMetricRecordDurationCallback)(const void *metric,
                                                                          uint64_t value_ms,
                                                                          const void *attributes);

typedef struct TemporalCoreCustomMetricAttributeValueString {
  const uint8_t *data;
  size_t size;
} TemporalCoreCustomMetricAttributeValueString;

typedef union TemporalCoreCustomMetricAttributeValue {
  struct TemporalCoreCustomMetricAttributeValueString string_value;
  int64_t int_value;
  double float_value;
  bool bool_value;
} TemporalCoreCustomMetricAttributeValue;

typedef struct TemporalCoreCustomMetricAttribute {
  struct TemporalCoreByteArrayRef key;
  union TemporalCoreCustomMetricAttributeValue value;
  enum TemporalCoreMetricAttributeValueType value_type;
} TemporalCoreCustomMetricAttribute;

typedef const void *(*TemporalCoreCustomMetricMeterAttributesNewCallback)(const void *append_from,
                                                                          const struct TemporalCoreCustomMetricAttribute *attributes,
                                                                          size_t attributes_size);

typedef void (*TemporalCoreCustomMetricMeterAttributesFreeCallback)(const void *attributes);

typedef void (*TemporalCoreCustomMetricMeterMeterFreeCallback)(const struct TemporalCoreCustomMetricMeter *meter);

/**
 * No parameters in the callbacks below should be assumed to live beyond the
 * callbacks unless they are pointers to things that were created lang-side
 * originally. There are no guarantees on which thread these calls may be
 * invoked on.
 *
 * Attribute pointers may be null when recording if no attributes are associated with the metric.
 */
typedef struct TemporalCoreCustomMetricMeter {
  TemporalCoreCustomMetricMeterMetricNewCallback metric_new;
  TemporalCoreCustomMetricMeterMetricFreeCallback metric_free;
  TemporalCoreCustomMetricMeterMetricRecordIntegerCallback metric_record_integer;
  TemporalCoreCustomMetricMeterMetricRecordFloatCallback metric_record_float;
  TemporalCoreCustomMetricMeterMetricRecordDurationCallback metric_record_duration;
  TemporalCoreCustomMetricMeterAttributesNewCallback attributes_new;
  TemporalCoreCustomMetricMeterAttributesFreeCallback attributes_free;
  TemporalCoreCustomMetricMeterMeterFreeCallback meter_free;
} TemporalCoreCustomMetricMeter;

/**
 * Only one of opentelemetry, prometheus, or custom_meter can be present.
 */
typedef struct TemporalCoreMetricsOptions {
  const struct TemporalCoreOpenTelemetryOptions *opentelemetry;
  const struct TemporalCorePrometheusOptions *prometheus;
  /**
   * If present, this is freed by a callback within itself
   */
  const struct TemporalCoreCustomMetricMeter *custom_meter;
  bool attach_service_name;
  TemporalCoreNewlineDelimitedMapRef global_tags;
  struct TemporalCoreByteArrayRef metric_prefix;
} TemporalCoreMetricsOptions;

typedef struct TemporalCoreTelemetryOptions {
  const struct TemporalCoreLoggingOptions *logging;
  const struct TemporalCoreMetricsOptions *metrics;
} TemporalCoreTelemetryOptions;

typedef struct TemporalCoreRuntimeInfo {
  /**
   * The SDK runtime hosting Core.
   */
  enum TemporalCoreRuntimeType runtime_type;
  /**
   * The runtime version, or empty if it cannot be determined.
   */
  struct TemporalCoreByteArrayRef version;
} TemporalCoreRuntimeInfo;

typedef struct TemporalCoreRuntimeInfoArray {
  /**
   * Runtime entries read during runtime construction; ownership remains with the caller.
   */
  const struct TemporalCoreRuntimeInfo *data;
  size_t size;
} TemporalCoreRuntimeInfoArray;

typedef struct TemporalCoreRuntimeOptions {
  const struct TemporalCoreTelemetryOptions *telemetry;
  uint64_t worker_heartbeat_interval_millis;
  /**
   * SDK runtimes included in worker environment heartbeats. An empty array reports no runtime;
   * the bridge does not inherit Core's native-runtime default.
   */
  struct TemporalCoreRuntimeInfoArray runtime_info;
  /**
   * If true, worker heartbeats omit all runtime, hosting, and platform information.
   */
  bool disable_environment_info;
} TemporalCoreRuntimeOptions;

typedef struct TemporalCoreTestServerOptions {
  /**
   * Empty means default behavior
   */
  struct TemporalCoreByteArrayRef existing_path;
  struct TemporalCoreByteArrayRef sdk_name;
  struct TemporalCoreByteArrayRef sdk_version;
  struct TemporalCoreByteArrayRef download_version;
  /**
   * Empty means default behavior
   */
  struct TemporalCoreByteArrayRef download_dest_dir;
  /**
   * 0 means default behavior
   */
  uint16_t port;
  /**
   * Newline delimited
   */
  struct TemporalCoreByteArrayRef extra_args;
  /**
   * 0 means no TTL
   */
  uint64_t download_ttl_seconds;
} TemporalCoreTestServerOptions;

typedef struct TemporalCoreDevServerOptions {
  /**
   * Must always be present
   */
  const struct TemporalCoreTestServerOptions *test_server;
  struct TemporalCoreByteArrayRef namespace_;
  struct TemporalCoreByteArrayRef ip;
  /**
   * Empty means default behavior
   */
  struct TemporalCoreByteArrayRef database_filename;
  bool ui;
  uint16_t ui_port;
  struct TemporalCoreByteArrayRef log_format;
  struct TemporalCoreByteArrayRef log_level;
} TemporalCoreDevServerOptions;

/**
 * Anything besides user data must be freed if non-null.
 */
typedef void (*TemporalCoreEphemeralServerStartCallback)(void *user_data,
                                                         struct TemporalCoreEphemeralServer *success,
                                                         const struct TemporalCoreByteArray *success_target,
                                                         const struct TemporalCoreByteArray *fail);

typedef void (*TemporalCoreEphemeralServerShutdownCallback)(void *user_data,
                                                            const struct TemporalCoreByteArray *fail);

/**
 * Only runtime or fail will be non-null. Whichever is must be freed when done.
 */
typedef struct TemporalCoreWorkerOrFail {
  struct TemporalCoreWorker *worker;
  const struct TemporalCoreByteArray *fail;
} TemporalCoreWorkerOrFail;

typedef struct TemporalCoreWorkerVersioningNone {
  struct TemporalCoreByteArrayRef build_id;
} TemporalCoreWorkerVersioningNone;

typedef struct TemporalCoreWorkerDeploymentVersion {
  struct TemporalCoreByteArrayRef deployment_name;
  struct TemporalCoreByteArrayRef build_id;
} TemporalCoreWorkerDeploymentVersion;

typedef struct TemporalCoreWorkerDeploymentOptions {
  struct TemporalCoreWorkerDeploymentVersion version;
  bool use_worker_versioning;
  int32_t default_versioning_behavior;
} TemporalCoreWorkerDeploymentOptions;

typedef struct TemporalCoreLegacyBuildIdBasedStrategy {
  struct TemporalCoreByteArrayRef build_id;
} TemporalCoreLegacyBuildIdBasedStrategy;

typedef enum TemporalCoreWorkerVersioningStrategy_Tag {
  TemporalCoreWorkerVersioningStrategy_None,
  TemporalCoreWorkerVersioningStrategy_DeploymentBased,
  TemporalCoreWorkerVersioningStrategy_LegacyBuildIdBased,
} TemporalCoreWorkerVersioningStrategy_Tag;

typedef struct TemporalCoreWorkerVersioningStrategy {
  TemporalCoreWorkerVersioningStrategy_Tag tag;
  union {
    struct {
      struct TemporalCoreWorkerVersioningNone none;
    };
    struct {
      struct TemporalCoreWorkerDeploymentOptions deployment_based;
    };
    struct {
      struct TemporalCoreLegacyBuildIdBasedStrategy legacy_build_id_based;
    };
  };
} TemporalCoreWorkerVersioningStrategy;

typedef struct TemporalCoreFixedSizeSlotSupplier {
  uintptr_t num_slots;
} TemporalCoreFixedSizeSlotSupplier;

typedef struct TemporalCoreResourceBasedTunerOptions {
  double target_memory_usage;
  double target_cpu_usage;
} TemporalCoreResourceBasedTunerOptions;

typedef struct TemporalCoreResourceBasedSlotSupplier {
  uintptr_t minimum_slots;
  uintptr_t maximum_slots;
  uint64_t ramp_throttle_ms;
  struct TemporalCoreResourceBasedTunerOptions tuner_options;
} TemporalCoreResourceBasedSlotSupplier;

typedef struct TemporalCoreSlotReserveCtx {
  enum TemporalCoreSlotKindType slot_type;
  struct TemporalCoreByteArrayRef task_queue;
  struct TemporalCoreByteArrayRef worker_identity;
  struct TemporalCoreByteArrayRef worker_build_id;
  bool is_sticky;
} TemporalCoreSlotReserveCtx;

typedef void (*TemporalCoreCustomSlotSupplierReserveCallback)(const struct TemporalCoreSlotReserveCtx *ctx,
                                                              const struct TemporalCoreSlotReserveCompletionCtx *completion_ctx,
                                                              void *user_data);

typedef void (*TemporalCoreCustomSlotSupplierCancelReserveCallback)(const struct TemporalCoreSlotReserveCompletionCtx *completion_ctx,
                                                                    void *user_data);

typedef uintptr_t (*TemporalCoreCustomSlotSupplierTryReserveCallback)(const struct TemporalCoreSlotReserveCtx *ctx,
                                                                      void *user_data);

typedef enum TemporalCoreSlotInfo_Tag {
  TemporalCoreSlotInfo_WorkflowSlotInfo,
  TemporalCoreSlotInfo_ActivitySlotInfo,
  TemporalCoreSlotInfo_LocalActivitySlotInfo,
  TemporalCoreSlotInfo_NexusSlotInfo,
} TemporalCoreSlotInfo_Tag;

typedef struct TemporalCoreSlotInfo_TemporalCoreWorkflowSlotInfo_Body {
  struct TemporalCoreByteArrayRef workflow_type;
  bool is_sticky;
} TemporalCoreSlotInfo_TemporalCoreWorkflowSlotInfo_Body;

typedef struct TemporalCoreSlotInfo_TemporalCoreActivitySlotInfo_Body {
  struct TemporalCoreByteArrayRef activity_type;
} TemporalCoreSlotInfo_TemporalCoreActivitySlotInfo_Body;

typedef struct TemporalCoreSlotInfo_TemporalCoreLocalActivitySlotInfo_Body {
  struct TemporalCoreByteArrayRef activity_type;
} TemporalCoreSlotInfo_TemporalCoreLocalActivitySlotInfo_Body;

typedef struct TemporalCoreSlotInfo_TemporalCoreNexusSlotInfo_Body {
  struct TemporalCoreByteArrayRef operation;
  struct TemporalCoreByteArrayRef service;
} TemporalCoreSlotInfo_TemporalCoreNexusSlotInfo_Body;

typedef struct TemporalCoreSlotInfo {
  TemporalCoreSlotInfo_Tag tag;
  union {
    TemporalCoreSlotInfo_TemporalCoreWorkflowSlotInfo_Body workflow_slot_info;
    TemporalCoreSlotInfo_TemporalCoreActivitySlotInfo_Body activity_slot_info;
    TemporalCoreSlotInfo_TemporalCoreLocalActivitySlotInfo_Body local_activity_slot_info;
    TemporalCoreSlotInfo_TemporalCoreNexusSlotInfo_Body nexus_slot_info;
  };
} TemporalCoreSlotInfo;

typedef struct TemporalCoreSlotMarkUsedCtx {
  struct TemporalCoreSlotInfo slot_info;
  /**
   * Lang-issued permit ID.
   */
  uintptr_t slot_permit;
} TemporalCoreSlotMarkUsedCtx;

typedef void (*TemporalCoreCustomSlotSupplierMarkUsedCallback)(const struct TemporalCoreSlotMarkUsedCtx *ctx,
                                                               void *user_data);

typedef struct TemporalCoreSlotReleaseCtx {
  const struct TemporalCoreSlotInfo *slot_info;
  /**
   * Lang-issued permit ID.
   */
  uintptr_t slot_permit;
} TemporalCoreSlotReleaseCtx;

typedef void (*TemporalCoreCustomSlotSupplierReleaseCallback)(const struct TemporalCoreSlotReleaseCtx *ctx,
                                                              void *user_data);

typedef bool (*TemporalCoreCustomSlotSupplierAvailableSlotsCallback)(uintptr_t *available_slots,
                                                                     void *user_data);

typedef void (*TemporalCoreCustomSlotSupplierFreeCallback)(const struct TemporalCoreCustomSlotSupplierCallbacks *userimpl);

typedef struct TemporalCoreCustomSlotSupplierCallbacks {
  /**
   * Called to initiate asynchronous slot reservation. `ctx` contains information about
   * reservation request. The pointer is only valid for the duration of the function call; the
   * implementation should copy the data out of it for later use, and return as soon as possible.
   *
   * When slot is reserved, the implementation should call [`temporal_core_complete_async_reserve`]
   * with the same `completion_ctx` as passed to this function. Reservation cannot be cancelled
   * by Lang, but it can be cancelled by Core through [`cancel_reserve`](Self::cancel_reserve)
   * callback. If reservation was cancelled, [`temporal_core_complete_async_cancel_reserve`]
   * should be called instead.
   *
   * Slot reservation cannot error. The implementation should recover from errors and keep trying
   * to reserve a slot until it eventually succeeds, or until reservation is cancelled by Core.
   */
  TemporalCoreCustomSlotSupplierReserveCallback reserve;
  /**
   * Called to cancel slot reservation. `completion_ctx` specifies which reservation is being
   * cancelled; the matching [`reserve`](Self::reserve) call was made with the same `completion_ctx`.
   * After cancellation, the implementation should call [`temporal_core_complete_async_cancel_reserve`]
   * with the same `completion_ctx`. Calling [`temporal_core_complete_async_reserve`] is not
   * needed after cancellation.
   */
  TemporalCoreCustomSlotSupplierCancelReserveCallback cancel_reserve;
  /**
   * Called to try an immediate slot reservation. The callback should return 0 if immediate
   * reservation is not currently possible, or permit ID if reservation was successful. Permit ID
   * is arbitrary, but must be unique among live reservations as it's later used for [`mark_used`](Self::mark_used)
   * and [`release`](Self::release) callbacks.
   */
  TemporalCoreCustomSlotSupplierTryReserveCallback try_reserve;
  /**
   * Called after successful reservation to mark slot as used. See [`SlotSupplier`](temporalio_sdk_core::SlotSupplier)
   * trait for details.
   */
  TemporalCoreCustomSlotSupplierMarkUsedCallback mark_used;
  /**
   * Called to free a previously reserved slot.
   */
  TemporalCoreCustomSlotSupplierReleaseCallback release;
  /**
   * Called to retrieve the number of available slots if known. If the implementation knows how
   * many slots are available at the moment, it should set the value behind the `available_slots`
   * pointer and return true. If that number is unknown, it should return false.
   *
   * This function pointer can be set to null. It will be treated as if the number of available
   * slots is never known.
   */
  TemporalCoreCustomSlotSupplierAvailableSlotsCallback available_slots;
  /**
   * Called when the slot supplier is being dropped. All resources should be freed.
   */
  TemporalCoreCustomSlotSupplierFreeCallback free;
  /**
   * Passed as an extra argument to the callbacks.
   */
  void *user_data;
} TemporalCoreCustomSlotSupplierCallbacks;

typedef struct TemporalCoreCustomSlotSupplierCallbacksImpl {
  const struct TemporalCoreCustomSlotSupplierCallbacks *_0;
} TemporalCoreCustomSlotSupplierCallbacksImpl;

typedef enum TemporalCoreSlotSupplier_Tag {
  TemporalCoreSlotSupplier_FixedSize,
  TemporalCoreSlotSupplier_ResourceBased,
  TemporalCoreSlotSupplier_Custom,
} TemporalCoreSlotSupplier_Tag;

typedef struct TemporalCoreSlotSupplier {
  TemporalCoreSlotSupplier_Tag tag;
  union {
    struct {
      struct TemporalCoreFixedSizeSlotSupplier fixed_size;
    };
    struct {
      struct TemporalCoreResourceBasedSlotSupplier resource_based;
    };
    struct {
      struct TemporalCoreCustomSlotSupplierCallbacksImpl custom;
    };
  };
} TemporalCoreSlotSupplier;

typedef struct TemporalCoreTunerHolder {
  struct TemporalCoreSlotSupplier workflow_slot_supplier;
  struct TemporalCoreSlotSupplier activity_slot_supplier;
  struct TemporalCoreSlotSupplier local_activity_slot_supplier;
  struct TemporalCoreSlotSupplier nexus_task_slot_supplier;
} TemporalCoreTunerHolder;

typedef struct TemporalCoreWorkerTaskTypes {
  bool enable_workflows;
  bool enable_local_activities;
  bool enable_remote_activities;
  bool enable_nexus;
} TemporalCoreWorkerTaskTypes;

typedef struct TemporalCorePollerBehaviorSimpleMaximum {
  uintptr_t simple_maximum;
} TemporalCorePollerBehaviorSimpleMaximum;

typedef struct TemporalCorePollerBehaviorAutoscaling {
  uintptr_t minimum;
  uintptr_t maximum;
  uintptr_t initial;
} TemporalCorePollerBehaviorAutoscaling;

typedef struct TemporalCorePollerBehavior {
  const struct TemporalCorePollerBehaviorSimpleMaximum *simple_maximum;
  const struct TemporalCorePollerBehaviorAutoscaling *autoscaling;
} TemporalCorePollerBehavior;

typedef struct TemporalCoreWorkerOptions {
  struct TemporalCoreByteArrayRef namespace_;
  struct TemporalCoreByteArrayRef task_queue;
  struct TemporalCoreWorkerVersioningStrategy versioning_strategy;
  struct TemporalCoreByteArrayRef identity_override;
  uint32_t max_cached_workflows;
  struct TemporalCoreTunerHolder tuner;
  struct TemporalCoreWorkerTaskTypes task_types;
  uint64_t sticky_queue_schedule_to_start_timeout_millis;
  uint64_t max_heartbeat_throttle_interval_millis;
  uint64_t default_heartbeat_throttle_interval_millis;
  double max_activities_per_second;
  double max_task_queue_activities_per_second;
  uint64_t graceful_shutdown_period_millis;
  struct TemporalCorePollerBehavior workflow_task_poller_behavior;
  float nonsticky_to_sticky_poll_ratio;
  struct TemporalCorePollerBehavior activity_task_poller_behavior;
  struct TemporalCorePollerBehavior nexus_task_poller_behavior;
  bool nondeterminism_as_workflow_fail;
  struct TemporalCoreByteArrayRefArray nondeterminism_as_workflow_fail_for_types;
  struct TemporalCoreByteArrayRefArray plugins;
  struct TemporalCoreByteArrayRefArray storage_drivers;
  /**
   * If set, the worker won't proactively fail completions whose payloads exceed the namespace
   * error limits; oversized payloads are sent and the server enforces the limit.
   * NOTE: Experimental
   */
  bool disable_payload_error_limit;
  /**
   * Maximum number of activity slots that may be reserved for eager execution when completing
   * a workflow task. Zero disables eager activity execution.
   */
  uint32_t max_eager_activity_reservations_per_workflow_task;
} TemporalCoreWorkerOptions;

/**
 * If fail is present, it must be freed.
 */
typedef void (*TemporalCoreWorkerCallback)(void *user_data, const struct TemporalCoreByteArray *fail);

/**
 * If success or fail are present, they must be freed. They will both be null
 * if this is a result of a poll shutdown.
 */
typedef void (*TemporalCoreWorkerPollCallback)(void *user_data,
                                               const struct TemporalCoreByteArray *success,
                                               const struct TemporalCoreByteArray *fail);

typedef struct TemporalCoreWorkerReplayerOrFail {
  struct TemporalCoreWorker *worker;
  struct TemporalCoreWorkerReplayPusher *worker_replay_pusher;
  const struct TemporalCoreByteArray *fail;
} TemporalCoreWorkerReplayerOrFail;

typedef struct TemporalCoreWorkerReplayPushResult {
  const struct TemporalCoreByteArray *fail;
} TemporalCoreWorkerReplayPushResult;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

struct TemporalCoreCancellationToken *temporal_core_cancellation_token_new(void);

void temporal_core_cancellation_token_cancel(struct TemporalCoreCancellationToken *token);

void temporal_core_cancellation_token_free(struct TemporalCoreCancellationToken *token);

/**
 * Runtime must live as long as client. Options and user data must live through
 * callback.
 */
void temporal_core_client_connect(struct TemporalCoreRuntime *runtime,
                                  const struct TemporalCoreConnectionOptions *options,
                                  void *user_data,
                                  TemporalCoreClientConnectCallback callback);

void temporal_core_client_free(struct TemporalCoreConnection *client);

void temporal_core_client_update_metadata(struct TemporalCoreConnection *client,
                                          TemporalCoreMetadataRef metadata);

void temporal_core_client_update_binary_metadata(struct TemporalCoreConnection *client,
                                                 TemporalCoreMetadataRef metadata);

void temporal_core_client_update_api_key(struct TemporalCoreConnection *client,
                                         struct TemporalCoreByteArrayRef api_key);

/**
 * Get a reference to the service name.
 *
 * Note, this is only valid until temporal_core_client_grpc_override_request_respond is called.
 */
struct TemporalCoreByteArrayRef temporal_core_client_grpc_override_request_service(const struct TemporalCoreClientGrpcOverrideRequest *req);

/**
 * Get a reference to the RPC name.
 *
 * Note, this is only valid until temporal_core_client_grpc_override_request_respond is called.
 */
struct TemporalCoreByteArrayRef temporal_core_client_grpc_override_request_rpc(const struct TemporalCoreClientGrpcOverrideRequest *req);

/**
 * Get a reference to the service headers.
 *
 * Note, this is only valid until temporal_core_client_grpc_override_request_respond is called.
 */
TemporalCoreMetadataRef temporal_core_client_grpc_override_request_headers(const struct TemporalCoreClientGrpcOverrideRequest *req);

/**
 * Get a reference to the request protobuf bytes.
 *
 * Note, this is only valid until temporal_core_client_grpc_override_request_respond is called.
 */
struct TemporalCoreByteArrayRef temporal_core_client_grpc_override_request_proto(const struct TemporalCoreClientGrpcOverrideRequest *req);

/**
 * Complete the request, freeing all request data.
 *
 * The data referenced in the response must live until this function returns. Once this call is
 * made, none of the request data should be considered valid.
 */
void temporal_core_client_grpc_override_request_respond(struct TemporalCoreClientGrpcOverrideRequest *req,
                                                        struct TemporalCoreClientGrpcOverrideResponse resp);

/**
 * Client, options, and user data must live through callback.
 */
void temporal_core_client_rpc_call(struct TemporalCoreConnection *client,
                                   const struct TemporalCoreRpcCallOptions *options,
                                   void *user_data,
                                   TemporalCoreClientRpcCallCallback callback);

/**
 * Load all client profiles from given sources.
 * Returns ClientConfigOrFail with either success JSON or error message.
 * The returned ByteArrays must be freed by the caller.
 */
struct TemporalCoreClientEnvConfigOrFail temporal_core_client_env_config_load(const struct TemporalCoreClientEnvConfigLoadOptions *options);

/**
 * Load a single client profile from given sources with env overrides.
 * Returns ClientConfigProfileOrFail with either success JSON or error message.
 * The returned ByteArrays must be freed by the caller.
 */
struct TemporalCoreClientEnvConfigProfileOrFail temporal_core_client_env_config_profile_load(const struct TemporalCoreClientEnvConfigProfileLoadOptions *options);

struct TemporalCoreMetricMeter *temporal_core_metric_meter_new(struct TemporalCoreRuntime *runtime);

void temporal_core_metric_meter_free(struct TemporalCoreMetricMeter *meter);

struct TemporalCoreMetricAttributes *temporal_core_metric_attributes_new(const struct TemporalCoreMetricMeter *meter,
                                                                         const struct TemporalCoreMetricAttribute *attrs,
                                                                         size_t size);

struct TemporalCoreMetricAttributes *temporal_core_metric_attributes_new_append(const struct TemporalCoreMetricMeter *meter,
                                                                                const struct TemporalCoreMetricAttributes *orig,
                                                                                const struct TemporalCoreMetricAttribute *attrs,
                                                                                size_t size);

void temporal_core_metric_attributes_free(struct TemporalCoreMetricAttributes *attrs);

struct TemporalCoreMetric *temporal_core_metric_new(const struct TemporalCoreMetricMeter *meter,
                                                    const struct TemporalCoreMetricOptions *options);

void temporal_core_metric_free(struct TemporalCoreMetric *metric);

void temporal_core_metric_record_integer(const struct TemporalCoreMetric *metric,
                                         uint64_t value,
                                         const struct TemporalCoreMetricAttributes *attrs);

void temporal_core_metric_record_float(const struct TemporalCoreMetric *metric,
                                       double value,
                                       const struct TemporalCoreMetricAttributes *attrs);

void temporal_core_metric_record_integer_signed(const struct TemporalCoreMetric *metric,
                                                int64_t value,
                                                const struct TemporalCoreMetricAttributes *attrs);

void temporal_core_metric_record_duration(const struct TemporalCoreMetric *metric,
                                          uint64_t value_ms,
                                          const struct TemporalCoreMetricAttributes *attrs);

struct TemporalCoreRandom *temporal_core_random_new(uint64_t seed);

void temporal_core_random_free(struct TemporalCoreRandom *random);

int32_t temporal_core_random_int32_range(struct TemporalCoreRandom *random,
                                         int32_t min,
                                         int32_t max,
                                         bool max_inclusive);

double temporal_core_random_double_range(struct TemporalCoreRandom *random,
                                         double min,
                                         double max,
                                         bool max_inclusive);

void temporal_core_random_fill_bytes(struct TemporalCoreRandom *random,
                                     struct TemporalCoreByteArrayRef bytes);

struct TemporalCoreRuntimeOrFail temporal_core_runtime_new(const struct TemporalCoreRuntimeOptions *options);

void temporal_core_runtime_free(struct TemporalCoreRuntime *runtime);

void temporal_core_byte_array_free(struct TemporalCoreRuntime *runtime,
                                   const struct TemporalCoreByteArray *bytes);

struct TemporalCoreByteArrayRef temporal_core_forwarded_log_target(const struct TemporalCoreForwardedLog *log);

struct TemporalCoreByteArrayRef temporal_core_forwarded_log_message(const struct TemporalCoreForwardedLog *log);

uint64_t temporal_core_forwarded_log_timestamp_millis(const struct TemporalCoreForwardedLog *log);

struct TemporalCoreByteArrayRef temporal_core_forwarded_log_fields_json(const struct TemporalCoreForwardedLog *log);

/**
 * Runtime must live as long as server. Options and user data must live through
 * callback.
 */
void temporal_core_ephemeral_server_start_dev_server(struct TemporalCoreRuntime *runtime,
                                                     const struct TemporalCoreDevServerOptions *options,
                                                     void *user_data,
                                                     TemporalCoreEphemeralServerStartCallback callback);

/**
 * Runtime must live as long as server. Options and user data must live through
 * callback.
 */
void temporal_core_ephemeral_server_start_test_server(struct TemporalCoreRuntime *runtime,
                                                      const struct TemporalCoreTestServerOptions *options,
                                                      void *user_data,
                                                      TemporalCoreEphemeralServerStartCallback callback);

void temporal_core_ephemeral_server_free(struct TemporalCoreEphemeralServer *server);

void temporal_core_ephemeral_server_shutdown(struct TemporalCoreEphemeralServer *server,
                                             void *user_data,
                                             TemporalCoreEphemeralServerShutdownCallback callback);

struct TemporalCoreWorkerOrFail temporal_core_worker_new(struct TemporalCoreConnection *connection,
                                                         const struct TemporalCoreWorkerOptions *options);

void temporal_core_worker_free(struct TemporalCoreWorker *worker);

void temporal_core_worker_validate(struct TemporalCoreWorker *worker,
                                   void *user_data,
                                   TemporalCoreWorkerCallback callback);

const struct TemporalCoreByteArray *temporal_core_worker_replace_client(struct TemporalCoreWorker *worker,
                                                                        struct TemporalCoreConnection *new_connection);

void temporal_core_worker_poll_workflow_activation(struct TemporalCoreWorker *worker,
                                                   void *user_data,
                                                   TemporalCoreWorkerPollCallback callback);

void temporal_core_worker_poll_activity_task(struct TemporalCoreWorker *worker,
                                             void *user_data,
                                             TemporalCoreWorkerPollCallback callback);

void temporal_core_worker_poll_nexus_task(struct TemporalCoreWorker *worker,
                                          void *user_data,
                                          TemporalCoreWorkerPollCallback callback);

void temporal_core_worker_complete_workflow_activation(struct TemporalCoreWorker *worker,
                                                       struct TemporalCoreByteArrayRef completion,
                                                       void *user_data,
                                                       TemporalCoreWorkerCallback callback);

void temporal_core_worker_complete_activity_task(struct TemporalCoreWorker *worker,
                                                 struct TemporalCoreByteArrayRef completion,
                                                 void *user_data,
                                                 TemporalCoreWorkerCallback callback);

void temporal_core_worker_complete_nexus_task(struct TemporalCoreWorker *worker,
                                              struct TemporalCoreByteArrayRef completion,
                                              void *user_data,
                                              TemporalCoreWorkerCallback callback);

/**
 * Returns error if any. Must be freed if returned.
 */
const struct TemporalCoreByteArray *temporal_core_worker_record_activity_heartbeat(struct TemporalCoreWorker *worker,
                                                                                   struct TemporalCoreByteArrayRef heartbeat);

void temporal_core_worker_request_workflow_eviction(struct TemporalCoreWorker *worker,
                                                    struct TemporalCoreByteArrayRef run_id);

void temporal_core_worker_initiate_shutdown(struct TemporalCoreWorker *worker);

void temporal_core_worker_finalize_shutdown(struct TemporalCoreWorker *worker,
                                            void *user_data,
                                            TemporalCoreWorkerCallback callback);

struct TemporalCoreWorkerReplayerOrFail temporal_core_worker_replayer_new(struct TemporalCoreRuntime *runtime,
                                                                          const struct TemporalCoreWorkerOptions *options);

void temporal_core_worker_replay_pusher_free(struct TemporalCoreWorkerReplayPusher *worker_replay_pusher);

struct TemporalCoreWorkerReplayPushResult temporal_core_worker_replay_push(struct TemporalCoreWorker *worker,
                                                                           struct TemporalCoreWorkerReplayPusher *worker_replay_pusher,
                                                                           struct TemporalCoreByteArrayRef workflow_id,
                                                                           struct TemporalCoreByteArrayRef history);

/**
 * Completes asynchronous slot reservation started by a call to [`CustomSlotSupplierCallbacks::reserve`].
 *
 * `completion_ctx` must be the same as the one passed to the matching [`reserve`](CustomSlotSupplierCallbacks::reserve)
 * call. `permit_id` is arbitrary, but must be unique among live reservations as it's later used
 * for [`mark_used`](CustomSlotSupplierCallbacks::mark_used) and [`release`](CustomSlotSupplierCallbacks::release)
 * callbacks.
 *
 * This function returns true if the reservation was completed successfully, or false if the
 * reservation was cancelled before completion. If this function returns false, the implementation
 * should call [`temporal_core_complete_async_cancel_reserve`] with the same `completion_ctx`.
 *
 * **Caution:** if this function returns true, `completion_ctx` gets freed. Afterwards, calling
 * either [`temporal_core_complete_async_reserve`] or [`temporal_core_complete_async_cancel_reserve`]
 * with the same `completion_ctx` will cause **memory corruption!**
 */
bool temporal_core_complete_async_reserve(const struct TemporalCoreSlotReserveCompletionCtx *completion_ctx,
                                          uintptr_t permit_id);

/**
 * Completes cancellation of asynchronous slot reservation.
 *
 * Cancellation can only be initiated by Core. It's done by calling [`CustomSlotSupplierCallbacks::cancel_reserve`]
 * after an earlier call to [`CustomSlotSupplierCallbacks::reserve`].
 *
 * `completion_ctx` must be the same as the one passed to the matching [`cancel_reserve`](CustomSlotSupplierCallbacks::cancel_reserve)
 * call.
 *
 * This function returns true on successful cancellation, or false if cancellation was not
 * requested for the given `completion_ctx`. A false value indicates there's likely a logic bug in
 * the implementation where it doesn't correctly wait for [`cancel_reserve`](CustomSlotSupplierCallbacks::cancel_reserve)
 * callback to be called.
 *
 * **Caution:** if this function returns true, `completion_ctx` gets freed. Afterwards, calling
 * either [`temporal_core_complete_async_reserve`] or [`temporal_core_complete_async_cancel_reserve`]
 * with the same `completion_ctx` will cause **memory corruption!**
 */
bool temporal_core_complete_async_cancel_reserve(const struct TemporalCoreSlotReserveCompletionCtx *completion_ctx);

#ifdef __cplusplus
}  // extern "C"
#endif  // __cplusplus
