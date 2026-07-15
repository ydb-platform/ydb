#pragma once

// Single source of truth for all observability identifiers (metric names,
// metric attributes/labels, span names, span attribute keys, etc.).
//
// Anything user-visible in traces or metrics is intentionally a `constexpr`
// constant in this header so we don't accidentally diverge between the metric
// emitter and the test that asserts on the same name.

#include <string_view>

namespace NYdb::inline Dev::NObservability {

// ---------------------------------------------------------------------------
// OTel Semconv attribute keys shared between span attributes and metric labels.
// ---------------------------------------------------------------------------
namespace AttrKey {

inline constexpr std::string_view kDbSystemName         = "db.system.name";
inline constexpr std::string_view kDbNamespace          = "db.namespace";
inline constexpr std::string_view kDbOperationName      = "db.operation.name";
inline constexpr std::string_view kDbResponseStatusCode = "db.response.status_code";
inline constexpr std::string_view kServerAddress        = "server.address";
inline constexpr std::string_view kServerPort           = "server.port";

} // namespace AttrKey

// ---------------------------------------------------------------------------
// Tracing: instrumentation scope.
// ---------------------------------------------------------------------------
namespace Tracer {

inline constexpr std::string_view kSdkName = "ydb-cpp-sdk";

} // namespace Tracer

// ---------------------------------------------------------------------------
// Tracing: span names.
// ---------------------------------------------------------------------------
namespace SpanName {

inline constexpr std::string_view kRetryRoot    = "RunWithRetry";
inline constexpr std::string_view kRetryAttempt = "Try";

} // namespace SpanName

// ---------------------------------------------------------------------------
// Tracing: span attribute keys (and a few well-known values).
// Kept as plain strings (not enums) to mirror OTel Semconv exactly.
// ---------------------------------------------------------------------------
namespace SpanAttr {

using AttrKey::kDbSystemName;
using AttrKey::kDbNamespace;
using AttrKey::kDbOperationName;
using AttrKey::kDbResponseStatusCode;
using AttrKey::kServerAddress;
using AttrKey::kServerPort;

inline constexpr std::string_view kNetworkPeerAddress  = "network.peer.address";
inline constexpr std::string_view kNetworkPeerPort     = "network.peer.port";

inline constexpr std::string_view kErrorType           = "error.type";

inline constexpr std::string_view kExceptionType       = "exception.type";
inline constexpr std::string_view kExceptionMessage    = "exception.message";
inline constexpr std::string_view kExceptionStacktrace = "exception.stacktrace";

inline constexpr std::string_view kYdbClientApi        = "ydb.client.api";
inline constexpr std::string_view kYdbNodeId           = "ydb.node.id";
inline constexpr std::string_view kYdbNodeDc           = "ydb.node.dc";
inline constexpr std::string_view kYdbRetryCount       = "ydb.retry.count";
inline constexpr std::string_view kYdbRetryAttempt     = "ydb.retry.attempt";
inline constexpr std::string_view kYdbRetryBackoffMs   = "ydb.retry.backoff_ms";

} // namespace SpanAttr

namespace SpanEvent {

inline constexpr std::string_view kException = "exception";

} // namespace SpanEvent

namespace SpanValue {

// db.system.name value for YDB.
inline constexpr std::string_view kDbSystemYdb = "ydb";
// ydb.client.api fallback when the caller did not set a client type.
inline constexpr std::string_view kClientApiUnspecified = "Unspecified";

} // namespace SpanValue

// ---------------------------------------------------------------------------
// Metrics: instrumentation scope.
// ---------------------------------------------------------------------------
namespace Meter {

inline constexpr std::string_view kSdkName = "ydb-cpp-sdk";

} // namespace Meter

// ---------------------------------------------------------------------------
// Metrics: instrument names.
// ---------------------------------------------------------------------------
namespace MetricName {

// Operation-level instruments (per RPC attempt).
inline constexpr std::string_view kOperationDuration          = "ydb.client.operation.duration";
inline constexpr std::string_view kOperationFailed            = "ydb.client.operation.failed";

// Session-pool namespaces. The leaf names below are appended to one of these.
inline constexpr std::string_view kSessionPrefixQuery         = "ydb.query.session";
inline constexpr std::string_view kSessionPrefixTable         = "ydb.table.session";
inline constexpr std::string_view kSessionPrefixGeneric       = "ydb.session";

// Leaf names for session-pool metrics (combined as "<prefix>.<leaf>").
inline constexpr std::string_view kSessionLeafCount           = "count";
inline constexpr std::string_view kSessionLeafCreateTime      = "create_time";
inline constexpr std::string_view kSessionLeafPendingRequests = "pending_requests";
inline constexpr std::string_view kSessionLeafTimeouts        = "timeouts";
inline constexpr std::string_view kSessionLeafMin             = "min";
inline constexpr std::string_view kSessionLeafMax             = "max";

// Tag suffixes for the session-pool namespaces.
inline constexpr std::string_view kSessionTagPoolNameSuffix   = "pool.name";
inline constexpr std::string_view kSessionTagStateSuffix      = "state";

} // namespace MetricName

// ---------------------------------------------------------------------------
// Metrics: instrument units (UCUM / Semconv recommended forms).
// ---------------------------------------------------------------------------
namespace MetricUnit {

inline constexpr std::string_view kSeconds   = "s";
inline constexpr std::string_view kOperation = "{operation}";
inline constexpr std::string_view kRequest   = "{request}";
inline constexpr std::string_view kTimeout   = "{timeout}";
inline constexpr std::string_view kSession   = "{session}";

} // namespace MetricUnit

// ---------------------------------------------------------------------------
// Metrics: shared label keys (same Semconv strings as SpanAttr / AttrKey).
// ---------------------------------------------------------------------------
namespace MetricLabel {

using AttrKey::kDbSystemName;
using AttrKey::kDbNamespace;
using AttrKey::kDbOperationName;
using AttrKey::kDbResponseStatusCode;
using AttrKey::kServerAddress;
using AttrKey::kServerPort;

} // namespace MetricLabel

// ---------------------------------------------------------------------------
// Metrics: shared label values / pool states.
// ---------------------------------------------------------------------------
namespace MetricValue {

// `db.system.name` value used by all SDK-emitted metrics.
inline constexpr std::string_view kDbSystemYdb = "ydb";

// Possible values for the `<prefix>.state` label on `<prefix>.count`.
inline constexpr std::string_view kSessionStateIdle = "idle";
inline constexpr std::string_view kSessionStateUsed = "used";

} // namespace MetricValue

} // namespace NYdb::inline Dev::NObservability
