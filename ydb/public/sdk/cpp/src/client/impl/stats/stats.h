#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/metrics/metrics.h>

#include <ydb/public/sdk/cpp/src/client/impl/observability/constants.h>
#include <ydb/public/sdk/cpp/src/library/grpc/client/grpc_client_low.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>

#include <atomic>
#include <chrono>
#include <memory>
#include <string_view>
#include <vector>

namespace NYdb::inline Dev {
namespace NSdkStats {

inline std::string YdbClientApiAttributeValue(const std::string& clientType) {
    return clientType.empty() ? std::string("Unspecified") : clientType;
}

// works only for case normal (foo_bar) underscore

inline std::string UnderscoreToUpperCamel(const std::string& in) {
    std::string result;
    result.reserve(in.size());

    if (in.empty())
        return {};

    result.push_back(toupper(in[0]));

    size_t i = 1;

    while (i < in.size()) {
        if (in[i] == '_') {
            if (++i < in.size()) {
                result.push_back(toupper(in[i++]));
            } else {
                break;
            }
        } else {
            result.push_back(tolower(in[i++]));
        }
    }
    return result;
}

template<typename TPointer>
class TAtomicPointer {
public:

    TAtomicPointer(TPointer* pointer = nullptr) {
        Set(pointer);
    }

    TAtomicPointer(const TAtomicPointer& other) {
        Set(other.Get());
    }

    TAtomicPointer& operator=(const TAtomicPointer& other) {
        Set(other.Get());
        return *this;
    }

    TPointer* Get() const {
        return Pointer_.load();
    }

    void Set(TPointer* pointer) {
        Pointer_.store(pointer);
    }

private:
    std::atomic<TPointer*> Pointer_;
};

template<typename TPointer>
class TAtomicCounter: public TAtomicPointer<TPointer> {
    public:
        void Add(ui64 value) {
            if (auto counter = this->Get()) {
                counter->Add(value);
            }
        }

        void Inc() {
            if (auto counter = this->Get()) {
                counter->Inc();
            }
        }

        void Dec() {
            if (auto counter = this->Get()) {
                counter->Dec();
            }
        }

        void SetValue(ui64 value) {
            if (auto counter = this->Get()) {
                counter->Set(value);
            }
        }
};

template<typename TCounter>
class FastLocalCounter {
public:
    FastLocalCounter(TAtomicCounter<TCounter>& counter)
    : Counter(counter), Value(0)
    { }

    ~FastLocalCounter() {
        Counter.Add(Value);
    }

    FastLocalCounter<TCounter>& operator++ () {
        ++Value;
        return *this;
    }

    TAtomicCounter<TCounter>& Counter;
    ui64 Value;
};

template<typename TPointer>
class TAtomicHistogram: public TAtomicPointer<TPointer> {
public:

    void Record(i64 value) {
        if (auto histogram = this->Get()) {
            histogram->Record(value);
        }
    }

    bool IsCollecting() {
        return this->Get() != nullptr;
    }
};

// Sessions count for all clients
// Every client has 3 TSessionCounter for active, in session pool, in settler sessions
// TSessionCounters in different clients with same role share one sensor
class TSessionCounter: public TAtomicPointer<::NMonitoring::TIntGauge> {
public:

    // Call with mutex
    void Apply(i64 newValue) {
        if (auto gauge = this->Get()) {
            gauge->Add(newValue - oldValue);
            oldValue = newValue;
        }
    }

    ~TSessionCounter() {
        ::NMonitoring::TIntGauge* gauge = this->Get();
        if (gauge) {
            gauge->Add(-oldValue);
        }
    }

private:
    i64 oldValue = 0;
};

struct TStatCollector {
    using TMetricRegistry = ::NMonitoring::TMetricRegistry;

public:
    struct TEndpointElectorStatCollector {

        TEndpointElectorStatCollector(::NMonitoring::TIntGauge* endpointCount = nullptr
        , ::NMonitoring::TIntGauge* pessimizationRatio = nullptr
        , ::NMonitoring::TIntGauge* activeEndpoints = nullptr)
        : EndpointCount(endpointCount)
        , PessimizationRatio(pessimizationRatio)
        , EndpointActive(activeEndpoints)
        { }

        ::NMonitoring::TIntGauge* EndpointCount;
        ::NMonitoring::TIntGauge* PessimizationRatio;
        ::NMonitoring::TIntGauge* EndpointActive;
    };

    struct TSessionPoolStatCollector {
        TSessionPoolStatCollector(::NMonitoring::TIntGauge* activeSessions = nullptr
        , ::NMonitoring::TIntGauge* inPoolSessions = nullptr
        , ::NMonitoring::TRate* fakeSessions = nullptr
        , ::NMonitoring::TIntGauge* waiters = nullptr
        , std::shared_ptr<NMetrics::IMetricRegistry> externalRegistry = {}
        , std::string clientType = {}
        , std::string poolName = {}
    ) : ActiveSessions(activeSessions)
        , InPoolSessions(inPoolSessions)
        , FakeSessions(fakeSessions)
        , Waiters(waiters)
        , ExternalRegistry_(std::move(externalRegistry))
        , ClientType_(std::move(clientType))
        , PoolName_(std::move(poolName))
    {}

        ::NMonitoring::TIntGauge* ActiveSessions;
        ::NMonitoring::TIntGauge* InPoolSessions;
        ::NMonitoring::TRate* FakeSessions;
        ::NMonitoring::TIntGauge* Waiters;

        void UpdateConnectionCount(std::int64_t idleCount, std::int64_t usedCount) {
            if (!ExternalRegistry_) {
                return;
            }
            EmitSessionCount(NObservability::MetricValue::kSessionStateIdle, idleCount);
            EmitSessionCount(NObservability::MetricValue::kSessionStateUsed, usedCount);
        }

        void IncPendingRequests() {
            if (!ExternalRegistry_) {
                return;
            }
            ExternalRegistry_->Counter(
                MetricName(NObservability::MetricName::kSessionLeafPendingRequests),
                BasePoolLabels(),
                "Number of times a caller has started waiting for an open session.",
                std::string(NObservability::MetricUnit::kRequest)
            )->Inc();
        }

        void IncConnectionTimeouts() {
            if (!ExternalRegistry_) {
                return;
            }
            ExternalRegistry_->Counter(
                MetricName(NObservability::MetricName::kSessionLeafTimeouts),
                BasePoolLabels(),
                "Session-acquisition timeouts.",
                std::string(NObservability::MetricUnit::kTimeout)
            )->Inc();
        }

        void RecordConnectionCreateTime(double seconds) {
            if (!ExternalRegistry_) {
                return;
            }
            ExternalRegistry_->Histogram(
                MetricName(NObservability::MetricName::kSessionLeafCreateTime),
                {0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10},
                BasePoolLabels(),
                "Time spent on creating a new session (CreateSession + first AttachStream message).",
                std::string(NObservability::MetricUnit::kSeconds)
            )->Record(seconds);
        }

        void RecordPoolLimits(std::int64_t minPoolSize, std::int64_t maxPoolSize) {
            if (!ExternalRegistry_) {
                return;
            }
            ExternalRegistry_->Gauge(
                MetricName(NObservability::MetricName::kSessionLeafMin),
                BasePoolLabels(),
                "Configured MinPoolSize.",
                std::string(NObservability::MetricUnit::kSession)
            )->Set(static_cast<double>(minPoolSize));
            ExternalRegistry_->Gauge(
                MetricName(NObservability::MetricName::kSessionLeafMax),
                BasePoolLabels(),
                "Configured MaxPoolSize.",
                std::string(NObservability::MetricUnit::kSession)
            )->Set(static_cast<double>(maxPoolSize));
        }

        bool HasExternalRegistry() const {
            return static_cast<bool>(ExternalRegistry_);
        }

        const std::string& GetClientType() const {
            return ClientType_;
        }

        std::string MetricNamespace() const {
            return PoolMetricNamespace(ClientType_);
        }

    private:
        static std::string PoolMetricNamespace(const std::string& clientType) {
            if (clientType == "Query") {
                return std::string(NObservability::MetricName::kSessionPrefixQuery);
            }
            if (clientType == "Table") {
                return std::string(NObservability::MetricName::kSessionPrefixTable);
            }
            return std::string(NObservability::MetricName::kSessionPrefixGeneric);
        }

        std::string MetricName(std::string_view leaf) const {
            std::string out = PoolMetricNamespace(ClientType_);
            out.push_back('.');
            out.append(leaf);
            return out;
        }

        std::string PoolNameTagKey() const {
            return MetricName(NObservability::MetricName::kSessionTagPoolNameSuffix);
        }

        std::string StateTagKey() const {
            return MetricName(NObservability::MetricName::kSessionTagStateSuffix);
        }

        std::string EffectivePoolName() const {
            return PoolName_;
        }

        NMetrics::TLabels BasePoolLabels() const {
            return {
                {PoolNameTagKey(), EffectivePoolName()},
            };
        }

        void EmitSessionCount(std::string_view state, std::int64_t value) {
            auto labels = BasePoolLabels();
            labels[StateTagKey()] = std::string(state);
            ExternalRegistry_->Gauge(
                MetricName(NObservability::MetricName::kSessionLeafCount),
                labels,
                "Current pool session count for the given state.",
                std::string(NObservability::MetricUnit::kSession)
            )->Set(static_cast<double>(value));
        }

        std::shared_ptr<NMetrics::IMetricRegistry> ExternalRegistry_;
        std::string ClientType_;
        std::string PoolName_;
    };

    struct TClientRetryOperationStatCollector {

        TClientRetryOperationStatCollector() : MetricRegistry_(), Database_() {}

        TClientRetryOperationStatCollector(::NMonitoring::TMetricRegistry* registry,
                                           const std::string& database,
                                           const std::string& clientType)
            : MetricRegistry_(registry)
            , Database_(database)
            , ClientType_(clientType)
        { }

        void IncSyncRetryOperation(const EStatus& status) {
            if (auto registry = MetricRegistry_.Get()) {
                std::string statusName = TStringBuilder() << status;
                std::string sensor = TStringBuilder() << "RetryOperation/" << UnderscoreToUpperCamel(statusName);
                registry->Rate({ {"database", Database_}, {"ydb_client", ClientType_}, {"sensor", sensor} })->Inc();
            }
        }

        void IncAsyncRetryOperation(const EStatus& status) {
            if (auto registry = MetricRegistry_.Get()) {
                std::string statusName = TStringBuilder() << status;
                std::string sensor = TStringBuilder() << "RetryOperation/" << UnderscoreToUpperCamel(statusName);
                registry->Rate({ {"database", Database_}, {"ydb_client", ClientType_}, {"sensor", sensor} })->Inc();
            }
        }

    private:
        TAtomicPointer<::NMonitoring::TMetricRegistry> MetricRegistry_;
        std::string Database_;
        std::string ClientType_;
    };

    struct TClientOperationStatCollector {
        TClientOperationStatCollector()
            : MetricRegistry_()
        {}

        TClientOperationStatCollector(::NMonitoring::TMetricRegistry* registry,
                                      const std::string& database,
                                      const std::string& clientType,
                                      std::shared_ptr<NMetrics::IMetricRegistry> externalRegistry = {},
                                      std::string serverAddress = {},
                                      std::uint16_t serverPort = 0)
            : MetricRegistry_(registry)
            , ExternalRegistry_(std::move(externalRegistry))
            , Database_(database)
            , ClientType_(clientType)
            , ServerAddress_(std::move(serverAddress))
            , ServerPort_(serverPort)
        {}

        void IncRequestCount(const std::string& operationName) {
            if (auto registry = MetricRegistry_.Get()) {
                registry->Rate({
                    {"database", Database_},
                    {"ydb_client", ClientType_},
                    {"operation", operationName},
                    {"sensor", "Request/Operations"}
                })->Inc();
            }
        }

        void IncErrorCount(const std::string& operationName, EStatus status) {
            if (status == EStatus::SUCCESS) {
                return;
            }
            if (auto registry = MetricRegistry_.Get()) {
                registry->Rate({
                    {"database", Database_},
                    {"ydb_client", ClientType_},
                    {"operation", operationName},
                    {"status", TStringBuilder() << status},
                    {"sensor", "Request/OperationErrors"}
                })->Inc();
            }
            if (ExternalRegistry_) {
                using namespace NObservability;
                NMetrics::TLabels labels = {
                    {std::string(MetricLabel::kDbSystemName), std::string(MetricValue::kDbSystemYdb)},
                    {std::string(MetricLabel::kDbNamespace), Database_},
                    {std::string(MetricLabel::kDbOperationName), operationName},
                    {std::string(MetricLabel::kDbResponseStatusCode), TStringBuilder() << status},
                };
                if (!ServerAddress_.empty()) {
                    labels[std::string(MetricLabel::kServerAddress)] = ServerAddress_;
                }
                if (ServerPort_ != 0) {
                    labels[std::string(MetricLabel::kServerPort)] = TStringBuilder() << ServerPort_;
                }
                ExternalRegistry_->Counter(
                    std::string(MetricName::kOperationFailed),
                    labels,
                    "Number of unsuccessful database client operation attempts.",
                    std::string(MetricUnit::kOperation)
                )->Inc();
            }
        }

        void RecordLatency(const std::string& operationName, double durationSeconds, EStatus status) {
            (void)status;
            if (auto registry = MetricRegistry_.Get()) {
                registry->HistogramRate({
                    {"database", Database_},
                    {"ydb_client", ClientType_},
                    {"operation", operationName},
                    {"sensor", "Request/OperationLatencyMs"}
                }, ::NMonitoring::ExponentialHistogram(20, 2, 1))->Record(
                    static_cast<i64>(durationSeconds * 1000.0));
            }
            if (ExternalRegistry_) {
                using namespace NObservability;
                NMetrics::TLabels labels = {
                    {std::string(MetricLabel::kDbSystemName), std::string(MetricValue::kDbSystemYdb)},
                    {std::string(MetricLabel::kDbNamespace), Database_},
                    {std::string(MetricLabel::kDbOperationName), operationName},
                };
                if (!ServerAddress_.empty()) {
                    labels[std::string(MetricLabel::kServerAddress)] = ServerAddress_;
                }
                if (ServerPort_ != 0) {
                    labels[std::string(MetricLabel::kServerPort)] = TStringBuilder() << ServerPort_;
                }
                ExternalRegistry_->Histogram(
                    std::string(MetricName::kOperationDuration),
                    {0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 5, 10},
                    labels,
                    "Latency of each actual operation attempt (e.g. ExecuteQuery / Commit / Rollback).",
                    std::string(MetricUnit::kSeconds)
                )->Record(durationSeconds);
            }
        }

    private:
        TAtomicPointer<::NMonitoring::TMetricRegistry> MetricRegistry_;
        std::shared_ptr<NMetrics::IMetricRegistry> ExternalRegistry_;
        std::string Database_;
        std::string ClientType_;
        std::string ServerAddress_;
        std::uint16_t ServerPort_ = 0;
    };

    struct TClientStatCollector {

        TClientStatCollector(::NMonitoring::TRate* cacheMiss = nullptr
        , ::NMonitoring::THistogram* querySize = nullptr
        , ::NMonitoring::THistogram* paramsSize = nullptr
        , ::NMonitoring::TRate* sessionRemoved = nullptr
        , ::NMonitoring::TRate* requestMigrated = nullptr
        , TClientRetryOperationStatCollector retryOperationStatCollector = TClientRetryOperationStatCollector()
        , TClientOperationStatCollector operationStatCollector = TClientOperationStatCollector())
        : CacheMiss(cacheMiss)
        , QuerySize(querySize)
        , ParamsSize(paramsSize)
        , SessionRemovedDueBalancing(sessionRemoved)
        , RequestMigrated(requestMigrated)
        , RetryOperationStatCollector(retryOperationStatCollector)
        , OperationStatCollector(operationStatCollector)
        { }

        ::NMonitoring::TRate* CacheMiss;
        ::NMonitoring::THistogram* QuerySize;
        ::NMonitoring::THistogram* ParamsSize;
        ::NMonitoring::TRate* SessionRemovedDueBalancing;
        ::NMonitoring::TRate* RequestMigrated;
        TClientRetryOperationStatCollector RetryOperationStatCollector;
        TClientOperationStatCollector OperationStatCollector;
    };

    TStatCollector(const std::string& database
        , TMetricRegistry* sensorsRegistry
        , std::shared_ptr<NMetrics::IMetricRegistry> externalMetricRegistry = {}
        , const std::string& discoveryEndpoint = {}
    ) : Database_(database)
        , DatabaseLabel_({"database", database})
        , ExternalMetricRegistry_(std::move(externalMetricRegistry))
        , DiscoveryEndpoint_(discoveryEndpoint)
    {
        ParseDiscoveryEndpoint(discoveryEndpoint, ServerAddress_, ServerPort_);
        if (sensorsRegistry) {
            SetMetricRegistry(sensorsRegistry);
        }
    }

    static std::string ResolvePoolName(const std::string& explicitPoolName,
                                       const std::string& database,
                                       const std::string& discoveryEndpoint) {
        if (!explicitPoolName.empty()) {
            return explicitPoolName;
        }
        std::string out;
        out.reserve(database.size() + discoveryEndpoint.size() + 1);
        out.append(database);
        out.push_back('@');
        out.append(discoveryEndpoint);
        return out;
    }

    static void ParseDiscoveryEndpoint(const std::string& endpoint,
                                       std::string& outHost,
                                       std::uint16_t& outPort) noexcept {
        outHost.clear();
        outPort = 0;
        if (endpoint.empty()) {
            return;
        }
        std::string_view view(endpoint);
        for (std::string_view scheme : {"grpcs://", "grpc://"}) {
            if (view.substr(0, scheme.size()) == scheme) {
                view.remove_prefix(scheme.size());
                break;
            }
        }
        std::string_view host;
        std::string_view portStr;
        if (!view.empty() && view.front() == '[') {
            const auto end = view.find(']');
            if (end == std::string_view::npos || end + 1 >= view.size() || view[end + 1] != ':') {
                return;
            }
            host = view.substr(1, end - 1);
            portStr = view.substr(end + 2);
        } else {
            const auto colon = view.rfind(':');
            if (colon == std::string_view::npos) {
                return;
            }
            host = view.substr(0, colon);
            portStr = view.substr(colon + 1);
        }
        std::uint32_t port = 0;
        for (char c : portStr) {
            if (c < '0' || c > '9') {
                return;
            }
            port = port * 10 + static_cast<std::uint32_t>(c - '0');
            if (port > 65535) {
                return;
            }
        }
        if (port == 0 || host.empty()) {
            return;
        }
        outHost.assign(host);
        outPort = static_cast<std::uint16_t>(port);
    }

    void SetMetricRegistry(TMetricRegistry* sensorsRegistry) {
        Y_ABORT_UNLESS(sensorsRegistry, "TMetricRegistry is null in stats collector.");
        MetricRegistryPtr_.Set(sensorsRegistry);
        DiscoveryDuePessimization_.Set(sensorsRegistry->Rate({ DatabaseLabel_,      {"sensor", "Discovery/TooManyBadEndpoints"} }));
        DiscoveryDueExpiration_.Set(sensorsRegistry->Rate({ DatabaseLabel_,         {"sensor", "Discovery/Regular"} }));
        DiscoveryFailDueTransportError_.Set(sensorsRegistry->Rate({ DatabaseLabel_, {"sensor", "Discovery/FailedTransportError"} }));
        RequestFailDueQueueOverflow_.Set(sensorsRegistry->Rate({ DatabaseLabel_,    {"sensor", "Request/FailedDiscoveryQueueOverflow"} }));
        RequestFailDueNoEndpoint_.Set(sensorsRegistry->Rate({ DatabaseLabel_,       {"sensor", "Request/FailedNoEndpoint"} }));
        RequestFailDueTransportError_.Set(sensorsRegistry->Rate({ DatabaseLabel_,   {"sensor", "Request/FailedTransportError"} }));
        SessionCV_.Set(sensorsRegistry->IntGauge({ DatabaseLabel_,                  {"sensor", "SessionBalancer/Variation"} }));
        GRpcInFlight_.Set(sensorsRegistry->IntGauge({ DatabaseLabel_,               {"sensor", "Grpc/InFlight"} }));

        RequestLatency_.Set(sensorsRegistry->HistogramRate({ DatabaseLabel_, {"sensor", "Request/Latency"} },
            ::NMonitoring::ExponentialHistogram(20, 2, 1)));
        ResultSize_.Set(sensorsRegistry->HistogramRate({ DatabaseLabel_, {"sensor", "Request/ResultSize"} },
            ::NMonitoring::ExponentialHistogram(20, 2, 32)));
    }

    void IncDiscoveryDuePessimization() {
        DiscoveryDuePessimization_.Inc();
    }

    void IncDiscoveryDueExpiration() {
        DiscoveryDueExpiration_.Inc();
    }

    void IncDiscoveryFailDueTransportError() {
        DiscoveryFailDueTransportError_.Inc();
    }

    void IncReqFailQueueOverflow() {
        RequestFailDueQueueOverflow_.Inc();
    }

    void IncReqFailNoEndpoint() {
        RequestFailDueNoEndpoint_.Inc();
    }

    void IncReqFailDueTransportError() {
        RequestFailDueTransportError_.Inc();
    }

    void IncRequestLatency(TDuration duration) {
        RequestLatency_.Record(duration.MilliSeconds());
    }

    void IncResultSize(const size_t& size) {
        ResultSize_.Record(size);
    }

    void IncCounter(const std::string& sensor) {
        if (auto registry = MetricRegistryPtr_.Get()) {
            registry->Counter({ {"database", Database_}, {"sensor", sensor} })->Inc();
        }
    }

    void SetSessionCV(ui32 cv) {
        SessionCV_.SetValue(cv);
    }

    void IncGRpcInFlight () {
        GRpcInFlight_.Inc();
    }

    void DecGRpcInFlight () {
        GRpcInFlight_.Dec();
    }

    TEndpointElectorStatCollector GetEndpointElectorStatCollector() {
        if (auto registry = MetricRegistryPtr_.Get()) {
            auto endpointCoint = registry->IntGauge({ DatabaseLabel_,      {"sensor", "Endpoints/Total"} });
            auto pessimizationRatio = registry->IntGauge({ DatabaseLabel_, {"sensor", "Endpoints/BadRatio"} });
            auto activeEndpoints = registry->IntGauge({ DatabaseLabel_,    {"sensor", "Endpoints/Good"} });
            return TEndpointElectorStatCollector(endpointCoint, pessimizationRatio, activeEndpoints);
        }

        return TEndpointElectorStatCollector();
    }

    TSessionPoolStatCollector GetSessionPoolStatCollector(const std::string& clientType
        , const std::string& explicitPoolName = {}
    ) {
        const std::string poolName = ResolvePoolName(explicitPoolName, Database_, DiscoveryEndpoint_);

        if (auto registry = MetricRegistryPtr_.Get()) {
            auto activeSessions = registry->IntGauge({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Sessions/InUse"} });
            auto inPoolSessions = registry->IntGauge({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Sessions/InPool"} });
            auto fakeSessions = registry->Rate({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Sessions/SessionsLimitExceeded"} });
            auto waiters = registry->IntGauge({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Sessions/WaitForReturn"} });

            return TSessionPoolStatCollector(activeSessions, inPoolSessions, fakeSessions, waiters,
                ExternalMetricRegistry_, clientType, poolName);
        }

        return TSessionPoolStatCollector(nullptr, nullptr, nullptr, nullptr,
            ExternalMetricRegistry_, clientType, poolName);
    }

    TClientStatCollector GetClientStatCollector(const std::string& clientType) {
        if (auto registry = MetricRegistryPtr_.Get()) {
            ::NMonitoring::TRate* cacheMiss = nullptr;
            ::NMonitoring::TRate* sessionRemovedDueBalancing = nullptr;
            ::NMonitoring::TRate* requestMigrated = nullptr;

            if (clientType == "Table") {
                cacheMiss = registry->Rate({ DatabaseLabel_, {"ydb_client", clientType},
                    {"sensor", "Request/ClientQueryCacheMiss"} });
                sessionRemovedDueBalancing = registry->Rate({ DatabaseLabel_, {"ydb_client", clientType},
                    {"sensor", "SessionBalancer/SessionsRemoved"} });
                requestMigrated = registry->Rate({ DatabaseLabel_, {"ydb_client", clientType},
                    {"sensor", "SessionBalancer/RequestsMigrated"} });
            }

            auto querySize = registry->HistogramRate({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Request/QuerySize"} }, ::NMonitoring::ExponentialHistogram(20, 2, 32));
            auto paramsSize = registry->HistogramRate({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Request/ParamsSize"} }, ::NMonitoring::ExponentialHistogram(10, 2, 32));

            return TClientStatCollector(cacheMiss, querySize, paramsSize, sessionRemovedDueBalancing, requestMigrated,
                TClientRetryOperationStatCollector(MetricRegistryPtr_.Get(), Database_, clientType),
                TClientOperationStatCollector(MetricRegistryPtr_.Get(), Database_, clientType,
                    ExternalMetricRegistry_, ServerAddress_, ServerPort_));
        }

        return TClientStatCollector(nullptr, nullptr, nullptr, nullptr, nullptr,
            TClientRetryOperationStatCollector(nullptr, Database_, clientType),
            TClientOperationStatCollector(nullptr, Database_, clientType,
                ExternalMetricRegistry_, ServerAddress_, ServerPort_));
    }

    bool IsCollecting() {
        return MetricRegistryPtr_.Get() != nullptr;
    }

    void IncSessionsOnHost(const std::string& host);
    void DecSessionsOnHost(const std::string& host);

    void IncTransportErrorsByHost(const std::string& host);

    void IncGRpcInFlightByHost(const std::string& host);
    void DecGRpcInFlightByHost(const std::string& host);

private:
    const std::string Database_;
    const ::NMonitoring::TLabel DatabaseLabel_;
    std::shared_ptr<NMetrics::IMetricRegistry> ExternalMetricRegistry_;
    std::string DiscoveryEndpoint_;
    std::string ServerAddress_;
    std::uint16_t ServerPort_ = 0;
    TAtomicPointer<TMetricRegistry> MetricRegistryPtr_;
    TAtomicCounter<::NMonitoring::TRate> DiscoveryDuePessimization_;
    TAtomicCounter<::NMonitoring::TRate> DiscoveryDueExpiration_;
    TAtomicCounter<::NMonitoring::TRate> RequestFailDueQueueOverflow_;
    TAtomicCounter<::NMonitoring::TRate> RequestFailDueNoEndpoint_;
    TAtomicCounter<::NMonitoring::TRate> RequestFailDueTransportError_;
    TAtomicCounter<::NMonitoring::TRate> DiscoveryFailDueTransportError_;
    TAtomicCounter<::NMonitoring::TIntGauge> SessionCV_;
    TAtomicCounter<::NMonitoring::TIntGauge> GRpcInFlight_;
    TAtomicHistogram<::NMonitoring::THistogram> RequestLatency_;
    TAtomicHistogram<::NMonitoring::THistogram> ResultSize_;
};

} // namespace NSdkStats
} // namespace NYdb
