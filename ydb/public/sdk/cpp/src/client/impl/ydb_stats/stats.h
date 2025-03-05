#pragma once

#include <ydb-cpp-sdk/client/types/status_codes.h>

#include <src/library/grpc/client/grpc_client_low.h>
#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/monlib/metrics/histogram_collector.h>

#include <atomic>
#include <memory>

namespace NYdb::inline V3 {
namespace NSdkStats {

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
        , ::NMonitoring::TIntGauge* waiters = nullptr)
        : ActiveSessions(activeSessions)
        , InPoolSessions(inPoolSessions)
        , FakeSessions(fakeSessions)
        , Waiters(waiters)
        { }

        ::NMonitoring::TIntGauge* ActiveSessions;
        ::NMonitoring::TIntGauge* InPoolSessions;
        ::NMonitoring::TRate* FakeSessions;
        ::NMonitoring::TIntGauge* Waiters;
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

    struct TClientStatCollector {

        TClientStatCollector(::NMonitoring::TRate* cacheMiss = nullptr
        , ::NMonitoring::THistogram* querySize = nullptr
        , ::NMonitoring::THistogram* paramsSize = nullptr
        , ::NMonitoring::TRate* sessionRemoved = nullptr
        , ::NMonitoring::TRate* requestMigrated = nullptr
        , TClientRetryOperationStatCollector retryOperationStatCollector = TClientRetryOperationStatCollector())
        : CacheMiss(cacheMiss)
        , QuerySize(querySize)
        , ParamsSize(paramsSize)
        , SessionRemovedDueBalancing(sessionRemoved)
        , RequestMigrated(requestMigrated)
        , RetryOperationStatCollector(retryOperationStatCollector)
        { }

        ::NMonitoring::TRate* CacheMiss;
        ::NMonitoring::THistogram* QuerySize;
        ::NMonitoring::THistogram* ParamsSize;
        ::NMonitoring::TRate* SessionRemovedDueBalancing;
        ::NMonitoring::TRate* RequestMigrated;
        TClientRetryOperationStatCollector RetryOperationStatCollector;
    };

    TStatCollector(const std::string& database, TMetricRegistry* sensorsRegistry)
        : Database_(database)
        , DatabaseLabel_({"database", database})
    {
        if (sensorsRegistry) {
            SetMetricRegistry(sensorsRegistry);
        }
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

    TSessionPoolStatCollector GetSessionPoolStatCollector(const std::string& clientType) {
        if (auto registry = MetricRegistryPtr_.Get()) {
            auto activeSessions = registry->IntGauge({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Sessions/InUse"} });
            auto inPoolSessions = registry->IntGauge({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Sessions/InPool"} });
            auto fakeSessions = registry->Rate({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Sessions/SessionsLimitExceeded"} });
            auto waiters = registry->IntGauge({ DatabaseLabel_, {"ydb_client", clientType},
                {"sensor", "Sessions/WaitForReturn"} });

            return TSessionPoolStatCollector(activeSessions, inPoolSessions, fakeSessions, waiters);
        }

        return TSessionPoolStatCollector();
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
                TClientRetryOperationStatCollector(MetricRegistryPtr_.Get(), Database_, clientType));
        }

        return TClientStatCollector();
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
