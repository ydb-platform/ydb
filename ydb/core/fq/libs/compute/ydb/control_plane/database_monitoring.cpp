#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/api/events.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/queue.h>

#define LOG_E(stream) LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseMonitoring]: " << stream)
#define LOG_W(stream) LOG_WARN_S( *NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseMonitoring]: " << stream)
#define LOG_I(stream) LOG_INFO_S( *NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseMonitoring]: " << stream)
#define LOG_D(stream) LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseMonitoring]: " << stream)
#define LOG_T(stream) LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::FQ_RUN_ACTOR, "[ydb] [ComputeDatabaseMonitoring]: " << stream)

namespace NFq {

class TComputeDatabaseMonitoringActor : public NActors::TActorBootstrapped<TComputeDatabaseMonitoringActor> {
    struct TCounters {
        ::NMonitoring::TDynamicCounterPtr Counters;
        struct TCommonMetrics {
            ::NMonitoring::TDynamicCounters::TCounterPtr Ok;
            ::NMonitoring::TDynamicCounters::TCounterPtr Error;
            ::NMonitoring::THistogramPtr LatencyMs;
        };

        TCommonMetrics CpuLoadRequest;
        ::NMonitoring::TDynamicCounters::TCounterPtr InstantLoadPercentage;
        ::NMonitoring::TDynamicCounters::TCounterPtr AverageLoadPercentage;
        ::NMonitoring::TDynamicCounters::TCounterPtr QuotedLoadPercentage;
        ::NMonitoring::TDynamicCounters::TCounterPtr AvailableLoadPercentage;
        ::NMonitoring::TDynamicCounters::TCounterPtr TargetLoadPercentage;
        ::NMonitoring::TDynamicCounters::TCounterPtr PendingQueueSize;
        ::NMonitoring::TDynamicCounters::TCounterPtr PendingQueueOverload;

        explicit TCounters(const ::NMonitoring::TDynamicCounterPtr& counters)
            : Counters(counters)
        {
            Register();
        }

    private:
        void Register() {
            ::NMonitoring::TDynamicCounterPtr component = Counters->GetSubgroup("component", "ComputeDatabaseMonitoring");
            auto subComponent = component->GetSubgroup("subcomponent", "CpuLoadRequest");
            RegisterCommonMetrics(CpuLoadRequest, subComponent);
            InstantLoadPercentage = subComponent->GetCounter("InstantLoadPercentage", false);
            AverageLoadPercentage = subComponent->GetCounter("AverageLoadPercentage", false);
            QuotedLoadPercentage = subComponent->GetCounter("QuotedLoadPercentage", false);
            AvailableLoadPercentage = subComponent->GetCounter("AvailableLoadPercentage", false);
            TargetLoadPercentage = subComponent->GetCounter("TargetLoadPercentage", false);
            PendingQueueSize = subComponent->GetCounter("PendingQueueSize", false);
            PendingQueueOverload = subComponent->GetCounter("PendingQueueOverload", true);
        }

        void RegisterCommonMetrics(TCommonMetrics& metrics, ::NMonitoring::TDynamicCounterPtr subComponent) {
            metrics.Ok = subComponent->GetCounter("Ok", true);
            metrics.Error = subComponent->GetCounter("Error", true);
            metrics.LatencyMs = subComponent->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
        }

        static ::NMonitoring::IHistogramCollectorPtr GetLatencyHistogramBuckets() {
            return ::NMonitoring::ExplicitHistogram({0, 1, 2, 5, 10, 20, 50, 100, 500, 1000, 2000, 5000, 10000, 30000, 50000, 500000});
        }
    };

public:
    using TBase = NActors::TActorBootstrapped<TComputeDatabaseMonitoringActor>;
    TComputeDatabaseMonitoringActor(const TActorId& monitoringClientActorId, NFq::NConfig::TLoadControlConfig config, const ::NMonitoring::TDynamicCounterPtr& counters)
        : MonitoringClientActorId(monitoringClientActorId)
        , Counters(counters)
        , MonitoringRequestDelay(GetDuration(config.GetMonitoringRequestDelay(), TDuration::Seconds(1)))
        , AverageLoadInterval(std::max<TDuration>(GetDuration(config.GetAverageLoadInterval(), TDuration::Seconds(10)), TDuration::Seconds(1)))
        , MaxClusterLoad(std::min<ui32>(config.GetMaxClusterLoadPercentage(), 100) / 100.0)
        , DefaultQueryLoad(config.GetDefaultQueryLoadPercentage() ? std::min<ui32>(config.GetDefaultQueryLoadPercentage(), 100) / 100.0 : 0.1)
        , PendingQueueSize(config.GetPendingQueueSize())
        , Strict(config.GetStrict())
        , CpuNumber(config.GetCpuNumber())
    {
        *Counters.AvailableLoadPercentage = 100;
        *Counters.TargetLoadPercentage = static_cast<ui64>(MaxClusterLoad * 100);
    }

    static constexpr char ActorName[] = "FQ_COMPUTE_DATABASE_MONITORING_ACTOR";

    void Bootstrap() {
        LOG_E("Monitoring Bootstrap, client " << MonitoringClientActorId.ToString());
        Become(&TComputeDatabaseMonitoringActor::StateFunc);
        SendCpuLoadRequest();
    }

    // these TEvCpuLoadRequest and TEvCpuLoadResponse *are* unrelated, just same events are used
    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCpuLoadRequest, Handle);
        hFunc(TEvYdbCompute::TEvCpuLoadResponse, Handle);
        hFunc(TEvYdbCompute::TEvCpuQuotaRequest, Handle);
        hFunc(TEvYdbCompute::TEvCpuQuotaAdjust, Handle);
        cFunc(NActors::TEvents::TEvWakeup::EventType, SendCpuLoadRequest);
    )

    void Handle(TEvYdbCompute::TEvCpuLoadRequest::TPtr& ev) {
        auto response = std::make_unique<TEvYdbCompute::TEvCpuLoadResponse>(InstantLoad, AverageLoad);
        if (!Ready) {
            response->Issues.AddIssue("CPU Load is unavailable");
        }
        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    void Handle(TEvYdbCompute::TEvCpuLoadResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();

        auto now = TInstant::Now();
        if (!response.Issues) {
            auto delta = now - LastCpuLoad;
            LastCpuLoad = now;

            if (response.CpuNumber) {
                CpuNumber = response.CpuNumber;
            }

            InstantLoad = response.InstantLoad;
            // exponential moving average
            if (!Ready || delta >= AverageLoadInterval) {
                AverageLoad = InstantLoad;
                QuotedLoad = InstantLoad;
            } else {
                auto ratio = static_cast<double>(delta.GetValue()) / AverageLoadInterval.GetValue();
                AverageLoad = (1 - ratio) * AverageLoad + ratio * InstantLoad;
                QuotedLoad = (1 - ratio) * QuotedLoad + ratio * InstantLoad;
            }
            Ready = true;
            Counters.CpuLoadRequest.Ok->Inc();
            *Counters.InstantLoadPercentage = static_cast<ui64>(InstantLoad * 100);
            *Counters.AverageLoadPercentage = static_cast<ui64>(AverageLoad * 100);
            CheckPendingQueue();
            *Counters.QuotedLoadPercentage = static_cast<ui64>(QuotedLoad * 100);
        } else {
            LOG_E("CPU Load Request FAILED: " << response.Issues.ToOneLineString());
            Counters.CpuLoadRequest.Error->Inc();
            CheckLoadIsOutdated();
        }
        Counters.CpuLoadRequest.LatencyMs->Collect((now - StartCpuLoad).MilliSeconds());

        // TODO: make load pulling reactive
        // 1. Long period (i.e. AverageLoadInterval/2) when idle (no requests)
        // 2. Active pulling when busy

        if (MonitoringRequestDelay) {
            Schedule(MonitoringRequestDelay, new NActors::TEvents::TEvWakeup());
        } else {
            SendCpuLoadRequest();
        }
    }

    void Handle(TEvYdbCompute::TEvCpuQuotaRequest::TPtr& ev) {
        auto& request = *ev.Get()->Get();

        if (request.Quota > 1.0) {
            Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(NYdb::EStatus::OVERLOADED, NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Incorrect quota value (exceeds 1.0) " << request.Quota}}), 0, ev->Cookie);
        } else {
            if (!request.Quota) {
                request.Quota = DefaultQueryLoad;
            }
            CheckLoadIsOutdated();
            if (MaxClusterLoad > 0.0 && ((!Ready && Strict) || QuotedLoad >= MaxClusterLoad)) {
                if (PendingQueue.size() >= PendingQueueSize) {
                    Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(NYdb::EStatus::OVERLOADED, NYql::TIssues{
                        NYql::TIssue{TStringBuilder{}
                        << "Cluster is overloaded, current quoted load " << static_cast<ui64>(QuotedLoad * 100)
                        << "%, average load " << static_cast<ui64>(AverageLoad * 100) << "%"
                        }}), 0, ev->Cookie);
                    Counters.PendingQueueOverload->Inc();
                } else {
                    PendingQueue.push(ev);
                    Counters.PendingQueueSize->Inc();
                }
            } else {
                QuotedLoad += request.Quota;
                *Counters.QuotedLoadPercentage = static_cast<ui64>(QuotedLoad * 100);
                Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(), 0, ev->Cookie);
            }
        }
    }

    void Handle(TEvYdbCompute::TEvCpuQuotaAdjust::TPtr& ev) {
        if (CpuNumber) {
            auto& request = *ev.Get()->Get();
            if (request.Duration && request.Duration < AverageLoadInterval / 2 && request.Quota <= 1.0) {
                auto load = (request.CpuSecondsConsumed * 1000 / request.Duration.MilliSeconds()) / CpuNumber;
                auto quota = request.Quota ? request.Quota : DefaultQueryLoad;
                if (quota > load) {
                    auto adjustment = (quota - load) / 2;
                    if (QuotedLoad > adjustment) {
                        QuotedLoad -= adjustment;
                    } else {
                        QuotedLoad = 0.0;
                    }
                    CheckPendingQueue();
                    *Counters.QuotedLoadPercentage = static_cast<ui64>(QuotedLoad * 100);
                }
            }
        }
    }

    void SendCpuLoadRequest() {
        StartCpuLoad = TInstant::Now();
        Send(MonitoringClientActorId, new TEvYdbCompute::TEvCpuLoadRequest(""));
    }

    void CheckLoadIsOutdated() {
        // TODO: support timeout to decline quota after request pending time is over, not load info
        if (TInstant::Now() - LastCpuLoad > AverageLoadInterval) {
            Ready = false;
            QuotedLoad = 0.0;
            if (Strict) {
                while (PendingQueue.size()) {
                    auto& ev = PendingQueue.front();
                    Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(NYdb::EStatus::OVERLOADED, NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Cluster load info is not available"}}), 0, ev->Cookie);
                    PendingQueue.pop();
                    Counters.PendingQueueSize->Dec();
                }
            }
        }
    }

    void CheckPendingQueue() {
        auto now = TInstant::Now();
        while (QuotedLoad < MaxClusterLoad && PendingQueue.size()) {
            auto& ev = PendingQueue.front();
            auto& request = *ev.Get()->Get();
            if (request.Deadline && now >= request.Deadline) {
                Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(NYdb::EStatus::CANCELLED, NYql::TIssues{
                    NYql::TIssue{TStringBuilder{} << "Deadline reached " << request.Deadline}}), 0, ev->Cookie);
            } else {
                QuotedLoad += request.Quota;
                Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(), 0, ev->Cookie);
            }
            PendingQueue.pop();
            Counters.PendingQueueSize->Dec();
        }
    }

private:
    TInstant StartCpuLoad;
    TInstant LastCpuLoad;
    TActorId MonitoringClientActorId;
    TCounters Counters;

    double InstantLoad = 0.0;
    double AverageLoad = 0.0;
    double QuotedLoad = 0.0;
    bool Ready = false;

    const TDuration MonitoringRequestDelay;
    const TDuration AverageLoadInterval;
    const double MaxClusterLoad;
    const double DefaultQueryLoad;
    const ui32 PendingQueueSize;
    const bool Strict;
    ui32 CpuNumber = 0;

    TQueue<TEvYdbCompute::TEvCpuQuotaRequest::TPtr> PendingQueue;
};

std::unique_ptr<NActors::IActor> CreateDatabaseMonitoringActor(const NActors::TActorId& monitoringClientActorId, NFq::NConfig::TLoadControlConfig config, const ::NMonitoring::TDynamicCounterPtr& counters) {
    return std::make_unique<TComputeDatabaseMonitoringActor>(monitoringClientActorId, config, counters);
}

}
