#include <ydb/core/fq/libs/compute/ydb/events/events.h>
#include <ydb/core/fq/libs/control_plane_storage/util.h>

#include <ydb/core/kqp/workload_service/common/cpu_quota_manager.h>

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
        ::NMonitoring::TDynamicCounterPtr SubComponent;

        ::NMonitoring::THistogramPtr CpuLoadRequestLatencyMs;
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
            SubComponent = component->GetSubgroup("subcomponent", "CpuLoadRequest");
            CpuLoadRequestLatencyMs = SubComponent->GetHistogram("LatencyMs", GetLatencyHistogramBuckets());
            TargetLoadPercentage = SubComponent->GetCounter("TargetLoadPercentage", false);
            PendingQueueSize = SubComponent->GetCounter("PendingQueueSize", false);
            PendingQueueOverload = SubComponent->GetCounter("PendingQueueOverload", true);
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
        , MaxClusterLoad(std::min<ui32>(config.GetMaxClusterLoadPercentage(), 100) / 100.0)
        , PendingQueueSize(config.GetPendingQueueSize())
        , Strict(config.GetStrict())
        , CpuQuotaManager(
            GetDuration(config.GetMonitoringRequestDelay(), TDuration::Seconds(1)),
            std::max<TDuration>(GetDuration(config.GetAverageLoadInterval(), TDuration::Seconds(10)), TDuration::Seconds(1)),
            TDuration::Zero(),
            config.GetDefaultQueryLoadPercentage() ? std::min<ui32>(config.GetDefaultQueryLoadPercentage(), 100) / 100.0 : 0.1,
            config.GetStrict(),
            config.GetCpuNumber(),
            Counters.SubComponent
        )
    {
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
        auto response = std::make_unique<TEvYdbCompute::TEvCpuLoadResponse>(CpuQuotaManager.GetInstantLoad(), CpuQuotaManager.GetAverageLoad());
        if (!CpuQuotaManager.CheckLoadIsOutdated()) {
            response->Issues.AddIssue("CPU Load is unavailable");
        }
        Send(ev->Sender, response.release(), 0, ev->Cookie);
    }

    void Handle(TEvYdbCompute::TEvCpuLoadResponse::TPtr& ev) {
        const auto& response = *ev.Get()->Get();
        if (response.Issues) {
            LOG_E("CPU Load Request FAILED: " << response.Issues.ToOneLineString());
        }
        Counters.CpuLoadRequestLatencyMs->Collect((TInstant::Now() - StartCpuLoad).MilliSeconds());

        CpuQuotaManager.UpdateCpuLoad(response.InstantLoad, response.CpuNumber, !response.Issues);
        CheckPendingQueue();

        // TODO: make load pulling reactive
        // 1. Long period (i.e. AverageLoadInterval/2) when idle (no requests)
        // 2. Active pulling when busy

        if (auto delay = CpuQuotaManager.GetMonitoringRequestDelay()) {
            Schedule(delay, new NActors::TEvents::TEvWakeup());
        } else {
            SendCpuLoadRequest();
        }
    }

    void Handle(TEvYdbCompute::TEvCpuQuotaRequest::TPtr& ev) {
        auto& request = *ev.Get()->Get();

        if (request.Quota > 1.0) {
            Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(-1, NYdb::EStatus::OVERLOADED, NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Incorrect quota value (exceeds 1.0) " << request.Quota}}), 0, ev->Cookie);
        } else {
            auto response = CpuQuotaManager.RequestCpuQuota(request.Quota, MaxClusterLoad);
            CheckPendingQueue();
            if (response.Status == NYdb::EStatus::OVERLOADED && PendingQueue.size() < PendingQueueSize) {
                PendingQueue.push(ev);
                Counters.PendingQueueSize->Inc();
            } else {
                if (response.Status == NYdb::EStatus::OVERLOADED) {
                    Counters.PendingQueueOverload->Inc();
                }
                Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(response.CurrentLoad, response.Status, response.Issues), 0, ev->Cookie);
            }
        }
    }

    void Handle(TEvYdbCompute::TEvCpuQuotaAdjust::TPtr& ev) {
        auto& request = *ev.Get()->Get();
        CpuQuotaManager.AdjustCpuQuota(request.Quota, request.Duration, request.CpuSecondsConsumed);
        CheckPendingQueue();
    }

    void SendCpuLoadRequest() {
        StartCpuLoad = TInstant::Now();
        Send(MonitoringClientActorId, new TEvYdbCompute::TEvCpuLoadRequest(""));
    }

    void CheckLoadIsOutdated() {
        // TODO: support timeout to decline quota after request pending time is over, not load info
        if (Strict && !CpuQuotaManager.CheckLoadIsOutdated()) {
            while (PendingQueue.size()) {
                auto& ev = PendingQueue.front();
                Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(-1, NYdb::EStatus::OVERLOADED, NYql::TIssues{NYql::TIssue{TStringBuilder{} << "Cluster load info is not available"}}), 0, ev->Cookie);
                PendingQueue.pop();
                Counters.PendingQueueSize->Dec();
            }
        }
    }

    void CheckPendingQueue() {
        CheckLoadIsOutdated();

        auto now = TInstant::Now();
        while (PendingQueue.size()) {
            auto& ev = PendingQueue.front();
            auto& request = *ev.Get()->Get();
            if (request.Deadline && now >= request.Deadline) {
                Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(-1, NYdb::EStatus::CANCELLED, NYql::TIssues{
                    NYql::TIssue{TStringBuilder{} << "Deadline reached " << request.Deadline}}), 0, ev->Cookie);
            } else {
                auto response = CpuQuotaManager.RequestCpuQuota(request.Quota, MaxClusterLoad);
                if (response.Status == NYdb::EStatus::OVERLOADED) {
                    break;
                }

                Send(ev->Sender, new TEvYdbCompute::TEvCpuQuotaResponse(response.CurrentLoad, response.Status, response.Issues), 0, ev->Cookie);
            }

            PendingQueue.pop();
            Counters.PendingQueueSize->Dec();
        }
    }

private:
    TActorId MonitoringClientActorId;
    TCounters Counters;
    const double MaxClusterLoad;
    const ui32 PendingQueueSize;
    const bool Strict;

    NKikimr::NKqp::NWorkload::TCpuQuotaManager CpuQuotaManager;
    TQueue<TEvYdbCompute::TEvCpuQuotaRequest::TPtr> PendingQueue;

    TInstant StartCpuLoad;
};

std::unique_ptr<NActors::IActor> CreateDatabaseMonitoringActor(const NActors::TActorId& monitoringClientActorId, NFq::NConfig::TLoadControlConfig config, const ::NMonitoring::TDynamicCounterPtr& counters) {
    return std::make_unique<TComputeDatabaseMonitoringActor>(monitoringClientActorId, config, counters);
}

}
