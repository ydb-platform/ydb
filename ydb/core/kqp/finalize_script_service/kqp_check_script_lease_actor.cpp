#include "kqp_finalize_script_actor.h"

#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>

namespace NKikimr::NKqp {

namespace {

class TScriptExecutionLeaseCheckActor : public TActorBootstrapped<TScriptExecutionLeaseCheckActor> {
    static constexpr TDuration CHECK_PERIOD = TDuration::Seconds(1);
    static constexpr TDuration REFRESH_NODES_PERIOD = TDuration::Minutes(1);

    enum class EWakeup {
        RefreshNodesInfo,
        ScheduleRefreshScriptExecutions,
        RefreshScriptExecutions,
    };

public:
    TScriptExecutionLeaseCheckActor(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters)
        : QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
    {}

    void Bootstrap() {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Bootstrap");
        Become(&TScriptExecutionLeaseCheckActor::MainState);

        RefreshNodesInfo();
        ScheduleRefreshScriptExecutions();
    }

    STRICT_STFUNC(MainState,
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);
        hFunc(TEvRefreshScriptExecutionLeasesResponse, Handle);
    )

    void Handle(TEvents::TEvWakeup::TPtr& ev) {
        switch (static_cast<EWakeup>(ev->Get()->Tag)) {
            case EWakeup::RefreshNodesInfo:
                RefreshNodesInfo();
                break;
            case EWakeup::ScheduleRefreshScriptExecutions:
                ScheduleRefreshScriptExecutions();
                break;
            case EWakeup::RefreshScriptExecutions:
                const auto& checkerId = Register(CreateRefreshScriptExecutionLeasesActor(SelfId(), QueryServiceConfig, Counters));
                LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Start lease checker: " << checkerId);
                break;
        }
    }

    void Handle(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        const auto nodesCount = ev->Get()->Nodes.size();
        WaitRefreshNodes = false;
        RefreshLeasePeriod = std::max(nodesCount, static_cast<size_t>(1)) * CHECK_PERIOD;

        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Handle interconnect nodes info, number of nodes #" << nodesCount << ", new RefreshLeasePeriod: " << RefreshLeasePeriod);
    }

    void Handle(TEvRefreshScriptExecutionLeasesResponse::TPtr& ev) {
        WaitRefreshScriptExecutions = false;
        if (!ev->Get()->Success) {
            LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Refresh failed with issues: " << ev->Get()->Issues.ToOneLineString());
        } else {
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Refresh successfully completed");
        }
    }

private:
    void RefreshNodesInfo() {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Do RefreshNodesInfo (WaitRefreshNodes: " << WaitRefreshNodes << "), next refresh after " << REFRESH_NODES_PERIOD);
        Schedule(REFRESH_NODES_PERIOD, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::RefreshNodesInfo)));

        if (!WaitRefreshNodes) {
            WaitRefreshNodes = true;
            Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
        }
    }

    void ScheduleRefreshScriptExecutions() {
        LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Do ScheduleRefreshScriptExecutions (WaitRefreshScriptExecutions: " << WaitRefreshScriptExecutions << "), next refresh after " << RefreshLeasePeriod);
        Schedule(RefreshLeasePeriod, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::ScheduleRefreshScriptExecutions)));

        if (!WaitRefreshScriptExecutions) {
            WaitRefreshScriptExecutions = true;

            // Start background checks at random time during CHECK_PERIOD * (node count)
            // to reduce the number of tli
            const auto leaseCheckTime = RefreshLeasePeriod * RandomNumber<double>();
            Schedule(leaseCheckTime, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::RefreshScriptExecutions)));
            LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_PROXY, LogPrefix() << "Schedule lease check after " << leaseCheckTime);
        }
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[ScriptExecutions] [TScriptExecutionLeaseCheckActor] ";
    }

private:
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;

    TDuration RefreshLeasePeriod = CHECK_PERIOD;

    bool WaitRefreshNodes = false;
    bool WaitRefreshScriptExecutions = false;
};

}  // anonymous namespace

IActor* CreateScriptExecutionLeaseCheckActor(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TIntrusivePtr<TKqpCounters> counters) {
    return new TScriptExecutionLeaseCheckActor(queryServiceConfig, counters);
}

}  // namespace NKikimr::NKqp
