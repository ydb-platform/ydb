#include "kqp_finalize_script_actor.h"

#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::KQP_PROXY

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
    TScriptExecutionLeaseCheckActor(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TDuration startupTimeout, TIntrusivePtr<TKqpCounters> counters)
        : QueryServiceConfig(queryServiceConfig)
        , Counters(counters)
        , StartupTimeout(startupTimeout)
    {}

    void Bootstrap() {
        YDB_LOG_DEBUG("Bootstrap",
            {"LogPrefix", LogPrefix()});
        Become(&TScriptExecutionLeaseCheckActor::MainState);

        const auto& creatorId = Register(CreateScriptExecutionsTablesCreator(AppData()->FeatureFlags.GetEnableSecureScriptExecutions()));
        YDB_LOG_DEBUG("Start script executions tables",
            {"LogPrefix", LogPrefix()},
            {"creator", creatorId});
    }

    STRICT_STFUNC(MainState,
        hFunc(TEvents::TEvWakeup, Handle);
        hFunc(TEvTenantNodeEnumerator::TEvLookupResult, Handle);
        hFunc(TEvRefreshScriptExecutionLeasesResponse, Handle);
        hFunc(TEvScriptExecutionsTablesCreationFinished, Handle);
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
                YDB_LOG_DEBUG("Start lease",
                    {"LogPrefix", LogPrefix()},
                    {"checker", checkerId});
                break;
        }
    }

    void Handle(TEvTenantNodeEnumerator::TEvLookupResult::TPtr& ev) {
        WaitRefreshNodes = false;

        if (!ev->Get()->Success) {
            YDB_LOG_WARN("Failed to discover tenant nodes",
                {"LogPrefix", LogPrefix()});
            return;
        }

        const auto nodesCount = ev->Get()->AssignedNodes.size();
        RefreshLeasePeriod = std::max(nodesCount, static_cast<size_t>(1)) * CHECK_PERIOD;
        HasNodesInfo = true;

        YDB_LOG_DEBUG("Handle discover tenant nodes result, number of nodes, new",
            {"LogPrefix", LogPrefix()},
            {"nodesCount", nodesCount},
            {"RefreshLeasePeriod", RefreshLeasePeriod});
    }

    void Handle(TEvRefreshScriptExecutionLeasesResponse::TPtr& ev) {
        WaitRefreshScriptExecutions = false;
        if (!ev->Get()->Success) {
            YDB_LOG_ERROR("Refresh failed with",
                {"LogPrefix", LogPrefix()},
                {"issues", ev->Get()->Issues.ToOneLineString()});
        } else {
            YDB_LOG_DEBUG("Refresh successfully completed",
                {"LogPrefix", LogPrefix()});
        }
    }

    void Handle(TEvScriptExecutionsTablesCreationFinished::TPtr& ev) {
        if (!ev->Get()->Success) {
            YDB_LOG_ERROR("Script executions tables creation failed with",
                {"LogPrefix", LogPrefix()},
                {"issues", ev->Get()->Issues.ToOneLineString()});
            return PassAway();
        }

        YDB_LOG_DEBUG("Script executions tables creation finished, start lease checks",
            {"LogPrefix", LogPrefix()});
        RefreshNodesInfo();
        Schedule(StartupTimeout, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::ScheduleRefreshScriptExecutions)));
    }

private:
    void RefreshNodesInfo() {
        YDB_LOG_DEBUG("Do RefreshNodesInfo ), next refresh after",
            {"LogPrefix", LogPrefix()},
            {"(WaitRefreshNodes", WaitRefreshNodes},
            {"REFRESH_NODES_PERIOD", REFRESH_NODES_PERIOD});
        Schedule(REFRESH_NODES_PERIOD, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::RefreshNodesInfo)));

        if (!WaitRefreshNodes) {
            WaitRefreshNodes = true;
            Register(CreateTenantNodeEnumerationLookup(SelfId(), AppData()->TenantName));
        }
    }

    void ScheduleRefreshScriptExecutions() {
        YDB_LOG_DEBUG("Do ScheduleRefreshScriptExecutions ), next refresh after",
            {"LogPrefix", LogPrefix()},
            {"(WaitRefreshScriptExecutions", WaitRefreshScriptExecutions},
            {"RefreshLeasePeriod", RefreshLeasePeriod});
        Schedule(RefreshLeasePeriod, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::ScheduleRefreshScriptExecutions)));

        if (!HasNodesInfo) {
            YDB_LOG_DEBUG("Skip ScheduleRefreshScriptExecutions, node info is not arrived",
                {"LogPrefix", LogPrefix()});
            return;
        }

        if (!WaitRefreshScriptExecutions) {
            WaitRefreshScriptExecutions = true;

            // Start background checks at random time during CHECK_PERIOD * (node count)
            // to reduce the number of tli
            const auto leaseCheckTime = RefreshLeasePeriod * RandomNumber<double>();
            Schedule(leaseCheckTime, new TEvents::TEvWakeup(static_cast<ui64>(EWakeup::RefreshScriptExecutions)));
            YDB_LOG_DEBUG("Schedule lease check after",
                {"LogPrefix", LogPrefix()},
                {"leaseCheckTime", leaseCheckTime});
        }
    }

    TString LogPrefix() const {
        return TStringBuilder() << "[ScriptExecutions] [TScriptExecutionLeaseCheckActor] ";
    }

private:
    const NKikimrConfig::TQueryServiceConfig QueryServiceConfig;
    const TIntrusivePtr<TKqpCounters> Counters;
    const TDuration StartupTimeout;

    TDuration RefreshLeasePeriod = CHECK_PERIOD;

    bool WaitRefreshNodes = false;
    bool WaitRefreshScriptExecutions = false;
    bool HasNodesInfo = false;
};

}  // anonymous namespace

IActor* CreateScriptExecutionLeaseCheckActor(const NKikimrConfig::TQueryServiceConfig& queryServiceConfig, TDuration startupTimeout, TIntrusivePtr<TKqpCounters> counters) {
    return new TScriptExecutionLeaseCheckActor(queryServiceConfig, startupTimeout, counters);
}

}  // namespace NKikimr::NKqp
