#include "manager.h"
#include "service.h"

#include <ydb/core/config/validation/validators.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_schedulable_work_factory.h>
#include <ydb/core/kqp/runtime/scheduler/tree/dynamic.h>
#include <ydb/core/resource_pools/resource_pool_settings.h>
#include <ydb/core/tx/conveyor_composite/tracing/probes.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/string/join.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_CONVEYOR

namespace NKikimr::NConveyorComposite {

LWTRACE_USING(YDB_CONVEYOR_COMPOSITE_PROVIDER);

TDistributor::TDistributor(const NConfig::TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals)
    : Config(config)
    , ConveyorName("COMPOSITE_CONVEYOR")
    , Counters(ConveyorName, conveyorSignals) {
}

void TDistributor::Bootstrap() {
    NLwTraceMonPage::ProbeRegistry().AddProbesList(LWTRACE_GET_PROBES(YDB_CONVEYOR_COMPOSITE_PROVIDER));
    Manager = std::make_unique<TTasksManager>(ConveyorName, Config, SelfId(), Counters);
    YDB_LOG_NOTICE("",
        {"name", ConveyorName},
        {"action", "conveyor_registered"},
        {"config", Config.DebugString()},
        {"actorId", SelfId()},
        {"manager", Manager->DebugString()});
    Become(&TDistributor::StateMain);
    SubscribeToCompositeConveyorConfig();
}

void TDistributor::SubscribeToCompositeConveyorConfig() {
    Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
        new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest(
            (ui32)NKikimrConsole::TConfigItem::CompositeConveyorConfigItem),
        NActors::IEventHandle::FlagTrackDelivery);
}

void TDistributor::ScheduleConfigSubscriptionRetry() {
    Schedule(TDuration::Seconds(1), new TEvInternal::TEvRetryConfigSubscription());
}

void TDistributor::HandleMain(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr& /*ev*/) {
    YDB_LOG_DEBUG("",
        {"name", ConveyorName},
        {"action", "subscribed_for_composite_conveyor_config"});
}

TConclusion<NConfig::TConfig> TDistributor::ParseAndValidateConfig(const NKikimrConfig::TCompositeConveyorConfig& config) const {
    std::vector<TString> validationErrors;
    if (NKikimr::NConfig::ValidateCompositeConveyorConfig(config, validationErrors) == NKikimr::NConfig::EValidationResult::Error) {
        return TConclusionStatus::Fail(JoinSeq("; ", validationErrors));
    }
    if (config.GetEnabled() != Config.IsEnabled()) {
        return TConclusionStatus::Fail("runtime Enabled update is not supported: requested=" + ::ToString(config.GetEnabled())
            + ", effective=" + ::ToString(Config.IsEnabled()));
    }
    return NConfig::TConfig::BuildFromProto(config);
}

void TDistributor::HandleMain(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    // Acknowledge receipt; applying the config may wait for in-flight tasks.
    ReplyConfigNotification(ev);

    const auto& record = ev->Get()->Record;
    const auto& appConfig = record.GetConfig();

    YDB_LOG_INFO("",
        {"name", ConveyorName},
        {"action", "composite_conveyor_config_received"});

    if (!appConfig.HasCompositeConveyorConfig()) {
        Counters.BadConfigNotifications->Inc();
        YDB_LOG_ERROR("",
            {"actorId", SelfId()},
            {"action", "composite_conveyor_config_rejected"},
            {"error", "config deletion not supported in runtime updates"});
        return;
    }

    auto parsedConfig = ParseAndValidateConfig(appConfig.GetCompositeConveyorConfig());
    if (parsedConfig.IsFail()) {
        Counters.BadConfigNotifications->Inc();
        YDB_LOG_ERROR("",
            {"actorId", SelfId()},
            {"action", "composite_conveyor_config_rejected"},
            {"error", parsedConfig.GetErrorMessage()});
        return;
    }
    Config = parsedConfig.DetachResult();
    IsUpdateInProcess = true;
    TryApplyUpdate();
    Y_UNUSED(Manager->DrainTasks());
}

void TDistributor::ReplyConfigNotification(const NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    auto response = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(ev->Get()->Record);
    Send(ev->Sender, response.Release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
}

void TDistributor::TryApplyUpdate() {
    if (!IsUpdateInProcess) {
        return;
    }

    Manager->PrepareConfigUpdate(Config);
    if (!Manager->IsReadyForUpdate()) {
        return;
    }

    Manager->ApplyConfigUpdate(Config, SelfId(), Counters);
    IsUpdateInProcess = false;
}

void TDistributor::HandleMain(NActors::TEvents::TEvUndelivered::TPtr& ev) {
    switch (ev->Get()->SourceType) {
        case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
            YDB_LOG_WARN("",
                {"name", ConveyorName},
                {"action", "composite_conveyor_config_subscription_undelivered"});
            ScheduleConfigSubscriptionRetry();
            break;
        case NConsole::TEvConsole::EvConfigNotificationResponse:
            YDB_LOG_WARN("",
                {"name", ConveyorName},
                {"action", "composite_conveyor_config_response_undelivered"});
            ScheduleConfigSubscriptionRetry();
            break;
        default:
            YDB_LOG_WARN("",
                {"name", ConveyorName},
                {"action", "unexpected_undelivered_event"},
                {"sourceType", ev->Get()->SourceType});
            break;
    }
}

void TDistributor::HandleMain(TEvInternal::TEvRetryConfigSubscription::TPtr& /*ev*/) {
    YDB_LOG_WARN("",
        {"name", ConveyorName},
        {"action", "retry_composite_conveyor_config_subscription"});
    SubscribeToCompositeConveyorConfig();
}

void TDistributor::HandleMain(TEvInternal::TEvTaskProcessedResult::TPtr& evExt) {
    auto& ev = *evExt->Get();
    const TDuration backSendDuration = (TMonotonic::Now() - ev.GetConstructInstant());

    if (LWPROBE_ENABLED(TaskProcessedResult)) {
        for (const auto& result : ev.GetResults()) {
            LWPROBE(TaskProcessedResult, ConveyorName, ToString(result.GetCategory()), result.GetScope()->GetScopeId(), result.GetProcessId(), backSendDuration);
        }
    }

    TWorkersPool& workersPool = Manager->MutableWorkersPool(ev.GetWorkersPoolId());
    workersPool.GetCounters()->PackExecuteHistogram->Collect(
        (ev.GetResults().back().GetFinish() - ev.GetResults().front().GetStart()).MicroSeconds());
    workersPool.GetCounters()->PackSizeHistogram->Collect(ev.GetResults().size());
    workersPool.GetCounters()->SendBackHistogram->Collect(backSendDuration.MicroSeconds());
    workersPool.GetCounters()->SendFwdHistogram->Collect(ev.GetForwardSendDuration().MicroSeconds());

    workersPool.GetCounters()->SendBackDuration->Add(backSendDuration.MicroSeconds());
    workersPool.GetCounters()->SendFwdDuration->Add(ev.GetForwardSendDuration().MicroSeconds());

    workersPool.AddDeliveryDuration(ev.GetForwardSendDuration() + backSendDuration);
    workersPool.PutTaskResults(ev.DetachResults(), ev.GetWorkersPoolId(), ev.GetWorkerIdx());
    workersPool.ReleaseWorker(ev.GetWorkerIdx());
    TryApplyUpdate();
    Y_UNUSED(Manager->DrainTasks());
}

void TDistributor::HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev) {
    auto& event = *ev->Get();
    LWPROBE(RegisterProcess, ConveyorName, ToString(event.GetCategory()), event.GetScopeId(), event.GetInternalProcessId());
    auto& cat = Manager->MutableCategoryVerified(event.GetCategory());
    std::shared_ptr<TProcessScope> scope = cat.UpsertScope(event.GetScopeId(), event.GetCPULimits());
    const auto identity = Manager->RegisterProcess(
        event.GetCategory(), event.GetInternalProcessId(), std::move(scope), event.GetWorkloadManagerQueryIdentity());
    if (!identity) {
        return;
    }

    const auto schedulerServiceId = NKqp::MakeKqpSchedulerServiceId(SelfId().NodeId());
    Send(schedulerServiceId, new NKqp::NScheduler::TEvAddDatabase(identity->GetDatabaseId()));
    Send(schedulerServiceId, new NKqp::NScheduler::TEvAddPool(identity->GetDatabaseId(), identity->GetPoolId()));

    auto addQuery = MakeHolder<NKqp::NScheduler::TEvAddQuery>();
    addQuery->DatabaseId = identity->GetDatabaseId();
    addQuery->PoolId = identity->GetPoolId();
    addQuery->QueryId = identity->GetQueryId();
    Send(schedulerServiceId, addQuery.Release(), 0, identity->GetQueryId());
}

void TDistributor::HandleMain(NKqp::NScheduler::TEvQueryResponse::TPtr& ev) {
    const auto& query = ev->Get()->Query;
    if (!query) {
        return;
    }

    const auto& fullPoolId = query->GetFullPoolId();
    TWorkloadManagerQueryIdentity identity(fullPoolId.DatabaseId, fullPoolId.PoolId, ev->Cookie);
    auto context = std::make_shared<NKqp::NScheduler::TSchedulableWorkFactory>(
        query, fullPoolId.PoolId != NResourcePool::DEFAULT_POOL_ID);
    Manager->SetWorkloadManagerQueryContext(identity, std::move(context));
}

void TDistributor::HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev) {
    auto& event = *ev->Get();
    LWPROBE(UnregisterProcess, ConveyorName, ToString(event.GetCategory()), event.GetInternalProcessId());
    const auto identity = Manager->UnregisterProcess(event.GetCategory(), event.GetInternalProcessId());
    if (!identity) {
        return;
    }

    auto removeQuery = MakeHolder<NKqp::NScheduler::TEvRemoveQuery>();
    removeQuery->QueryId = identity->GetQueryId();
    Send(NKqp::MakeKqpSchedulerServiceId(SelfId().NodeId()), removeQuery.Release());
}

void TDistributor::HandleMain(TEvExecution::TEvNewTask::TPtr& ev) {
    auto& event = *ev->Get();
    const TDuration d = TMonotonic::Now() - event.GetConstructInstant();
    LWPROBE(NewTask, ConveyorName, ToString(event.GetCategory()), event.GetInternalProcessId(), d);
    Counters.ReceiveTaskDuration->Add(d.MicroSeconds());
    Counters.ReceiveTaskHistogram->Collect(d.MicroSeconds());
    auto& cat = Manager->MutableCategoryVerified(ev->Get()->GetCategory());
    cat.RegisterTask(ev->Get()->GetInternalProcessId(), ev->Get()->DetachTask());
    Y_UNUSED(Manager->DrainTasks());
    cat.GetCounters()->WaitingQueueSize->Set(cat.GetWaitingQueueSize());
}

}   // namespace NKikimr::NConveyorComposite
