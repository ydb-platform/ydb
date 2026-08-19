#include "manager.h"
#include "service.h"

#include <ydb/core/kqp/query_data/kqp_predictor.h>
#include <ydb/core/tx/conveyor_composite/tracing/probes.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

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

void TDistributor::HandleMain(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    auto& record = ev->Get()->Record;
    const auto& appConfig = record.GetConfig();

    if (!appConfig.HasCompositeConveyorConfig()) {
        ReplyConfigNotification(ev);
        return;
    }

    const auto& candidateProto = appConfig.GetCompositeConveyorConfig();
    YDB_LOG_INFO("",
        {"name", ConveyorName},
        {"action", "composite_conveyor_config_received"},
        {"hasConfig", true});

    auto configConclusion = NConfig::TConfig::BuildFromProto(candidateProto);
    if (configConclusion.IsFail()) {
        YDB_LOG_ERROR("",
            {"name", ConveyorName},
            {"action", "composite_conveyor_config_rejected"},
            {"error", configConclusion.GetErrorMessage()});
        ReplyConfigNotification(ev);
        return;
    }

    auto desiredConfig = configConclusion.DetachResult();
    auto validation = Manager->ValidateConfigUpdate(desiredConfig);
    if (validation.IsFail()) {
        YDB_LOG_ERROR("",
            {"name", ConveyorName},
            {"action", "composite_conveyor_config_update_rejected"},
            {"error", validation.GetErrorMessage()});
        ReplyConfigNotification(ev);
        return;
    }

    AFL_VERIFY(!PendingConfigNotification);
    PendingConfigNotification = std::move(ev);
    if (Manager->StartConfigUpdate(desiredConfig)) {
        CompleteConfigUpdate();
    }
    Y_UNUSED(Manager->DrainTasks());
}

void TDistributor::ReplyConfigNotification(const NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
    auto response = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(ev->Get()->Record);
    Send(ev->Sender, response.Release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
}

void TDistributor::CompleteConfigUpdate() {
    AFL_VERIFY(PendingConfigNotification);
    AFL_VERIFY(!Manager->HasWorkersUpdateInProgress());

    ReplyConfigNotification(PendingConfigNotification);
    PendingConfigNotification.Reset();
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

void TDistributor::HandleMain(TEvInternal::TEvWorkerCPULimitUpdated::TPtr& ev) {
    if (Manager->OnWorkerCPULimitUpdated(*ev->Get())) {
        CompleteConfigUpdate();
    }
    Y_UNUSED(Manager->DrainTasks());
}

void TDistributor::HandleMain(TEvInternal::TEvWorkerStopped::TPtr& ev) {
    if (Manager->OnWorkerStopped(*ev->Get())) {
        CompleteConfigUpdate();
    }
    Y_UNUSED(Manager->DrainTasks());
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
    Y_UNUSED(Manager->DrainTasks());
}

void TDistributor::HandleMain(TEvExecution::TEvRegisterProcess::TPtr& ev) {
    auto& event = *ev->Get();
    LWPROBE(RegisterProcess, ConveyorName, ToString(event.GetCategory()), event.GetScopeId(), event.GetInternalProcessId());
    auto& cat = Manager->MutableCategoryVerified(event.GetCategory());
    std::shared_ptr<TProcessScope> scope = cat.UpsertScope(event.GetScopeId(), event.GetCPULimits());
    cat.RegisterProcess(event.GetInternalProcessId(), std::move(scope));
}

void TDistributor::HandleMain(TEvExecution::TEvUnregisterProcess::TPtr& ev) {
    auto& event = *ev->Get();
    LWPROBE(UnregisterProcess, ConveyorName, ToString(event.GetCategory()), event.GetInternalProcessId());
    auto* evData = ev->Get();
    Manager->MutableCategoryVerified(evData->GetCategory()).UnregisterProcess(evData->GetInternalProcessId());
}

void TDistributor::HandleMain(TEvExecution::TEvNewTask::TPtr& ev) {
    auto& event = *ev->Get();
    const TDuration d = TMonotonic::Now() - event.GetConstructInstant();
    LWPROBE(NewTask, ConveyorName, ToString(event.GetCategory()), event.GetInternalProcessId(), d);
    Counters.ReceiveTaskDuration->Add(d.MicroSeconds());
    Counters.ReceiveTaskHistogram->Collect(d.MicroSeconds());
    const auto category = ev->Get()->GetCategory();
    auto& cat = Manager->MutableCategoryVerified(category);
    auto task = ev->Get()->DetachTask();
    if (Manager->HasFreeWorkerForCategory(category) || cat.GetWaitingQueueSize() < cat.GetQueueSizeLimit()) {
        cat.RegisterTask(ev->Get()->GetInternalProcessId(), std::move(task));
        Y_UNUSED(Manager->DrainTasks());
    } else {
        cat.GetCounters()->OverlimitRate->Inc();
        YDB_LOG_ERROR("",
            {"action", "queue_overlimit"},
            {"sender", ev->Sender},
            {"category", ::ToString(category)},
            {"queue", cat.GetWaitingQueueSize()},
            {"limit", cat.GetQueueSizeLimit()});
        task->OnCannotExecute("composite conveyor overloaded for category " + ::ToString(category) + " (" +
            ::ToString(cat.GetWaitingQueueSize()) + " >= " + ::ToString(cat.GetQueueSizeLimit()) + ")");
    }
    cat.GetCounters()->WaitingQueueSize->Set(cat.GetWaitingQueueSize());
}

}   // namespace NKikimr::NConveyorComposite
