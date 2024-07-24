#include "kqp_node_service.h"
#include "kqp_node_state.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/runtime/kqp_read_actor.h>
#include <ydb/core/kqp/runtime/kqp_read_iterator_common.h>
#include <ydb/core/kqp/common/kqp_resolve.h>

#include <ydb/library/wilson_ids/wilson.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/string/join.h>

namespace NKikimr {
namespace NKqp {

using namespace NActors;

namespace {

#define LOG_C(stream) LOG_CRIT_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_I(stream) LOG_INFO_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_W(stream) LOG_WARN_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::KQP_NODE, stream)

// Min interval between stats send from scan/compute actor to executor
constexpr TDuration MinStatInterval = TDuration::MilliSeconds(20);
// Max interval in case of no activety
constexpr TDuration MaxStatInterval = TDuration::MilliSeconds(100);

template <class TTasksCollection>
TString TasksIdsStr(const TTasksCollection& tasks) {
    TVector<ui64> ids;
    for (auto& task: tasks) {
        ids.push_back(task.GetId());
    }
    return TStringBuilder() << "[" << JoinSeq(", ", ids) << "]";
}

class TKqpNodeService : public TActorBootstrapped<TKqpNodeService> {
    using TBase = TActorBootstrapped<TKqpNodeService>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_NODE_SERVICE;
    }

    TKqpNodeService(const NKikimrConfig::TTableServiceConfig& config,
        std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
        std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory,
        const TIntrusivePtr<TKqpCounters>& counters,
        NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
        : Config(config.GetResourceManager())
        , Counters(counters)
        , ResourceManager_(std::move(resourceManager))
        , CaFactory_(std::move(caFactory))
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FederatedQuerySetup(federatedQuerySetup)
        , State_(std::make_shared<TNodeServiceState>())
    {
        if (config.HasIteratorReadsRetrySettings()) {
            SetIteratorReadsRetrySettings(config.GetIteratorReadsRetrySettings());
        }
        if (config.HasIteratorReadQuotaSettings()) {
            SetIteratorReadsQuotaSettings(config.GetIteratorReadQuotaSettings());
        }
    }

    void Bootstrap() {
        LOG_I("Starting KQP Node service");

        // Subscribe for TableService config changes
        ui32 tableServiceConfigKind = (ui32) NKikimrConsole::TConfigItem::TableServiceConfigItem;
        Send(NConsole::MakeConfigsDispatcherID(SelfId().NodeId()),
             new NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionRequest({tableServiceConfigKind}),
             IEventHandle::FlagTrackDelivery);

        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "kqp_node", "KQP Node", false,
                TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }

        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup());
        Become(&TKqpNodeService::WorkState);
    }

private:
    STATEFN(WorkState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleWork);
            hFunc(TEvKqpNode::TEvFinishKqpTask, HandleWork); // used only for unit tests
            hFunc(TEvKqpNode::TEvCancelKqpTasksRequest, HandleWork);
            hFunc(TEvents::TEvWakeup, HandleWork);
            // misc
            hFunc(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse, HandleWork);
            hFunc(NConsole::TEvConsole::TEvConfigNotificationRequest, HandleWork);
            hFunc(TEvents::TEvUndelivered, HandleWork);
            hFunc(TEvents::TEvPoison, HandleWork);
            hFunc(NMon::TEvHttpInfo, HandleWork);
            default: {
                Y_ABORT("Unexpected event 0x%x for TKqpResourceManagerService", ev->GetTypeRewrite());
            }
        }
    }

    static constexpr double SecToUsec = 1e6;

    void HandleWork(TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
        NWilson::TSpan sendTasksSpan(TWilsonKqp::KqpNodeSendTasks, NWilson::TTraceId(ev->TraceId), "KqpNode.SendTasks", NWilson::EFlags::AUTO_END);

        NHPTimer::STime workHandlerStart = ev->SendTime;
        auto& msg = ev->Get()->Record;
        Counters->NodeServiceStartEventDelivery->Collect(NHPTimer::GetTimePassed(&workHandlerStart) * SecToUsec);

        auto requester = ev->Sender;

        ui64 txId = msg.GetTxId();

        YQL_ENSURE(msg.GetStartAllOrFail()); // todo: support partial start

        LOG_D("TxId: " << txId << ", new compute tasks request from " << requester
            << " with " << msg.GetTasks().size() << " tasks: " << TasksIdsStr(msg.GetTasks()));

        auto now = TAppData::TimeProvider->Now();
        NKqpNode::TTasksRequest request(txId, ev->Sender, now);
        auto& msgRtSettings = msg.GetRuntimeSettings();
        if (msgRtSettings.GetTimeoutMs() > 0) {
            // compute actor should not arm timer since in case of timeout it will receive TEvAbortExecution from Executer
            auto timeout = TDuration::MilliSeconds(msgRtSettings.GetTimeoutMs());
            request.Deadline = now + timeout + /* gap */ TDuration::Seconds(5);
        }

        auto& bucket = State_->GetStateBucketByTx(txId);

        if (bucket.Exists(txId, requester)) {
            LOG_E("TxId: " << txId << ", requester: " << requester << ", request already exists");
            return ReplyError(txId, request.Executer, msg, NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR);
        }

        NRm::EKqpMemoryPool memoryPool;
        if (msg.GetRuntimeSettings().GetExecType() == NYql::NDqProto::TComputeRuntimeSettings::SCAN) {
            memoryPool = NRm::EKqpMemoryPool::ScanQuery;
        } else if (msg.GetRuntimeSettings().GetExecType() == NYql::NDqProto::TComputeRuntimeSettings::DATA) {
            memoryPool = NRm::EKqpMemoryPool::DataQuery;
        } else {
            memoryPool = NRm::EKqpMemoryPool::Unspecified;
        }

        auto reply = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        reply->Record.SetTxId(txId);

        NYql::NDq::TComputeRuntimeSettings runtimeSettingsBase;
        runtimeSettingsBase.ReportStatsSettings = NYql::NDq::TReportStatsSettings{MinStatInterval, MaxStatInterval};

        TShardsScanningPolicy scanPolicy(Config.GetShardsScanningPolicy());

        NComputeActor::TComputeStagesWithScan computesByStage;

        const TString& serializedGUCSettings = ev->Get()->Record.HasSerializedGUCSettings() ?
            ev->Get()->Record.GetSerializedGUCSettings() : "";

        // start compute actors
        TMaybe<NYql::NDqProto::TRlPath> rlPath = Nothing();
        if (msgRtSettings.HasRlPath()) {
            rlPath.ConstructInPlace(msgRtSettings.GetRlPath());
        }

        TIntrusivePtr<NRm::TTxState> txInfo = MakeIntrusive<NRm::TTxState>(
            txId, TInstant::Now(), ResourceManager_->GetCounters());

        const ui32 tasksCount = msg.GetTasks().size();
        for (auto& dqTask: *msg.MutableTasks()) {
            auto result = CaFactory_->CreateKqpComputeActor({
                .ExecuterId = request.Executer,
                .TxId = txId,
                .Task = &dqTask,
                .TxInfo = txInfo,
                .RuntimeSettings = runtimeSettingsBase,
                .TraceId = NWilson::TTraceId(ev->TraceId),
                .Arena = ev->Get()->Arena,
                .SerializedGUCSettings = serializedGUCSettings,
                .NumberOfTasks = tasksCount,
                .OutputChunkMaxSize = msg.GetOutputChunkMaxSize(),
                .MemoryPool = memoryPool,
                .WithSpilling = msgRtSettings.GetUseSpilling(),
                .StatsMode = msgRtSettings.GetStatsMode(),
                .Deadline = TInstant(),
                .ShareMailbox = false,
                .RlPath = rlPath,
                .ComputesByStages = &computesByStage,
                .State = State_
            });

            if (const auto* rmResult = std::get_if<NRm::TKqpRMAllocateResult>(&result)) {
                ReplyError(txId, request.Executer, msg, rmResult->GetStatus(), rmResult->GetFailReason());
                bucket.NewRequest(std::move(request));
                TerminateTx(txId, rmResult->GetFailReason());
                return;
            }

            auto& taskCtx = request.InFlyTasks[dqTask.GetId()];
            YQL_ENSURE(taskCtx.TaskId == 0);
            taskCtx.TaskId = dqTask.GetId();
            YQL_ENSURE(taskCtx.TaskId != 0);

            TActorId* actorId = std::get_if<TActorId>(&result);
            Y_ABORT_UNLESS(actorId);
            taskCtx.ComputeActorId = *actorId;

            LOG_D("TxId: " << txId << ", executing task: " << taskCtx.TaskId << " on compute actor: " << taskCtx.ComputeActorId);

            auto* startedTask = reply->Record.AddStartedTasks();
            startedTask->SetTaskId(taskCtx.TaskId);
            ActorIdToProto(taskCtx.ComputeActorId, startedTask->MutableActorId());
        }

        for (auto&& i : computesByStage) {
            for (auto&& m : i.second.MutableMetaInfo()) {
                Register(CreateKqpScanFetcher(msg.GetSnapshot(), std::move(m.MutableActorIds()),
                    m.GetMeta(), runtimeSettingsBase, txId, scanPolicy, Counters, NWilson::TTraceId(ev->TraceId)));
            }
        }

        Send(request.Executer, reply.Release(), IEventHandle::FlagTrackDelivery, txId);

        Counters->NodeServiceProcessTime->Collect(NHPTimer::GetTimePassed(&workHandlerStart) * SecToUsec);

        bucket.NewRequest(std::move(request));
    }

    // used only for unit tests
    void HandleWork(TEvKqpNode::TEvFinishKqpTask::TPtr& ev) {
        auto& msg = *ev->Get();
        auto& bucket = State_->GetStateBucketByTx(msg.TxId);
        auto tasksToAbort = bucket.GetTasksByTxId(msg.TxId);

        if (!tasksToAbort.empty()) {
            TStringBuilder finalReason;
            finalReason << "node service cancelled the task, because of direct request "
                << ", NodeId: "<< SelfId().NodeId()
                << ", TxId: " << msg.TxId;

            LOG_E(finalReason);
            for (const auto& [taskId, computeActorId]: tasksToAbort) {
                if (msg.TaskId != taskId)
                    continue;

                auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::ABORTED, finalReason);
                Send(computeActorId, abortEv.release());
            }
        }
    }

    void HandleWork(TEvKqpNode::TEvCancelKqpTasksRequest::TPtr& ev) {
        THPTimer timer;
        ui64 txId = ev->Get()->Record.GetTxId();
        auto& reason = ev->Get()->Record.GetReason();

        LOG_W("TxId: " << txId << ", terminate transaction, reason: " << reason);
        TerminateTx(txId, reason);

        Counters->NodeServiceProcessCancelTime->Collect(timer.Passed() * SecToUsec);
    }

    void TerminateTx(ui64 txId, const TString& reason, NYql::NDqProto::StatusIds_StatusCode status = NYql::NDqProto::StatusIds::UNSPECIFIED) {
        auto& bucket = State_->GetStateBucketByTx(txId);
        auto tasksToAbort = bucket.GetTasksByTxId(txId);

        if (!tasksToAbort.empty()) {
            TStringBuilder finalReason;
            finalReason << "node service cancelled the task, because it " << reason
                << ", NodeId: "<< SelfId().NodeId()
                << ", TxId: " << txId;

            LOG_E(finalReason);
            for (const auto& [taskId, computeActorId]: tasksToAbort) {
                auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(status, reason);
                Send(computeActorId, abortEv.release());
            }
        }
    }

    void HandleWork(TEvents::TEvWakeup::TPtr& ev) {
        Schedule(TDuration::Seconds(1), ev->Release().Release());
        for (auto& bucket : State_->Buckets) {
            auto expiredRequests = bucket.ClearExpiredRequests();
            for (auto& cxt : expiredRequests) {
                TerminateTx(cxt.TxId, "reached execution deadline", NYql::NDqProto::StatusIds::TIMEOUT);
            }
        }
    }

private:
    static void HandleWork(NConsole::TEvConfigsDispatcher::TEvSetConfigSubscriptionResponse::TPtr&) {
        LOG_D("Subscribed for config changes");
    }

    void HandleWork(NConsole::TEvConsole::TEvConfigNotificationRequest::TPtr& ev) {
        auto &event = ev->Get()->Record;

        if (event.GetConfig().GetTableServiceConfig().GetResourceManager().IsInitialized()) {
            Config.Swap(event.MutableConfig()->MutableTableServiceConfig()->MutableResourceManager());

#define FORCE_VALUE(name) if (!Config.Has ## name ()) Config.Set ## name(Config.Get ## name());
            FORCE_VALUE(ComputeActorsCount)
            FORCE_VALUE(ChannelBufferSize)
            FORCE_VALUE(MkqlLightProgramMemoryLimit)
            FORCE_VALUE(MkqlHeavyProgramMemoryLimit)
            FORCE_VALUE(QueryMemoryLimit)
            FORCE_VALUE(PublishStatisticsIntervalSec);
            FORCE_VALUE(MaxTotalChannelBuffersSize);
            FORCE_VALUE(MinChannelBufferSize);
#undef FORCE_VALUE

            LOG_I("Updated table service config: " << Config.DebugString());
        }

        CaFactory_->ApplyConfig(event.GetConfig().GetTableServiceConfig().GetResourceManager());

        if (event.GetConfig().GetTableServiceConfig().HasIteratorReadsRetrySettings()) {
            SetIteratorReadsRetrySettings(event.GetConfig().GetTableServiceConfig().GetIteratorReadsRetrySettings());
        }

        if (event.GetConfig().GetTableServiceConfig().HasIteratorReadQuotaSettings()) {
            SetIteratorReadsQuotaSettings(event.GetConfig().GetTableServiceConfig().GetIteratorReadQuotaSettings());
        }

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);

    }

    void SetIteratorReadsQuotaSettings(const NKikimrConfig::TTableServiceConfig::TIteratorReadQuotaSettings& settings) {
        SetDefaultIteratorQuotaSettings(settings.GetMaxRows(), settings.GetMaxBytes());
    }

    void SetIteratorReadsRetrySettings(const NKikimrConfig::TTableServiceConfig::TIteratorReadsRetrySettings& settings) {
        auto ptr = MakeIntrusive<NKikimr::NKqp::TIteratorReadBackoffSettings>();
        ptr->StartRetryDelay = TDuration::MilliSeconds(settings.GetStartDelayMs());
        ptr->MaxShardAttempts = settings.GetMaxShardRetries();
        ptr->MaxShardResolves = settings.GetMaxShardResolves();
        ptr->UnsertaintyRatio = settings.GetUnsertaintyRatio();
        ptr->Multiplier = settings.GetMultiplier();
        if (settings.GetMaxTotalRetries()) {
            ptr->MaxTotalRetries = settings.GetMaxTotalRetries();
        }
        if (settings.GetIteratorResponseTimeoutMs()) {
            ptr->ReadResponseTimeout = TDuration::MilliSeconds(settings.GetIteratorResponseTimeoutMs());
        }
        ptr->MaxRetryDelay = TDuration::MilliSeconds(settings.GetMaxDelayMs());
        SetReadIteratorBackoffSettings(ptr);
    }

    void HandleWork(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case TEvKqpNode::TEvStartKqpTasksResponse::EventType: {
                ui64 txId = ev->Cookie;
                TStringBuilder reason;
                reason << "executer lost: " << (int) ev->Get()->Reason;
                TerminateTx(txId, reason, NYql::NDqProto::StatusIds::ABORTED);
                break;
            }

            case NConsole::TEvConfigsDispatcher::EvSetConfigSubscriptionRequest:
                LOG_C("Failed to deliver subscription request to config dispatcher");
                break;

            case NConsole::TEvConsole::EvConfigNotificationResponse:
                LOG_E("Failed to deliver config notification response");
                break;

            default:
                LOG_E("Undelivered event with unexpected source type: " << ev->Get()->SourceType);
                break;
        }
    }

    void HandleWork(TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleWork(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        HTML(str) {
            PRE() {
                str << "Current config:" << Endl;
                str << Config.DebugString() << Endl;
                str << Endl;

                str << Endl << "Transactions:" << Endl;
                for (auto& bucket : State_->Buckets) {
                    bucket.GetInfo(str);
                }
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    void ReplyError(ui64 txId, TActorId executer, const NKikimrKqp::TEvStartKqpTasksRequest& request,
        NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason reason, const TString& message = "")
    {
        auto ev = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        ev->Record.SetTxId(txId);
        for (auto& task : request.GetTasks()) {
            auto* resp = ev->Record.AddNotStartedTasks();
            resp->SetTaskId(task.GetId());
            resp->SetReason(reason);
            resp->SetMessage(message);
        }
        Send(executer, ev.Release());
    }

private:
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;
    TIntrusivePtr<TKqpCounters> Counters;
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;

    std::shared_ptr<TNodeServiceState> State_;
};


} // anonymous namespace

IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    std::shared_ptr<NRm::IKqpResourceManager> resourceManager,
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> caFactory,
    TIntrusivePtr<TKqpCounters> counters, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
{
    return new TKqpNodeService(tableServiceConfig, std::move(resourceManager), std::move(caFactory),
        counters, std::move(asyncIoFactory), federatedQuerySetup);
}

} // namespace NKqp
} // namespace NKikimr
