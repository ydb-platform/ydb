#include "kqp_node_service.h"
#include "kqp_node_state.h"

#include <ydb/core/actorlib_impl/long_timer.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/mon/mon.h>

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/compute_actor/kqp_compute_actor.h>
#include <ydb/core/kqp/rm_service/kqp_resource_estimation.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/core/kqp/common/kqp_resolve.h>

#include <ydb/core/base/wilson.h>

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <library/cpp/actors/wilson/wilson_span.h>

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

constexpr ui64 BucketsCount = 64;

class TKqpNodeService : public TActorBootstrapped<TKqpNodeService> {
    using TBase = TActorBootstrapped<TKqpNodeService>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_NODE_SERVICE;
    }

    static void FinishKqpTask(ui64 txId, ui64 taskId, bool success, const NYql::TIssues& issues,
        NKqpNode::TState& bucket) {
        LOG_D("TxId: " << txId << ", finish compute task: " << taskId << ", success: " << success
                << ", message: " << issues.ToOneLineString());

        auto ctx = bucket.RemoveTask(txId, taskId, success);

        if (!ctx) {
            LOG_E("TxId: " << txId << ", task: " << taskId << " unknown task");
            return;
        }

        if (ctx->ComputeActorsNumber == 0) {
            LOG_D("TxId: " << txId << ", requester: " << ctx->Requester << " completed");
            GetKqpResourceManager()->FreeResources(txId);
        } else {
            LOG_D("TxId: " << txId << ", finish compute task: " << taskId
                    << (success ? "" : " (cancelled)")
                    << ", remains " << ctx->ComputeActorsNumber << " compute actors and "
                    << ctx->TotalMemory << " bytes in the current request");
            GetKqpResourceManager()->FreeResources(txId, taskId);
        }

        if (ctx->FinixTx) {
            LOG_D("TxId: " << txId << ", requester: " << ctx->Requester << " completed");
        }
    }

    TKqpNodeService(const NKikimrConfig::TTableServiceConfig& config, const TIntrusivePtr<TKqpCounters>& counters,
        IKqpNodeComputeActorFactory* caFactory, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory)
        : Config(config.GetResourceManager())
        , Counters(counters)
        , CaFactory(caFactory)
        , AsyncIoFactory(std::move(asyncIoFactory))
    {}

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
                Y_FAIL("Unexpected event 0x%x for TKqpResourceManagerService", ev->GetTypeRewrite());
            }
        }
    }

    class TMetaScan {
    private:
        YDB_ACCESSOR_DEF(std::vector<NActors::TActorId>, ActorIds);
        YDB_ACCESSOR_DEF(NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta, Meta);
    public:
        explicit TMetaScan(const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta)
            : Meta(meta)
        {

        }
    };

    class TComputeStageInfo {
    private:
        YDB_ACCESSOR_DEF(std::deque<TMetaScan>, MetaInfo);
        std::map<ui32, TMetaScan*> MetaWithIds;
    public:
        TComputeStageInfo() = default;

        bool GetMetaById(const ui32 metaId, NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& result) const {
            auto it = MetaWithIds.find(metaId);
            if (it == MetaWithIds.end()) {
                return false;
            }
            result = it->second->GetMeta();
            return true;
        }

        TMetaScan& MergeMetaReads(const NYql::NDqProto::TDqTask& task, const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta, const bool forceOneToMany) {
            YQL_ENSURE(meta.ReadsSize(), "unexpected merge with no reads");
            if (forceOneToMany || !task.HasMetaId()) {
                MetaInfo.emplace_back(TMetaScan(meta));
                return MetaInfo.back();
            } else {
                auto it = MetaWithIds.find(task.GetMetaId());
                if (it == MetaWithIds.end()) {
                    MetaInfo.emplace_back(TMetaScan(meta));
                    return *MetaWithIds.emplace(task.GetMetaId(), &MetaInfo.back()).first->second;
                } else {
                    return *it->second;
                }
            }
        }
    };

    class TComputeStagesWithScan {
    private:
        std::map<ui32, TComputeStageInfo> Stages;
    public:
        std::map<ui32, TComputeStageInfo>::iterator begin() {
            return Stages.begin();
        }

        std::map<ui32, TComputeStageInfo>::iterator end() {
            return Stages.end();
        }

        bool GetMetaById(const NYql::NDqProto::TDqTask& dqTask, NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& result) const {
            if (!dqTask.HasMetaId()) {
                return false;
            }
            auto it = Stages.find(dqTask.GetStageId());
            if (it == Stages.end()) {
                return false;
            } else {
                return it->second.GetMetaById(dqTask.GetMetaId(), result);
            }
        }

        TMetaScan& UpsertTaskWithScan(const NYql::NDqProto::TDqTask& dqTask, const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta, const bool forceOneToMany) {
            auto it = Stages.find(dqTask.GetStageId());
            if (it == Stages.end()) {
                it = Stages.emplace(dqTask.GetStageId(), TComputeStageInfo()).first;
            }
            return it->second.MergeMetaReads(dqTask, meta, forceOneToMany);
        }
    };

    void HandleWork(TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
        NWilson::TSpan sendTasksSpan(TWilsonKqp::KqpNodeSendTasks, NWilson::TTraceId(ev->TraceId), "KqpNode.SendTasks", NWilson::EFlags::AUTO_END);

        auto& msg = ev->Get()->Record;
        auto requester = ev->Sender;

        ui64 txId = msg.GetTxId();
        bool isScan = msg.HasSnapshot();

        YQL_ENSURE(msg.GetStartAllOrFail()); // todo: support partial start

        LOG_D("TxId: " << txId << ", new " << (isScan ? "scan " : "") << "compute tasks request from " << requester
            << " with " << msg.GetTasks().size() << " tasks: " << TasksIdsStr(msg.GetTasks()));

        NKqpNode::TTasksRequest request;
        request.Executer = ActorIdFromProto(msg.GetExecuterActorId());

        auto& bucket = GetStateBucketByTx(txId);

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

        ui32 requestChannels = 0;
        for (auto& dqTask : *msg.MutableTasks()) {
            auto estimation = EstimateTaskResources(dqTask, Config, msg.GetTasks().size());
            LOG_D("Resource estimation complete"
                << ", TxId: " << txId << ", task id: " << dqTask.GetId() << ", node id: " << SelfId().NodeId()
                << ", estimated resources: " << estimation.ToString());

            NKqpNode::TTaskContext& taskCtx = request.InFlyTasks[dqTask.GetId()];
            YQL_ENSURE(taskCtx.TaskId == 0);
            taskCtx.TaskId = dqTask.GetId();
            taskCtx.Memory = estimation.TotalMemoryLimit;
            taskCtx.Channels = estimation.ChannelBuffersCount;
            taskCtx.ChannelSize = estimation.ChannelBufferMemoryLimit;

            LOG_D("TxId: " << txId << ", task: " << taskCtx.TaskId << ", requested memory: " << taskCtx.Memory);

            requestChannels += estimation.ChannelBuffersCount;
            request.TotalMemory += taskCtx.Memory;
        }

        LOG_D("TxId: " << txId << ", channels: " << requestChannels
            << ", computeActors: " << msg.GetTasks().size() << ", memory: " << request.TotalMemory);

        auto txMemory = bucket.GetTxMemory(txId, memoryPool) + request.TotalMemory;
        if (txMemory > Config.GetQueryMemoryLimit()) {
            LOG_N("TxId: " << txId << ", requested too many memory: " << request.TotalMemory
                << "(" << txMemory << " for this Tx), limit: " << Config.GetQueryMemoryLimit());

            Counters->RmNotEnoughMemory->Inc();

            return ReplyError(txId, request.Executer, msg, NKikimrKqp::TEvStartKqpTasksResponse::QUERY_MEMORY_LIMIT_EXCEEDED,
                TStringBuilder() << "Required: " << txMemory << ", limit: " << Config.GetQueryMemoryLimit());
        }

        TVector<ui64> allocatedTasks;
        allocatedTasks.reserve(msg.GetTasks().size());
        for (auto& task : request.InFlyTasks) {
            NRm::TKqpResourcesRequest resourcesRequest;
            resourcesRequest.ExecutionUnits = 1;
            resourcesRequest.MemoryPool = memoryPool;

            // !!!!!!!!!!!!!!!!!!!!!
            // we have to allocate memory instead of reserve only. currently, this memory will not be used for request processing.
            resourcesRequest.Memory = Min<double>(task.second.Memory, 1 << 19) /* 512kb limit for check that memory exists for processing with minimal requirements */;

            NRm::TKqpNotEnoughResources resourcesResponse;
            if (!ResourceManager()->AllocateResources(txId, task.first, resourcesRequest, &resourcesResponse)) {
                NKikimrKqp::TEvStartKqpTasksResponse::ENotStartedTaskReason failReason = NKikimrKqp::TEvStartKqpTasksResponse::INTERNAL_ERROR;
                TStringBuilder error;

                if (resourcesResponse.ExecutionUnits()) {
                    error << "TxId: " << txId << ", NodeId: " << SelfId().NodeId() << ", not enough compute actors, requested " << msg.GetTasks().size();
                    LOG_N(error);

                    failReason = NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_EXECUTION_UNITS;
                }

                if (resourcesResponse.ScanQueryMemory()) {
                    error << "TxId: " << txId << ", NodeId: " << SelfId().NodeId() << ", not enough memory, requested " << task.second.Memory;
                    LOG_N(error);

                    failReason = NKikimrKqp::TEvStartKqpTasksResponse::NOT_ENOUGH_MEMORY;
                }

                if (resourcesResponse.QueryMemoryLimit()) {
                    error << "TxId: " << txId << ", NodeId: " << SelfId().NodeId() << ", memory limit exceeded, requested " << task.second.Memory;
                    LOG_N(error);

                    failReason = NKikimrKqp::TEvStartKqpTasksResponse::QUERY_MEMORY_LIMIT_EXCEEDED;
                }

                for (ui64 taskId : allocatedTasks) {
                    ResourceManager()->FreeResources(txId, taskId);
                }

                ReplyError(txId, request.Executer, msg, failReason, error);
                return;
            }

            allocatedTasks.push_back(task.first);
        }

        auto reply = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        reply->Record.SetTxId(txId);

        NYql::NDq::TComputeMemoryLimits memoryLimits;
        memoryLimits.ChannelBufferSize = 0;
        memoryLimits.MkqlLightProgramMemoryLimit = Config.GetMkqlLightProgramMemoryLimit();
        memoryLimits.MkqlHeavyProgramMemoryLimit = Config.GetMkqlHeavyProgramMemoryLimit();
        if (Config.GetEnableInstantMkqlMemoryAlloc()) {
            memoryLimits.AllocateMemoryFn = [rm = ResourceManager(), memoryPool](const auto& txId, ui64 taskId, ui64 memory) {
                NRm::TKqpResourcesRequest resources;
                resources.MemoryPool = memoryPool;
                resources.Memory = memory;

                if (rm->AllocateResources(std::get<ui64>(txId), taskId, resources)) {
                    return true;
                }

                LOG_W("Can not allocate memory. TxId: " << txId << ", taskId: " << taskId << ", memory: +" << memory);
                return false;
            };
        }

        NYql::NDq::TComputeRuntimeSettings runtimeSettingsBase;
        auto& msgRtSettings = msg.GetRuntimeSettings();
        if (msgRtSettings.GetTimeoutMs() > 0) {
            // compute actor should not arm timer since in case of timeout it will receive TEvAbortExecution from Executer
            auto timeout = TDuration::MilliSeconds(msgRtSettings.GetTimeoutMs());
            request.Deadline = TAppData::TimeProvider->Now() + timeout + /* gap */ TDuration::Seconds(5);
            bucket.InsertExpiringRequest(request.Deadline, txId, requester);
        }

        runtimeSettingsBase.ExtraMemoryAllocationPool = memoryPool;
        runtimeSettingsBase.FailOnUndelivery = msgRtSettings.GetExecType() != NYql::NDqProto::TComputeRuntimeSettings::SCAN;

        runtimeSettingsBase.StatsMode = msgRtSettings.GetStatsMode();
        runtimeSettingsBase.UseSpilling = msgRtSettings.GetUseSpilling();

        if (msgRtSettings.HasRlPath()) {
            runtimeSettingsBase.RlPath = msgRtSettings.GetRlPath();
        }

        runtimeSettingsBase.ReportStatsSettings = NYql::NDq::TReportStatsSettings{MinStatInterval, MaxStatInterval};

        TShardsScanningPolicy scanPolicy(Config.GetShardsScanningPolicy());

        TComputeStagesWithScan computesByStage;

        // start compute actors
        for (int i = 0; i < msg.GetTasks().size(); ++i) {
            auto& dqTask = *msg.MutableTasks(i);
            auto& taskCtx = request.InFlyTasks[dqTask.GetId()];
            YQL_ENSURE(taskCtx.TaskId != 0);

            memoryLimits.ChannelBufferSize = taskCtx.ChannelSize;
            Y_VERIFY_DEBUG(memoryLimits.ChannelBufferSize >= Config.GetMinChannelBufferSize(),
                "actual size: %ld, min: %ld", memoryLimits.ChannelBufferSize, Config.GetMinChannelBufferSize());

            auto runtimeSettings = runtimeSettingsBase;
            runtimeSettings.TerminateHandler = [txId, taskId = dqTask.GetId(), &bucket]
                (bool success, const NYql::TIssues& issues) {
                    FinishKqpTask(txId, taskId, success, issues, bucket);
                };

            NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta meta;
            const auto tableKindExtract = [](const NKikimrTxDataShard::TKqpTransaction::TScanTaskMeta& meta) {
                ETableKind result = (ETableKind)meta.GetTable().GetTableKind();
                if (result == ETableKind::Unknown) {
                    // For backward compatibility
                    result = meta.GetTable().GetSysViewInfo().empty() ? ETableKind::Datashard : ETableKind::SysView;
                }
                return result;
            };
            ETableKind tableKind = ETableKind::Unknown;
            if (dqTask.HasMetaId()) {
                YQL_ENSURE(computesByStage.GetMetaById(dqTask, meta) || dqTask.GetMeta().UnpackTo(&meta), "cannot take meta on MetaId exists in tasks");
                tableKind = tableKindExtract(meta);
            } else if (dqTask.GetMeta().UnpackTo(&meta)) {
                tableKind = tableKindExtract(meta);
            }

            IActor* computeActor;
            if (tableKind == ETableKind::Datashard || tableKind == ETableKind::Olap) {
                auto& info = computesByStage.UpsertTaskWithScan(dqTask, meta, !AppData()->FeatureFlags.GetEnableSeparationComputeActorsFromRead());
                computeActor = CreateKqpScanComputeActor(request.Executer, txId, std::move(dqTask),
                    AsyncIoFactory, AppData()->FunctionRegistry, runtimeSettings, memoryLimits,
                    NWilson::TTraceId(ev->TraceId));
                taskCtx.ComputeActorId = Register(computeActor);
                info.MutableActorIds().emplace_back(taskCtx.ComputeActorId);
            } else {
                if (Y_LIKELY(!CaFactory)) {
                    computeActor = CreateKqpComputeActor(request.Executer, txId, std::move(dqTask), AsyncIoFactory,
                        AppData()->FunctionRegistry, runtimeSettings, memoryLimits, NWilson::TTraceId(ev->TraceId));
                    taskCtx.ComputeActorId = Register(computeActor);
                } else {
                    computeActor = CaFactory->CreateKqpComputeActor(request.Executer, txId, std::move(dqTask),
                                                                    runtimeSettings, memoryLimits);
                    taskCtx.ComputeActorId = computeActor->SelfId();
                }
            }

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

        bucket.NewRequest(txId, requester, std::move(request), memoryPool);
    }

    // used only for unit tests
    void HandleWork(TEvKqpNode::TEvFinishKqpTask::TPtr& ev) {
        auto& msg = *ev->Get();
        FinishKqpTask(msg.TxId, msg.TaskId, msg.Success, msg.Issues, GetStateBucketByTx(msg.TxId));
    }

    void HandleWork(TEvKqpNode::TEvCancelKqpTasksRequest::TPtr& ev) {
        ui64 txId = ev->Get()->Record.GetTxId();
        auto& reason = ev->Get()->Record.GetReason();

        LOG_W("TxId: " << txId << ", terminate transaction, reason: " << reason);
        TerminateTx(txId, reason);
    }

    void TerminateTx(ui64 txId, const TString& reason) {
        auto& bucket = GetStateBucketByTx(txId);
        auto tasksToAbort = bucket.RemoveTx(txId);

        if (!tasksToAbort.empty()) {
            LOG_D("TxId: " << txId << ", cancel granted resources");
            ResourceManager()->FreeResources(txId);

            for (const auto& tasksRequest: tasksToAbort) {
                for (const auto& [taskId, task] : tasksRequest.InFlyTasks) {
                    auto abortEv = MakeHolder<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::UNSPECIFIED,
                        reason);
                    Send(task.ComputeActorId, abortEv.Release());
                }
            }
        }
    }

    void HandleWork(TEvents::TEvWakeup::TPtr& ev) {
        Schedule(TDuration::Seconds(1), ev->Release().Release());
        std::vector<ui64> txIdsToFree;
        for (auto& bucket : Buckets) {
            auto expiredRequests = bucket.ClearExpiredRequests();
            for (auto& cxt : expiredRequests) {
                    LOG_D("txId: " << cxt.RequestId.TxId << ", requester: " << cxt.RequestId.Requester
                        << ", execution timeout, request: " << cxt.Exists);
                    if (!cxt.Exists) {
                        // it is ok since in most cases requests is finished by exlicit TEvAbortExecution from their Executer
                        LOG_I("txId: " << cxt.RequestId.TxId << ", requester: " << cxt.RequestId.Requester
                            << ", unknown request");
                        continue;
                    }
                    // don't send to executer and compute actors, they will be destroyed by TEvAbortExecution in that order:
                    // KqpProxy -> SessionActor -> Executer -> ComputeActor
                    ResourceManager()->FreeResources(cxt.RequestId.TxId);
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
            FORCE_VALUE(EnableInstantMkqlMemoryAlloc);
            FORCE_VALUE(MaxTotalChannelBuffersSize);
            FORCE_VALUE(MinChannelBufferSize);
#undef FORCE_VALUE

            LOG_I("Updated table service config: " << Config.DebugString());
        }

        auto responseEv = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationResponse>(event);
        Send(ev->Sender, responseEv.Release(), IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void HandleWork(TEvents::TEvUndelivered::TPtr& ev) {
        switch (ev->Get()->SourceType) {
            case TEvKqpNode::TEvStartKqpTasksResponse::EventType: {
                ui64 txId = ev->Cookie;
                LOG_E("TxId: " << txId << ", executer lost: " << (int) ev->Get()->Reason);

                TerminateTx(txId, "executer lost");
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
                for (auto& bucket : Buckets) {
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

    NRm::IKqpResourceManager* ResourceManager() {
        if (Y_LIKELY(ResourceManager_)) {
            return ResourceManager_;
        }
        ResourceManager_ = GetKqpResourceManager();
        return ResourceManager_;
    }

    NKqpNode::TState& GetStateBucketByTx(ui64 txId) {
        return Buckets[txId % Buckets.size()];
    }

private:
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;
    TIntrusivePtr<TKqpCounters> Counters;
    IKqpNodeComputeActorFactory* CaFactory;
    NRm::IKqpResourceManager* ResourceManager_ = nullptr;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;

    //state sharded by TxId
    std::array<NKqpNode::TState, BucketsCount> Buckets;
};


} // anonymous namespace

IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    TIntrusivePtr<TKqpCounters> counters, IKqpNodeComputeActorFactory* caFactory, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory)
{
    return new TKqpNodeService(tableServiceConfig, counters, caFactory, std::move(asyncIoFactory));
}

} // namespace NKqp
} // namespace NKikimr
