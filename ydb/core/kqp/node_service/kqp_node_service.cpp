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

constexpr ui64 BucketsCount = 64;
using TBucketArray = std::array<NKqpNode::TState, BucketsCount>;

NKqpNode::TState& GetStateBucketByTx(std::shared_ptr<TBucketArray> buckets, ui64 txId) {
    return (*buckets)[txId % buckets->size()];
}

void FinishKqpTask(ui64 txId, ui64 taskId, bool success, NKqpNode::TState& bucket, std::shared_ptr<NRm::IKqpResourceManager> ResourceManager) {
    bucket.RemoveTask(txId, taskId, success);
    ResourceManager->FreeResources(txId, taskId);
}

struct TMemoryQuotaManager : public NYql::NDq::TGuaranteeQuotaManager {

    TMemoryQuotaManager(std::shared_ptr<NRm::IKqpResourceManager> resourceManager
        , NRm::EKqpMemoryPool memoryPool
        , std::shared_ptr<TBucketArray> buckets
        , ui64 txId
        , ui64 taskId
        , ui64 limit
        , bool instantAlloc)
    : NYql::NDq::TGuaranteeQuotaManager(limit, limit)
    , ResourceManager(std::move(resourceManager))
    , MemoryPool(memoryPool)
    , Buckets(std::move(buckets))
    , TxId(txId)
    , TaskId(taskId)
    , InstantAlloc(instantAlloc)
    {
    }

    ~TMemoryQuotaManager() override {
        FinishKqpTask(TxId, TaskId, Success, GetStateBucketByTx(Buckets, TxId), ResourceManager);
    }

    bool AllocateExtraQuota(ui64 extraSize) override {

        if (!InstantAlloc) {
            LOG_W("Memory allocation prohibited. TxId: " << TxId << ", taskId: " << TaskId << ", memory: +" << extraSize);
            return false;
        }

        auto result = ResourceManager->AllocateResources(TxId, TaskId,
            NRm::TKqpResourcesRequest{.MemoryPool = MemoryPool, .Memory = extraSize});

        if (!result) {
            LOG_W("Can not allocate memory. TxId: " << TxId << ", taskId: " << TaskId << ", memory: +" << extraSize);
            return false;
        }

        return true;
    }

    void FreeExtraQuota(ui64 extraSize) override {
        ResourceManager->FreeResources(TxId, TaskId,
            NRm::TKqpResourcesRequest{.MemoryPool = MemoryPool, .Memory = extraSize}
        );
    }

    void TerminateHandler(bool success, const NYql::TIssues& issues) {
        LOG_D("TxId: " << TxId << ", finish compute task: " << TaskId << ", success: " << success
            << ", message: " << issues.ToOneLineString());
        Success = success;
    }

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager;
    NRm::EKqpMemoryPool MemoryPool;
    std::shared_ptr<TBucketArray> Buckets;
    ui64 TxId;
    ui64 TaskId;
    bool InstantAlloc;
    bool Success = true;
};

class TKqpNodeService : public TActorBootstrapped<TKqpNodeService> {
    using TBase = TActorBootstrapped<TKqpNodeService>;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::KQP_NODE_SERVICE;
    }

    TKqpNodeService(const NKikimrConfig::TTableServiceConfig& config, const TIntrusivePtr<TKqpCounters>& counters,
        IKqpNodeComputeActorFactory* caFactory, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
        const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
        : Config(config.GetResourceManager())
        , Counters(counters)
        , CaFactory(caFactory)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FederatedQuerySetup(federatedQuerySetup)
    {
        Buckets = std::make_shared<TBucketArray>();
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

    static constexpr double SecToUsec = 1e6;

    void HandleWork(TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {
        NWilson::TSpan sendTasksSpan(TWilsonKqp::KqpNodeSendTasks, NWilson::TTraceId(ev->TraceId), "KqpNode.SendTasks", NWilson::EFlags::AUTO_END);

        NHPTimer::STime workHandlerStart = ev->SendTime;
        auto& msg = ev->Get()->Record;
        Counters->NodeServiceStartEventDelivery->Collect(NHPTimer::GetTimePassed(&workHandlerStart) * SecToUsec);

        auto requester = ev->Sender;

        ui64 txId = msg.GetTxId();
        const ui64 outputChunkMaxSize = msg.GetOutputChunkMaxSize();

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

        auto& bucket = GetStateBucketByTx(Buckets, txId);

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
        }

        LOG_D("TxId: " << txId << ", channels: " << requestChannels
            << ", computeActors: " << msg.GetTasks().size() << ", memory: " << request.CalculateTotalMemory());

        TVector<ui64> allocatedTasks;
        allocatedTasks.reserve(msg.GetTasks().size());
        for (auto& task : request.InFlyTasks) {
            NRm::TKqpResourcesRequest resourcesRequest;
            resourcesRequest.MemoryPool = memoryPool;
            resourcesRequest.ExecutionUnits = 1;

            // !!!!!!!!!!!!!!!!!!!!!
            // we have to allocate memory instead of reserve only. currently, this memory will not be used for request processing.
            resourcesRequest.Memory = Min<double>(task.second.Memory, 1 << 19) /* 512kb limit for check that memory exists for processing with minimal requirements */;

            auto result = ResourceManager()->AllocateResources(txId, task.first, resourcesRequest);

            if (!result) {
                for (ui64 taskId : allocatedTasks) {
                    ResourceManager()->FreeResources(txId, taskId);
                }

                ReplyError(txId, request.Executer, msg, result.GetStatus(), result.GetFailReason());
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

        NYql::NDq::TComputeRuntimeSettings runtimeSettingsBase;
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

            {
                ui32 inputChannelsCount = 0;
                for (auto&& i : dqTask.GetInputs()) {
                    inputChannelsCount += i.ChannelsSize();
                }
                memoryLimits.ChannelBufferSize = std::max<ui32>(taskCtx.ChannelSize / std::max<ui32>(1, inputChannelsCount), Config.GetMinChannelBufferSize());
                memoryLimits.OutputChunkMaxSize = outputChunkMaxSize;
                AFL_DEBUG(NKikimrServices::KQP_COMPUTE)("event", "channel_info")
                    ("ch_size", taskCtx.ChannelSize)("ch_count", taskCtx.Channels)("ch_limit", memoryLimits.ChannelBufferSize)
                    ("inputs", dqTask.InputsSize())("input_channels_count", inputChannelsCount);
            }

            auto& taskOpts = dqTask.GetProgram().GetSettings();
            auto limit = taskOpts.GetHasMapJoin() || taskOpts.GetHasStateAggregation()
                ? memoryLimits.MkqlHeavyProgramMemoryLimit
                : memoryLimits.MkqlLightProgramMemoryLimit;

            memoryLimits.MemoryQuotaManager = std::make_shared<TMemoryQuotaManager>(
                ResourceManager(),
                memoryPool,
                Buckets,
                txId,
                dqTask.GetId(),
                limit,
                Config.GetEnableInstantMkqlMemoryAlloc());

            auto runtimeSettings = runtimeSettingsBase;
            NYql::NDq::IMemoryQuotaManager::TWeakPtr memoryQuotaManager = memoryLimits.MemoryQuotaManager;
            runtimeSettings.TerminateHandler = [memoryQuotaManager]
                (bool success, const NYql::TIssues& issues) {
                    auto manager = memoryQuotaManager.lock();
                    if (manager) {
                        static_cast<TMemoryQuotaManager*>(manager.get())->TerminateHandler(success, issues);
                    }
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
                computeActor = CreateKqpScanComputeActor(request.Executer, txId, &dqTask,
                    AsyncIoFactory, runtimeSettings, memoryLimits,
                    NWilson::TTraceId(ev->TraceId), ev->Get()->Arena);
                taskCtx.ComputeActorId = Register(computeActor);
                info.MutableActorIds().emplace_back(taskCtx.ComputeActorId);
            } else {
                std::shared_ptr<TGUCSettings> GUCSettings;
                if (ev->Get()->Record.HasSerializedGUCSettings()) {
                    GUCSettings = std::make_shared<TGUCSettings>(ev->Get()->Record.GetSerializedGUCSettings());
                }
                if (Y_LIKELY(!CaFactory)) {
                    computeActor = CreateKqpComputeActor(request.Executer, txId, &dqTask, AsyncIoFactory,
                        runtimeSettings, memoryLimits, NWilson::TTraceId(ev->TraceId), ev->Get()->Arena, FederatedQuerySetup, GUCSettings);
                    taskCtx.ComputeActorId = Register(computeActor);
                } else {
                    computeActor = CaFactory->CreateKqpComputeActor(request.Executer, txId, &dqTask,
                        runtimeSettings, memoryLimits, NWilson::TTraceId(ev->TraceId), ev->Get()->Arena);
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

        Counters->NodeServiceProcessTime->Collect(NHPTimer::GetTimePassed(&workHandlerStart) * SecToUsec);

        bucket.NewRequest(std::move(request));
    }

    // used only for unit tests
    void HandleWork(TEvKqpNode::TEvFinishKqpTask::TPtr& ev) {
        auto& msg = *ev->Get();
        FinishKqpTask(msg.TxId, msg.TaskId, msg.Success, GetStateBucketByTx(Buckets, msg.TxId), GetKqpResourceManager());
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
        auto& bucket = GetStateBucketByTx(Buckets, txId);
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
        for (auto& bucket : *Buckets) {
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
            FORCE_VALUE(EnableInstantMkqlMemoryAlloc);
            FORCE_VALUE(MaxTotalChannelBuffersSize);
            FORCE_VALUE(MinChannelBufferSize);
#undef FORCE_VALUE

            LOG_I("Updated table service config: " << Config.DebugString());
        }

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
                for (auto& bucket : *Buckets) {
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

    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager() {
        if (Y_LIKELY(ResourceManager_)) {
            return ResourceManager_;
        }
        ResourceManager_ = GetKqpResourceManager();
        return ResourceManager_;
    }

private:
    NKikimrConfig::TTableServiceConfig::TResourceManager Config;
    TIntrusivePtr<TKqpCounters> Counters;
    IKqpNodeComputeActorFactory* CaFactory;
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    NYql::NDq::IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const std::optional<TKqpFederatedQuerySetup> FederatedQuerySetup;

    //state sharded by TxId
    std::shared_ptr<TBucketArray> Buckets;
};


} // anonymous namespace

IActor* CreateKqpNodeService(const NKikimrConfig::TTableServiceConfig& tableServiceConfig,
    TIntrusivePtr<TKqpCounters> counters, IKqpNodeComputeActorFactory* caFactory, NYql::NDq::IDqAsyncIoFactory::TPtr asyncIoFactory,
    const std::optional<TKqpFederatedQuerySetup>& federatedQuerySetup)
{
    return new TKqpNodeService(tableServiceConfig, counters, caFactory, std::move(asyncIoFactory), federatedQuerySetup);
}

} // namespace NKqp
} // namespace NKikimr
