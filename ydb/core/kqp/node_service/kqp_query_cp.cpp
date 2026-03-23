#include "kqp_node_service.h"
#include "kqp_node_state.h"
#include "kqp_query_cp.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NKqp {

class TKqpQueryManager : public NActors::TActor<TKqpQueryManager> {
public:
    TKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state, std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory)
        : TActor(&TThis::StateFunc)
        , Counters_(counters)
        , State_(state)
        , ResourceManager_(resourceManager)
        , CaFactory_(caFactory)
    {
    }

    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NActors::TEvents::TEvPoison, HandlePoison);
            hFunc(TEvKqpNode::TEvStartKqpTasksRequest, HandleStart);
        }
    }

    void HandlePoison(NActors::TEvents::TEvPoison::TPtr&) {
        PassAway();
    }

    void HandleStart(TEvKqpNode::TEvStartKqpTasksRequest::TPtr& ev) {

        auto& msg = ev->Get()->Record;
        ui64 txId = msg.GetTxId();

        const auto executerId = ev->Sender;

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
        TMaybe<ui64> lockTxId = msg.HasLockTxId()
            ? TMaybe<ui64>(msg.GetLockTxId())
            : Nothing();
        ui32 lockNodeId = msg.GetLockNodeId();
        TMaybe<NKikimrDataEvents::ELockMode> lockMode = msg.HasLockMode()
            ? TMaybe<NKikimrDataEvents::ELockMode>(msg.GetLockMode())
            : Nothing();

        auto& runtimeSettings = msg.GetRuntimeSettings();

        NComputeActor::TComputeStagesWithScan computesByStage;

        const TString& serializedGUCSettings = ev->Get()->Record.HasSerializedGUCSettings() ?
            ev->Get()->Record.GetSerializedGUCSettings() : "";

        // start compute actors
        TMaybe<NYql::NDqProto::TRlPath> rlPath = Nothing();
        if (runtimeSettings.HasRlPath()) {
            rlPath.ConstructInPlace(runtimeSettings.GetRlPath());
        }

        const auto& poolId = msg.GetPoolId().empty() ? NResourcePool::DEFAULT_POOL_ID : msg.GetPoolId();

        TIntrusivePtr<NRm::TTxState> txInfo = MakeIntrusive<NRm::TTxState>(
            txId, TInstant::Now(), ResourceManager_->GetCounters(),
            poolId, msg.GetMemoryPoolPercent(),
            msg.GetDatabase(),  CaFactory_->GetVerboseMemoryLimitException());

        auto query = State_->GetSchedulerQuery(txId, executerId);

        const ui32 tasksCount = msg.GetTasks().size();
        for (auto& dqTask: *msg.MutableTasks()) {
            const auto taskId = dqTask.GetId();

            NComputeActor::IKqpNodeComputeActorFactory::TCreateArgs createArgs{
                .ExecuterId = executerId,
                .TxId = txId,
                .LockTxId = lockTxId,
                .LockNodeId = lockNodeId,
                .LockMode = lockMode,
                .Task = &dqTask,
                .TxInfo = txInfo,
                .ReportStatsSettings = ReportStatsSettingsFromProto(runtimeSettings),
                .TraceId = NWilson::TTraceId(ev->TraceId),
                .Arena = ev->Get()->Arena,
                .SerializedGUCSettings = serializedGUCSettings,
                .NumberOfTasks = tasksCount,
                .OutputChunkMaxSize = msg.GetOutputChunkMaxSize(),
                .MemoryPool = memoryPool,
                .WithSpilling = runtimeSettings.GetUseSpilling(),
                .StatsMode = runtimeSettings.GetStatsMode(),
                .WithProgressStats = runtimeSettings.GetWithProgressStats(),
                .Deadline = TInstant(),
                .ShareMailbox = false,
                .RlPath = rlPath,
                .ComputesByStages = &computesByStage,
                .State = State_, // pass state to later inform when task is finished
                .Database = msg.GetDatabase(),
                .Query = query,
                // TODO: block tracking mode is not set!
            };
            if (msg.HasUserToken() && msg.GetUserToken()) {
                createArgs.UserToken.Reset(MakeIntrusive<NACLib::TUserToken>(msg.GetUserToken()));
            }

            auto result = CaFactory_->CreateKqpComputeActor(std::move(createArgs));

            if (const auto* rmResult = std::get_if<NRm::TKqpRMAllocateResult>(&result)) {

                // ReplyError(txId, executerId, msg, rmResult->GetStatus(), ev->Cookie, rmResult->GetFailReason());
                auto evResp = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
                evResp->Record.SetTxId(txId);
                for (auto& task : msg.GetTasks()) {
                    auto* resp = evResp->Record.AddNotStartedTasks();
                    resp->SetTaskId(task.GetId());
                    resp->SetReason(rmResult->GetStatus());
                    resp->SetMessage(rmResult->GetFailReason());
                    resp->SetRequestId(ev->Cookie);
                }
                Send(executerId, evResp.Release());

                // TerminateTx(txId, executerId, rmResult->GetFailReason());
                State_->MarkRequestAsCancelled(txId, executerId);

                if (auto tasksToAbort = State_->GetTasksByTxId(txId, executerId); !tasksToAbort.empty()) {
                    STLOG_E("Node service cancelled the task, because it " << rmResult->GetFailReason(),
                        (node_id, SelfId().NodeId()),
                        (tx_id, txId));
                    for (const auto& [taskId, computeActorId]: tasksToAbort) {
                        auto abortEv = std::make_unique<TEvKqp::TEvAbortExecution>(NYql::NDqProto::StatusIds::UNSPECIFIED, rmResult->GetFailReason());
                        Send(computeActorId, abortEv.release());
                    }
                }

                return;
            }

            TActorId* actorId = std::get_if<TActorId>(&result);
            auto* startedTask = reply->Record.AddStartedTasks();
            Y_ENSURE(actorId);

            startedTask->SetTaskId(taskId);
            ActorIdToProto(*actorId, startedTask->MutableActorId());
            if (State_->OnTaskStarted(txId, executerId, taskId, *actorId)) {
                STLOG_D("Executing task",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (task_id, taskId),
                    (compute_actor_id, *actorId),
                    (trace_id, ev->TraceId.GetHexTraceIdLowerCase()));
            } else {
                STLOG_D("Task finished in an instant",
                    (node_id, SelfId().NodeId()),
                    (tx_id, txId),
                    (task_id, taskId),
                    (compute_actor_id, *actorId),
                    (trace_id, ev->TraceId.GetHexTraceIdLowerCase()));
            }
        }

        TCPULimits cpuLimits;
        if (msg.GetPoolMaxCpuShare() > 0) {
            // Share <= 0 means disabled limit
            cpuLimits.DeserializeFromProto(msg).Validate();
        }

        for (auto&& i : computesByStage) {
            for (auto&& m : i.second.MutableMetaInfo()) {
                Register(CreateKqpScanFetcher(msg.GetSnapshot(), std::move(m.MutableActorIds()),
                    m.GetMeta(), NYql::NDq::TComputeRuntimeSettings(), msg.GetDatabase(), txId, lockTxId, lockNodeId, lockMode,
                    CaFactory_->GetShardsScanningPolicy(), Counters_, NWilson::TTraceId(ev->TraceId), cpuLimits));
            }
        }

        Send(executerId, reply.Release(), IEventHandle::FlagTrackDelivery, txId);
    }
private:
    TIntrusivePtr<TKqpCounters> Counters_;
    std::shared_ptr<TNodeState> State_;
    std::shared_ptr<NRm::IKqpResourceManager> ResourceManager_;
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
};

NActors::IActor* CreateKqpQueryManager(TIntrusivePtr<TKqpCounters>& counters, std::shared_ptr<TNodeState>& state,
    std::shared_ptr<NRm::IKqpResourceManager>& resourceManager, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory) {
    return new TKqpQueryManager(counters, state, resourceManager, caFactory);
}

} // namespace NKikimr::NKqp
