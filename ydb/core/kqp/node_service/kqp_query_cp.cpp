#include "kqp_node_service.h"
#include "kqp_node_state.h"
#include "kqp_query_cp.h"

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr::NKqp {

class TKqpQueryManager : public NActors::TActor<TKqpQueryManager> {
public:
    TKqpQueryManager(std::shared_ptr<TNodeState>& state, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory)
        : TActor(&TThis::StateFunc)
        , State_(state)
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
/*
        NRm::EKqpMemoryPool memoryPool;
        if (msg.GetRuntimeSettings().GetExecType() == NYql::NDqProto::TComputeRuntimeSettings::SCAN) {
            memoryPool = NRm::EKqpMemoryPool::ScanQuery;
        } else if (msg.GetRuntimeSettings().GetExecType() == NYql::NDqProto::TComputeRuntimeSettings::DATA) {
            memoryPool = NRm::EKqpMemoryPool::DataQuery;
        } else {
            memoryPool = NRm::EKqpMemoryPool::Unspecified;
        }
*/
        auto reply = MakeHolder<TEvKqpNode::TEvStartKqpTasksResponse>();
        reply->Record.SetTxId(txId);
/*
        auto& runtimeSettings = msg.GetRuntimeSettings();

        NYql::NDq::TComputeRuntimeSettings runtimeSettingsBase;
        runtimeSettingsBase.ReportStatsSettings = NYql::NDq::TReportStatsSettings{
            .MinInterval = runtimeSettings.HasMinStatsSendIntervalMs() ? TDuration::MilliSeconds(runtimeSettings.GetMinStatsSendIntervalMs()) : MinStatInterval,
            .MaxInterval = runtimeSettings.HasMaxStatsSendIntervalMs() ? TDuration::MilliSeconds(runtimeSettings.GetMaxStatsSendIntervalMs()) : MaxStatInterval,
        };

        TShardsScanningPolicy scanPolicy(Config.GetShardsScanningPolicy());

        NComputeActor::TComputeStagesWithScan computesByStage;

        const TString& serializedGUCSettings = ev->Get()->Record.HasSerializedGUCSettings() ?
            ev->Get()->Record.GetSerializedGUCSettings() : "";

        // start compute actors
        TMaybe<NYql::NDqProto::TRlPath> rlPath = Nothing();
        if (runtimeSettings.HasRlPath()) {
            rlPath.ConstructInPlace(runtimeSettings.GetRlPath());
        }

        TIntrusivePtr<NRm::TTxState> txInfo = MakeIntrusive<NRm::TTxState>(
            txId, TInstant::Now(), ResourceManager_->GetCounters(),
            poolId, msg.GetMemoryPoolPercent(),
            msg.GetDatabase(), Config.GetVerboseMemoryLimitException());

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
                .RuntimeSettings = runtimeSettingsBase,
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

            // NOTE: keep in mind that a task can start, execute and finish before we reach the end of this method.

            if (const auto* rmResult = std::get_if<NRm::TKqpRMAllocateResult>(&result)) {
                ReplyError(txId, executerId, msg, rmResult->GetStatus(), ev->Cookie, rmResult->GetFailReason());
                TerminateTx(txId, executerId, rmResult->GetFailReason());
                co_return;
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
                    m.GetMeta(), runtimeSettingsBase, msg.GetDatabase(), txId, lockTxId, lockNodeId, lockMode,
                    scanPolicy, Counters, NWilson::TTraceId(ev->TraceId), cpuLimits));
            }
        }
*/
        Send(executerId, reply.Release(), IEventHandle::FlagTrackDelivery, txId);
    }
private:
    std::shared_ptr<TNodeState> State_;
    std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory> CaFactory_;
};

NActors::IActor* CreateKqpQueryManager(std::shared_ptr<TNodeState>& state, std::shared_ptr<NComputeActor::IKqpNodeComputeActorFactory>& caFactory) {
    return new TKqpQueryManager(state, caFactory);
}

} // namespace NKikimr::NKqp
