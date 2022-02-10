#include "dq_compute_actor.h"
#include "dq_async_compute_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql {
namespace NDq {

using namespace NActors;

namespace {

bool IsDebugLogEnabled(const TActorSystem* actorSystem) {
    auto* settings = actorSystem->LoggerSettings();
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, NKikimrServices::KQP_TASKS_RUNNER);
}

} // anonymous namespace

class TDqAsyncComputeActor : public TDqComputeActorBase<TDqAsyncComputeActor>
                       , public NTaskRunnerActor::ITaskRunnerActor::ICallbacks
{

    using TBase = TDqComputeActorBase<TDqAsyncComputeActor>;

public:
    static constexpr char ActorName[] = "DQ_COMPUTE_ACTOR";

    TDqAsyncComputeActor(const TActorId& executerId, const TTxId& txId, NDqProto::TDqTask&& task,
        IDqSourceActorFactory::TPtr sourceActorFactory, IDqSinkActorFactory::TPtr sinkActorFactory,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        const NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory)
        : TBase(executerId, txId, std::move(task), std::move(sourceActorFactory), std::move(sinkActorFactory), settings, memoryLimits)
        , TaskRunnerActorFactory(taskRunnerActorFactory)
    {}

    void DoBootstrap() {
        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem)) {
            logger = [actorSystem, txId = this->GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_TASKS_RUNNER, "TxId: " << txId
                    << ", task: " << taskId << ": " << message);
            };
        }

        NActors::IActor* actor;
        std::tie(TaskRunnerActor, actor) = TaskRunnerActorFactory->Create(
            this, TStringBuilder() << this->GetTxId());
        TaskRunnerActorId = this->RegisterWithSameMailbox(actor);

        TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = MemoryLimits.ChannelBufferSize;
        limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

        this->Become(&TDqAsyncComputeActor::StateFuncBase);

        // TODO:
        std::shared_ptr<IDqTaskRunnerExecutionContext> execCtx = std::shared_ptr<IDqTaskRunnerExecutionContext>(new TDqTaskRunnerExecutionContext());

        this->Send(TaskRunnerActorId,
                   new NTaskRunnerActor::TEvTaskRunnerCreate(
                       GetTask(), limits, execCtx));
    }

    void FillExtraStats(NDqProto::TDqComputeActorStats* /* dst */, bool /* last */) {
    }

private:
    STFUNC(StateFuncBase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NTaskRunnerActor::TEvTaskRunFinished, OnRunFinished);
            HFunc(NTaskRunnerActor::TEvSourcePushFinished, OnSourcePushFinished);
            HFunc(NTaskRunnerActor::TEvChannelPopFinished, OnPopFinished);
            HFunc(NTaskRunnerActor::TEvTaskRunnerCreateFinished, OnTaskRunnerCreatead);
            HFunc(NTaskRunnerActor::TEvContinueRun, OnContinueRun); // push finished
            default:
                TDqComputeActorBase<TDqAsyncComputeActor>::StateFuncBase(ev, ctx);
        };
    };

    void DrainOutputChannel(TOutputChannelInfo& outputChannel, const TDqComputeActorChannels::TPeerState& peerState) override {
        YQL_ENSURE(!outputChannel.Finished || Checkpoints);

        if (outputChannel.PopStarted) {
            return;
        }

        const bool wasFinished = outputChannel.Finished;
        auto channelId = outputChannel.ChannelId;

        const ui32 allowedOvercommit = AllowedChannelsOvercommit();

        const i64 toSend = peerState.PeerFreeSpace + allowedOvercommit - peerState.InFlightBytes;

        CA_LOG_D("About to drain channelId: " << channelId
            << ", hasPeer: " << outputChannel.HasPeer
            << ", peerFreeSpace: " << peerState.PeerFreeSpace
            << ", inFlightBytes: " << peerState.InFlightBytes
            << ", inFlightRows: " << peerState.InFlightRows
            << ", inFlightCount: " << peerState.InFlightCount
            << ", allowedOvercommit: " << allowedOvercommit
            << ", toSend: " << toSend
//            << ", finished: " << outputChannel.Channel->IsFinished());
            );

        outputChannel.PopStarted = true;
        ProcessOutputsState.Inflight ++;
        if (toSend <= 0) {
            if (Y_UNLIKELY(outputChannel.Stats)) {
                outputChannel.Stats->BlockedByCapacity++;
            }
            this->Send(this->SelfId(), new NTaskRunnerActor::TEvChannelPopFinished(channelId));
            return;
        }

        this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvPop(channelId, wasFinished, toSend));
    }

    void DrainSink(ui64 outputIndex, TSinkInfo& sinkInfo) override {
        if (sinkInfo.Finished && !Checkpoints) {
            return;
        }

        if (sinkInfo.PopStarted) {
            return;
        }

        Y_VERIFY(sinkInfo.SinkActor);
        Y_VERIFY(sinkInfo.Actor);

        const ui32 allowedOvercommit = AllowedChannelsOvercommit();
        const i64 sinkActorFreeSpaceBeforeSend = sinkInfo.SinkActor->GetFreeSpace();

        i64 toSend = sinkActorFreeSpaceBeforeSend + allowedOvercommit;
        CA_LOG_D("About to drain sink " << outputIndex
            << ". FreeSpace: " << sinkActorFreeSpaceBeforeSend
            << ", allowedOvercommit: " << allowedOvercommit
            << ", toSend: " << toSend
                 //<< ", finished: " << sinkInfo.Sink->IsFinished());
            );

        sinkInfo.PopStarted = true;
        ProcessOutputsState.Inflight ++;
        sinkInfo.SinkActorFreeSpaceBeforeSend = sinkActorFreeSpaceBeforeSend;
    }

    void SourcePush(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, TSourceInfo& source, i64 space, bool finished) override {
        if (space <= 0) {
            return;
        }

        ProcessSourcesState.Inflight ++;
        source.PushStarted = true;
        source.Finished = finished;
        TaskRunnerActor->SourcePush(Cookie++, source.Index, std::move(batch), space, finished);
    }

    void TakeInputChannelData(NDqProto::TChannelData&& channelData, bool ack) override {
        TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(channelData.GetChannelId());
        YQL_ENSURE(inputChannel, "task: " << Task.GetId() << ", unknown input channelId: " << channelData.GetChannelId());

        auto finished = channelData.GetFinished();

        auto ev = (channelData.GetData().GetRows())
            ? MakeHolder<NTaskRunnerActor::TEvPush>(
                channelData.GetChannelId(),
                std::move(*channelData.MutableData()),
                finished,
                /*askFreeSpace = */ true)
            : MakeHolder<NTaskRunnerActor::TEvPush>(
                channelData.GetChannelId(), finished, /*askFreeSpace = */ true);

        this->Send(TaskRunnerActorId, ev.Release(), 0, Cookie);

        if (channelData.HasCheckpoint()) {
            Y_VERIFY(inputChannel->CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
            Y_VERIFY(Checkpoints);
            const auto& checkpoint = channelData.GetCheckpoint();
            inputChannel->Pause(checkpoint);
            Checkpoints->RegisterCheckpoint(checkpoint, channelData.GetChannelId());
        }

        TakeInputChannelDataRequests[Cookie++] = TTakeInputChannelData{ack, channelData.GetChannelId()};
    }

    void PassAway() override {
        if (TaskRunnerActor) {
            TaskRunnerActor->PassAway();
        }
        NActors::IActor::PassAway();
    }

    void DoExecuteImpl() override {
        PollSourceActors();
        if (ProcessSourcesState.Inflight == 0) {
            this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvContinueRun());
        }
    }

    bool SayHelloOnBootstrap() override {
        return false;
    }

    i64 GetInputChannelFreeSpace(ui64 channelId) const override {
        const TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(inputChannel, "task: " << Task.GetId() << ", unknown input channelId: " << channelId);

        return inputChannel->FreeSpace;
    }

    i64 SourceFreeSpace(TSourceInfo& source) override {
        return source.FreeSpace;
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() override {
        return TypeEnv->BindAllocator();
    }

    std::optional<TGuard<NKikimr::NMiniKQL::TScopedAlloc>> MaybeBindAllocator() override {
        std::optional<TGuard<NKikimr::NMiniKQL::TScopedAlloc>> guard;
        if (TypeEnv) {
            guard.emplace(TypeEnv->BindAllocator());
        }
        return guard;
    }

    void OnTaskRunnerCreatead(NTaskRunnerActor::TEvTaskRunnerCreateFinished::TPtr& ev, const NActors::TActorContext& ) {
        const auto& secureParams = ev->Get()->SecureParams;
        const auto& taskParams = ev->Get()->TaskParams;
        const auto& typeEnv = ev->Get()->TypeEnv;
        const auto& holderFactory = ev->Get()->HolderFactory;

        TypeEnv = const_cast<NKikimr::NMiniKQL::TTypeEnvironment*>(&typeEnv);
        FillChannelMaps(holderFactory, typeEnv, secureParams, taskParams);

        {
            // say "Hello" to executer
            auto ev = MakeHolder<TEvDqCompute::TEvState>();
            ev->Record.SetState(NDqProto::COMPUTE_STATE_EXECUTING);
            ev->Record.SetTaskId(Task.GetId());

            this->Send(ExecuterId, ev.Release(), NActors::IEventHandle::FlagTrackDelivery);
        }

        ContinueExecute();
    }

    void OnRunFinished(NTaskRunnerActor::TEvTaskRunFinished::TPtr& ev, const NActors::TActorContext& ) {
        auto sourcesState = GetSourcesState();
        auto status = ev->Get()->RunStatus;

        CA_LOG_D("Resume execution, run status: " << status);

        for (const auto& [channelId, freeSpace] : ev->Get()->InputChannelFreeSpace) {
            auto it = InputChannelsMap.find(channelId);
            if (it != InputChannelsMap.end()) {
                it->second.FreeSpace = freeSpace;
            }
        }

        if (status != ERunStatus::Finished) {
            PollSources(std::move(sourcesState));
        }

        if ((status == ERunStatus::PendingInput || status == ERunStatus::Finished) && Checkpoints && Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved() && ReadyToCheckpoint()) {
            Checkpoints->DoCheckpoint();
        }

        ProcessOutputsImpl(status);
    }

    void OnSourcePushFinished(NTaskRunnerActor::TEvSourcePushFinished::TPtr& ev, const NActors::TActorContext& ) {
        auto it = SourcesMap.find(ev->Get()->Index);
        Y_VERIFY(it != SourcesMap.end());
        auto& source = it->second;
        source.PushStarted = false;
        // source.FreeSpace = ev->Get()->FreeSpace; TODO:XXX get freespace on run
        ProcessSourcesState.Inflight--;
        if (ProcessSourcesState.Inflight == 0) {
            this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvContinueRun());
        }
    }

    void OnPopFinished(NTaskRunnerActor::TEvChannelPopFinished::TPtr& ev, const NActors::TActorContext&) {
        auto channelId = ev->Get()->ChannelId;
        auto finished = ev->Get()->Finished;
        auto dataWasSent = ev->Get()->Changed;
        auto it = OutputChannelsMap.find(channelId);
        Y_VERIFY(it != OutputChannelsMap.end());

        TOutputChannelInfo& outputChannel = it->second;
        outputChannel.Finished = finished;
        if (finished) {
            FinishedOutputChannels.insert(channelId);
        }

        outputChannel.PopStarted = false;
        ProcessOutputsState.Inflight --;
        ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;

        for (ui32 i = 0; i < ev->Get()->Data.size(); i++) {
            auto& chunk = ev->Get()->Data[i];
            NDqProto::TChannelData channelData;
            channelData.SetChannelId(channelId);
            // set finished only for last chunk
            channelData.SetFinished(finished && i==ev->Get()->Data.size()-1);
            channelData.MutableData()->Swap(&chunk);
            Channels->SendChannelData(std::move(channelData));
        }
        if (ev->Get()->Data.empty() && dataWasSent) {
            NDqProto::TChannelData channelData;
            channelData.SetChannelId(channelId);
            channelData.SetFinished(finished);
            Channels->SendChannelData(std::move(channelData));
        }

        ProcessOutputsState.DataWasSent |= dataWasSent;

        ProcessOutputsState.AllOutputsFinished =
            FinishedOutputChannels.size() == OutputChannelsMap.size() &&
            FinishedSinks.size() == SinksMap.size();

        CheckRunStatus();
    }

    void OnContinueRun(NTaskRunnerActor::TEvContinueRun::TPtr& ev, const NActors::TActorContext& ) {
        auto it = TakeInputChannelDataRequests.find(ev->Cookie);
        YQL_ENSURE(it != TakeInputChannelDataRequests.end());

        if (it->second.Ack) {
            TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(it->second.ChannelId);
            Channels->SendChannelDataAck(it->second.ChannelId, inputChannel->FreeSpace);
        }

        ResumeExecution();
    }

    void SinkSend(ui64 index,
                  NKikimr::NMiniKQL::TUnboxedValueVector&& batch,
                  TMaybe<NDqProto::TCheckpoint>&& checkpoint,
                  i64 size,
                  i64 checkpointSize,
                  bool finished,
                  bool changed) override
    {
        auto outputIndex = index;
        auto dataWasSent = finished || changed;
        auto dataSize = size;
        auto it = SinksMap.find(outputIndex);
        Y_VERIFY(it != SinksMap.end());

        TSinkInfo& sinkInfo = it->second;
        sinkInfo.Finished = finished;
        if (finished) {
            FinishedSinks.insert(outputIndex);
        }
        if (checkpoint) {
            CA_LOG_I("Resume inputs");
            ResumeInputs();
        }

        sinkInfo.PopStarted = false;
        ProcessOutputsState.Inflight --;
        ProcessOutputsState.HasDataToSend |= !sinkInfo.Finished;

        auto guard = BindAllocator();
        sinkInfo.SinkActor->SendData(std::move(batch), size, std::move(checkpoint), finished);
        CA_LOG_D("sink " << outputIndex << ": sent " << dataSize << " bytes of data and " << checkpointSize << " bytes of checkpoint barrier");

        CA_LOG_D("Drain sink " << outputIndex
            << ". Free space decreased: " << (sinkInfo.SinkActorFreeSpaceBeforeSend - sinkInfo.SinkActor->GetFreeSpace())
            << ", sent data from buffer: " << dataSize);

        ProcessOutputsState.DataWasSent |= dataWasSent;
        ProcessOutputsState.AllOutputsFinished =
            FinishedOutputChannels.size() == OutputChannelsMap.size() &&
            FinishedSinks.size() == SinksMap.size();
        CheckRunStatus();
    }

    NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv;
    NTaskRunnerActor::ITaskRunnerActor* TaskRunnerActor;
    NActors::TActorId TaskRunnerActorId;
    NTaskRunnerActor::ITaskRunnerActorFactory::TPtr TaskRunnerActorFactory;

    THashSet<ui64> FinishedOutputChannels;
    THashSet<ui64> FinishedSinks;
    struct TProcessSourcesState {
        int Inflight = 0;
    };
    TProcessSourcesState ProcessSourcesState;

    struct TTakeInputChannelData {
        bool Ack;
        ui64 ChannelId;
    };
    THashMap<ui64, TTakeInputChannelData> TakeInputChannelDataRequests;
    ui64 Cookie = 0;
};


IActor* CreateDqAsyncComputeActor(const TActorId& executerId, const TTxId& txId, NYql::NDqProto::TDqTask&& task,
    IDqSourceActorFactory::TPtr sourceActorFactory, IDqSinkActorFactory::TPtr sinkActorFactory,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    const NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory)
{
    return new TDqAsyncComputeActor(executerId, txId, std::move(task), std::move(sourceActorFactory),
        std::move(sinkActorFactory), settings, memoryLimits, taskRunnerActorFactory);
}

} // namespace NDq
} // namespace NYql
