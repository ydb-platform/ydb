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
    return settings && settings->Satisfies(NActors::NLog::EPriority::PRI_DEBUG, NKikimrServices::KQP_COMPUTE);
}

} // anonymous namespace

class TDqAsyncComputeActor : public TDqComputeActorBase<TDqAsyncComputeActor>
                       , public NTaskRunnerActor::ITaskRunnerActor::ICallbacks
{
    using TBase = TDqComputeActorBase<TDqAsyncComputeActor>;

public:
    static constexpr char ActorName[] = "DQ_COMPUTE_ACTOR";

    static constexpr bool HasAsyncTaskRunner = true;

    TDqAsyncComputeActor(const TActorId& executerId, const TTxId& txId, NDqProto::TDqTask&& task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        const NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory)
        : TBase(executerId, txId, std::move(task), std::move(asyncIoFactory), functionRegistry, settings, memoryLimits, /* ownMemoryQuota = */ false)
        , TaskRunnerActorFactory(taskRunnerActorFactory)
        , ReadyToCheckpointFlag(false)
        , SentStatsRequest(false)
    {}

    void DoBootstrap() {
        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem)) {
            logger = [actorSystem, txId = this->GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
                LOG_DEBUG_S(*actorSystem, NKikimrServices::KQP_COMPUTE, "TxId: " << txId
                    << ", task: " << taskId << ": " << message);
            };
        }

        NActors::IActor* actor;
        THashSet<ui32> inputWithDisabledCheckpointing;
        for (const auto&[idx, inputInfo]: InputChannelsMap) {
            if (inputInfo.CheckpointingMode == NDqProto::CHECKPOINTING_MODE_DISABLED) {
                inputWithDisabledCheckpointing.insert(idx);
            }
        }
        std::tie(TaskRunnerActor, actor) = TaskRunnerActorFactory->Create(
            this, TStringBuilder() << this->GetTxId(), std::move(inputWithDisabledCheckpointing), InitMemoryQuota());
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
            HFunc(NTaskRunnerActor::TEvAsyncInputPushFinished, OnAsyncInputPushFinished);
            HFunc(NTaskRunnerActor::TEvChannelPopFinished, OnPopFinished);
            HFunc(NTaskRunnerActor::TEvTaskRunnerCreateFinished, OnTaskRunnerCreated);
            HFunc(NTaskRunnerActor::TEvContinueRun, OnContinueRun); // push finished
            hFunc(TEvDqCompute::TEvStateRequest, OnStateRequest);
            hFunc(NTaskRunnerActor::TEvStatistics, OnStatisticsResponse);
            hFunc(NTaskRunnerActor::TEvLoadTaskRunnerFromStateDone, OnTaskRunnerLoaded);
            default:
                TDqComputeActorBase<TDqAsyncComputeActor>::StateFuncBase(ev, ctx);
        };

        ReportEventElapsedTime();
    };

    void OnStateRequest(TEvDqCompute::TEvStateRequest::TPtr& ev) {
        CA_LOG_D("Got TEvStateRequest from actor " << ev->Sender << " TaskId: " << Task.GetId() << " PingCookie: " << ev->Cookie);
        if (!SentStatsRequest) {
            this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvStatistics(GetIds(SinksMap)));
            SentStatsRequest = true;
        }
        WaitingForStateResponse.push_back({ev->Sender, ev->Cookie});
    }

    void OnStatisticsResponse(NTaskRunnerActor::TEvStatistics::TPtr& ev) {
        SentStatsRequest = false;
        if (ev->Get()->Stats) {
            CA_LOG_D("update task runner stats");
            TaskRunnerStats = std::move(ev->Get()->Stats);
        }
        auto record = NDqProto::TEvComputeActorState();
        record.SetState(NDqProto::COMPUTE_STATE_EXECUTING);
        record.SetStatusCode(NYql::NDqProto::StatusIds::SUCCESS);
        record.SetTaskId(Task.GetId());
        FillStats(record.MutableStats(), /* last */ false);
        for (const auto& [actorId, cookie] : WaitingForStateResponse) {
            auto state = MakeHolder<TEvDqCompute::TEvState>();
            state->Record = record;
            this->Send(actorId, std::move(state), NActors::IEventHandle::FlagTrackDelivery, cookie);
        }
        WaitingForStateResponse.clear();
    }

    const TDqAsyncOutputBufferStats* GetSinkStats(ui64 outputIdx, const TAsyncOutputInfoBase& sinkInfo) const override {
        Y_UNUSED(sinkInfo);
        return TaskRunnerStats.GetSinkStats(outputIdx);
    }

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
            CA_LOG_D("Cannot drain channel cause it blocked by capacity, channelId: " << channelId);
            auto ev = MakeHolder<NTaskRunnerActor::TEvChannelPopFinished>(channelId);
            Y_VERIFY(!ev->Finished);
            this->Send(this->SelfId(), std::move(ev));  // try again, ev.Finished == false
            return;
        }

        this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvPop(channelId, wasFinished, toSend));
    }

    void DrainAsyncOutput(ui64 outputIndex, TAsyncOutputInfoBase& sinkInfo) override {
        if (sinkInfo.Finished && !Checkpoints) {
            return;
        }

        if (sinkInfo.PopStarted) {
            return;
        }

        Y_VERIFY(sinkInfo.AsyncOutput);
        Y_VERIFY(sinkInfo.Actor);

        const ui32 allowedOvercommit = AllowedChannelsOvercommit();
        const i64 sinkFreeSpaceBeforeSend = sinkInfo.AsyncOutput->GetFreeSpace();

        i64 toSend = sinkFreeSpaceBeforeSend + allowedOvercommit;
        CA_LOG_D("About to drain sink " << outputIndex
            << ". FreeSpace: " << sinkFreeSpaceBeforeSend
            << ", allowedOvercommit: " << allowedOvercommit
            << ", toSend: " << toSend
                 //<< ", finished: " << sinkInfo.Buffer->IsFinished());
            );

        sinkInfo.PopStarted = true;
        ProcessOutputsState.Inflight ++;
        sinkInfo.FreeSpaceBeforeSend = sinkFreeSpaceBeforeSend;
        this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvSinkPop(outputIndex, sinkFreeSpaceBeforeSend));
    }

    bool DoHandleChannelsAfterFinishImpl() override {
        Y_VERIFY(Checkpoints);
        auto req = GetCheckpointRequest();
        if (!req.Defined()) {
            return true;  // handled channels syncronously
        }
        CA_LOG_D("DoHandleChannelsAfterFinishImpl");
        this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvContinueRun(std::move(req), /* checkpointOnly = */ true));
        return false;
    }

    void PeerFinished(ui64 channelId) override {
        // no TaskRunner => no outputChannel.Channel, nothing to Finish
        TOutputChannelInfo* outputChannel = OutputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(outputChannel, "task: " << Task.GetId() << ", output channelId: " << channelId);

        outputChannel->Finished = true;
        this->Send(TaskRunnerActorId, MakeHolder<NTaskRunnerActor::TEvPush>(channelId, /* finish = */ true, /* askFreeSpace = */ false, /* pauseAfterPush = */ false, /* isOut = */ true), 0, Cookie);  // finish channel
        DoExecute();
    }

    void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, TAsyncInputInfoBase& source, i64 space, bool finished) override {
        if (space <= 0) {
            return;
        }

        ProcessSourcesState.Inflight ++;
        source.PushStarted = true;
        source.Finished = finished;
        TaskRunnerActor->AsyncInputPush(Cookie++, source.Index, std::move(batch), space, finished);
    }

    void TakeInputChannelData(NDqProto::TChannelData&& channelData, bool ack) override {
        CA_LOG_D("task: " << Task.GetId() << ", took input");
        TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(channelData.GetChannelId());
        YQL_ENSURE(inputChannel, "task: " << Task.GetId() << ", unknown input channelId: " << channelData.GetChannelId());

        auto finished = channelData.GetFinished();

        auto ev = (channelData.GetData().GetRows())
            ? MakeHolder<NTaskRunnerActor::TEvPush>(
                channelData.GetChannelId(),
                std::move(*channelData.MutableData()),
                finished,
                /*askFreeSpace = */ true,
                /* pauseAfterPush = */ channelData.HasCheckpoint())
            : MakeHolder<NTaskRunnerActor::TEvPush>(
                channelData.GetChannelId(), finished, /*askFreeSpace = */ true, /* pauseAfterPush = */ channelData.HasCheckpoint());

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

    TMaybe<NTaskRunnerActor::TCheckpointRequest> GetCheckpointRequest() {
        TMaybe<NTaskRunnerActor::TCheckpointRequest> req = Nothing();
        if (!CheckpointRequestedFromTaskRunner && Checkpoints && Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved()) {
            CheckpointRequestedFromTaskRunner = true;
            req = {GetIds(OutputChannelsMap), GetIds(SinksMap), Checkpoints->GetPendingCheckpoint()};
        }
        return req;
    }

    void DoExecuteImpl() override {
        PollAsyncInput();
        if (ProcessSourcesState.Inflight == 0) {
            auto req = GetCheckpointRequest();
            CA_LOG_D("DoExecuteImpl: " << (bool) req);
            this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvContinueRun(std::move(req), /* checkpointOnly = */ false));
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

    i64 AsyncIoFreeSpace(TAsyncInputInfoBase& source) override {
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

    void OnTaskRunnerCreated(NTaskRunnerActor::TEvTaskRunnerCreateFinished::TPtr& ev, const NActors::TActorContext& ) {
        const auto& secureParams = ev->Get()->SecureParams;
        const auto& taskParams = ev->Get()->TaskParams;
        const auto& typeEnv = ev->Get()->TypeEnv;
        const auto& holderFactory = ev->Get()->HolderFactory;

        TypeEnv = const_cast<NKikimr::NMiniKQL::TTypeEnvironment*>(&typeEnv);
        FillIoMaps(holderFactory, typeEnv, secureParams, taskParams);

        {
            // say "Hello" to executer
            auto ev = MakeHolder<TEvDqCompute::TEvState>();
            ev->Record.SetState(NDqProto::COMPUTE_STATE_EXECUTING);
            ev->Record.SetTaskId(Task.GetId());

            this->Send(ExecuterId, ev.Release(), NActors::IEventHandle::FlagTrackDelivery);
        }

        ContinueExecute();
    }

    bool ReadyToCheckpoint() const override {
        return ReadyToCheckpointFlag;
    }

    void OnRunFinished(NTaskRunnerActor::TEvTaskRunFinished::TPtr& ev, const NActors::TActorContext& ) {
        MkqlMemoryLimit = ev->Get()->MkqlMemoryLimit;
        ProfileStats = std::move(ev->Get()->ProfileStats);
        auto sourcesState = GetSourcesState();
        auto status = ev->Get()->RunStatus;

        CA_LOG_D("Resume execution, run status: " << status << " checkpoint: " << (bool) ev->Get()->ProgramState);

        for (const auto& [channelId, freeSpace] : ev->Get()->InputChannelFreeSpace) {
            auto it = InputChannelsMap.find(channelId);
            if (it != InputChannelsMap.end()) {
                it->second.FreeSpace = freeSpace;
            }
        }

        if (status != ERunStatus::Finished) {
            PollSources(std::move(sourcesState));
        }

        ReadyToCheckpointFlag = (bool) ev->Get()->ProgramState;
        if (ev->Get()->CheckpointRequestedFromTaskRunner) {
            CheckpointRequestedFromTaskRunner = false;
        }
        if (ReadyToCheckpointFlag) {
            Y_VERIFY(!ProgramState);
            ProgramState = std::move(ev->Get()->ProgramState);
            Checkpoints->DoCheckpoint();
        }
        ProcessOutputsImpl(status);
        if (status == ERunStatus::Finished) {
            ReportStats(TInstant::Now());
        }
    }

    void SaveState(const NDqProto::TCheckpoint& checkpoint, NDqProto::TComputeActorState& state) const override {
        CA_LOG_D("Save state");
        Y_VERIFY(ProgramState);
        state.MutableMiniKqlProgram()->Swap(&*ProgramState);
        ProgramState.Destroy();

        // TODO:(whcrc) maybe save Sources before Program?
        for (auto& [inputIndex, source] : SourcesMap) {
            YQL_ENSURE(source.AsyncInput, "Source[" << inputIndex << "] is not created");
            NDqProto::TSourceState& sourceState = *state.AddSources();
            source.AsyncInput->SaveState(checkpoint, sourceState);
            sourceState.SetInputIndex(inputIndex);
        }
    }

    void OnAsyncInputPushFinished(NTaskRunnerActor::TEvAsyncInputPushFinished::TPtr& ev, const NActors::TActorContext& ) {
        auto it = SourcesMap.find(ev->Get()->Index);
        Y_VERIFY(it != SourcesMap.end());
        auto& source = it->second;
        source.PushStarted = false;
        // source.FreeSpace = ev->Get()->FreeSpace; TODO:XXX get freespace on run
        ProcessSourcesState.Inflight--;
        if (ProcessSourcesState.Inflight == 0) {
            CA_LOG_D("send TEvContinueRun on OnAsyncInputPushFinished");
            this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvContinueRun());
        }
    }

    void OnPopFinished(NTaskRunnerActor::TEvChannelPopFinished::TPtr& ev, const NActors::TActorContext&) {
        if (ev->Get()->Stats) {
            TaskRunnerStats = std::move(ev->Get()->Stats);
        }
        CA_LOG_D("OnPopFinished, stats: " << *TaskRunnerStats.Get());
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

        if (ev->Get()->Checkpoint.Defined()) {
            ResumeInputs();
        }
        for (ui32 i = 0; i < ev->Get()->Data.size(); i++) {
            auto& chunk = ev->Get()->Data[i];
            NDqProto::TChannelData channelData;
            channelData.SetChannelId(channelId);
            // set finished only for last chunk
            channelData.SetFinished(finished && i==ev->Get()->Data.size()-1);
            if (i==ev->Get()->Data.size()-1 && ev->Get()->Checkpoint.Defined()) {
                channelData.MutableCheckpoint()->Swap(&*ev->Get()->Checkpoint);
            }
            channelData.MutableData()->Swap(&chunk);
            Channels->SendChannelData(std::move(channelData));
        }
        if (ev->Get()->Data.empty() && dataWasSent) {
            NDqProto::TChannelData channelData;
            channelData.SetChannelId(channelId);
            channelData.SetFinished(finished);
            if (ev->Get()->Checkpoint.Defined()) {
                channelData.MutableCheckpoint()->Swap(&*ev->Get()->Checkpoint);
            }
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

        TAsyncOutputInfoBase& sinkInfo = it->second;
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

        {
            auto guard = BindAllocator();
            NKikimr::NMiniKQL::TUnboxedValueVector data = std::move(batch);
            sinkInfo.AsyncOutput->SendData(std::move(data), size, std::move(checkpoint), finished);
        }

        Y_VERIFY(batch.empty());
        CA_LOG_D("sink " << outputIndex << ": sent " << dataSize << " bytes of data and " << checkpointSize << " bytes of checkpoint barrier");

        CA_LOG_D("Drain sink " << outputIndex
            << ". Free space decreased: " << (sinkInfo.FreeSpaceBeforeSend - sinkInfo.AsyncOutput->GetFreeSpace())
            << ", sent data from buffer: " << dataSize);

        ProcessOutputsState.DataWasSent |= dataWasSent;
        ProcessOutputsState.AllOutputsFinished =
            FinishedOutputChannels.size() == OutputChannelsMap.size() &&
            FinishedSinks.size() == SinksMap.size();
        CheckRunStatus();
    }

    // for logging in the base class
    ui64 GetMkqlMemoryLimit() const override {
        return MkqlMemoryLimit;
    }

    const TDqMemoryQuota::TProfileStats* GetProfileStats() const override {
        return &ProfileStats;
    }

    const NYql::NDq::TTaskRunnerStatsBase* GetTaskRunnerStats() override {
        return TaskRunnerStats.Get();
    }

    template<typename TSecond>
    TVector<ui32> GetIds(const THashMap<ui64, TSecond>& collection) {
        TVector<ui32> ids;
        std::transform(collection.begin(), collection.end(), std::back_inserter(ids), [](const auto& p) {
            return p.first;
        });
        return ids;
    }

    void InjectBarrierToOutputs(const NDqProto::TCheckpoint&) override {
        Y_VERIFY(CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
        // already done in task_runner_actor
    }

    void DoLoadRunnerState(TString&& blob) override {
        this->Send(TaskRunnerActorId, new NTaskRunnerActor::TEvLoadTaskRunnerFromState(std::move(blob)));
    }

    void OnTaskRunnerLoaded(NTaskRunnerActor::TEvLoadTaskRunnerFromStateDone::TPtr& ev) {
        Checkpoints->AfterStateLoading(std::move(ev->Get()->Error));
    }

    NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv = nullptr;
    NTaskRunnerActor::ITaskRunnerActor* TaskRunnerActor = nullptr;
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
    NDq::TDqTaskRunnerStatsView TaskRunnerStats;
    bool ReadyToCheckpointFlag;
    TVector<std::pair<NActors::TActorId, ui64>> WaitingForStateResponse;
    bool SentStatsRequest;
    mutable THolder<NDqProto::TMiniKqlProgramState> ProgramState;
    ui64 MkqlMemoryLimit = 0;
    TDqMemoryQuota::TProfileStats ProfileStats;
    bool CheckpointRequestedFromTaskRunner = false;
};


IActor* CreateDqAsyncComputeActor(const TActorId& executerId, const TTxId& txId, NYql::NDqProto::TDqTask&& task,
    IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    const NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory)
{
    return new TDqAsyncComputeActor(executerId, txId, std::move(task), std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, taskRunnerActorFactory);
}

} // namespace NDq
} // namespace NYql
