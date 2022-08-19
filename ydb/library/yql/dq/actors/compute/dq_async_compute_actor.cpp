#include "dq_compute_actor.h"
#include "dq_async_compute_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/core/base/quoter.h>

namespace NYql {
namespace NDq {

constexpr TDuration MIN_QUOTED_CPU_TIME = TDuration::MilliSeconds(10);

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
        const NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
        const ::NMonitoring::TDynamicCounterPtr& taskCounters,
        const TActorId& quoterServiceActorId)
        : TBase(executerId, txId, std::move(task), std::move(asyncIoFactory), functionRegistry, settings, memoryLimits, /* ownMemoryQuota = */ false, false, taskCounters)
        , TaskRunnerActorFactory(taskRunnerActorFactory)
        , ReadyToCheckpointFlag(false)
        , SentStatsRequest(false)
        , QuoterServiceActorId(quoterServiceActorId)
    {
        InitExtraMonCounters(taskCounters);
    }

    void DoBootstrap() {
        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem)) {
            logger = [actorSystem, txId = GetTxId(), taskId = GetTask().GetId()] (const TString& message) {
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
            this, GetTxId(), GetTask().GetId(), std::move(inputWithDisabledCheckpointing), InitMemoryQuota());
        TaskRunnerActorId = RegisterWithSameMailbox(actor);

        TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = MemoryLimits.ChannelBufferSize;
        limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

        Become(&TDqAsyncComputeActor::StateFuncWrapper<&TDqAsyncComputeActor::StateFuncBody>);

        // TODO:
        std::shared_ptr<IDqTaskRunnerExecutionContext> execCtx = std::shared_ptr<IDqTaskRunnerExecutionContext>(new TDqTaskRunnerExecutionContext());

        Send(TaskRunnerActorId,
            new NTaskRunnerActor::TEvTaskRunnerCreate(
                GetTask(), limits, execCtx));

        CA_LOG_D("Use CPU quota: " << UseCpuQuota() << ". Rate limiter resource: { \"" << Task.GetRateLimiter() << "\", \"" << Task.GetRateLimiterResource() << "\" }");
    }

    void FillExtraStats(NDqProto::TDqComputeActorStats* /* dst */, bool /* last */) {
    }

    void InitExtraMonCounters(const ::NMonitoring::TDynamicCounterPtr& taskCounters) {
        if (taskCounters && UseCpuQuota()) {
            CpuTimeGetQuotaLatency = taskCounters->GetSubgroup("subsystem", "mkql")->GetHistogram("CpuTimeGetQuotaLatencyMs", NMonitoring::ExplicitHistogram({0, 1, 5, 10, 50, 100, 500, 1000, 10'000, 60'000, 600'000}));
            CpuTimeQuotaWaitDelay = taskCounters->GetSubgroup("subsystem", "mkql")->GetHistogram("CpuTimeQuotaWaitDelayMs", NMonitoring::ExplicitHistogram({0, 1, 5, 10, 50, 100, 500, 1000, 10'000, 60'000, 600'000}));
        }
    }

private:
    STFUNC(StateFuncBody) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NTaskRunnerActor::TEvTaskRunFinished, OnRunFinished);
            HFunc(NTaskRunnerActor::TEvAsyncInputPushFinished, OnAsyncInputPushFinished);
            HFunc(NTaskRunnerActor::TEvChannelPopFinished, OnPopFinished);
            HFunc(NTaskRunnerActor::TEvTaskRunnerCreateFinished, OnTaskRunnerCreated);
            HFunc(NTaskRunnerActor::TEvContinueRun, OnContinueRun); // push finished
            hFunc(TEvDqCompute::TEvStateRequest, OnStateRequest);
            hFunc(NTaskRunnerActor::TEvStatistics, OnStatisticsResponse);
            hFunc(NTaskRunnerActor::TEvLoadTaskRunnerFromStateDone, OnTaskRunnerLoaded);
            hFunc(TEvDqCompute::TEvInjectCheckpoint, OnInjectCheckpoint);
            hFunc(TEvDqCompute::TEvRestoreFromCheckpoint, OnRestoreFromCheckpoint);
            hFunc(NKikimr::TEvQuota::TEvClearance, OnCpuQuotaGiven);
            default:
                TBase::BaseStateFuncBody(ev, ctx);
        };
    };

    void OnStateRequest(TEvDqCompute::TEvStateRequest::TPtr& ev) {
        CA_LOG_T("Got TEvStateRequest from actor " << ev->Sender << " TaskId: " << Task.GetId() << " PingCookie: " << ev->Cookie);
        if (!SentStatsRequest) {
            Send(TaskRunnerActorId, new NTaskRunnerActor::TEvStatistics(GetIds(SinksMap)));
            SentStatsRequest = true;
        }
        WaitingForStateResponse.push_back({ev->Sender, ev->Cookie});
    }

    void FillIssues(NYql::TIssues& issues) {
        auto applyToAllIssuesHolders = [this](auto& function){
            function(SourcesMap);
            function(InputTransformsMap);
            function(SinksMap);
            function(OutputTransformsMap);
        };

        uint64_t expectingSize = 0;
        auto addSize = [&expectingSize](auto& issuesHolder) {
            for (auto& [inputIndex, source]: issuesHolder) {
                expectingSize += source.IssuesBuffer.GetAllAddedIssuesCount();
            }
        };

        auto exhaustIssues = [&issues](auto& issuesHolder){
            for (auto& [inputIndex, source]: issuesHolder) {
            auto sourceIssues = source.IssuesBuffer.Dump();
                for (auto& issueInfo: sourceIssues) {
                    issues.AddIssues(issueInfo.Issues);
                }
            }
        };

        applyToAllIssuesHolders(addSize);
        issues.Reserve(expectingSize);
        applyToAllIssuesHolders(exhaustIssues);
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
        NYql::TIssues issues;
        FillIssues(issues);

        IssuesToMessage(issues, record.MutableIssues());
        FillStats(record.MutableStats(), /* last */ false);
        for (const auto& [actorId, cookie] : WaitingForStateResponse) {
            auto state = MakeHolder<TEvDqCompute::TEvState>();
            state->Record = record;
            Send(actorId, std::move(state), NActors::IEventHandle::FlagTrackDelivery, cookie);
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

        CA_LOG_T("About to drain channelId: " << channelId
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
        ProcessOutputsState.Inflight++;
        if (toSend <= 0) {
            if (Y_UNLIKELY(outputChannel.Stats)) {
                outputChannel.Stats->BlockedByCapacity++;
            }
            CA_LOG_D("Cannot drain channel cause it blocked by capacity, channelId: " << channelId);
            auto ev = MakeHolder<NTaskRunnerActor::TEvChannelPopFinished>(channelId);
            Y_VERIFY(!ev->Finished);
            Send(SelfId(), std::move(ev));  // try again, ev.Finished == false
            return;
        }

        Send(TaskRunnerActorId, new NTaskRunnerActor::TEvPop(channelId, wasFinished, toSend));
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
        CA_LOG_T("About to drain sink " << outputIndex
            << ". FreeSpace: " << sinkFreeSpaceBeforeSend
            << ", allowedOvercommit: " << allowedOvercommit
            << ", toSend: " << toSend
                 //<< ", finished: " << sinkInfo.Buffer->IsFinished());
            );

        sinkInfo.PopStarted = true;
        ProcessOutputsState.Inflight++;
        sinkInfo.FreeSpaceBeforeSend = sinkFreeSpaceBeforeSend;
        Send(TaskRunnerActorId, new NTaskRunnerActor::TEvSinkPop(outputIndex, sinkFreeSpaceBeforeSend));
    }

    bool DoHandleChannelsAfterFinishImpl() override {
        Y_VERIFY(Checkpoints);
        auto req = GetCheckpointRequest();
        if (!req.Defined()) {
            return true;  // handled channels syncronously
        }
        CA_LOG_D("DoHandleChannelsAfterFinishImpl");
        AskContinueRun(std::make_unique<NTaskRunnerActor::TEvContinueRun>(std::move(req), /* checkpointOnly = */ true));
        return false;
    }

    void PeerFinished(ui64 channelId) override {
        // no TaskRunner => no outputChannel.Channel, nothing to Finish
        CA_LOG_I("Peer finished, channel: " << channelId);
        TOutputChannelInfo* outputChannel = OutputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(outputChannel, "task: " << Task.GetId() << ", output channelId: " << channelId);

        outputChannel->Finished = true;
        ProcessOutputsState.Inflight++;
        Send(TaskRunnerActorId, MakeHolder<NTaskRunnerActor::TEvPop>(channelId, /* wasFinished = */ true, 0));  // finish channel
        DoExecute();
    }

    void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, TAsyncInputInfoBase& source, i64 space, bool finished) override {
        ProcessSourcesState.Inflight++;
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

        Send(TaskRunnerActorId, ev.Release(), 0, Cookie);

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
        if (UseCpuQuota() && CpuTimeSpent) {
            // Send the rest of CPU time that we haven't taken into account
            Send(QuoterServiceActorId,
                new NKikimr::TEvQuota::TEvRequest(
                    NKikimr::TEvQuota::EResourceOperator::And,
                    { NKikimr::TEvQuota::TResourceLeaf(Task.GetRateLimiter(), Task.GetRateLimiterResource(), CpuTimeSpent.MilliSeconds(), true) },
                    TDuration::Max()));
        }
        TBase::PassAway();
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
        TrySendAsyncChannelsData();

        PollAsyncInput();
        if (ProcessSourcesState.Inflight == 0) {
            auto req = GetCheckpointRequest();
            CA_LOG_T("DoExecuteImpl: " << (bool) req);
            AskContinueRun(std::make_unique<NTaskRunnerActor::TEvContinueRun>(std::move(req), /* checkpointOnly = */ false));
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

            Send(ExecuterId, ev.Release(), NActors::IEventHandle::FlagTrackDelivery);
        }

        if (DeferredInjectCheckpointEvent) {
            ForwardToCheckpoints(std::move(DeferredInjectCheckpointEvent));
        }
        if (DeferredRestoreFromCheckpointEvent) {
            ForwardToCheckpoints(std::move(DeferredRestoreFromCheckpointEvent));
        }

        ContinueExecute();
    }

    bool ReadyToCheckpoint() const override {
        return ReadyToCheckpointFlag;
    }

    void OnRunFinished(NTaskRunnerActor::TEvTaskRunFinished::TPtr& ev, const NActors::TActorContext& ) {
        ContinueRunInflight = false;
        TrySendAsyncChannelsData(); // send from previous cycle

        MkqlMemoryLimit = ev->Get()->MkqlMemoryLimit;
        ProfileStats = std::move(ev->Get()->ProfileStats);
        auto sourcesState = GetSourcesState();
        auto status = ev->Get()->RunStatus;

        CA_LOG_T("Resume execution, run status: " << status << " checkpoint: " << (bool) ev->Get()->ProgramState);

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

        if (UseCpuQuota()) {
            CpuTimeSpent += ev->Get()->ComputeTime;
            AskCpuQuota();
            ProcessContinueRun();
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
            CA_LOG_T("send TEvContinueRun on OnAsyncInputPushFinished");
            AskContinueRun(std::make_unique<NTaskRunnerActor::TEvContinueRun>());
        }
    }

    void OnPopFinished(NTaskRunnerActor::TEvChannelPopFinished::TPtr& ev, const NActors::TActorContext&) {
        if (ev->Get()->Stats) {
            TaskRunnerStats = std::move(ev->Get()->Stats);
        }
        CA_LOG_T("OnPopFinished, stats: " << *TaskRunnerStats.Get());
        auto it = OutputChannelsMap.find(ev->Get()->ChannelId);
        Y_VERIFY(it != OutputChannelsMap.end());
        TOutputChannelInfo& outputChannel = it->second;
        Y_VERIFY(!outputChannel.AsyncData); // have finished previous cycle
        outputChannel.AsyncData.ConstructInPlace();
        outputChannel.AsyncData->Data = std::move(ev->Get()->Data);
        outputChannel.AsyncData->Checkpoint = std::move(ev->Get()->Checkpoint);
        outputChannel.AsyncData->Finished = ev->Get()->Finished;
        outputChannel.AsyncData->Changed = ev->Get()->Changed;

        if (TrySendAsyncChannelData(outputChannel)) {
            CheckRunStatus();
        }
    }

    bool TrySendAsyncChannelData(TOutputChannelInfo& outputChannel) {
        if (!outputChannel.AsyncData) {
            return false;
        }

        // If the channel has finished, then the data received after drain is no longer needed
        const bool shouldSkipData = Channels->ShouldSkipData(outputChannel.ChannelId);
        if (!shouldSkipData && !Channels->CanSendChannelData(outputChannel.ChannelId)) { // When channel will be connected, they will call resume execution.
            return false;
        }

        auto& asyncData = *outputChannel.AsyncData;
        outputChannel.Finished = asyncData.Finished || shouldSkipData;
        if (outputChannel.Finished) {
            FinishedOutputChannels.insert(outputChannel.ChannelId);
        }

        outputChannel.PopStarted = false;
        ProcessOutputsState.Inflight--;
        ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
        ProcessOutputsState.LastPopReturnedNoData = asyncData.Data.empty();

        if (!shouldSkipData) {
            if (asyncData.Checkpoint.Defined()) {
                ResumeInputs();
            }
            for (ui32 i = 0; i < asyncData.Data.size(); i++) {
                auto& chunk = asyncData.Data[i];
                NDqProto::TChannelData channelData;
                channelData.SetChannelId(outputChannel.ChannelId);
                // set finished only for last chunk
                const bool lastChunk = i == asyncData.Data.size() - 1;
                channelData.SetFinished(asyncData.Finished && lastChunk);
                if (lastChunk && asyncData.Checkpoint.Defined()) {
                    channelData.MutableCheckpoint()->Swap(&*asyncData.Checkpoint);
                }
                channelData.MutableData()->Swap(&chunk);
                Channels->SendChannelData(std::move(channelData));
            }
            if (asyncData.Data.empty() && asyncData.Changed) {
                NDqProto::TChannelData channelData;
                channelData.SetChannelId(outputChannel.ChannelId);
                channelData.SetFinished(asyncData.Finished);
                if (asyncData.Checkpoint.Defined()) {
                    channelData.MutableCheckpoint()->Swap(&*asyncData.Checkpoint);
                }
                Channels->SendChannelData(std::move(channelData));
            }
        }

        ProcessOutputsState.DataWasSent |= asyncData.Changed;

        ProcessOutputsState.AllOutputsFinished =
            FinishedOutputChannels.size() == OutputChannelsMap.size() &&
            FinishedSinks.size() == SinksMap.size();

        outputChannel.AsyncData = Nothing();

        return true;
    }

    bool TrySendAsyncChannelsData() {
        bool result = false;
        for (auto& [channelId, outputChannel] : OutputChannelsMap) {
            result |= TrySendAsyncChannelData(outputChannel);
        }
        if (result) {
            CheckRunStatus();
        }
        return result;
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
        ProcessOutputsState.Inflight--;
        ProcessOutputsState.HasDataToSend |= !sinkInfo.Finished;

        {
            auto guard = BindAllocator();
            NKikimr::NMiniKQL::TUnboxedValueVector data = std::move(batch);
            sinkInfo.AsyncOutput->SendData(std::move(data), size, std::move(checkpoint), finished);
        }

        Y_VERIFY(batch.empty());
        CA_LOG_T("Sink " << outputIndex << ": sent " << dataSize << " bytes of data and " << checkpointSize << " bytes of checkpoint barrier");

        CA_LOG_T("Drain sink " << outputIndex
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
        Send(TaskRunnerActorId, new NTaskRunnerActor::TEvLoadTaskRunnerFromState(std::move(blob)));
    }

    void OnTaskRunnerLoaded(NTaskRunnerActor::TEvLoadTaskRunnerFromStateDone::TPtr& ev) {
        Checkpoints->AfterStateLoading(std::move(ev->Get()->Error));
    }

    template <class TEvPtr>
    void ForwardToCheckpoints(TEvPtr&& ev) {
        auto* x = reinterpret_cast<TAutoPtr<NActors::IEventHandle>*>(&ev);
        Checkpoints->Receive(*x, TActivationContext::AsActorContext());
        ev = nullptr;
    }

    void OnInjectCheckpoint(TEvDqCompute::TEvInjectCheckpoint::TPtr& ev) {
        if (TypeEnv) {
            ForwardToCheckpoints(std::move(ev));
        } else {
            Y_VERIFY(!DeferredInjectCheckpointEvent);
            DeferredInjectCheckpointEvent = std::move(ev);
        }
    }

    void OnRestoreFromCheckpoint(TEvDqCompute::TEvRestoreFromCheckpoint::TPtr& ev) {
        if (TypeEnv) {
            ForwardToCheckpoints(std::move(ev));
        } else {
            Y_VERIFY(!DeferredRestoreFromCheckpointEvent);
            DeferredRestoreFromCheckpointEvent = std::move(ev);
        }
    }

    bool UseCpuQuota() const {
        return QuoterServiceActorId && Task.GetRateLimiter() && Task.GetRateLimiterResource();
    }

    void AskCpuQuota() {
        Y_VERIFY(!CpuTimeQuotaAsked);
        if (CpuTimeSpent >= MIN_QUOTED_CPU_TIME) {
            CA_LOG_D("Ask CPU quota: " << CpuTimeSpent.MilliSeconds() << "ms");
            Send(QuoterServiceActorId,
                new NKikimr::TEvQuota::TEvRequest(
                    NKikimr::TEvQuota::EResourceOperator::And,
                    { NKikimr::TEvQuota::TResourceLeaf(Task.GetRateLimiter(), Task.GetRateLimiterResource(), CpuTimeSpent.MilliSeconds(), true) },
                    TDuration::Max()));
            CpuTimeQuotaAsked = TInstant::Now();
            CpuTimeSpent = TDuration::Zero();
        }
    }

    void AskContinueRun(std::unique_ptr<NTaskRunnerActor::TEvContinueRun> continueRunEvent) {
        if (!UseCpuQuota()) {
            Send(TaskRunnerActorId, continueRunEvent.release());
            return;
        }

        ContinueRunEvents.emplace_back(std::move(continueRunEvent), TInstant::Now());
        ProcessContinueRun();
    }

    void ProcessContinueRun() {
        if (!ContinueRunEvents.empty() && !CpuTimeQuotaAsked && !ContinueRunInflight) {
            Send(TaskRunnerActorId, ContinueRunEvents.front().first.release());
            const TDuration quotaWaitDelay = TInstant::Now() - ContinueRunEvents.front().second;
            CpuTimeQuotaWaitDelay->Collect(quotaWaitDelay.MilliSeconds());
            ContinueRunEvents.pop_front();
            ContinueRunInflight = true;
        }
    }

    void OnCpuQuotaGiven(NKikimr::TEvQuota::TEvClearance::TPtr& ev) {
        const TDuration delay = TInstant::Now() - CpuTimeQuotaAsked;
        CpuTimeQuotaAsked = TInstant::Zero();
        CA_LOG_D("CPU quota delay: " << delay.MilliSeconds() << "ms");
        if (CpuTimeGetQuotaLatency) {
            CpuTimeGetQuotaLatency->Collect(delay.MilliSeconds());
        }

        if (ev->Get()->Result != NKikimr::TEvQuota::TEvClearance::EResult::Success) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssue("Error getting CPU quota"));
            return;
        }

        ProcessContinueRun();
    }

    NKikimr::NMiniKQL::TTypeEnvironment* TypeEnv = nullptr;
    NTaskRunnerActor::ITaskRunnerActor* TaskRunnerActor = nullptr;
    NActors::TActorId TaskRunnerActorId;
    NTaskRunnerActor::ITaskRunnerActorFactory::TPtr TaskRunnerActorFactory;
    TEvDqCompute::TEvInjectCheckpoint::TPtr DeferredInjectCheckpointEvent;
    TEvDqCompute::TEvRestoreFromCheckpoint::TPtr DeferredRestoreFromCheckpointEvent;

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

    // Cpu quota
    TActorId QuoterServiceActorId;
    TInstant CpuTimeQuotaAsked;
    TDuration CpuTimeSpent;
    std::deque<std::pair<std::unique_ptr<NTaskRunnerActor::TEvContinueRun>, TInstant>> ContinueRunEvents;
    bool ContinueRunInflight = false;
    NMonitoring::THistogramPtr CpuTimeGetQuotaLatency;
    NMonitoring::THistogramPtr CpuTimeQuotaWaitDelay;
};


IActor* CreateDqAsyncComputeActor(const TActorId& executerId, const TTxId& txId, NYql::NDqProto::TDqTask&& task,
    IDqAsyncIoFactory::TPtr asyncIoFactory,
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    const NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    const TActorId& quoterServiceActorId)
{
    return new TDqAsyncComputeActor(executerId, txId, std::move(task), std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, taskRunnerActorFactory, taskCounters, quoterServiceActorId);
}

} // namespace NDq
} // namespace NYql
