#pragma once

#include "dq_compute_actor.h"
#include "dq_compute_actor_channels.h"
#include "dq_compute_actor_checkpoints.h"
#include "dq_compute_actor_sinks.h"
#include "dq_compute_actor_sources.h"
#include "dq_compute_issues_buffer.h" 

#include <ydb/core/scheme/scheme_tabledefs.h> // TODO: TTableId
#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/tablet_flat/util_basics.h> // TODO: IDestructable

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <library/cpp/actors/core/interconnect.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>
#include <util/system/hostname.h>

#if defined CA_LOG_D || defined CA_LOG_I || defined CA_LOG_E || defined CA_LOG_C
#   error log macro definition clash
#endif

#define CA_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". " << s)
#define CA_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". " << s)
#define CA_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". " << s)
#define CA_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". " << s)
#define CA_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". " << s)
#define CA_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". " << s)
#define CA_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". " << s)
#define CA_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". " << s)


namespace NYql {
namespace NDq {

enum : ui64 {
    ComputeActorNonProtobufStateVersion = 1,
    ComputeActorCurrentStateVersion = 2,
};

constexpr ui32 IssuesBufferSize = 16; 

template<typename TDerived>
class TDqComputeActorBase : public NActors::TActorBootstrapped<TDerived>
                          , public TDqComputeActorChannels::ICallbacks
                          , public TDqComputeActorCheckpoints::ICallbacks
                          , public IDqSourceActor::ICallbacks
                          , public IDqSinkActor::ICallbacks
{
protected:
    enum EEvWakeupTag : ui64 {
        TimeoutTag = 1,
        PeriodicStatsTag = 2,
        RlSendAllowedTag = 101,
        RlNoResourceTag = 102,
    };

public:
    void Bootstrap() {
        try {
            CA_LOG_D("Start compute actor " << this->SelfId() << ", task: " << Task.GetId());

            Channels = new TDqComputeActorChannels(this->SelfId(), TxId, Task, !RuntimeSettings.FailOnUndelivery,
                RuntimeSettings.StatsMode, MemoryLimits.ChannelBufferSize, this, this->GetActivityType());
            this->RegisterWithSameMailbox(Channels);

            if (RuntimeSettings.Timeout) {
                CA_LOG_D("Set execution timeout " << *RuntimeSettings.Timeout);
                this->Schedule(*RuntimeSettings.Timeout, new NActors::TEvents::TEvWakeup(EEvWakeupTag::TimeoutTag));
            }

            if (auto reportStatsSettings = RuntimeSettings.ReportStatsSettings) {
                if (reportStatsSettings->MaxInterval) {
                    CA_LOG_D("Set periodic stats " << reportStatsSettings->MaxInterval);
                    this->Schedule(reportStatsSettings->MaxInterval, new NActors::TEvents::TEvWakeup(EEvWakeupTag::PeriodicStatsTag));
                }
            }

            if (SayHelloOnBootstrap()) {
                // say "Hello" to executer
                auto ev = MakeHolder<TEvDqCompute::TEvState>();
                ev->Record.SetState(NDqProto::COMPUTE_STATE_EXECUTING);
                ev->Record.SetTaskId(Task.GetId());

                this->Send(ExecuterId, ev.Release(), NActors::IEventHandle::FlagTrackDelivery);
                this->Become(&TDqComputeActorBase::StateFuncBase);
            }

            static_cast<TDerived*>(this)->DoBootstrap();
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            InternalError(TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Mkql memory limit exceeded, limit: " << MkqlMemoryLimit
                << ", host: " << HostName()
                << ", canAllocateExtraMemory: " << CanAllocateExtraMemory);
        } catch (const yexception& e) {
            InternalError(TIssuesIds::DEFAULT_ERROR, e.what());
        }

        ReportEventElapsedTime();
    }

protected:
    TDqComputeActorBase(const NActors::TActorId& executerId, const TTxId& txId, NDqProto::TDqTask&& task,
        IDqSourceActorFactory::TPtr sourceActorFactory, IDqSinkActorFactory::TPtr sinkActorFactory,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits)
        : ExecuterId(executerId)
        , TxId(txId)
        , Task(std::move(task))
        , RuntimeSettings(settings)
        , MemoryLimits(memoryLimits)
        , CanAllocateExtraMemory(RuntimeSettings.ExtraMemoryAllocationPool != 0 && MemoryLimits.AllocateMemoryFn)
        , SourceActorFactory(std::move(sourceActorFactory))
        , SinkActorFactory(std::move(sinkActorFactory))
        , CheckpointingMode(GetTaskCheckpointingMode(Task))
        , State(Task.GetCreateSuspended() ? NDqProto::COMPUTE_STATE_UNKNOWN : NDqProto::COMPUTE_STATE_EXECUTING)
        , Running(!Task.GetCreateSuspended())
    {
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            BasicStats = std::make_unique<TBasicStats>();
        }
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_PROFILE) {
            ProfileStats = std::make_unique<TProfileStats>();
        }
        InitializeTask();
    }

    TDqComputeActorBase(const NActors::TActorId& executerId, const TTxId& txId, const NDqProto::TDqTask& task,
        IDqSourceActorFactory::TPtr sourceActorFactory, IDqSinkActorFactory::TPtr sinkActorFactory,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits)
        : ExecuterId(executerId)
        , TxId(txId)
        , Task(task)
        , RuntimeSettings(settings)
        , MemoryLimits(memoryLimits)
        , CanAllocateExtraMemory(RuntimeSettings.ExtraMemoryAllocationPool != 0 && MemoryLimits.AllocateMemoryFn)
        , SourceActorFactory(std::move(sourceActorFactory))
        , SinkActorFactory(std::move(sinkActorFactory))
        , State(Task.GetCreateSuspended() ? NDqProto::COMPUTE_STATE_UNKNOWN : NDqProto::COMPUTE_STATE_EXECUTING)
        , Running(!Task.GetCreateSuspended())
    {
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            BasicStats = std::make_unique<TBasicStats>();
        }
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_PROFILE) {
            ProfileStats = std::make_unique<TProfileStats>();
        }
        InitializeTask();
    }

    void ReportEventElapsedTime() {
        if (BasicStats) {
            ui64 elapsedMicros = NActors::TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000;
            BasicStats->CpuTime += TDuration::MicroSeconds(elapsedMicros);
        }
    }

    TString GetEventTypeString(TAutoPtr<::NActors::IEventHandle>& ev) {
        try {
            if (NActors::IEventBase* eventBase = ev->GetBase()) {
                return eventBase->ToStringHeader();
            }
        } catch (...) {
        }
        return "Unknown type";
    }

    STFUNC(StateFuncBase) {
        const bool reportTime = this->CurrentStateFunc() == &TDqComputeActorBase::StateFuncBase;

        try {
            switch (ev->GetTypeRewrite()) {
                hFunc(TEvDqCompute::TEvResumeExecution, HandleExecuteBase);
                hFunc(TEvDqCompute::TEvChannelsInfo, HandleExecuteBase);
                hFunc(TEvDq::TEvAbortExecution, HandleExecuteBase);
                hFunc(NActors::TEvents::TEvWakeup, HandleExecuteBase);
                hFunc(NActors::TEvents::TEvUndelivered, HandleExecuteBase);
                FFunc(TEvDqCompute::TEvChannelData::EventType, Channels->Receive);
                FFunc(TEvDqCompute::TEvChannelDataAck::EventType, Channels->Receive);
                hFunc(TEvDqCompute::TEvRun, HandleExecuteBase);
                hFunc(TEvDqCompute::TEvStateRequest, HandleExecuteBase);
                hFunc(TEvDqCompute::TEvNewCheckpointCoordinator, HandleExecuteBase);
                FFunc(TEvDqCompute::TEvInjectCheckpoint::EventType, Checkpoints->Receive);
                FFunc(TEvDqCompute::TEvCommitState::EventType, Checkpoints->Receive);
                FFunc(TEvDqCompute::TEvRestoreFromCheckpoint::EventType, Checkpoints->Receive);
                hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, HandleExecuteBase);
                hFunc(NActors::TEvInterconnect::TEvNodeConnected, HandleExecuteBase);
                default: {
                    CA_LOG_C("TDqComputeActorBase, unexpected event: " << ev->GetTypeRewrite() << " (" << GetEventTypeString(ev) << ")");
                    InternalError(TIssuesIds::DEFAULT_ERROR, "Unexpected event");
                }
            }
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            InternalError(TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Mkql memory limit exceeded, limit: " << MkqlMemoryLimit
                << ", host: " << HostName()
                << ", canAllocateExtraMemory: " << CanAllocateExtraMemory);
        } catch (const yexception& e) {
            InternalError(TIssuesIds::DEFAULT_ERROR, e.what());
        }

        if (reportTime) {
            ReportEventElapsedTime();
        }
    }

protected:
    void DoExecute() {
        auto guard = BindAllocator();
        auto* alloc = guard.GetMutex();

        if (State == NDqProto::COMPUTE_STATE_FINISHED) {
            DoHandleChannelsAfterFinishImpl();
        } else {
            DoExecuteImpl();
        }

        if (alloc->GetAllocated() - alloc->GetUsed() > MemoryLimits.MinMemFreeSize) {
            alloc->ReleaseFreePages();
            if (MemoryLimits.FreeMemoryFn) {
                auto newLimit = std::max(alloc->GetAllocated(), CalcMkqlMemoryLimit());
                if (MkqlMemoryLimit > newLimit) {
                    auto freedSize = MkqlMemoryLimit - newLimit;
                    MkqlMemoryLimit = newLimit;
                    alloc->SetLimit(newLimit);
                    MemoryLimits.FreeMemoryFn(TxId, Task.GetId(), freedSize);
                    CA_LOG_I("[Mem] memory shrinked, new limit: " << MkqlMemoryLimit);
                }
            }
        }

        auto now = TInstant::Now();

        if (Y_UNLIKELY(ProfileStats)) {
            ProfileStats->MkqlMaxUsedMemory = std::max(ProfileStats->MkqlMaxUsedMemory, alloc->GetPeakAllocated());
            CA_LOG_D("Peak memory usage: " << ProfileStats->MkqlMaxUsedMemory);
        }

        ReportStats(now);
    }

    virtual void DoExecuteImpl() {
        auto sourcesState = GetSourcesState();

        PollSourceActors(); 
        ERunStatus status = TaskRunner->Run();

        CA_LOG_D("Resume execution, run status: " << status);

        if (status != ERunStatus::Finished) {
            PollSources(std::move(sourcesState));
        }

        if ((status == ERunStatus::PendingInput || status == ERunStatus::Finished) && Checkpoints && Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved() && ReadyToCheckpoint()) {
            Checkpoints->DoCheckpoint();
        }

        ProcessOutputsImpl(status);
    }

    void DoHandleChannelsAfterFinishImpl() {
        Y_VERIFY(Checkpoints);

        if (Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved() && ReadyToCheckpoint()) {
            Checkpoints->DoCheckpoint();
        }

        // Send checkpoints to output channels.
        ProcessOutputsImpl(ERunStatus::Finished);
    }

    void ProcessOutputsImpl(ERunStatus status) {
        ProcessOutputsState.LastRunStatus = status;

        if (ProcessOutputsState.Inflight == 0) {
            ProcessOutputsState = TProcessOutputsState();
        }

        for (auto& entry : OutputChannelsMap) {
            const ui64 channelId = entry.first;
            TOutputChannelInfo& outputChannel = entry.second;

            if (!outputChannel.HasPeer) {
                // Channel info not complete, skip until dst info is available
                ProcessOutputsState.ChannelsReady = false;
                ProcessOutputsState.HasDataToSend = true;
                ProcessOutputsState.AllOutputsFinished = false;
                CA_LOG_D("Can not drain channelId: " << channelId << ", no dst actor id");
                if (Y_UNLIKELY(outputChannel.Stats)) {
                    outputChannel.Stats->NoDstActorId++;
                }
                continue;
            }

            if (!outputChannel.Finished || Checkpoints) {
                if (Channels->CanSendChannelData(channelId)) {
                    auto peerState = Channels->GetOutputChannelInFlightState(channelId);
                    DrainOutputChannel(outputChannel, peerState);
                } else {
                    ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
                }
            } else {
                CA_LOG_D("Do not drain channelId: " << channelId << ", finished");
                ProcessOutputsState.AllOutputsFinished &= outputChannel.Finished;
            }
        }

        for (auto& entry : SinksMap) {
            const ui64 outputIndex = entry.first;
            TSinkInfo& sinkInfo = entry.second;
            DrainSink(outputIndex, sinkInfo);
        }

        CheckRunStatus();
    }

    void CheckRunStatus() {
        if (ProcessOutputsState.Inflight != 0) {
            return;
        }

        auto status = ProcessOutputsState.LastRunStatus;

        if (status == ERunStatus::PendingInput && ProcessOutputsState.AllOutputsFinished) {
            CA_LOG_D("All outputs have been finished. Consider finished");
            status = ERunStatus::Finished; 
        } 
 
        if (status != ERunStatus::Finished) {
            // If the incoming channel's buffer was full at the moment when last ChannelDataAck event had been sent,
            // there will be no attempts to send a new piece of data from the other side of this channel.
            // So, if there is space in the channel buffer (and on previous step is was full), we send ChannelDataAck
            // event with the last known seqNo, and the process on the other side of this channel updates its state
            // and sends us a new batch of data.
            bool pollSent = false;
            for (auto& [channelId, inputChannel] : InputChannelsMap) {
                pollSent |= Channels->PollChannel(channelId, GetInputChannelFreeSpace(channelId));
            }
            if (!pollSent) {
                if (ProcessOutputsState.DataWasSent) {
                    ContinueExecute();
                }
                return;
            }
        }

        if (status == ERunStatus::PendingOutput) {
            if (ProcessOutputsState.DataWasSent) {
                // we have sent some data, so we have space in output channel(s)
                ContinueExecute();
            }
            return;
        }

        // Handle finishing of our task.
        if (status == ERunStatus::Finished && State != NDqProto::COMPUTE_STATE_FINISHED) {
            if (ProcessOutputsState.HasDataToSend || !ProcessOutputsState.ChannelsReady) {
                CA_LOG_D("Continue execution, either output buffers are not empty or not all channels are ready"
                    << ", hasDataToSend: " << ProcessOutputsState.HasDataToSend << ", channelsReady: " << ProcessOutputsState.ChannelsReady);
            } else {
                if (!Channels->FinishInputChannels()) {
                    CA_LOG_D("Continue execution, not all input channels are initialized");
                    return;
                }
                if (Channels->CheckInFlight("Tasks execution finished")) {
                    State = NDqProto::COMPUTE_STATE_FINISHED;
                    CA_LOG_D("Compute state finished. All channels finished");
                    ReportStateAndMaybeDie(TIssue("success"));
                }
            }
        }
    }

protected:
    void Terminate(bool success, const TString& message) {

        if (MkqlMemoryLimit && MemoryLimits.FreeMemoryFn) {
            MemoryLimits.FreeMemoryFn(TxId, Task.GetId(), MkqlMemoryLimit);
            MkqlMemoryLimit = 0;
        }

        if (Channels) {
            TAutoPtr<NActors::IEventHandle> handle = new NActors::IEventHandle(Channels->SelfId(), this->SelfId(),
                new NActors::TEvents::TEvPoison);
            Channels->Receive(handle, NActors::TActivationContext::AsActorContext());
        }

        if (Checkpoints) {
            TAutoPtr<NActors::IEventHandle> handle = new NActors::IEventHandle(Checkpoints->SelfId(), this->SelfId(),
                new NActors::TEvents::TEvPoison);
            Checkpoints->Receive(handle, NActors::TActivationContext::AsActorContext());
        }

        for (auto& [_, source] : SourcesMap) {
            if (source.Actor) {
                source.SourceActor->PassAway();
            }
        }

        for (auto& [_, sink] : SinksMap) {
            if (sink.Actor) {
                sink.SinkActor->PassAway();
            }
        }

        for (auto& [_, outputChannel] : OutputChannelsMap) {
            if (outputChannel.Channel) {
                outputChannel.Channel->Terminate();
            }
        }

        if (RuntimeSettings.TerminateHandler) {
            RuntimeSettings.TerminateHandler(success, message);
        }

        this->PassAway();
    }

    void ReportStateAndMaybeDie(TIssue&& issue) {
        ReportStateAndMaybeDie(
            State == NDqProto::COMPUTE_STATE_FINISHED ?
            Ydb::StatusIds::STATUS_CODE_UNSPECIFIED
            : Ydb::StatusIds::ABORTED, TIssues({issue}));
    }

    void ReportStateAndDie(NDqProto::EComputeState state, TIssue&& issue) {
        auto execEv = MakeHolder<TEvDqCompute::TEvState>();
        auto& record = execEv->Record;

        record.SetState(state);
        if (state != NDqProto::COMPUTE_STATE_FINISHED) {
            record.SetStatus(Ydb::StatusIds::ABORTED);
        } else {
            record.SetStatus(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED);
        }
        record.SetTaskId(Task.GetId());
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            FillStats(record.MutableStats(), /* last */ true);
        }

        if (issue.Message) {
            IssueToMessage(issue, record.MutableIssues()->Add());
        }

        this->Send(ExecuterId, execEv.Release());

        TerminateSources(issue.Message, state == NDqProto::COMPUTE_STATE_FINISHED);
        Terminate(state == NDqProto::COMPUTE_STATE_FINISHED, issue.Message);
    }

    void ReportStateAndMaybeDie(Ydb::StatusIds::StatusCode status, const TIssues& issues)
    {
        auto execEv = MakeHolder<TEvDqCompute::TEvState>();
        auto& record = execEv->Record;

        record.SetState(State);
        record.SetStatus(status);
        record.SetTaskId(Task.GetId());
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            FillStats(record.MutableStats(), /* last */ true);
        }
        IssuesToMessage(issues, record.MutableIssues());

        this->Send(ExecuterId, execEv.Release());

        if (Checkpoints && State == NDqProto::COMPUTE_STATE_FINISHED) {
            // checkpointed CAs must not self-destroy
            return;
        }

        TerminateSources(NDqProto::EComputeState_Name(State), State == NDqProto::COMPUTE_STATE_FINISHED);
        Terminate(State == NDqProto::COMPUTE_STATE_FINISHED, NDqProto::EComputeState_Name(State));
    }

    void InternalError(TIssuesIds::EIssueCode issueCode, const TString& message) {
        CA_LOG_E(TIssuesIds::EIssueCode_Name(issueCode) << ": " << message << ".");
        TIssue issue(message);
        SetIssueCode(issueCode, issue);
        std::optional<TGuard<NKikimr::NMiniKQL::TScopedAlloc>> guard = MaybeBindAllocator();
        State = NDqProto::COMPUTE_STATE_FAILURE;
        ReportStateAndMaybeDie(std::move(issue));
    }

    void ContinueExecute() {
        if (!ResumeEventScheduled && Running) {
            ResumeEventScheduled = true;
            this->Send(this->SelfId(), new TEvDqCompute::TEvResumeExecution());
        }
    }

public:
    i64 GetInputChannelFreeSpace(ui64 channelId) const override {
        const TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(inputChannel, "task: " << Task.GetId() << ", unknown input channelId: " << channelId);

        return inputChannel->Channel->GetFreeSpace();
    }

    void TakeInputChannelData(NDqProto::TChannelData&& channelData, bool ack) override {
        TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(channelData.GetChannelId());
        YQL_ENSURE(inputChannel, "task: " << Task.GetId() << ", unknown input channelId: " << channelData.GetChannelId());

        auto channel = inputChannel->Channel;

        if (channelData.GetData().GetRows()) {
            auto guard = BindAllocator();
            channel->Push(std::move(*channelData.MutableData()));
        }

        if (channelData.HasCheckpoint()) {
            Y_VERIFY(inputChannel->CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
            Y_VERIFY(Checkpoints);
            const auto& checkpoint = channelData.GetCheckpoint();
            inputChannel->Pause(checkpoint);
            Checkpoints->RegisterCheckpoint(checkpoint, channelData.GetChannelId());
        }

        if (channelData.GetFinished()) {
            channel->Finish();
        }

        if (ack) {
            Channels->SendChannelDataAck(channel->GetChannelId(), channel->GetFreeSpace());
        }

        ResumeExecution();
    }

    void PeerFinished(ui64 channelId) override {
        TOutputChannelInfo* outputChannel = OutputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(outputChannel, "task: " << Task.GetId() << ", output channelId: " << channelId);

        outputChannel->Finished = true;
        outputChannel->Channel->Finish();

        CA_LOG_D("task: " << Task.GetId() << ", output channelId: " << channelId << " finished prematurely, "
            << " about to clear buffer");

        {
            auto guard = BindAllocator();
            ui32 dropRows = outputChannel->Channel->Drop();

            CA_LOG_I("task: " << Task.GetId() << ", output channelId: " << channelId << " finished prematurely, "
                << "drop " << dropRows << " rows");
        }

        DoExecute();
    }

    void ResumeExecution() override {
        ContinueExecute();
    }

    void OnSinkStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override {
        Y_VERIFY(Checkpoints); // If we are checkpointing, we must have already constructed "checkpoints" object.
        Checkpoints->OnSinkStateSaved(std::move(state), outputIndex, checkpoint);
    }

protected:
    bool ReadyToCheckpoint() const override {
        for (auto& [id, channelInfo] : InputChannelsMap) {
            if (channelInfo.CheckpointingMode == NDqProto::CHECKPOINTING_MODE_DISABLED) {
                continue;
            }

            if (!channelInfo.IsPaused()) {
                return false;
            }
            if (!channelInfo.Channel->Empty()) {
                return false;
            }
        }
        return true;
    }

    void SaveState(const NDqProto::TCheckpoint& checkpoint, NDqProto::TComputeActorState& state) const override {
        CA_LOG_D("Save state");
        NDqProto::TMiniKqlProgramState& mkqlProgramState = *state.MutableMiniKqlProgram();
        mkqlProgramState.SetRuntimeVersion(NDqProto::RUNTIME_VERSION_YQL_1_0);
        NDqProto::TStateData::TData& data = *mkqlProgramState.MutableData()->MutableStateData();
        data.SetVersion(ComputeActorCurrentStateVersion);
        data.SetBlob(TaskRunner->Save());

        for (auto& [inputIndex, source] : SourcesMap) {
            YQL_ENSURE(source.SourceActor, "Source[" << inputIndex << "] is not created");
            NDqProto::TSourceState& sourceState = *state.AddSources();
            source.SourceActor->SaveState(checkpoint, sourceState);
            sourceState.SetInputIndex(inputIndex);
        }
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        CA_LOG_D("Commit state");
        for (auto& [inputIndex, source] : SourcesMap) {
            Y_VERIFY(source.SourceActor);
            source.SourceActor->CommitState(checkpoint);
        }
    }

    void InjectBarrierToOutputs(const NDqProto::TCheckpoint& checkpoint) override {
        Y_VERIFY(CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
        for (const auto& [id, channelInfo] : OutputChannelsMap) {
            channelInfo.Channel->Push(NDqProto::TCheckpoint(checkpoint));
        }
        for (const auto& [outputIndex, sink] : SinksMap) {
            sink.Sink->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void ResumeInputs() override {
        for (auto& [id, channelInfo] : InputChannelsMap) {
            channelInfo.Resume();
        }
    }

    void LoadState(const NDqProto::TComputeActorState& state) override {
        CA_LOG_D("Load state");
        auto guard = BindAllocator();
        const NDqProto::TMiniKqlProgramState& mkqlProgramState = state.GetMiniKqlProgram();
        const ui64 version = mkqlProgramState.GetData().GetStateData().GetVersion();
        YQL_ENSURE(version && version <= ComputeActorCurrentStateVersion && version != ComputeActorNonProtobufStateVersion, "Unsupported state version: " << version);
        if (version == ComputeActorCurrentStateVersion) {
            for (const NDqProto::TSourceState& sourceState : state.GetSources()) {
                TSourceInfo* source = SourcesMap.FindPtr(sourceState.GetInputIndex());
                YQL_ENSURE(source, "Failed to load state. Source with input index " << sourceState.GetInputIndex() << " was not found");
                YQL_ENSURE(source->SourceActor, "Source[" << sourceState.GetInputIndex() << "] is not created");
                source->SourceActor->LoadState(sourceState);
            }
            for (const NDqProto::TSinkState& sinkState : state.GetSinks()) {
                TSinkInfo* sink = SinksMap.FindPtr(sinkState.GetOutputIndex());
                YQL_ENSURE(sink, "Failed to load state. Sink with input index " << sinkState.GetOutputIndex() << " was not found");
                YQL_ENSURE(sink->SinkActor, "Sink[" << sinkState.GetOutputIndex() << "] is not created");
                sink->SinkActor->LoadState(sinkState);
            }
            if (const TString& blob = mkqlProgramState.GetData().GetStateData().GetBlob()) {
                TaskRunner->Load(blob);
            }
            return;
        }
        ythrow yexception() << "Invalid state version " << version;
    }

    void Start() override {
        Running = true;
        State = NDqProto::COMPUTE_STATE_EXECUTING;
        ContinueExecute();
    }

    void Stop() override {
        Running = false;
        State = NDqProto::COMPUTE_STATE_UNKNOWN;
    }

protected:
    struct TInputChannelInfo {
        ui64 ChannelId;
        IDqInputChannel::TPtr Channel;
        bool HasPeer = false; 
        std::optional<NDqProto::TCheckpoint> PendingCheckpoint;
        const NDqProto::ECheckpointingMode CheckpointingMode;
        ui64 FreeSpace = 0;

        explicit TInputChannelInfo(ui64 channelId, NDqProto::ECheckpointingMode checkpointingMode)
            : ChannelId(channelId)
            , CheckpointingMode(checkpointingMode)
        {
        }

        bool IsPaused() const {
            return PendingCheckpoint.has_value();
        }

        void Pause(const NDqProto::TCheckpoint& checkpoint) {
            YQL_ENSURE(!IsPaused());
            YQL_ENSURE(CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
            PendingCheckpoint = checkpoint;
            Channel->Pause();
        }

        void Resume() {
            PendingCheckpoint.reset();
            Channel->Resume();
        }
    };

    struct TSourceInfo {
        ui64 Index;
        IDqSource::TPtr Source;
        IDqSourceActor* SourceActor = nullptr;
        NActors::IActor* Actor = nullptr;
        TIssuesBuffer IssuesBuffer; 
        bool Finished = false;
        i64 FreeSpace = 1;
        bool PushStarted = false;
 
        TSourceInfo(ui64 index) : Index(index), IssuesBuffer(IssuesBufferSize) {}
    };

    struct TOutputChannelInfo {
        ui64 ChannelId;
        IDqOutputChannel::TPtr Channel;
        bool HasPeer = false;
        bool Finished = false; // != Channel->IsFinished() // If channel is in finished state, it sends only checkpoints.
        bool PopStarted = false;

        explicit TOutputChannelInfo(ui64 channelId)
            : ChannelId(channelId)
        { }

        struct TStats {
            ui64 BlockedByCapacity = 0;
            ui64 NoDstActorId = 0;
        };
        THolder<TStats> Stats;
    };

    struct TSinkInfo {
        IDqSink::TPtr Sink;
        IDqSinkActor* SinkActor = nullptr;
        NActors::IActor* Actor = nullptr;
        bool Finished = false; // If sink is in finished state, it receives only checkpoints.
        TIssuesBuffer IssuesBuffer; 
        bool PopStarted = false;
        i64 SinkActorFreeSpaceBeforeSend = 0;
 
        TSinkInfo() : IssuesBuffer(IssuesBufferSize) {} 
    };

protected:
    // virtual methods (TODO: replace with static_cast<TDerived*>(this)->Foo()

    virtual THolder<NKikimr::IDestructable> GetSourcesState() {
        return nullptr;
    }

    virtual void PollSources(THolder<NKikimr::IDestructable> /* state */) {
    }

    virtual void TerminateSources(const TString& /* message */, bool /* success */) {
    }

    virtual TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TaskRunner->BindAllocator();
    }

    virtual std::optional<TGuard<NKikimr::NMiniKQL::TScopedAlloc>> MaybeBindAllocator() {
        std::optional<TGuard<NKikimr::NMiniKQL::TScopedAlloc>> guard;
        if (!TaskRunner->GetTypeEnv().GetAllocator().IsAttached()) {
            guard.emplace(TaskRunner->BindAllocator());
        }
        return guard;
    }

    virtual void SourcePush(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, TSourceInfo& source, i64 space, bool finished) {
        source.Source->Push(std::move(batch), space);
        if (finished) {
            source.Source->Finish();
            source.Finished = true;
        }
    }

    virtual i64 SourceFreeSpace(TSourceInfo& source) {
        return source.Source->GetFreeSpace();
    }

    virtual bool SayHelloOnBootstrap() {
        return true;
    }

protected:
    void HandleExecuteBase(TEvDqCompute::TEvResumeExecution::TPtr&) {
        ResumeEventScheduled = false;
        if (Running) {
            DoExecute();
        }
    }

    void HandleExecuteBase(TEvDqCompute::TEvChannelsInfo::TPtr& ev) {
        auto& record = ev->Get()->Record;

        CA_LOG_D("Received channels info: " << record.ShortDebugString());

        for (auto& channelUpdate : record.GetUpdate()) {
            TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(channelUpdate.GetId()); 
            if (inputChannel && !inputChannel->HasPeer && channelUpdate.GetSrcEndpoint().HasActorId()) { 
                auto peer = NActors::ActorIdFromProto(channelUpdate.GetSrcEndpoint().GetActorId());
 
                CA_LOG_D("Update input channelId: " << channelUpdate.GetId() << ", peer: " << peer); 
 
                Channels->SetInputChannelPeer(channelUpdate.GetId(), peer); 
                inputChannel->HasPeer = true; 
 
                continue; 
            } 
 
            TOutputChannelInfo* outputChannel = OutputChannelsMap.FindPtr(channelUpdate.GetId());
            if (outputChannel && !outputChannel->HasPeer && channelUpdate.GetDstEndpoint().HasActorId()) { 
                auto peer = NActors::ActorIdFromProto(channelUpdate.GetDstEndpoint().GetActorId());

                CA_LOG_D("Update output channelId: " << channelUpdate.GetId() << ", peer: " << peer);

                Channels->SetOutputChannelPeer(channelUpdate.GetId(), peer);
                outputChannel->HasPeer = true;
 
                continue; 
            }
 
            YQL_ENSURE(inputChannel || outputChannel, "Unknown channelId: " << channelUpdate.GetId() << ", task: " << Task.GetId()); 
        }

        DoExecute();
    }

    void HandleExecuteBase(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = (EEvWakeupTag) ev->Get()->Tag;
        switch (tag) {
            case EEvWakeupTag::TimeoutTag: {
                auto abortEv = MakeHolder<TEvDq::TEvAbortExecution>(Ydb::StatusIds::TIMEOUT, TStringBuilder()
                    << "Timeout event from compute actor " << this->SelfId()
                    << ", TxId: " << TxId << ", task: " << Task.GetId());

                this->Send(ExecuterId, abortEv.Release());

                TerminateSources("timeout exceeded", false);
                Terminate(false, "timeout exceeded");
                break;
            }
            case EEvWakeupTag::PeriodicStatsTag: {
                const auto maxInterval = RuntimeSettings.ReportStatsSettings->MaxInterval;
                this->Schedule(maxInterval, new NActors::TEvents::TEvWakeup(EEvWakeupTag::PeriodicStatsTag));

                auto now = NActors::TActivationContext::Now();
                if (now - LastSendStatsTime >= maxInterval) {
                    ReportStats(now);
                }
                break;
            }
            default:
                static_cast<TDerived*>(this)->HandleEvWakeup(tag);
        }
    }

    void HandleEvWakeup(EEvWakeupTag tag) {
        CA_LOG_E("Unhandled wakeup tag " << (ui64)tag);
    }

    void HandleExecuteBase(NActors::TEvents::TEvUndelivered::TPtr& ev) {
        ui32 lostEventType = ev->Get()->SourceType;
        switch (lostEventType) {
            case TEvDqCompute::TEvState::EventType: {
                CA_LOG_E("Handle undelivered TEvState event, abort execution");
                this->TerminateSources("executer lost", false);
                Terminate(false, "executer lost");
                break;
            }
            default: {
                CA_LOG_C("Handle unexpected event undelivery: " << lostEventType);
            }
        }
    }

    void HandleExecuteBase(TEvDqCompute::TEvRun::TPtr& ev) {
        CA_LOG_D("Got TEvRun from actor " << ev->Sender);
        Start();

        // Event from coordinator should be processed to confirm seq no.
        TAutoPtr<NActors::IEventHandle> iev(ev.Release());
        if (Checkpoints) {
            Checkpoints->Receive(iev, NActors::TActivationContext::AsActorContext());
        }
    }

    void HandleExecuteBase(TEvDqCompute::TEvStateRequest::TPtr& ev) {
        CA_LOG_D("Got TEvStateRequest from actor " << ev->Sender << " TaskId: " << Task.GetId() << " PingCookie: " << ev->Cookie);
        auto evState = MakeHolder<TEvDqCompute::TEvState>();
        evState->Record.SetState(NDqProto::COMPUTE_STATE_EXECUTING);
        evState->Record.SetStatus(Ydb::StatusIds::SUCCESS);
        evState->Record.SetTaskId(Task.GetId());
        FillStats(evState->Record.MutableStats(), /* last */ false);
        this->Send(ev->Sender, evState.Release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void HandleExecuteBase(TEvDqCompute::TEvNewCheckpointCoordinator::TPtr& ev) {
        if (!Checkpoints) {
            Checkpoints = new TDqComputeActorCheckpoints(TxId, Task, this);
            Checkpoints->Init(this->SelfId(), this->RegisterWithSameMailbox(Checkpoints));
            Channels->SetCheckpointsSupport();
        }
        TAutoPtr<NActors::IEventHandle> handle = new NActors::IEventHandle(Checkpoints->SelfId(), ev->Sender, ev->Release().Release());
        Checkpoints->Receive(handle, NActors::TActivationContext::AsActorContext());
    }

    void HandleExecuteBase(TEvDq::TEvAbortExecution::TPtr& ev) {
        TString message = ev->Get()->Record.GetMessage();
        CA_LOG_E("Handle abort execution event from: " << ev->Sender
            << ", status: " << Ydb::StatusIds_StatusCode_Name(ev->Get()->Record.GetStatusCode())
            << ", reason: " << message);

        bool success = ev->Get()->Record.GetStatusCode() == Ydb::StatusIds::SUCCESS;

        this->TerminateSources(message, success);

        if (ev->Sender != ExecuterId) {
            NActors::TActivationContext::Send(ev->Forward(ExecuterId));
        }

        Terminate(success, message);
    }

    void HandleExecuteBase(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        TAutoPtr<NActors::IEventHandle> iev(ev.Release());
        if (Checkpoints) {
            Checkpoints->Receive(iev, NActors::TActivationContext::AsActorContext());
        }
    }

    void HandleExecuteBase(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        TAutoPtr<NActors::IEventHandle> iev(ev.Release());
        if (Checkpoints) {
            Checkpoints->Receive(iev, NActors::TActivationContext::AsActorContext());
        }
    }

    ui32 AllowedChannelsOvercommit() const {
        const auto& fc = GetDqExecutionSettings().FlowControl;
        const ui32 allowedOvercommit = (fc.InFlightBytesOvercommit - 1.f) * MemoryLimits.ChannelBufferSize;
        return allowedOvercommit;
    }

private:

    virtual void DrainOutputChannel(TOutputChannelInfo& outputChannel, const TDqComputeActorChannels::TPeerState& peerState) {
        YQL_ENSURE(!outputChannel.Finished || Checkpoints);

        const bool wasFinished = outputChannel.Finished;
        auto channelId = outputChannel.Channel->GetChannelId();

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
            << ", finished: " << outputChannel.Channel->IsFinished());

        ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
        ProcessOutputsState.AllOutputsFinished &= outputChannel.Finished;

        if (toSend <= 0) {
            if (Y_UNLIKELY(outputChannel.Stats)) {
                outputChannel.Stats->BlockedByCapacity++;
            }
        }

        i64 remains = toSend;
        while (remains > 0 && (!outputChannel.Finished || Checkpoints)) {
            ui32 sent = this->SendChannelDataChunk(outputChannel, remains);
            if (sent == 0) {
                break;
            }
            remains -= sent;
        }

        ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
        ProcessOutputsState.AllOutputsFinished &= outputChannel.Finished;
        ProcessOutputsState.DataWasSent |= (!wasFinished && outputChannel.Finished) || remains != toSend;
    }

    ui32 SendChannelDataChunk(TOutputChannelInfo& outputChannel, ui64 bytes) {
        auto channel = outputChannel.Channel;

        NDqProto::TData data;
        NDqProto::TCheckpoint checkpoint;

        bool hasData = channel->Pop(data, bytes);
        bool hasCheckpoint = channel->Pop(checkpoint);
        if (!hasData && !hasCheckpoint) {
            if (!channel->IsFinished()) {
                CA_LOG_D("output channelId: " << channel->GetChannelId() << ", nothing to send and is not finished");
                return 0; // channel is empty and not finished yet
            }
        }
        const bool wasFinished = outputChannel.Finished;
        outputChannel.Finished = channel->IsFinished();
        const bool becameFinished = !wasFinished && outputChannel.Finished;

        ui32 dataSize = data.GetRaw().size();
        ui32 checkpointSize = checkpoint.ByteSize();

        NDqProto::TChannelData channelData;
        channelData.SetChannelId(channel->GetChannelId());
        channelData.SetFinished(outputChannel.Finished);
        if (hasData) {
            channelData.MutableData()->Swap(&data);
        }
        if (hasCheckpoint) {
            channelData.MutableCheckpoint()->Swap(&checkpoint);
            CA_LOG_I("Resume inputs");
            ResumeInputs();
        }

        if (hasData || hasCheckpoint || becameFinished) {
            Channels->SendChannelData(std::move(channelData));
            return dataSize + checkpointSize;
        }
        return 0;
    }

    virtual void DrainSink(ui64 outputIndex, TSinkInfo& sinkInfo) {
        ProcessOutputsState.AllOutputsFinished &= sinkInfo.Finished;
        if (sinkInfo.Finished && !Checkpoints) {
            return;
        }

        Y_VERIFY(sinkInfo.Sink);
        Y_VERIFY(sinkInfo.SinkActor);
        Y_VERIFY(sinkInfo.Actor);

        const ui32 allowedOvercommit = AllowedChannelsOvercommit();
        const i64 sinkActorFreeSpaceBeforeSend = sinkInfo.SinkActor->GetFreeSpace();

        i64 toSend = sinkActorFreeSpaceBeforeSend + allowedOvercommit;
        CA_LOG_D("About to drain sink " << outputIndex
            << ". FreeSpace: " << sinkActorFreeSpaceBeforeSend
            << ", allowedOvercommit: " << allowedOvercommit
            << ", toSend: " << toSend
            << ", finished: " << sinkInfo.Sink->IsFinished());

        i64 sent = 0;
        while (toSend > 0 && (!sinkInfo.Finished || Checkpoints)) {
            const ui32 sentChunk = SendSinkDataChunk(outputIndex, sinkInfo, toSend);
            if (sentChunk == 0) {
                break;
            }
            sent += sentChunk;
            toSend = sinkInfo.SinkActor->GetFreeSpace() + allowedOvercommit;
        }

        CA_LOG_D("Drain sink " << outputIndex
            << ". Free space decreased: " << (sinkActorFreeSpaceBeforeSend - sinkInfo.SinkActor->GetFreeSpace())
            << ", sent data from buffer: " << sent);

        ProcessOutputsState.HasDataToSend |= !sinkInfo.Finished;
        ProcessOutputsState.DataWasSent |= sinkInfo.Finished || sent;
    }

    ui32 SendSinkDataChunk(ui64 outputIndex, TSinkInfo& sinkInfo, ui64 bytes) {
        auto sink = sinkInfo.Sink;

        NKikimr::NMiniKQL::TUnboxedValueVector dataBatch;
        NDqProto::TCheckpoint checkpoint;

        const ui64 dataSize = !sinkInfo.Finished ? sink->Pop(dataBatch, bytes) : 0;
        const bool hasCheckpoint = sink->Pop(checkpoint);
        if (!dataSize && !hasCheckpoint) {
            if (!sink->IsFinished()) {
                CA_LOG_D("sink " << outputIndex << ": nothing to send and is not finished");
                return 0; // sink is empty and not finished yet
            }
        }
        sinkInfo.Finished = sink->IsFinished();

        YQL_ENSURE(!dataSize || !dataBatch.empty()); // dataSize != 0 => !dataBatch.empty() // even if we're about to send empty rows.

        const ui32 checkpointSize = hasCheckpoint ? checkpoint.ByteSize() : 0;

        TMaybe<NDqProto::TCheckpoint> maybeCheckpoint;
        if (hasCheckpoint) {
            maybeCheckpoint = checkpoint;
            CA_LOG_I("Resume inputs");
            ResumeInputs();
        }

        sinkInfo.SinkActor->SendData(std::move(dataBatch), dataSize, maybeCheckpoint, sinkInfo.Finished);
        CA_LOG_D("sink " << outputIndex << ": sent " << dataSize << " bytes of data and " << checkpointSize << " bytes of checkpoint barrier");

        return dataSize + checkpointSize;
    }

protected:
    const TMaybe<NDqProto::TRlPath>& GetRlPath() const {
        return RuntimeSettings.RlPath;
    }

    TTxId GetTxId() const {
        return TxId;
    }

    const NDqProto::TDqTask& GetTask() const {
        return Task;
    }

    NDqProto::EDqStatsMode GetStatsMode() const {
        return RuntimeSettings.StatsMode;
    }

    bool GetUseLLVM() const {
        return RuntimeSettings.UseLLVM;
    }

    const TComputeMemoryLimits& GetMemoryLimits() const {
        return MemoryLimits;
    }

protected:
    void SetTaskRunner(const TIntrusivePtr<IDqTaskRunner>& taskRunner) {
        TaskRunner = taskRunner;
    }

    void PrepareTaskRunner(const IDqTaskRunnerExecutionContext& execCtx = TDqTaskRunnerExecutionContext()) {
        YQL_ENSURE(TaskRunner);

        auto guard = TaskRunner->BindAllocator(MkqlMemoryLimit);
        auto* alloc = guard.GetMutex();

        if (CanAllocateExtraMemory) {
            alloc->Ref().SetIncreaseMemoryLimitCallback([this, alloc](ui64 limit, ui64 required) {
                RequestExtraMemory(required - limit, alloc);
            });
        }

        TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = MemoryLimits.ChannelBufferSize;
        limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

        TaskRunner->Prepare(Task, limits, execCtx);

        FillChannelMaps(
            TaskRunner->GetHolderFactory(),
            TaskRunner->GetTypeEnv(),
            TaskRunner->GetSecureParams(),
            TaskRunner->GetTaskParams());
    }

    void FillChannelMaps(
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const THashMap<TString, TString>& secureParams,
        const THashMap<TString, TString>& taskParams)
    {
        if (TaskRunner) {
            for (auto& [channelId, channel] : InputChannelsMap) {
                channel.Channel = TaskRunner->GetInputChannel(channelId);
            }
        }
        for (auto& [inputIndex, source] : SourcesMap) {
            if (TaskRunner) { source.Source = TaskRunner->GetSource(inputIndex); Y_VERIFY(source.Source);}
            Y_VERIFY(SourceActorFactory);
            const auto& inputDesc = Task.GetInputs(inputIndex);
            const ui64 i = inputIndex; // Crutch for clang
            CA_LOG_D("Create source actor for input " << i << " " << inputDesc);
            std::tie(source.SourceActor, source.Actor) = SourceActorFactory->CreateDqSourceActor(
                IDqSourceActorFactory::TArguments{ 
                    .InputDesc = inputDesc, 
                    .InputIndex = inputIndex, 
                    .TxId = TxId, 
                    .SecureParams = secureParams,
                    .TaskParams = taskParams,
                    .Callback = this, 
                    .TypeEnv = typeEnv,
                    .HolderFactory = holderFactory
                }); 
            this->RegisterWithSameMailbox(source.Actor);
        }
        if (TaskRunner) {
            for (auto& [channelId, channel] : OutputChannelsMap) {
                channel.Channel = TaskRunner->GetOutputChannel(channelId);
            }
        }
        for (auto& [outputIndex, sink] : SinksMap) {
            if (TaskRunner) { sink.Sink = TaskRunner->GetSink(outputIndex); }
            Y_VERIFY(SinkActorFactory);
            const auto& outputDesc = Task.GetOutputs(outputIndex);
            const ui64 i = outputIndex; // Crutch for clang
            CA_LOG_D("Create sink actor for output " << i << " " << outputDesc);
            std::tie(sink.SinkActor, sink.Actor) = SinkActorFactory->CreateDqSinkActor(
                IDqSinkActorFactory::TArguments { 
                    .OutputDesc = outputDesc, 
                    .OutputIndex = outputIndex, 
                    .TxId = TxId, 
                    .SecureParams = secureParams,
                    .Callback = this, 
                    .TypeEnv = typeEnv,
                    .HolderFactory = holderFactory
                }); 
            this->RegisterWithSameMailbox(sink.Actor);
        }
    }

    void PollSourceActors() { // TODO: rename to PollSources()
        // Don't produce any input from sources if we're about to save checkpoint.
        if (!Running || (Checkpoints && Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved())) {
            return;
        }

        for (auto& [inputIndex, source] : SourcesMap) {
            Y_VERIFY(!TaskRunner || source.Source);
            if (source.Finished) {
                const ui64 indexForLogging = inputIndex; // Crutch for clang
                CA_LOG_D("Skip polling source[" << indexForLogging << "]: finished");
                continue;
            }
            const i64 freeSpace = SourceFreeSpace(source);
            if (freeSpace > 0) {
                NKikimr::NMiniKQL::TUnboxedValueVector batch;
                Y_VERIFY(source.SourceActor);
                bool finished = false;
                const i64 space = source.SourceActor->GetSourceData(batch, finished, freeSpace);
                const ui64 index = inputIndex;
                CA_LOG_D("Poll source " << index
                    << ". Buffer free space: " << freeSpace
                    << ", read from source: " << space << " bytes, "
                    << batch.size() << " rows, finished: " << finished);
                SourcePush(std::move(batch), source, space, finished);
            }
        }
    }

    void OnNewSourceDataArrived(ui64 inputIndex) override {
        Y_VERIFY(SourcesMap.FindPtr(inputIndex));
        ContinueExecute();
    }

    void OnSourceError(ui64 inputIndex, const TIssues& issues, bool isFatal) override { 
        if (!isFatal) { 
            SourcesMap.at(inputIndex).IssuesBuffer.Push(issues); 
            return; 
        } 
 
        TString desc = issues.ToString();
        CA_LOG_E("Source[" << inputIndex << "] fatal error: " << desc);
        InternalError(TIssuesIds::DEFAULT_ERROR, desc);
    }

    void OnSinkError(ui64 outputIndex, const TIssues& issues, bool isFatal) override { 
        if (!isFatal) { 
            SinksMap.at(outputIndex).IssuesBuffer.Push(issues); 
            return; 
        } 
 
        TString desc = issues.ToString();
        CA_LOG_E("Sink[" << outputIndex << "] fatal error: " << desc);
        InternalError(TIssuesIds::DEFAULT_ERROR, desc);
    }

    virtual ui64 CalcMkqlMemoryLimit() {
        auto& opts = Task.GetProgram().GetSettings();
        return opts.GetHasMapJoin()/* || opts.GetHasSort()*/
            ? MemoryLimits.MkqlHeavyProgramMemoryLimit
            : MemoryLimits.MkqlLightProgramMemoryLimit;
    }

private:
    void InitializeTask() {
        for (ui32 i = 0; i < Task.InputsSize(); ++i) {
            const auto& inputDesc = Task.GetInputs(i);
            Y_VERIFY(!inputDesc.HasSource() || inputDesc.ChannelsSize() == 0); // HasSource => no channels
            if (inputDesc.HasSource()) {
                auto result = SourcesMap.emplace(i, TSourceInfo(i));
                YQL_ENSURE(result.second);
            } else {
                for (auto& channel : inputDesc.GetChannels()) {
                    auto result = InputChannelsMap.emplace(channel.GetId(), TInputChannelInfo(channel.GetId(), channel.GetCheckpointingMode()));
                    YQL_ENSURE(result.second);
                }
            }
        }

        for (ui32 i = 0; i < Task.OutputsSize(); ++i) {
            const auto& outputDesc = Task.GetOutputs(i);
            Y_VERIFY(!outputDesc.HasSink() || outputDesc.ChannelsSize() == 0); // HasSink => no channels
            Y_VERIFY(outputDesc.HasSink() || outputDesc.ChannelsSize() > 0);
            if (outputDesc.HasSink()) {
                auto result = SinksMap.emplace(i, TSinkInfo());
                YQL_ENSURE(result.second);
            } else {
                for (auto& channel : outputDesc.GetChannels()) {
                    TOutputChannelInfo outputChannel(channel.GetId());
                    outputChannel.HasPeer = channel.GetDstEndpoint().HasActorId();

                    if (Y_UNLIKELY(RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_PROFILE)) {
                        outputChannel.Stats = MakeHolder<typename TOutputChannelInfo::TStats>();
                    }

                    auto result = OutputChannelsMap.emplace(channel.GetId(), std::move(outputChannel));
                    YQL_ENSURE(result.second);
                }
            }
        }

        MkqlMemoryLimit = CalcMkqlMemoryLimit();
    }

    static ui64 AlignMemorySizeToMbBoundary(ui64 memory) {
        // allocate memory in 1_MB (2^20B) chunks, so requested value is rounded up to MB boundary
        constexpr ui64 alignMask = 1_MB - 1;
        return (memory + alignMask) & ~alignMask;
    }

    void RequestExtraMemory(ui64 memory, NKikimr::NMiniKQL::TScopedAlloc* alloc) {
        memory = std::max(AlignMemorySizeToMbBoundary(memory), MemoryLimits.MinMemAllocSize);

        CA_LOG_I("not enough memory, request +" << memory);

        if (MemoryLimits.AllocateMemoryFn(TxId, Task.GetId(), memory)) {
            MkqlMemoryLimit += memory;
            CA_LOG_I("[Mem] memory granted, new limit: " << MkqlMemoryLimit);
            alloc->SetLimit(MkqlMemoryLimit);
        } else {
            CA_LOG_W("[Mem] memory not granted");
//            throw yexception() << "Can not allocate extra memory, limit: " << MkqlMemoryLimit
//                << ", requested: " << memory << ", host: " << HostName();
        }

        if (Y_UNLIKELY(ProfileStats)) {
            ProfileStats->MkqlExtraMemoryBytes += memory;
            ProfileStats->MkqlExtraMemoryRequests++;
        }
    }

    void FillStats(NDqProto::TDqComputeActorStats* dst, bool last) {
        if (!BasicStats) {
            return;
        }

        if (last) {
            ReportEventElapsedTime();
        }

        dst->SetCpuTimeUs(BasicStats->CpuTime.MicroSeconds());

        if (ProfileStats) {
            dst->SetMkqlMaxMemoryUsage(ProfileStats->MkqlMaxUsedMemory);
            dst->SetMkqlExtraMemoryBytes(ProfileStats->MkqlExtraMemoryBytes);
            dst->SetMkqlExtraMemoryRequests(ProfileStats->MkqlExtraMemoryRequests);
        }

        if (TaskRunner) {
            TaskRunner->UpdateStats();

            if (auto* taskStats = TaskRunner->GetStats()) {
                auto* protoTask = dst->AddTasks();
                FillTaskRunnerStats(Task.GetId(), Task.GetStageId(), *taskStats, protoTask, (bool) ProfileStats);

                for (auto& [outputIndex, sinkInfo] : SinksMap) {
                    if (auto* sinkStats = sinkInfo.Sink ? sinkInfo.Sink->GetStats() : nullptr) {
                        protoTask->SetOutputRows(protoTask->GetOutputRows() + sinkStats->RowsIn);
                        protoTask->SetOutputBytes(protoTask->GetOutputBytes() + sinkStats->Bytes);

                        if (ProfileStats) {
                            auto* protoSink = protoTask->AddSinks();
                            protoSink->SetOutputIndex(outputIndex);

                            protoSink->SetChunks(sinkStats->Chunks);
                            protoSink->SetBytes(sinkStats->Bytes);
                            protoSink->SetRowsIn(sinkStats->RowsIn);
                            protoSink->SetRowsOut(sinkStats->RowsOut);

                            protoSink->SetMaxMemoryUsage(sinkStats->MaxMemoryUsage);
                            protoSink->SetErrorsCount(sinkInfo.IssuesBuffer.GetAllAddedIssuesCount());
                        }
                    }
                }

                if (ProfileStats) {
                    for (auto& protoSource : *protoTask->MutableSources()) {
                        if (auto* sourceInfo = SourcesMap.FindPtr(protoSource.GetInputIndex())) {
                            protoSource.SetErrorsCount(sourceInfo->IssuesBuffer.GetAllAddedIssuesCount());
                        }
                    }

                    for (auto& protoInputChannelStats : *protoTask->MutableInputChannels()) {
                        if (auto* caChannelStats = Channels->GetInputChannelStats(protoInputChannelStats.GetChannelId())) {
                            protoInputChannelStats.SetPollRequests(caChannelStats->PollRequests);
                            protoInputChannelStats.SetWaitTimeUs(caChannelStats->WaitTime.MicroSeconds());
                            protoInputChannelStats.SetResentMessages(caChannelStats->ResentMessages);
                        }
                    }

                    for (auto& protoOutputChannelStats : *protoTask->MutableOutputChannels()) {
                        if (auto* x = Channels->GetOutputChannelStats(protoOutputChannelStats.GetChannelId())) {
                            protoOutputChannelStats.SetResentMessages(x->ResentMessages);
                        }

                        if (auto* outputInfo = OutputChannelsMap.FindPtr(protoOutputChannelStats.GetChannelId())) {
                            if (auto *x = outputInfo->Stats.Get()) {
                                protoOutputChannelStats.SetBlockedByCapacity(x->BlockedByCapacity);
                                protoOutputChannelStats.SetNoDstActorId(x->NoDstActorId);
                            }
                        }
                    }
                }
            }
        }
 
        static_cast<TDerived*>(this)->FillExtraStats(dst, last);
 
        if (last) {
            BasicStats.reset();
            ProfileStats.reset();
        } 
    }

    void ReportStats(TInstant now) {
        if (!RuntimeSettings.ReportStatsSettings) {
            return;
        }

        if (now - LastSendStatsTime < RuntimeSettings.ReportStatsSettings->MinInterval) {
            return;
        }

        auto evState = std::make_unique<TEvDqCompute::TEvState>();
        evState->Record.SetState(NDqProto::COMPUTE_STATE_EXECUTING);
        evState->Record.SetTaskId(Task.GetId());
        FillStats(evState->Record.MutableStats(), /* last */ false);

        auto dbgPrintStats = [&]() {
            NProtoBuf::TextFormat::Printer printer;
            printer.SetUseShortRepeatedPrimitives(true);
            printer.SetSingleLineMode(true);
            printer.SetUseUtf8StringEscaping(true);

            TString result;
            printer.PrintToString(evState->Record.GetStats(), &result);
            return result;
        };

        CA_LOG_D("Send stats to executor actor " << ExecuterId << " TaskId: " << Task.GetId()
            << " Stats: " << dbgPrintStats());

        this->Send(ExecuterId, evState.release(), NActors::IEventHandle::FlagTrackDelivery);

        LastSendStatsTime = now;
    }

protected:
    const NActors::TActorId ExecuterId;
    const TTxId TxId;
    const NDqProto::TDqTask Task;
    const TComputeRuntimeSettings RuntimeSettings;
    const TComputeMemoryLimits MemoryLimits;
    const bool CanAllocateExtraMemory = false;
    const IDqSourceActorFactory::TPtr SourceActorFactory;
    const IDqSinkActorFactory::TPtr SinkActorFactory;
    const NDqProto::ECheckpointingMode CheckpointingMode;
    TIntrusivePtr<IDqTaskRunner> TaskRunner;
    TDqComputeActorChannels* Channels = nullptr;
    TDqComputeActorCheckpoints* Checkpoints = nullptr;
    THashMap<ui64, TInputChannelInfo> InputChannelsMap; // Channel id -> Channel info
    THashMap<ui64, TSourceInfo> SourcesMap; // Input index -> Source info
    THashMap<ui64, TOutputChannelInfo> OutputChannelsMap; // Channel id -> Channel info
    THashMap<ui64, TSinkInfo> SinksMap; // Output index -> Sink info
    ui64 MkqlMemoryLimit = 0;
    bool ResumeEventScheduled = false;
    NDqProto::EComputeState State;

    struct TBasicStats {
        TDuration CpuTime;
    };
    struct TProfileStats {
        ui64 MkqlMaxUsedMemory = 0;
        ui64 MkqlExtraMemoryBytes = 0;
        ui32 MkqlExtraMemoryRequests = 0;
    };
    std::unique_ptr<TBasicStats> BasicStats;
    std::unique_ptr<TProfileStats> ProfileStats;

    struct TProcessOutputsState {
        int Inflight = 0;
        bool ChannelsReady = true;
        bool HasDataToSend = false;
        bool DataWasSent = false;
        bool AllOutputsFinished = true;
        ERunStatus LastRunStatus = ERunStatus::PendingInput;
    };
    TProcessOutputsState ProcessOutputsState;

private:
    bool Running = true;
    TInstant LastSendStatsTime;
};

} // namespace NYql
} // namespace NNq
