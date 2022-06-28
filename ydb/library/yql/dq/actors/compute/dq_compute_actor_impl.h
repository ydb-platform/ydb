#pragma once

#include "dq_compute_actor.h"
#include "dq_compute_actor_channels.h"
#include "dq_compute_actor_checkpoints.h"
#include "dq_compute_actor_async_io.h"
#include "dq_compute_issues_buffer.h"
#include "dq_compute_memory_quota.h"

#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/protos/services.pb.h>

#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <library/cpp/actors/core/interconnect.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>
#include <util/system/hostname.h>

#include <any>

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

constexpr ui32 IssuesBufferSize = 16;

struct TSinkCallbacks : public IDqComputeActorAsyncOutput::ICallbacks {
    void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, bool isFatal) override final {
        OnSinkError(outputIndex, issues, isFatal);
    }

    void OnAsyncOutputStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override final {
        OnSinkStateSaved(std::move(state), outputIndex, checkpoint);
    }

    virtual void OnSinkError(ui64 outputIndex, const TIssues& issues, bool isFatal) = 0;
    virtual void OnSinkStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) = 0;
};

struct TOutputTransformCallbacks : public IDqComputeActorAsyncOutput::ICallbacks {
    void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, bool isFatal) override final {
        OnOutputTransformError(outputIndex, issues, isFatal);
    }

    void OnAsyncOutputStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override final {
        OnTransformStateSaved(std::move(state), outputIndex, checkpoint);
    }

    virtual void OnOutputTransformError(ui64 outputIndex, const TIssues& issues, bool isFatal) = 0;
    virtual void OnTransformStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) = 0;
};

template<typename TDerived>
class TDqComputeActorBase : public NActors::TActorBootstrapped<TDerived>
                          , public TDqComputeActorChannels::ICallbacks
                          , public TDqComputeActorCheckpoints::ICallbacks
                          , public TSinkCallbacks
                          , public TOutputTransformCallbacks
{
protected:
    enum EEvWakeupTag : ui64 {
        TimeoutTag = 1,
        PeriodicStatsTag = 2,
        RlSendAllowedTag = 101,
        RlNoResourceTag = 102,
    };

    static constexpr bool HasAsyncTaskRunner = false;

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
            InternalError(NYql::NDqProto::StatusIds::OVERLOADED, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Mkql memory limit exceeded, limit: " << GetMkqlMemoryLimit()
                << ", host: " << HostName()
                << ", canAllocateExtraMemory: " << CanAllocateExtraMemory);
        } catch (const std::exception& e) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::UNEXPECTED, e.what());
        }

        ReportEventElapsedTime();
    }

protected:
    TDqComputeActorBase(const NActors::TActorId& executerId, const TTxId& txId, NDqProto::TDqTask&& task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits, bool ownMemoryQuota = true, bool passExceptions = false)
        : ExecuterId(executerId)
        , TxId(txId)
        , Task(std::move(task))
        , RuntimeSettings(settings)
        , MemoryLimits(memoryLimits)
        , CanAllocateExtraMemory(RuntimeSettings.ExtraMemoryAllocationPool != 0 && MemoryLimits.AllocateMemoryFn)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FunctionRegistry(functionRegistry)
        , CheckpointingMode(GetTaskCheckpointingMode(Task))
        , State(Task.GetCreateSuspended() ? NDqProto::COMPUTE_STATE_UNKNOWN : NDqProto::COMPUTE_STATE_EXECUTING)
        , MemoryQuota(ownMemoryQuota ? InitMemoryQuota() : nullptr)
        , Running(!Task.GetCreateSuspended())
        , PassExceptions(passExceptions)
    {
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            BasicStats = std::make_unique<TBasicStats>();
        }
        InitializeTask();
    }

    TDqComputeActorBase(const NActors::TActorId& executerId, const TTxId& txId, const NDqProto::TDqTask& task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits)
        : ExecuterId(executerId)
        , TxId(txId)
        , Task(task)
        , RuntimeSettings(settings)
        , MemoryLimits(memoryLimits)
        , CanAllocateExtraMemory(RuntimeSettings.ExtraMemoryAllocationPool != 0 && MemoryLimits.AllocateMemoryFn)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FunctionRegistry(functionRegistry)
        , State(Task.GetCreateSuspended() ? NDqProto::COMPUTE_STATE_UNKNOWN : NDqProto::COMPUTE_STATE_EXECUTING)
        , MemoryQuota(InitMemoryQuota())
        , Running(!Task.GetCreateSuspended())
    {
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            BasicStats = std::make_unique<TBasicStats>();
        }
        InitializeTask();
    }

    void ReportEventElapsedTime() {
        if (BasicStats) {
            ui64 elapsedMicros = NActors::TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000ull;
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
                hFunc(IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived, OnNewAsyncInputDataArrived);
                hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, OnAsyncInputError);
                default: {
                    CA_LOG_C("TDqComputeActorBase, unexpected event: " << ev->GetTypeRewrite() << " (" << GetEventTypeString(ev) << ")");
                    InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite() << " (" << GetEventTypeString(ev) << ")");
                }
            }
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            InternalError(NYql::NDqProto::StatusIds::OVERLOADED, TIssuesIds::KIKIMR_PRECONDITION_FAILED, TStringBuilder()
                << "Mkql memory limit exceeded, limit: " << GetMkqlMemoryLimit()
                << ", host: " << HostName()
                << ", canAllocateExtraMemory: " << CanAllocateExtraMemory);
        } catch (const std::exception& e) {
            if (PassExceptions) {
                throw;
            }
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::UNEXPECTED, e.what());
        }

        if (reportTime) {
            ReportEventElapsedTime();
        }
    }

protected:
    THolder<TDqMemoryQuota> InitMemoryQuota() {
        return MakeHolder<TDqMemoryQuota>(
            CalcMkqlMemoryLimit(),
            MemoryLimits,
            TxId,
            Task.GetId(),
            RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_PROFILE,
            CanAllocateExtraMemory,
            NActors::TActivationContext::ActorSystem());
    }

    virtual ui64 GetMkqlMemoryLimit() const {
        Y_VERIFY(MemoryQuota);
        return MemoryQuota->GetMkqlMemoryLimit();
    }

    void DoExecute() {
        auto guard = BindAllocator();
        auto* alloc = guard.GetMutex();

        if (State == NDqProto::COMPUTE_STATE_FINISHED) {
            if (!DoHandleChannelsAfterFinishImpl()) {
                return;
            }
        } else {
            DoExecuteImpl();
        }

        if (MemoryQuota) {
            MemoryQuota->TryShrinkMemory(alloc);
        }

        ReportStats(TInstant::Now());
    }

    virtual void DoExecuteImpl() {
        auto sourcesState = GetSourcesState();

        PollAsyncInput();
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

    virtual bool DoHandleChannelsAfterFinishImpl() {
        Y_VERIFY(Checkpoints);

        if (Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved() && ReadyToCheckpoint()) {
            Checkpoints->DoCheckpoint();
        }

        // Send checkpoints to output channels.
        ProcessOutputsImpl(ERunStatus::Finished);
        return true;  // returns true, when channels were handled syncronously
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

        for (auto& [outputIndex, info] : OutputTransformsMap) {
            DrainAsyncOutput(outputIndex, info);
        }

        for (auto& [outputIndex, info] : SinksMap) {
            DrainAsyncOutput(outputIndex, info);
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
                    ReportStateAndMaybeDie(NYql::NDqProto::StatusIds::SUCCESS, {TIssue("success")});
                }
            }
        }
    }

protected:
    void Terminate(bool success, const TIssues& issues) {
        if (MemoryQuota) {
            MemoryQuota->TryReleaseQuota();
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
                source.AsyncInput->PassAway();
            }
        }

        for (auto& [_, transform] : InputTransformsMap) {
            if (transform.Actor) {
                transform.AsyncInput->PassAway();
            }
        }

        for (auto& [_, sink] : SinksMap) {
            if (sink.Actor) {
                sink.AsyncOutput->PassAway();
            }
        }

        for (auto& [_, transform] : OutputTransformsMap) {
            if (transform.Actor) {
                transform.AsyncOutput->PassAway();
            }
        }

        for (auto& [_, outputChannel] : OutputChannelsMap) {
            if (outputChannel.Channel) {
                outputChannel.Channel->Terminate();
            }
        }

        if (RuntimeSettings.TerminateHandler) {
            RuntimeSettings.TerminateHandler(success, issues);
        }

        {
            // free MKQL memory then destroy TaskRunner and Allocator
            if (auto guard = MaybeBindAllocator()) {
#define CLEANUP(what) decltype(what) what##_; what.swap(what##_);
                CLEANUP(InputChannelsMap);
                CLEANUP(SourcesMap);
                CLEANUP(InputTransformsMap);
                CLEANUP(OutputChannelsMap);
                CLEANUP(SinksMap);
                CLEANUP(OutputTransformsMap);
#undef CLEANUP
            }
        }

        this->PassAway();
        MemoryQuota = nullptr;
    }

    void Terminate(bool success, const TString& message) {
        Terminate(success, TIssues({TIssue(message)}));
    }

    void ReportStateAndMaybeDie(NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues)
    {
        auto execEv = MakeHolder<TEvDqCompute::TEvState>();
        auto& record = execEv->Record;

        record.SetState(State);
        record.SetStatusCode(statusCode);
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
        InternalError(NYql::NDqProto::StatusIds::PRECONDITION_FAILED, issueCode, message);
    }

    void InternalError(NYql::NDqProto::StatusIds::StatusCode statusCode, TIssuesIds::EIssueCode issueCode, const TString& message) {
        TIssue issue(message);
        SetIssueCode(issueCode, issue);
        InternalError(statusCode, std::move(issue));
    }

    void InternalError(NYql::NDqProto::StatusIds::StatusCode statusCode, TIssue issue) {
        InternalError(statusCode, TIssues({std::move(issue)}));
    }

    void InternalError(NYql::NDqProto::StatusIds::StatusCode statusCode, TIssues issues) {
        CA_LOG_E(InternalErrorLogString(statusCode, issues));

        std::optional<TGuard<NKikimr::NMiniKQL::TScopedAlloc>> guard = MaybeBindAllocator();
        State = NDqProto::COMPUTE_STATE_FAILURE;
        ReportStateAndMaybeDie(statusCode, issues);
    }

    TString InternalErrorLogString(NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues) {
        TStringBuilder log;
        log << "InternalError: " << NYql::NDqProto::StatusIds_StatusCode_Name(statusCode);
        if (issues) {
            const auto& issueCodeName = TIssuesIds::EIssueCode_Name(issues.begin()->GetCode());
            if (!issueCodeName.empty()) {
                log << ' ' << issueCodeName;
            }
            log << ": ";
            issues.PrintTo(log.Out, true /* oneLine */);
        }
        log << '.';
        return std::move(log);
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

    void OnTransformStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override {
        Y_VERIFY(Checkpoints); // If we are checkpointing, we must have already constructed "checkpoints" object.
        Checkpoints->OnTransformStateSaved(std::move(state), outputIndex, checkpoint);
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
        data.SetVersion(TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion);
        data.SetBlob(TaskRunner->Save());

        for (auto& [inputIndex, source] : SourcesMap) {
            YQL_ENSURE(source.AsyncInput, "Source[" << inputIndex << "] is not created");
            NDqProto::TSourceState& sourceState = *state.AddSources();
            source.AsyncInput->SaveState(checkpoint, sourceState);
            sourceState.SetInputIndex(inputIndex);
        }
    }

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override {
        CA_LOG_D("Commit state");
        for (auto& [inputIndex, source] : SourcesMap) {
            Y_VERIFY(source.AsyncInput);
            source.AsyncInput->CommitState(checkpoint);
        }
    }

    void InjectBarrierToOutputs(const NDqProto::TCheckpoint& checkpoint) override {
        Y_VERIFY(CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
        for (const auto& [id, channelInfo] : OutputChannelsMap) {
            if (!channelInfo.IsTransformOutput) {
                channelInfo.Channel->Push(NDqProto::TCheckpoint(checkpoint));
            }
        }
        for (const auto& [outputIndex, sink] : SinksMap) {
            sink.Buffer->Push(NDqProto::TCheckpoint(checkpoint));
        }
        for (const auto& [outputIndex, transform] : OutputTransformsMap) {
            transform.Buffer->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void ResumeInputs() override {
        for (auto& [id, channelInfo] : InputChannelsMap) {
            channelInfo.Resume();
        }
    }

    virtual void DoLoadRunnerState(TString&& blob) {
        TMaybe<TString> error = Nothing();
        try {
            TaskRunner->Load(blob);
        } catch (const std::exception& e) {
            error = e.what();
        }
        Checkpoints->AfterStateLoading(error);
    }

    void LoadState(NDqProto::TComputeActorState&& state) override {
        CA_LOG_D("Load state");
        TMaybe<TString> error = Nothing();
        const NDqProto::TMiniKqlProgramState& mkqlProgramState = state.GetMiniKqlProgram();
        auto guard = BindAllocator();
        try {
            const ui64 version = mkqlProgramState.GetData().GetStateData().GetVersion();
            YQL_ENSURE(version && version <= TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion && version != TDqComputeActorCheckpoints::ComputeActorNonProtobufStateVersion, "Unsupported state version: " << version);
            if (version != TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion) {
                ythrow yexception() << "Invalid state version " << version;
            }
            for (const NDqProto::TSourceState& sourceState : state.GetSources()) {
                TAsyncInputInfoBase* source = SourcesMap.FindPtr(sourceState.GetInputIndex());
                YQL_ENSURE(source, "Failed to load state. Source with input index " << sourceState.GetInputIndex() << " was not found");
                YQL_ENSURE(source->AsyncInput, "Source[" << sourceState.GetInputIndex() << "] is not created");
                source->AsyncInput->LoadState(sourceState);
            }
            for (const NDqProto::TSinkState& sinkState : state.GetSinks()) {
                TAsyncOutputInfoBase* sink = SinksMap.FindPtr(sinkState.GetOutputIndex());
                YQL_ENSURE(sink, "Failed to load state. Sink with output index " << sinkState.GetOutputIndex() << " was not found");
                YQL_ENSURE(sink->AsyncOutput, "Sink[" << sinkState.GetOutputIndex() << "] is not created");
                sink->AsyncOutput->LoadState(sinkState);
            }
        } catch (const std::exception& e) {
            error = e.what();
        }
        TString& blob = *state.MutableMiniKqlProgram()->MutableData()->MutableStateData()->MutableBlob();
        if (blob && !error.Defined()) {
            CA_LOG_D("State size: " << blob.size());
            DoLoadRunnerState(std::move(blob));
        } else {
            Checkpoints->AfterStateLoading(error);
        }
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
            if (Channel) {  // async actor doesn't hold channels, so channel is paused in task runner actor
                Channel->Pause();
            }
        }

        void Resume() {
            PendingCheckpoint.reset();
            if (Channel) {  // async actor doesn't hold channels, so channel is resumed in task runner actor
                Channel->Resume();
            }
        }
    };

    struct TAsyncInputInfoBase {
        ui64 Index;
        IDqAsyncInputBuffer::TPtr Buffer;
        IDqComputeActorAsyncInput* AsyncInput = nullptr;
        NActors::IActor* Actor = nullptr;
        TIssuesBuffer IssuesBuffer;
        bool Finished = false;
        i64 FreeSpace = 1;
        bool PushStarted = false;

        explicit TAsyncInputInfoBase(ui64 index) : Index(index), IssuesBuffer(IssuesBufferSize) {}
    };

    struct TAsyncInputTransformInfo : public TAsyncInputInfoBase {
        NUdf::TUnboxedValue InputBuffer;
        TMaybe<NKikimr::NMiniKQL::TProgramBuilder> ProgramBuilder;

        using TAsyncInputInfoBase::TAsyncInputInfoBase;
    };

    struct TOutputChannelInfo {
        ui64 ChannelId;
        IDqOutputChannel::TPtr Channel;
        bool HasPeer = false;
        bool Finished = false; // != Channel->IsFinished() // If channel is in finished state, it sends only checkpoints.
        bool PopStarted = false;
        bool IsTransformOutput = false; // Is this channel output of a transform.

        explicit TOutputChannelInfo(ui64 channelId)
            : ChannelId(channelId)
        { }

        struct TStats {
            ui64 BlockedByCapacity = 0;
            ui64 NoDstActorId = 0;
        };
        THolder<TStats> Stats;
    };

    struct TAsyncOutputInfoBase {
        IDqAsyncOutputBuffer::TPtr Buffer;
        IDqComputeActorAsyncOutput* AsyncOutput = nullptr;
        NActors::IActor* Actor = nullptr;
        bool Finished = false; // If sink/transform is in finished state, it receives only checkpoints.
        TIssuesBuffer IssuesBuffer;
        bool PopStarted = false;
        i64 FreeSpaceBeforeSend = 0;

        TAsyncOutputInfoBase() : IssuesBuffer(IssuesBufferSize) {}
    };

    struct TAsyncOutputTransformInfo : public TAsyncOutputInfoBase {
        IDqOutputConsumer::TPtr OutputBuffer;
        TMaybe<NKikimr::NMiniKQL::TProgramBuilder> ProgramBuilder;
    };

protected:
    // virtual methods (TODO: replace with static_cast<TDerived*>(this)->Foo()

    virtual std::any GetSourcesState() {
        return nullptr;
    }

    virtual void PollSources(std::any /* state */) {
    }

    virtual void TerminateSources(const TIssues& /* issues */, bool /* success */) {
    }

    void TerminateSources(const TString& message, bool success) {
        TerminateSources(TIssues({TIssue(message)}), success);
    }

    virtual TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return TaskRunner->BindAllocator();
    }

    virtual std::optional<TGuard<NKikimr::NMiniKQL::TScopedAlloc>> MaybeBindAllocator() {
        return TaskRunner->BindAllocator();
    }

    virtual void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueVector&& batch, TAsyncInputInfoBase& source, i64 space, bool finished) {
        source.Buffer->Push(std::move(batch), space);
        if (finished) {
            source.Buffer->Finish();
            source.Finished = true;
        }
    }

    virtual i64 AsyncIoFreeSpace(TAsyncInputInfoBase& source) {
        return source.Buffer->GetFreeSpace();
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

        if (Running) {  // waiting for TEvRun to start
            DoExecute();
        }
    }

    void HandleExecuteBase(NActors::TEvents::TEvWakeup::TPtr& ev) {
        auto tag = (EEvWakeupTag) ev->Get()->Tag;
        switch (tag) {
            case EEvWakeupTag::TimeoutTag: {
                auto abortEv = MakeHolder<TEvDq::TEvAbortExecution>(NYql::NDqProto::StatusIds::TIMEOUT, TStringBuilder()
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
        evState->Record.SetStatusCode(NYql::NDqProto::StatusIds::SUCCESS);
        evState->Record.SetTaskId(Task.GetId());
        FillStats(evState->Record.MutableStats(), /* last */ false);
        this->Send(ev->Sender, evState.Release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void HandleExecuteBase(TEvDqCompute::TEvNewCheckpointCoordinator::TPtr& ev) {
        if (!InputTransformsMap.empty()) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::UNEXPECTED, "Input transforms don't support checkpoints yet");
            return;
        }

        if (!Checkpoints) {
            Checkpoints = new TDqComputeActorCheckpoints(this->SelfId(), TxId, Task, this);
            Checkpoints->Init(this->SelfId(), this->RegisterWithSameMailbox(Checkpoints));
            Channels->SetCheckpointsSupport();
        }
        TAutoPtr<NActors::IEventHandle> handle = new NActors::IEventHandle(Checkpoints->SelfId(), ev->Sender, ev->Release().Release());
        Checkpoints->Receive(handle, NActors::TActivationContext::AsActorContext());
    }

    void HandleExecuteBase(TEvDq::TEvAbortExecution::TPtr& ev) {
        if (ev->Get()->Record.GetStatusCode() == NYql::NDqProto::StatusIds::INTERNAL_ERROR) {
            Y_VERIFY(ev->Get()->GetIssues().Size() == 1);
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, *ev->Get()->GetIssues().begin());
            return;
        }
        TIssues issues = ev->Get()->GetIssues();
        CA_LOG_E("Handle abort execution event from: " << ev->Sender
            << ", status: " << NYql::NDqProto::StatusIds_StatusCode_Name(ev->Get()->Record.GetStatusCode())
            << ", reason: " << issues.ToOneLineString());

        bool success = ev->Get()->Record.GetStatusCode() == NYql::NDqProto::StatusIds::SUCCESS;

        this->TerminateSources(issues, success);

        if (ev->Sender != ExecuterId) {
            NActors::TActivationContext::Send(ev->Forward(ExecuterId));
        }

        Terminate(success, issues);
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
    virtual const TDqMemoryQuota::TProfileStats* GetProfileStats() const {
        Y_VERIFY(MemoryQuota);
        return MemoryQuota->GetProfileStats();
    }

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

    virtual void DrainAsyncOutput(ui64 outputIndex, TAsyncOutputInfoBase& outputInfo) {
        ProcessOutputsState.AllOutputsFinished &= outputInfo.Finished;
        if (outputInfo.Finished && !Checkpoints) {
            return;
        }

        Y_VERIFY(outputInfo.Buffer);
        Y_VERIFY(outputInfo.AsyncOutput);
        Y_VERIFY(outputInfo.Actor);

        const ui32 allowedOvercommit = AllowedChannelsOvercommit();
        const i64 sinkFreeSpaceBeforeSend = outputInfo.AsyncOutput->GetFreeSpace();

        i64 toSend = sinkFreeSpaceBeforeSend + allowedOvercommit;
        CA_LOG_D("About to drain async output " << outputIndex
            << ". FreeSpace: " << sinkFreeSpaceBeforeSend
            << ", allowedOvercommit: " << allowedOvercommit
            << ", toSend: " << toSend
            << ", finished: " << outputInfo.Buffer->IsFinished());

        i64 sent = 0;
        while (toSend > 0 && (!outputInfo.Finished || Checkpoints)) {
            const ui32 sentChunk = SendDataChunkToAsyncOutput(outputIndex, outputInfo, toSend);
            if (sentChunk == 0) {
                break;
            }
            sent += sentChunk;
            toSend = outputInfo.AsyncOutput->GetFreeSpace() + allowedOvercommit;
        }

        CA_LOG_D("Drain async output " << outputIndex
            << ". Free space decreased: " << (sinkFreeSpaceBeforeSend - outputInfo.AsyncOutput->GetFreeSpace())
            << ", sent data from buffer: " << sent);

        ProcessOutputsState.HasDataToSend |= !outputInfo.Finished;
        ProcessOutputsState.DataWasSent |= outputInfo.Finished || sent;
    }

    ui32 SendDataChunkToAsyncOutput(ui64 outputIndex, TAsyncOutputInfoBase& outputInfo, ui64 bytes) {
        auto sink = outputInfo.Buffer;

        NKikimr::NMiniKQL::TUnboxedValueVector dataBatch;
        NDqProto::TCheckpoint checkpoint;

        const ui64 dataSize = !outputInfo.Finished ? sink->Pop(dataBatch, bytes) : 0;
        const bool hasCheckpoint = sink->Pop(checkpoint);
        if (!dataSize && !hasCheckpoint) {
            if (!sink->IsFinished()) {
                CA_LOG_D("sink " << outputIndex << ": nothing to send and is not finished");
                return 0; // sink is empty and not finished yet
            }
        }
        outputInfo.Finished = sink->IsFinished();

        YQL_ENSURE(!dataSize || !dataBatch.empty()); // dataSize != 0 => !dataBatch.empty() // even if we're about to send empty rows.

        const ui32 checkpointSize = hasCheckpoint ? checkpoint.ByteSize() : 0;

        TMaybe<NDqProto::TCheckpoint> maybeCheckpoint;
        if (hasCheckpoint) {
            maybeCheckpoint = checkpoint;
            CA_LOG_I("Resume inputs");
            ResumeInputs();
        }

        outputInfo.AsyncOutput->SendData(std::move(dataBatch), dataSize, maybeCheckpoint, outputInfo.Finished);
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

        auto guard = TaskRunner->BindAllocator(MemoryQuota->GetMkqlMemoryLimit());
        auto* alloc = guard.GetMutex();

        MemoryQuota->TrySetIncreaseMemoryLimitCallback(alloc);

        TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = MemoryLimits.ChannelBufferSize;
        limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

        TaskRunner->Prepare(Task, limits, execCtx);

        FillIoMaps(
            TaskRunner->GetHolderFactory(),
            TaskRunner->GetTypeEnv(),
            TaskRunner->GetSecureParams(),
            TaskRunner->GetTaskParams());
    }

    void FillIoMaps(
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
            if (TaskRunner) { source.Buffer = TaskRunner->GetSource(inputIndex); Y_VERIFY(source.Buffer);}
            Y_VERIFY(AsyncIoFactory);
            const auto& inputDesc = Task.GetInputs(inputIndex);
            const ui64 i = inputIndex; // Crutch for clang
            CA_LOG_D("Create source for input " << i << " " << inputDesc);
            std::tie(source.AsyncInput, source.Actor) = AsyncIoFactory->CreateDqSource(
                IDqAsyncIoFactory::TSourceArguments {
                    .InputDesc = inputDesc,
                    .InputIndex = inputIndex,
                    .TxId = TxId,
                    .SecureParams = secureParams,
                    .TaskParams = taskParams,
                    .ComputeActorId = this->SelfId(),
                    .TypeEnv = typeEnv,
                    .HolderFactory = holderFactory
                });
            this->RegisterWithSameMailbox(source.Actor);
        }
        for (auto& [inputIndex, transform] : InputTransformsMap) {
            if (TaskRunner) {
                transform.ProgramBuilder.ConstructInPlace(TaskRunner->GetTypeEnv(), *FunctionRegistry);
                std::tie(transform.InputBuffer, transform.Buffer) = TaskRunner->GetInputTransform(inputIndex);
                Y_VERIFY(AsyncIoFactory);
                const auto& inputDesc = Task.GetInputs(inputIndex);
                const ui64 i = inputIndex; // Crutch for clang
                CA_LOG_D("Create transform for input " << i << " " << inputDesc.ShortDebugString());
                std::tie(transform.AsyncInput, transform.Actor) = AsyncIoFactory->CreateDqInputTransform(
                    IDqAsyncIoFactory::TInputTransformArguments {
                        .InputDesc = inputDesc,
                        .InputIndex = inputIndex,
                        .TxId = TxId,
                        .TransformInput = transform.InputBuffer,
                        .SecureParams = secureParams,
                        .TaskParams = taskParams,
                        .ComputeActorId = this->SelfId(),
                        .TypeEnv = typeEnv,
                        .HolderFactory = holderFactory,
                        .ProgramBuilder = *transform.ProgramBuilder
                    });
                this->RegisterWithSameMailbox(transform.Actor);
            }
        }
        if (TaskRunner) {
            for (auto& [channelId, channel] : OutputChannelsMap) {
                channel.Channel = TaskRunner->GetOutputChannel(channelId);
            }
        }
        for (auto& [outputIndex, transform] : OutputTransformsMap) {
            if (TaskRunner) {
                transform.ProgramBuilder.ConstructInPlace(TaskRunner->GetTypeEnv(), *FunctionRegistry);
                std::tie(transform.Buffer, transform.OutputBuffer) = TaskRunner->GetOutputTransform(outputIndex);
                Y_VERIFY(AsyncIoFactory);
                const auto& outputDesc = Task.GetOutputs(outputIndex);
                const ui64 i = outputIndex; // Crutch for clang
                CA_LOG_D("Create transform for output " << i << " " << outputDesc.ShortDebugString());
                std::tie(transform.AsyncOutput, transform.Actor) = AsyncIoFactory->CreateDqOutputTransform(
                    IDqAsyncIoFactory::TOutputTransformArguments {
                        .OutputDesc = outputDesc,
                        .OutputIndex = outputIndex,
                        .TxId = TxId,
                        .TransformOutput = transform.OutputBuffer,
                        .Callback = static_cast<TOutputTransformCallbacks*>(this),
                        .SecureParams = secureParams,
                        .TypeEnv = typeEnv,
                        .HolderFactory = holderFactory,
                        .ProgramBuilder = *transform.ProgramBuilder
                    });
                this->RegisterWithSameMailbox(transform.Actor);
            }
        }
        for (auto& [outputIndex, sink] : SinksMap) {
            if (TaskRunner) { sink.Buffer = TaskRunner->GetSink(outputIndex); }
            Y_VERIFY(AsyncIoFactory);
            const auto& outputDesc = Task.GetOutputs(outputIndex);
            const ui64 i = outputIndex; // Crutch for clang
            CA_LOG_D("Create sink for output " << i << " " << outputDesc);
            std::tie(sink.AsyncOutput, sink.Actor) = AsyncIoFactory->CreateDqSink(
                IDqAsyncIoFactory::TSinkArguments {
                    .OutputDesc = outputDesc,
                    .OutputIndex = outputIndex,
                    .TxId = TxId,
                    .Callback = static_cast<TSinkCallbacks*>(this),
                    .SecureParams = secureParams,
                    .TypeEnv = typeEnv,
                    .HolderFactory = holderFactory
                });
            this->RegisterWithSameMailbox(sink.Actor);
        }
    }

    void PollAsyncInput(TAsyncInputInfoBase& info, ui64 inputIndex) {
        Y_VERIFY(!TaskRunner || info.Buffer);
        if (info.Finished) {
            const ui64 indexForLogging = inputIndex; // Crutch for clang
            CA_LOG_D("Skip polling async input[" << indexForLogging << "]: finished");
            return;
        }
        const i64 freeSpace = AsyncIoFreeSpace(info);
        if (freeSpace > 0) {
            NKikimr::NMiniKQL::TUnboxedValueVector batch;
            Y_VERIFY(info.AsyncInput);
            bool finished = false;
            const i64 space = info.AsyncInput->GetAsyncInputData(batch, finished, freeSpace);
            CA_LOG_D("Poll async input " << inputIndex
                << ". Buffer free space: " << freeSpace
                << ", read from async input: " << space << " bytes, "
                << batch.size() << " rows, finished: " << finished);

            if (!batch.empty()) {
                // If we have read some data, we must run such reading again
                // to process the case when async input notified us about new data
                // but we haven't read all of it.
                ContinueExecute();
            }
            AsyncInputPush(std::move(batch), info, space, finished);
        }
    }

    void PollAsyncInput() {
        // Don't produce any input from sources if we're about to save checkpoint.
        if (!Running || (Checkpoints && Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved())) {
            CA_LOG_D("Skip polling sources because of pending checkpoint");
            return;
        }

        for (auto& [inputIndex, source] : SourcesMap) {
            PollAsyncInput(source, inputIndex);
        }

        for (auto& [inputIndex, transform] : InputTransformsMap) {
            PollAsyncInput(transform, inputIndex);
        }
    }

    void OnNewAsyncInputDataArrived(const IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::TPtr& ev) {
        Y_VERIFY(SourcesMap.FindPtr(ev->Get()->InputIndex) || InputTransformsMap.FindPtr(ev->Get()->InputIndex));
        ContinueExecute();
    }

    void OnAsyncInputError(const IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
        if (SourcesMap.FindPtr(ev->Get()->InputIndex)) {
            OnSourceError(ev->Get()->InputIndex, ev->Get()->Issues, ev->Get()->IsFatal);
        } else if (InputTransformsMap.FindPtr(ev->Get()->InputIndex)) {
            OnInputTransformError(ev->Get()->InputIndex, ev->Get()->Issues, ev->Get()->IsFatal);
        } else {
            YQL_ENSURE(false, "Unexpected input index: " << ev->Get()->InputIndex);
        }
    }

    void OnSourceError(ui64 inputIndex, const TIssues& issues, bool isFatal) {
        if (!isFatal) {
            SourcesMap.at(inputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("Source[" << inputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(NYql::NDqProto::StatusIds::EXTERNAL_ERROR, issues);
    }

    void OnInputTransformError(ui64 inputIndex, const TIssues& issues, bool isFatal) {
        if (!isFatal) {
            InputTransformsMap.at(inputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("InputTransform[" << inputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(NYql::NDqProto::StatusIds::EXTERNAL_ERROR, issues);
    }

    void OnSinkError(ui64 outputIndex, const TIssues& issues, bool isFatal) override {
        if (!isFatal) {
            SinksMap.at(outputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("Sink[" << outputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(NYql::NDqProto::StatusIds::EXTERNAL_ERROR, issues);
    }

    void OnOutputTransformError(ui64 outputIndex, const TIssues& issues, bool isFatal) override {
        if (!isFatal) {
            OutputTransformsMap.at(outputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("OutputTransform[" << outputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(NYql::NDqProto::StatusIds::EXTERNAL_ERROR, issues);
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

            if (inputDesc.HasTransform()) {
                auto result = InputTransformsMap.emplace(std::piecewise_construct, std::make_tuple(i), std::make_tuple(i));
                YQL_ENSURE(result.second);
            }

            if (inputDesc.HasSource()) {
                auto result = SourcesMap.emplace(std::piecewise_construct, std::make_tuple(i), std::make_tuple(i));
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

            if (outputDesc.HasTransform()) {
                auto result = OutputTransformsMap.emplace(std::piecewise_construct, std::make_tuple(i), std::make_tuple());
                YQL_ENSURE(result.second);
            }

            if (outputDesc.HasSink()) {
                auto result = SinksMap.emplace(i, TAsyncOutputInfoBase());
                YQL_ENSURE(result.second);
            } else {
                for (auto& channel : outputDesc.GetChannels()) {
                    TOutputChannelInfo outputChannel(channel.GetId());
                    outputChannel.HasPeer = channel.GetDstEndpoint().HasActorId();
                    outputChannel.IsTransformOutput = outputDesc.HasTransform();

                    if (Y_UNLIKELY(RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_PROFILE)) {
                        outputChannel.Stats = MakeHolder<typename TOutputChannelInfo::TStats>();
                    }

                    auto result = OutputChannelsMap.emplace(channel.GetId(), std::move(outputChannel));
                    YQL_ENSURE(result.second);
                }
            }
        }
    }

    virtual const NYql::NDq::TTaskRunnerStatsBase* GetTaskRunnerStats() {
        if (!TaskRunner) {
            return nullptr;
        }
        TaskRunner->UpdateStats();
        return TaskRunner->GetStats();
    }

    virtual const TDqAsyncOutputBufferStats* GetSinkStats(ui64 outputIdx, const TAsyncOutputInfoBase& sinkInfo) const {
        Y_UNUSED(outputIdx);
        return sinkInfo.Buffer ? sinkInfo.Buffer->GetStats() : nullptr;
    }

public:
    void FillStats(NDqProto::TDqComputeActorStats* dst, bool last) {
        if (!BasicStats) {
            return;
        }

        if (last) {
            ReportEventElapsedTime();
        }

        dst->SetCpuTimeUs(BasicStats->CpuTime.MicroSeconds());

        if (GetProfileStats()) {
            dst->SetMkqlMaxMemoryUsage(GetProfileStats()->MkqlMaxUsedMemory);
            dst->SetMkqlExtraMemoryBytes(GetProfileStats()->MkqlExtraMemoryBytes);
            dst->SetMkqlExtraMemoryRequests(GetProfileStats()->MkqlExtraMemoryRequests);
        }

        if (auto* taskStats = GetTaskRunnerStats()) {
            auto* protoTask = dst->AddTasks();
            FillTaskRunnerStats(Task.GetId(), Task.GetStageId(), *taskStats, protoTask, (bool) GetProfileStats());

            // More accurate cpu time counter:
            if (TDerived::HasAsyncTaskRunner) {
                protoTask->SetCpuTimeUs(BasicStats->CpuTime.MicroSeconds() + taskStats->ComputeCpuTime.MicroSeconds() + taskStats->BuildCpuTime.MicroSeconds());
            }

            for (auto& [outputIndex, sinkInfo] : SinksMap) {
                if (auto* sinkStats = GetSinkStats(outputIndex, sinkInfo)) {
                    protoTask->SetOutputRows(protoTask->GetOutputRows() + sinkStats->RowsIn);
                    protoTask->SetOutputBytes(protoTask->GetOutputBytes() + sinkStats->Bytes);

                    if (GetProfileStats()) {
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

            if (GetProfileStats()) {
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

        static_cast<TDerived*>(this)->FillExtraStats(dst, last);

        if (last) {
            BasicStats.reset();
        }
        if (last && MemoryQuota) {
            MemoryQuota->ResetProfileStats();
        }
    }

protected:
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
    const IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    const NDqProto::ECheckpointingMode CheckpointingMode;
    TIntrusivePtr<IDqTaskRunner> TaskRunner;
    TDqComputeActorChannels* Channels = nullptr;
    TDqComputeActorCheckpoints* Checkpoints = nullptr;
    THashMap<ui64, TInputChannelInfo> InputChannelsMap; // Channel id -> Channel info
    THashMap<ui64, TAsyncInputInfoBase> SourcesMap; // Input index -> Source info
    THashMap<ui64, TAsyncInputTransformInfo> InputTransformsMap; // Input index -> Transforms info
    THashMap<ui64, TOutputChannelInfo> OutputChannelsMap; // Channel id -> Channel info
    THashMap<ui64, TAsyncOutputInfoBase> SinksMap; // Output index -> Sink info
    THashMap<ui64, TAsyncOutputTransformInfo> OutputTransformsMap; // Output index -> Transforms info
    bool ResumeEventScheduled = false;
    NDqProto::EComputeState State;

    struct TBasicStats {
        TDuration CpuTime;
    };
    std::unique_ptr<TBasicStats> BasicStats;

    struct TProcessOutputsState {
        int Inflight = 0;
        bool ChannelsReady = true;
        bool HasDataToSend = false;
        bool DataWasSent = false;
        bool AllOutputsFinished = true;
        ERunStatus LastRunStatus = ERunStatus::PendingInput;
    };
    TProcessOutputsState ProcessOutputsState;

    THolder<TDqMemoryQuota> MemoryQuota;
private:
    bool Running = true;
    TInstant LastSendStatsTime;
    bool PassExceptions = false;
};

} // namespace NYql
} // namespace NNq
