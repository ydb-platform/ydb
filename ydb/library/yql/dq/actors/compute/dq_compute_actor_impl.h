#pragma once

#include "dq_compute_actor_async_io.h"
#include "dq_compute_actor_channels.h"
#include "dq_compute_actor_checkpoints.h"
#include "dq_compute_actor_metrics.h"
#include "dq_compute_actor_watermarks.h"
#include "dq_compute_actor.h"
#include "dq_compute_issues_buffer.h"
#include "dq_compute_memory_quota.h"

#include <ydb/library/ydb_issue/issue_helpers.h>
#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/services/services.pb.h>

#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/dq/actors/dq.h>
#include <ydb/library/yql/dq/actors/compute/dq_request_context.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>
#include <util/system/hostname.h>


#include <any>
#include <queue>

#include "dq_compute_actor_async_input_helper.h"
#include "dq_compute_actor_log.h"

namespace NYql {
namespace NDq {

struct TSinkCallbacks : public IDqComputeActorAsyncOutput::ICallbacks {
    void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override final {
        OnSinkError(outputIndex, issues, fatalCode);
    }

    void OnAsyncOutputStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override final {
        OnSinkStateSaved(std::move(state), outputIndex, checkpoint);
    }

    void OnAsyncOutputFinished(ui64 outputIndex) override final {
        OnSinkFinished(outputIndex);
    }

    virtual void OnSinkError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) = 0;
    virtual void OnSinkStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) = 0;
    virtual void OnSinkFinished(ui64 outputIndex) = 0;
};

struct TOutputTransformCallbacks : public IDqComputeActorAsyncOutput::ICallbacks {
    void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override final {
        OnOutputTransformError(outputIndex, issues, fatalCode);
    }

    void OnAsyncOutputStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override final {
        OnTransformStateSaved(std::move(state), outputIndex, checkpoint);
    }

    void OnAsyncOutputFinished(ui64 outputIndex) override final {
        OnTransformFinished(outputIndex);
    }

    virtual void OnOutputTransformError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) = 0;
    virtual void OnTransformStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) = 0;
    virtual void OnTransformFinished(ui64 outputIndex) = 0;
};

namespace NDetails {

template <class T>
struct TComputeActorStateFuncHelper;

template <class T>
struct TComputeActorStateFuncHelper<void (T::*)(STFUNC_SIG)> {
    using TComputeActorClass = T;
};

} // namespace NDetails


template<typename TDerived, typename TAsyncInputHelper>
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

public:
    void Bootstrap() {
        try {
            {
                TStringBuilder prefixBuilder;
                prefixBuilder << "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". ";
                if (RequestContext) {
                    prefixBuilder << "Ctx: " << *RequestContext << ". ";
                }
                LogPrefix = prefixBuilder;
            }
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
                this->Become(&TDqComputeActorBase::StateFuncWrapper<&TDqComputeActorBase::BaseStateFuncBody>);
            }

            static_cast<TDerived*>(this)->DoBootstrap();
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            OnMemoryLimitExceptionHandler();
        } catch (const std::exception& e) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::UNEXPECTED, e.what());
        }

        ReportEventElapsedTime();
    }

protected:
    TDqComputeActorBase(const NActors::TActorId& executerId, const TTxId& txId, NDqProto::TDqTask* task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        bool ownMemoryQuota = true, bool passExceptions = false,
        const ::NMonitoring::TDynamicCounterPtr& taskCounters = nullptr,
        NWilson::TTraceId traceId = {},
        TIntrusivePtr<NActors::TProtoArenaHolder> arena = nullptr,
        const TGUCSettings::TPtr& GUCSettings = nullptr)
        : ExecuterId(executerId)
        , TxId(txId)
        , Task(task, std::move(arena))
        , RuntimeSettings(settings)
        , MemoryLimits(memoryLimits)
        , CanAllocateExtraMemory(RuntimeSettings.ExtraMemoryAllocationPool != 0)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FunctionRegistry(functionRegistry)
        , CheckpointingMode(GetTaskCheckpointingMode(Task))
        , State(Task.GetCreateSuspended() ? NDqProto::COMPUTE_STATE_UNKNOWN : NDqProto::COMPUTE_STATE_EXECUTING)
        , WatermarksTracker(this->SelfId(), TxId, Task.GetId())
        , TaskCounters(taskCounters)
        , MetricsReporter(taskCounters)
        , ComputeActorSpan(NKikimr::TWilsonKqp::ComputeActor, std::move(traceId), "ComputeActor")
        , Running(!Task.GetCreateSuspended())
        , PassExceptions(passExceptions)
    {
        Alloc = std::make_shared<NKikimr::NMiniKQL::TScopedAlloc>(
                    __LOCATION__,
                    NKikimr::TAlignedPagePoolCounters(),
                    true,
                    false
        );
        Alloc->SetGUCSettings(GUCSettings);
        InitMonCounters(taskCounters);
        if (ownMemoryQuota) {
            MemoryQuota = InitMemoryQuota();
        }
    }

    void InitMonCounters(const ::NMonitoring::TDynamicCounterPtr& taskCounters) {
        if (taskCounters) {
            MkqlMemoryQuota = taskCounters->GetCounter("MkqlMemoryQuota");
            OutputChannelSize = taskCounters->GetCounter("OutputChannelSize");
            SourceCpuTimeMs = taskCounters->GetCounter("SourceCpuTimeMs", true);
        }
    }

    void ReportEventElapsedTime() {
        if (RuntimeSettings.CollectBasic()) {
            ui64 elapsedMicros = NActors::TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000ull;
            CpuTime += TDuration::MicroSeconds(elapsedMicros);
        }
    }

    TString GetEventTypeString(TAutoPtr<::NActors::IEventHandle>& ev) {
        return ev->GetTypeName();
    }

    template <auto FuncBody>
    STFUNC(StateFuncWrapper) {
        try {
            static_assert(std::is_member_function_pointer_v<decltype(FuncBody)>);
            using TComputeActorClass = typename NDetails::TComputeActorStateFuncHelper<decltype(FuncBody)>::TComputeActorClass;
            TComputeActorClass* self = static_cast<TComputeActorClass*>(this);
            (self->*FuncBody)(ev);
        } catch (const NKikimr::TMemoryLimitExceededException& e) {
            OnMemoryLimitExceptionHandler();
        } catch (const std::exception& e) {
            if (PassExceptions) {
                throw;
            }
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::UNEXPECTED, e.what());
        }

        ReportEventElapsedTime();
    }

    STFUNC(BaseStateFuncBody) {
        MetricsReporter.ReportEvent(ev->GetTypeRewrite(), ev);

        switch (ev->GetTypeRewrite()) {
            hFunc(TEvDqCompute::TEvResumeExecution, HandleExecuteBase);
            hFunc(TEvDqCompute::TEvChannelsInfo, HandleExecuteBase);
            hFunc(TEvDq::TEvAbortExecution, HandleExecuteBase);
            hFunc(NActors::TEvents::TEvWakeup, HandleExecuteBase);
            hFunc(NActors::TEvents::TEvUndelivered, HandleExecuteBase);
            fFunc(TEvDqCompute::TEvChannelData::EventType, Channels->Receive);
            fFunc(TEvDqCompute::TEvChannelDataAck::EventType, Channels->Receive);
            hFunc(TEvDqCompute::TEvRun, HandleExecuteBase);
            hFunc(TEvDqCompute::TEvStateRequest, HandleExecuteBase);
            hFunc(TEvDqCompute::TEvNewCheckpointCoordinator, HandleExecuteBase);
            fFunc(TEvDqCompute::TEvInjectCheckpoint::EventType, Checkpoints->Receive);
            fFunc(TEvDqCompute::TEvCommitState::EventType, Checkpoints->Receive);
            fFunc(TEvDqCompute::TEvRestoreFromCheckpoint::EventType, Checkpoints->Receive);
            hFunc(NActors::TEvInterconnect::TEvNodeDisconnected, HandleExecuteBase);
            hFunc(NActors::TEvInterconnect::TEvNodeConnected, HandleExecuteBase);
            hFunc(IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived, OnNewAsyncInputDataArrived);
            hFunc(IDqComputeActorAsyncInput::TEvAsyncInputError, OnAsyncInputError);
            hFunc(TEvDqCompute::TEvError, HandleError);
            default: {
                CA_LOG_C("TDqComputeActorBase, unexpected event: " << ev->GetTypeRewrite() << " (" << GetEventTypeString(ev) << ")");
                InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssuesIds::DEFAULT_ERROR, TStringBuilder() << "Unexpected event: " << ev->GetTypeRewrite() << " (" << GetEventTypeString(ev) << ")");
            }
        }
    }

protected:
    THolder<TDqMemoryQuota> InitMemoryQuota() {
        return MakeHolder<TDqMemoryQuota>(
            MkqlMemoryQuota,
            CalcMkqlMemoryLimit(),
            MemoryLimits,
            TxId,
            Task.GetId(),
            RuntimeSettings.CollectFull(),
            CanAllocateExtraMemory,
            NActors::TActivationContext::ActorSystem());
    }

    virtual ui64 GetMkqlMemoryLimit() const {
        Y_ABORT_UNLESS(MemoryQuota);
        return MemoryQuota->GetMkqlMemoryLimit();
    }

    void DoExecute() {
        {
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

            ReportStats(TInstant::Now(), ESendStats::IfPossible);
        }
        if (Terminated) {
            DoTerminateImpl();
            MemoryQuota.Reset();
            MemoryLimits.MemoryQuotaManager.reset();
        }
    }

    virtual void DoExecuteImpl() = 0;
    virtual void DoTerminateImpl() {}

    virtual bool DoHandleChannelsAfterFinishImpl() = 0;

    void OnMemoryLimitExceptionHandler() {
        TString memoryConsumptionDetails = MemoryLimits.MemoryQuotaManager->MemoryConsumptionDetails();
        TStringBuilder failureReason = TStringBuilder()
            << "Mkql memory limit exceeded, limit: " << GetMkqlMemoryLimit()
            << ", host: " << HostName()
            << ", canAllocateExtraMemory: " << CanAllocateExtraMemory;

        if (!memoryConsumptionDetails.empty()) {
            failureReason << ", memory manager details: " << memoryConsumptionDetails;
        }

        InternalError(NYql::NDqProto::StatusIds::OVERLOADED, TIssuesIds::KIKIMR_PRECONDITION_FAILED, failureReason);
    }

    void ProcessOutputsImpl(ERunStatus status) {
        ProcessOutputsState.LastRunStatus = status;

        CA_LOG_T("ProcessOutputsState.Inflight: " << ProcessOutputsState.Inflight);
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
                CA_LOG_T("Can not drain channelId: " << channelId << ", no dst actor id");
                if (Y_UNLIKELY(outputChannel.Stats)) {
                    outputChannel.Stats->NoDstActorId++;
                }
                continue;
            }

            if (!outputChannel.Finished || Checkpoints) {
                if (Channels->CanSendChannelData(channelId)) {
                    DrainOutputChannel(outputChannel);
                } else {
                    ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
                }
            } else {
                CA_LOG_T("Do not drain channelId: " << channelId << ", finished");
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

    virtual void CheckRunStatus() {
        if (ProcessOutputsState.Inflight != 0) {
            return;
        }

        auto status = ProcessOutputsState.LastRunStatus;

        if (status == ERunStatus::PendingInput && ProcessOutputsState.AllOutputsFinished) {
            CA_LOG_D("All outputs have been finished. Consider finished");
            status = ERunStatus::Finished;
        }

        if (InputChannelsMap.empty() && SourcesMap.empty() && status == ERunStatus::PendingInput && ProcessOutputsState.LastPopReturnedNoData) {
            // fix for situation when:
            // a) stage receives data by itself (e.g. it has YtRead inside)
            // b) last run finished with YIELD status
            // c) last run returned NO data (=> guaranteed, that peer's free space is not less than before this run)
            //
            // n.b. if c) is not satisfied we will also call ContinueExecute on branch
            // "status != ERunStatus::Finished -> !pollSent -> ProcessOutputsState.DataWasSent"
            // but idk what is the logic behind this
            ContinueExecute(EResumeSource::CAPendingInput);
            return;
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
                    ContinueExecute(EResumeSource::CADataSent);
                }
                return;
            }
        }

        if (status == ERunStatus::PendingOutput) {
            if (ProcessOutputsState.DataWasSent) {
                // we have sent some data, so we have space in output channel(s)
                ContinueExecute(EResumeSource::CAPendingOutput);
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
                if (Channels->CheckInFlight("Tasks execution finished") && AllAsyncOutputsFinished()) {
                    State = NDqProto::COMPUTE_STATE_FINISHED;
                    CA_LOG_D("Compute state finished. All channels and sinks finished");
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
            Channels->Receive(handle);
        }

        if (Checkpoints) {
            TAutoPtr<NActors::IEventHandle> handle = new NActors::IEventHandle(Checkpoints->SelfId(), this->SelfId(),
                new NActors::TEvents::TEvPoison);
            Checkpoints->Receive(handle);
        }

        {
            auto guard = BindAllocator(); // Source/Sink could destroy mkql values inside PassAway, which requires allocator to be bound

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

            if (OutputChannelSize) {
                OutputChannelSize->Sub(OutputChannelsMap.size() * MemoryLimits.ChannelBufferSize);
            }

            for (auto& [_, outputChannel] : OutputChannelsMap) {
                if (outputChannel.Channel) {
                    outputChannel.Channel->Terminate();
                }
            }

            {
                if (guard) {
                    // free MKQL memory then destroy TaskRunner and Allocator
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
        }

        if (RuntimeSettings.TerminateHandler) {
            RuntimeSettings.TerminateHandler(success, issues);
        }

        this->PassAway();
        Terminated = true;
    }

    void Terminate(bool success, const TString& message) {
        Terminate(success, TIssues({TIssue(message)}));
    }

    void FillExtraData(NDqProto::TEvComputeActorState& state) {
        auto* extraData = state.MutableExtraData();
        for (auto& [index, input] : SourcesMap) {
            if (input.AsyncInput) {
                if (auto data = input.AsyncInput->ExtraData()) {
                    auto* entry = extraData->AddSourcesExtraData();
                    entry->SetIndex(index);
                    entry->MutableData()->CopyFrom(*data);
                }
            }
        }
        for (auto& [index, transform] : InputTransformsMap) {
            if (transform.AsyncInput) {
                if (auto data = transform.AsyncInput->ExtraData()) {
                    auto* entry = extraData->AddInputTransformsData();
                    entry->SetIndex(index);
                    entry->MutableData()->CopyFrom(*data);
                }
            }
        }
        for (auto& [index, output] : SinksMap) {
            if (output.AsyncOutput) {
                if (auto data = output.AsyncOutput->ExtraData()) {
                    auto* entry = extraData->AddSinksExtraData();
                    entry->SetIndex(index);
                    entry->MutableData()->CopyFrom(*data);
                }
            }
        }
    }

    void ReportStateAndMaybeDie(NYql::NDqProto::StatusIds::StatusCode statusCode, const TIssues& issues)
    {
        auto execEv = MakeHolder<TEvDqCompute::TEvState>();
        auto& record = execEv->Record;

        FillExtraData(record);

        record.SetState(State);
        record.SetStatusCode(statusCode);
        record.SetTaskId(Task.GetId());
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            FillStats(record.MutableStats(), /* last */ true);
        }
        IssuesToMessage(issues, record.MutableIssues());

        if (ComputeActorSpan) {
            ComputeActorSpan.End();
        }

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

    virtual void InvalidateMeminfo() {}

    void InternalError(NYql::NDqProto::StatusIds::StatusCode statusCode, TIssues issues) {
        CA_LOG_E(InternalErrorLogString(statusCode, issues));
        InvalidateMeminfo();
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

    void ContinueExecute(EResumeSource source = EResumeSource::Default) {
        if (!ResumeEventScheduled && Running) {
            ResumeEventScheduled = true;
            this->Send(this->SelfId(), new TEvDqCompute::TEvResumeExecution{source});
        }
    }

    void SendError(const TString& error) {
        this->Send(this->SelfId(), new TEvDqCompute::TEvError{error});
    }

protected: //TDqComputeActorChannels::ICallbacks
    //i64 GetInputChannelFreeSpace(ui64 channelId) is pure and must be overridded in derived class

    //void TakeInputChannelData(TChannelDataOOB&& channelData, bool ack) is pure and must be overridded in derived class

    // void PeerFinished(ui64 channelId) is pure and must be overridded in derived class

    void ResumeExecution(EResumeSource source) override final{
        ContinueExecute(source);
    }

protected:
    void OnSinkStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override final {
        Y_ABORT_UNLESS(Checkpoints); // If we are checkpointing, we must have already constructed "checkpoints" object.
        Checkpoints->OnSinkStateSaved(std::move(state), outputIndex, checkpoint);
    }

    void OnTransformStateSaved(TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override final {
        Y_ABORT_UNLESS(Checkpoints); // If we are checkpointing, we must have already constructed "checkpoints" object.
        Checkpoints->OnTransformStateSaved(std::move(state), outputIndex, checkpoint);
    }

    void OnSinkFinished(ui64 outputIndex) override final {
        SinksMap.at(outputIndex).FinishIsAcknowledged = true;
        ContinueExecute(EResumeSource::CASinkFinished);
    }

    void OnTransformFinished(ui64 outputIndex) override final {
        OutputTransformsMap.at(outputIndex).FinishIsAcknowledged = true;
        ContinueExecute(EResumeSource::CATransformFinished);
    }

protected: //TDqComputeActorCheckpoints::ICallbacks
    //bool ReadyToCheckpoint() is pure and must be overriden in a derived class

    void CommitState(const NDqProto::TCheckpoint& checkpoint) override final{
        CA_LOG_D("Commit state");
        for (auto& [inputIndex, source] : SourcesMap) {
            Y_ABORT_UNLESS(source.AsyncInput);
            source.AsyncInput->CommitState(checkpoint);
        }
    }

    // void InjectBarrierToOutputs(const NDqProto::TCheckpoint& checkpoint) is pure and must be overriden in a derived class

    void ResumeInputsByWatermark(TInstant watermark) {
        for (auto& [id, sourceInfo] : SourcesMap) {
            if (sourceInfo.WatermarksMode == NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED) {
                continue;
            }

            const auto channelId = id;
            CA_LOG_T("Resume source " << channelId << " by completed watermark");

            sourceInfo.ResumeByWatermark(watermark);
        }

        for (auto& [id, channelInfo] : InputChannelsMap) {
            if (channelInfo.WatermarksMode == NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED) {
                continue;
            }

            const auto channelId = id;
            CA_LOG_T("Resume input channel " << channelId << " by completed watermark");

            channelInfo.ResumeByWatermark(watermark);
        }
    }

    void ResumeInputsByCheckpoint() override final {
        for (auto& [id, channelInfo] : InputChannelsMap) {
            channelInfo.ResumeByCheckpoint();
        }
    }

protected:
    virtual void DoLoadRunnerState(TString&& blob) = 0;

    void LoadState(TComputeActorState&& state) override final {
        CA_LOG_D("Load state");
        TMaybe<TString> error = Nothing();
        const TMiniKqlProgramState& mkqlProgramState = *state.MiniKqlProgram;
        auto guard = BindAllocator();
        try {
            const ui64 version = mkqlProgramState.Data.Version;
            YQL_ENSURE(version && version <= TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion && version != TDqComputeActorCheckpoints::ComputeActorNonProtobufStateVersion, "Unsupported state version: " << version);
            if (version != TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion) {
                ythrow yexception() << "Invalid state version " << version;
            }
            for (const TSourceState& sourceState : state.Sources) {
                TAsyncInputHelper* source = SourcesMap.FindPtr(sourceState.InputIndex);
                YQL_ENSURE(source, "Failed to load state. Source with input index " << sourceState.InputIndex << " was not found");
                YQL_ENSURE(source->AsyncInput, "Source[" << sourceState.InputIndex << "] is not created");
                source->AsyncInput->LoadState(sourceState);
            }
            for (const TSinkState& sinkState : state.Sinks) {
                TAsyncOutputInfoBase* sink = SinksMap.FindPtr(sinkState.OutputIndex);
                YQL_ENSURE(sink, "Failed to load state. Sink with output index " << sinkState.OutputIndex << " was not found");
                YQL_ENSURE(sink->AsyncOutput, "Sink[" << sinkState.OutputIndex << "] is not created");
                sink->AsyncOutput->LoadState(sinkState);
            }
        } catch (const std::exception& e) {
            error = e.what();
            CA_LOG_E("Exception: " << error);
        }
        TString& blob = state.MiniKqlProgram->Data.Blob;
        if (blob && !error.Defined()) {
            CA_LOG_D("State size: " << blob.size());
            DoLoadRunnerState(std::move(blob));
        } else {
            Checkpoints->AfterStateLoading(error);
        }
    }

    void Start() override final {
        Running = true;
        State = NDqProto::COMPUTE_STATE_EXECUTING;
        ContinueExecute(EResumeSource::CAStart);
    }

    void Stop() override final {
        Running = false;
        State = NDqProto::COMPUTE_STATE_UNKNOWN;
    }

protected:
    struct TInputChannelInfo {
        const TString LogPrefix;
        ui64 ChannelId;
        ui32 SrcStageId;
        IDqInputChannel::TPtr Channel;
        bool HasPeer = false;
        std::queue<TInstant> PendingWatermarks;
        const NDqProto::EWatermarksMode WatermarksMode;
        std::optional<NDqProto::TCheckpoint> PendingCheckpoint;
        const NDqProto::ECheckpointingMode CheckpointingMode;
        i64 FreeSpace = 0;

        explicit TInputChannelInfo(
                const TString& logPrefix,
                ui64 channelId,
                ui32 srcStageId,
                NDqProto::EWatermarksMode watermarksMode,
                NDqProto::ECheckpointingMode checkpointingMode)
            : LogPrefix(logPrefix)
            , ChannelId(channelId)
            , SrcStageId(srcStageId)
            , WatermarksMode(watermarksMode)
            , CheckpointingMode(checkpointingMode)
        {
        }

        bool IsPaused() const {
            return PendingWatermarks.empty() || PendingCheckpoint.has_value();
        }

        void Pause(TInstant watermark) {
            YQL_ENSURE(WatermarksMode != NDqProto::WATERMARKS_MODE_DISABLED);

            PendingWatermarks.emplace(watermark);
        }

        void Pause(const NDqProto::TCheckpoint& checkpoint) {
            YQL_ENSURE(!PendingCheckpoint);
            YQL_ENSURE(CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
            PendingCheckpoint = checkpoint;
            if (Channel) {  // async actor doesn't hold channels, so channel is paused in task runner actor
                Channel->Pause();
            }
        }

        void ResumeByWatermark(TInstant watermark) {
            while (!PendingWatermarks.empty() && PendingWatermarks.front() <= watermark) {
                if (PendingWatermarks.front() != watermark) {
                    CA_LOG_W("Input channel " << ChannelId <<
                        " watermarks were collapsed. See YQ-1441. Dropped watermark: " << PendingWatermarks.front());
                }
                PendingWatermarks.pop();
            }
        }

        void ResumeByCheckpoint() {
            PendingCheckpoint.reset();
            if (Channel) {  // async actor doesn't hold channels, so channel is resumed in task runner actor
                Channel->Resume();
            }
        }
    };

    //Design note:
    //Inherited TComputeActorAsyncInputHelperSync represents output part of an input transform. A transform's output is an input for TaskRunner
    struct TAsyncInputTransformHelper: TComputeActorAsyncInputHelperSync {
        using TComputeActorAsyncInputHelperSync::TComputeActorAsyncInputHelperSync;
        NUdf::TUnboxedValue Input; //Expect a flow, that is the input for the transform
    };

    struct TOutputChannelInfo {
        ui64 ChannelId;
        ui32 DstStageId;
        IDqOutputChannel::TPtr Channel;
        bool HasPeer = false;
        bool Finished = false; // != Channel->IsFinished() // If channel is in finished state, it sends only checkpoints.
        bool EarlyFinish = false;
        bool PopStarted = false;
        bool IsTransformOutput = false; // Is this channel output of a transform.
        NDqProto::EWatermarksMode WatermarksMode = NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED;

        TOutputChannelInfo(ui64 channelId, ui32 dstStageId)
            : ChannelId(channelId), DstStageId(dstStageId)
        { }

        struct TStats {
            ui64 BlockedByCapacity = 0;
            ui64 NoDstActorId = 0;
            TDuration BlockedTime;
            std::optional<TInstant> StartBlockedTime;

        };
        THolder<TStats> Stats;

        struct TAsyncData { // Is used in case of async compute actor
            TVector<TDqSerializedBatch> Data;
            TMaybe<NDqProto::TWatermark> Watermark;
            TMaybe<NDqProto::TCheckpoint> Checkpoint;
            bool Finished = false;
            bool Changed = false;
        };
        TMaybe<TAsyncData> AsyncData;

        class TDrainedChannelMessage {
        private:
            TDqSerializedBatch Data;
            NDqProto::TWatermark Watermark;
            NDqProto::TCheckpoint Checkpoint;

            ui32 DataSize = 0;
            ui32 WatermarkSize = 0;
            ui32 CheckpointSize = 0;

            bool HasData = false;
            bool HasWatermark = false;
            bool HasCheckpoint = false;
            bool Finished = false;
        public:
            const NDqProto::TWatermark* GetWatermarkOptional() const {
                return HasWatermark ? &Watermark : nullptr;
            }
            const NDqProto::TCheckpoint* GetCheckpointOptional() const {
                return HasCheckpoint ? &Checkpoint : nullptr;
            }

            TChannelDataOOB BuildChannelData(const ui64 channelId) {
                TChannelDataOOB channelData;
                channelData.Proto.SetChannelId(channelId);
                channelData.Proto.SetFinished(Finished);
                if (HasData) {
                    channelData.Proto.MutableData()->Swap(&Data.Proto);
                    channelData.Payload = std::move(Data.Payload);
                }
                if (HasWatermark) {
                    channelData.Proto.MutableWatermark()->Swap(&Watermark);
                }
                if (HasCheckpoint) {
                    channelData.Proto.MutableCheckpoint()->Swap(&Checkpoint);
                }

                Y_ABORT_UNLESS(HasData || HasWatermark || HasCheckpoint || Finished);
                return channelData;
            }

            bool ReadData(const TOutputChannelInfo& outputChannel) {
                auto channel = outputChannel.Channel;

                HasData = channel->Pop(Data);
                HasWatermark = channel->Pop(Watermark);
                HasCheckpoint = channel->Pop(Checkpoint);
                Finished = !outputChannel.Finished && channel->IsFinished();

                if (!HasData && !HasWatermark && !HasCheckpoint && !Finished) {
                    return false;
                }

                DataSize = Data.Size();
                WatermarkSize = Watermark.ByteSize();
                CheckpointSize = Checkpoint.ByteSize();

                return true;
            }
        };

        std::vector<TDrainedChannelMessage> DrainChannel(const ui32 countLimit) {
            std::vector<TDrainedChannelMessage> result;
            if (Finished) {
                Y_ABORT_UNLESS(Channel->IsFinished());
                return result;
            }
            result.reserve(countLimit);
            for (ui32 i = 0; i < countLimit && !Finished; ++i) {
                TDrainedChannelMessage message;
                if (!message.ReadData(*this)) {
                    break;
                }
                result.emplace_back(std::move(message));
                if (Channel->IsFinished()) {
                    Finished = true;
                }
            }
            return result;
        }
    };

    struct TAsyncOutputInfoBase {
        TString Type;
        IDqAsyncOutputBuffer::TPtr Buffer;
        IDqComputeActorAsyncOutput* AsyncOutput = nullptr;
        NActors::IActor* Actor = nullptr;
        bool Finished = false; // If sink/transform is in finished state, it receives only checkpoints.
        bool FinishIsAcknowledged = false; // Async output has acknowledged its finish.
        TIssuesBuffer IssuesBuffer;
        bool PopStarted = false;
        i64 FreeSpaceBeforeSend = 0;

        TAsyncOutputInfoBase() : IssuesBuffer(IssuesBufferSize) {}
    };

    struct TAsyncOutputTransformInfo : public TAsyncOutputInfoBase {
        IDqOutputConsumer::TPtr OutputBuffer;
    };

protected:
    // virtual methods (TODO: replace with static_cast<TDerived*>(this)->Foo()

    virtual void TerminateSources(const TIssues& /* issues */, bool /* success */) {
    }

    void TerminateSources(const TString& message, bool success) {
        TerminateSources(TIssues({TIssue(message)}), success);
    }

    TGuard<NKikimr::NMiniKQL::TScopedAlloc> BindAllocator() {
        return Guard(GetAllocator());
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

                if (ComputeActorSpan) {
                    ComputeActorSpan.EndError(
                        TStringBuilder()
                            << "Timeout event from compute actor " << this->SelfId()
                            << ", TxId: " << TxId << ", task: " << Task.GetId()
                    );
                }

                this->Send(ExecuterId, abortEv.Release());

                TerminateSources("timeout exceeded", false);
                Terminate(false, "timeout exceeded");
                break;
            }
            case EEvWakeupTag::PeriodicStatsTag: {
                const auto maxInterval = RuntimeSettings.ReportStatsSettings->MaxInterval;
                this->Schedule(maxInterval, new NActors::TEvents::TEvWakeup(EEvWakeupTag::PeriodicStatsTag));

                ReportStats(NActors::TActivationContext::Now(), ESendStats::IfRequired);
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
            Checkpoints->Receive(iev);
        }
    }

    void HandleExecuteBase(TEvDqCompute::TEvStateRequest::TPtr& ev) {
        CA_LOG_T("Got TEvStateRequest from actor " << ev->Sender << " TaskId: " << Task.GetId() << " PingCookie: " << ev->Cookie);
        auto evState = MakeHolder<TEvDqCompute::TEvState>();
        evState->Record.SetState(NDqProto::COMPUTE_STATE_EXECUTING);
        evState->Record.SetStatusCode(NYql::NDqProto::StatusIds::SUCCESS);
        evState->Record.SetTaskId(Task.GetId());
        FillStats(evState->Record.MutableStats(), /* last */ false);
        this->Send(ev->Sender, evState.Release(), NActors::IEventHandle::FlagTrackDelivery, ev->Cookie);
    }

    void HandleExecuteBase(TEvDqCompute::TEvNewCheckpointCoordinator::TPtr& ev) {
        if (!Checkpoints) {
            Checkpoints = new TDqComputeActorCheckpoints(this->SelfId(), TxId, Task, this);
            Checkpoints->Init(this->SelfId(), this->RegisterWithSameMailbox(Checkpoints));
            Channels->SetCheckpointsSupport();
        }
        TAutoPtr<NActors::IEventHandle> handle = new NActors::IEventHandle(Checkpoints->SelfId(), ev->Sender, ev->Release().Release());
        Checkpoints->Receive(handle);
    }

    void HandleExecuteBase(TEvDq::TEvAbortExecution::TPtr& ev) {
        if (ev->Get()->Record.GetStatusCode() == NYql::NDqProto::StatusIds::INTERNAL_ERROR) {
            Y_ABORT_UNLESS(ev->Get()->GetIssues().Size() == 1);
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
            if (ComputeActorSpan) {
                ComputeActorSpan.End();
            }

            NActors::TActivationContext::Send(ev->Forward(ExecuterId));
        }

        Terminate(success, issues);
    }

    void HandleExecuteBase(NActors::TEvInterconnect::TEvNodeDisconnected::TPtr& ev) {
        TAutoPtr<NActors::IEventHandle> iev(ev.Release());
        if (Checkpoints) {
            Checkpoints->Receive(iev);
        }
    }

    void HandleExecuteBase(NActors::TEvInterconnect::TEvNodeConnected::TPtr& ev) {
        TAutoPtr<NActors::IEventHandle> iev(ev.Release());
        if (Checkpoints) {
            Checkpoints->Receive(iev);
        }
    }

    void HandleError(TEvDqCompute::TEvError::TPtr& ev) {
        InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, ev->Get()->Error);
    }

    ui32 AllowedChannelsOvercommit() const {
        const auto& fc = GetDqExecutionSettings().FlowControl;
        const ui32 allowedOvercommit = (fc.InFlightBytesOvercommit - 1.f) * MemoryLimits.ChannelBufferSize;
        return allowedOvercommit;
    }

protected:

    void UpdateBlocked(TOutputChannelInfo& outputChannel, const bool blocked) {
        if (Y_UNLIKELY(outputChannel.Stats)) {
            if (blocked) {
                outputChannel.Stats->BlockedByCapacity++;
                if (!outputChannel.Stats->StartBlockedTime) {
                    outputChannel.Stats->StartBlockedTime = TInstant::Now();
                }
            } else {
                if (outputChannel.Stats->StartBlockedTime) {
                    outputChannel.Stats->BlockedTime += TInstant::Now() - *outputChannel.Stats->StartBlockedTime;
                    outputChannel.Stats->StartBlockedTime.reset();
                }
            }
        }
    }

protected:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> GetAllocatorPtr() {
        return Alloc;
    }
    NKikimr::NMiniKQL::TScopedAlloc& GetAllocator() {
        return *Alloc.get();
    }

    virtual const TDqMemoryQuota::TProfileStats* GetMemoryProfileStats() const = 0;

    virtual void DrainOutputChannel(TOutputChannelInfo& outputChannel) = 0;

    virtual void DrainAsyncOutput(ui64 outputIndex, TAsyncOutputInfoBase& outputInfo) = 0;

    ui32 SendDataChunkToAsyncOutput(ui64 outputIndex, TAsyncOutputInfoBase& outputInfo, ui64 bytes) {
        auto sink = outputInfo.Buffer;

        NKikimr::NMiniKQL::TUnboxedValueBatch dataBatch(sink->GetOutputType());
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
            ResumeInputsByCheckpoint();
        }

        outputInfo.AsyncOutput->SendData(std::move(dataBatch), dataSize, maybeCheckpoint, outputInfo.Finished);
        CA_LOG_T("sink " << outputIndex << ": sent " << dataSize << " bytes of data and " << checkpointSize << " bytes of checkpoint barrier");

        return dataSize + checkpointSize;
    }

protected:
    const TMaybe<NDqProto::TRlPath>& GetRlPath() const {
        return RuntimeSettings.RlPath;
    }

    TTxId GetTxId() const {
        return TxId;
    }

    const TDqTaskSettings& GetTask() const {
        return Task;
    }

    TDqTaskSettings& GetTaskRef() {
        return Task;
    }

    NDqProto::EDqStatsMode GetStatsMode() const {
        return RuntimeSettings.StatsMode;
    }

    const TComputeMemoryLimits& GetMemoryLimits() const {
        return MemoryLimits;
    }

public:

    TVector<google::protobuf::Message*>& MutableTaskSourceSettings() {
        return Task.MutableSourceSettings();
    }

protected:
    void FillIoMaps(
        const NKikimr::NMiniKQL::THolderFactory& holderFactory,
        const NKikimr::NMiniKQL::TTypeEnvironment& typeEnv,
        const THashMap<TString, TString>& secureParams,
        const THashMap<TString, TString>& taskParams,
        const TVector<TString>& readRanges,
        IRandomProvider* randomProvider
        )
    {
        auto collectStatsLevel = StatsModeToCollectStatsLevel(RuntimeSettings.StatsMode);
        for (auto& [inputIndex, source] : SourcesMap) {
            Y_ABORT_UNLESS(AsyncIoFactory);
            const auto& inputDesc = Task.GetInputs(inputIndex);
            Y_ABORT_UNLESS(inputDesc.HasSource());
            source.Type = inputDesc.GetSource().GetType();
            source.ProgramBuilder.ConstructInPlace(typeEnv, *FunctionRegistry);
            const auto& settings = Task.GetSourceSettings();
            Y_ABORT_UNLESS(settings.empty() || inputIndex < settings.size());
            CA_LOG_D("Create source for input " << inputIndex << " " << inputDesc);
            try {
                std::tie(source.AsyncInput, source.Actor) = AsyncIoFactory->CreateDqSource(
                    IDqAsyncIoFactory::TSourceArguments {
                        .InputDesc = inputDesc,
                        .InputIndex = inputIndex,
                        .StatsLevel = collectStatsLevel,
                        .TxId = TxId,
                        .TaskId = Task.GetId(),
                        .SecureParams = secureParams,
                        .TaskParams = taskParams,
                        .ReadRanges = readRanges,
                        .ComputeActorId = this->SelfId(),
                        .TypeEnv = typeEnv,
                        .HolderFactory = holderFactory,
                        .ProgramBuilder = *source.ProgramBuilder,
                        .TaskCounters = TaskCounters,
                        .Alloc = Alloc,
                        .MemoryQuotaManager = MemoryLimits.MemoryQuotaManager,
                        .SourceSettings = (!settings.empty() ? settings.at(inputIndex) : nullptr),
                        .Arena = Task.GetArena(),
                        .TraceId = ComputeActorSpan.GetTraceId()
                    });
            } catch (const std::exception& ex) {
                throw yexception() << "Failed to create source " << inputDesc.GetSource().GetType() << ": " << ex.what();
            }
            this->RegisterWithSameMailbox(source.Actor);
        }
        for (auto& [inputIndex, transform] : InputTransformsMap) {
            Y_ABORT_UNLESS(AsyncIoFactory);
            const auto& inputDesc = Task.GetInputs(inputIndex);
            const auto typeNode = NKikimr::NMiniKQL::DeserializeNode(inputDesc.GetTransform().GetOutputType(), typeEnv);
            transform.ValueType = static_cast<NKikimr::NMiniKQL::TType*>(typeNode);
            CA_LOG_D("Create transform for input " << inputIndex << " " << inputDesc.ShortDebugString());
            try {
                auto guard = BindAllocator();
                std::tie(transform.AsyncInput, transform.Actor) = AsyncIoFactory->CreateDqInputTransform(
                    IDqAsyncIoFactory::TInputTransformArguments {
                        .InputDesc = inputDesc,
                        .InputIndex = inputIndex,
                        .StatsLevel = collectStatsLevel,
                        .TxId = TxId,
                        .TaskId = Task.GetId(),
                        .TransformInput = transform.Input,
                        .SecureParams = secureParams,
                        .TaskParams = taskParams,
                        .ComputeActorId = this->SelfId(),
                        .TypeEnv = typeEnv,
                        .HolderFactory = holderFactory,
                        .Alloc = Alloc,
                        .TraceId = ComputeActorSpan.GetTraceId()
                    });
            } catch (const std::exception& ex) {
                throw yexception() << "Failed to create input transform " << inputDesc.GetTransform().GetType() << ": " << ex.what();
            }
            this->RegisterWithSameMailbox(transform.Actor);
        }
        for (auto& [outputIndex, transform] : OutputTransformsMap) {
            Y_ABORT_UNLESS(AsyncIoFactory);
            const auto& outputDesc = Task.GetOutputs(outputIndex);
            CA_LOG_D("Create transform for output " << outputIndex << " " << outputDesc.ShortDebugString());
            try {
                std::tie(transform.AsyncOutput, transform.Actor) = AsyncIoFactory->CreateDqOutputTransform(
                    IDqAsyncIoFactory::TOutputTransformArguments {
                        .OutputDesc = outputDesc,
                        .OutputIndex = outputIndex,
                        .StatsLevel = collectStatsLevel,
                        .TxId = TxId,
                        .TransformOutput = transform.OutputBuffer,
                        .Callback = static_cast<TOutputTransformCallbacks*>(this),
                        .SecureParams = secureParams,
                        .TaskParams = taskParams,
                        .TypeEnv = typeEnv,
                        .HolderFactory = holderFactory,
                    });
            } catch (const std::exception& ex) {
                throw yexception() << "Failed to create output transform " << outputDesc.GetTransform().GetType() << ": " << ex.what();
            }
            this->RegisterWithSameMailbox(transform.Actor);
        }
        for (auto& [outputIndex, sink] : SinksMap) {
            Y_ABORT_UNLESS(AsyncIoFactory);
            const auto& outputDesc = Task.GetOutputs(outputIndex);
            Y_ABORT_UNLESS(outputDesc.HasSink());
            sink.Type = outputDesc.GetSink().GetType();
            CA_LOG_D("Create sink for output " << outputIndex << " " << outputDesc);
            try {
                std::tie(sink.AsyncOutput, sink.Actor) = AsyncIoFactory->CreateDqSink(
                    IDqAsyncIoFactory::TSinkArguments {
                        .OutputDesc = outputDesc,
                        .OutputIndex = outputIndex,
                        .StatsLevel = collectStatsLevel,
                        .TxId = TxId,
                        .Callback = static_cast<TSinkCallbacks*>(this),
                        .SecureParams = secureParams,
                        .TaskParams = taskParams,
                        .TypeEnv = typeEnv,
                        .HolderFactory = holderFactory,
                        .Alloc = Alloc,
                        .RandomProvider = randomProvider
                    });
            } catch (const std::exception& ex) {
                throw yexception() << "Failed to create sink " << outputDesc.GetSink().GetType() << ": " << ex.what();
            }
            this->RegisterWithSameMailbox(sink.Actor);
        }
    }

    void PollAsyncInput() {
        // Don't produce any input from sources if we're about to save checkpoint.
        if (!Running || (Checkpoints && Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved())) {
            CA_LOG_T("Skip polling sources because of pending checkpoint");
            return;
        }

        CA_LOG_T("Poll sources");
        for (auto& [inputIndex, source] : SourcesMap) {
            if (auto resume =  source.PollAsyncInput(MetricsReporter, WatermarksTracker, RuntimeSettings.AsyncInputPushLimit)) {
                ContinueExecute(*resume);
            }
        }

        CA_LOG_T("Poll inputs");
        for (auto& [inputIndex, transform] : InputTransformsMap) {
            if (auto resume = transform.PollAsyncInput(MetricsReporter, WatermarksTracker, RuntimeSettings.AsyncInputPushLimit)) {
                ContinueExecute(*resume);
            }
        }
    }

    void OnNewAsyncInputDataArrived(const IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::TPtr& ev) {
        Y_ABORT_UNLESS(SourcesMap.FindPtr(ev->Get()->InputIndex) || InputTransformsMap.FindPtr(ev->Get()->InputIndex));
        auto cpuTimeDelta = TakeSourceCpuTimeDelta();
        if (SourceCpuTimeMs) {
            SourceCpuTimeMs->Add(cpuTimeDelta.MilliSeconds());
        }
        CpuTimeSpent += cpuTimeDelta;
        ContinueExecute(EResumeSource::CANewAsyncInput);
    }

    void OnAsyncInputError(const IDqComputeActorAsyncInput::TEvAsyncInputError::TPtr& ev) {
        if (SourcesMap.FindPtr(ev->Get()->InputIndex)) {
            OnSourceError(ev->Get()->InputIndex, ev->Get()->Issues, ev->Get()->FatalCode);
        } else if (InputTransformsMap.FindPtr(ev->Get()->InputIndex)) {
            OnInputTransformError(ev->Get()->InputIndex, ev->Get()->Issues, ev->Get()->FatalCode);
        } else {
            YQL_ENSURE(false, "Unexpected input index: " << ev->Get()->InputIndex);
        }
    }

    void OnSourceError(ui64 inputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) {
        if (fatalCode == NYql::NDqProto::StatusIds::UNSPECIFIED) {
            SourcesMap.at(inputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("Source[" << inputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(fatalCode, issues);
    }

    void OnInputTransformError(ui64 inputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) {
        if (fatalCode == NYql::NDqProto::StatusIds::UNSPECIFIED) {
            InputTransformsMap.at(inputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("InputTransform[" << inputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(fatalCode, issues);
    }

    void OnSinkError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override final {
        if (fatalCode == NYql::NDqProto::StatusIds::UNSPECIFIED) {
            SinksMap.at(outputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("Sink[" << outputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(fatalCode, issues);
    }

    void OnOutputTransformError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override final {
        if (fatalCode == NYql::NDqProto::StatusIds::UNSPECIFIED) {
            OutputTransformsMap.at(outputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("OutputTransform[" << outputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(fatalCode, issues);
    }

    bool AllAsyncOutputsFinished() const {
        for (const auto& [outputIndex, sinkInfo] : SinksMap) {
            if (!sinkInfo.FinishIsAcknowledged) {
                CA_LOG_D("Waiting finish of sink[" << outputIndex << "]");
                return false;
            }
        }
        for (const auto& [outputIndex, transformInfo] : OutputTransformsMap) {
            if (!transformInfo.FinishIsAcknowledged) {
                CA_LOG_D("Waiting finish of transform[" << outputIndex << "]");
                return false;
            }
        }
        return true;
    }

    virtual ui64 CalcMkqlMemoryLimit() {
        auto& opts = Task.GetProgram().GetSettings();
        return opts.GetHasMapJoin()/* || opts.GetHasSort()*/
            ? MemoryLimits.MkqlHeavyProgramMemoryLimit
            : MemoryLimits.MkqlLightProgramMemoryLimit;
    }

protected:
    void InitializeTask() {
        for (ui32 i = 0; i < Task.InputsSize(); ++i) {
            const auto& inputDesc = Task.GetInputs(i);
            Y_ABORT_UNLESS(!inputDesc.HasSource() || inputDesc.ChannelsSize() == 0); // HasSource => no channels

            if (inputDesc.HasTransform()) {
                auto result = InputTransformsMap.emplace(
                    i,
                    TAsyncInputTransformHelper(LogPrefix, i, NDqProto::WATERMARKS_MODE_DISABLED)
                );
                YQL_ENSURE(result.second);
            }

            if (inputDesc.HasSource()) {
                const auto watermarksMode = inputDesc.GetSource().GetWatermarksMode();
                auto result = SourcesMap.emplace(
                    i,
                    static_cast<TDerived*>(this)->CreateInputHelper(LogPrefix, i, watermarksMode)
                );
                YQL_ENSURE(result.second);
            } else {
                for (auto& channel : inputDesc.GetChannels()) {
                    auto result = InputChannelsMap.emplace(
                        channel.GetId(),
                        TInputChannelInfo(
                            LogPrefix,
                            channel.GetId(),
                            channel.GetSrcStageId(),
                            channel.GetWatermarksMode(),
                            channel.GetCheckpointingMode())
                    );
                    YQL_ENSURE(result.second);
                }
            }
        }

        for (ui32 i = 0; i < Task.OutputsSize(); ++i) {
            const auto& outputDesc = Task.GetOutputs(i);
            Y_ABORT_UNLESS(!outputDesc.HasSink() || outputDesc.ChannelsSize() == 0); // HasSink => no channels

            if (outputDesc.HasTransform()) {
                auto result = OutputTransformsMap.emplace(std::piecewise_construct, std::make_tuple(i), std::make_tuple());
                YQL_ENSURE(result.second);
            }

            if (outputDesc.HasSink()) {
                auto result = SinksMap.emplace(i, TAsyncOutputInfoBase());
                YQL_ENSURE(result.second);
            } else {
                for (auto& channel : outputDesc.GetChannels()) {
                    TOutputChannelInfo outputChannel(channel.GetId(), channel.GetDstStageId());
                    outputChannel.HasPeer = channel.GetDstEndpoint().HasActorId();
                    outputChannel.IsTransformOutput = outputDesc.HasTransform();
                    outputChannel.WatermarksMode = channel.GetWatermarksMode();

                    if (Y_UNLIKELY(RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_PROFILE)) {
                        outputChannel.Stats = MakeHolder<typename TOutputChannelInfo::TStats>();
                    }

                    auto result = OutputChannelsMap.emplace(channel.GetId(), std::move(outputChannel));
                    YQL_ENSURE(result.second);
                }
            }
        }

        if (OutputChannelSize) {
            OutputChannelSize->Add(OutputChannelsMap.size() * MemoryLimits.ChannelBufferSize);
        }

        RequestContext = MakeIntrusive<NYql::NDq::TRequestContext>(Task.GetRequestContext());

        InitializeWatermarks();
    }

private:
    void InitializeWatermarks() {
        for (const auto& [id, source] : SourcesMap) {
            if (source.WatermarksMode == NDqProto::EWatermarksMode::WATERMARKS_MODE_DEFAULT) {
                WatermarksTracker.RegisterAsyncInput(id);
            }
        }

        for (const auto& [id, channel] : InputChannelsMap) {
            if (channel.WatermarksMode == NDqProto::EWatermarksMode::WATERMARKS_MODE_DEFAULT) {
                WatermarksTracker.RegisterInputChannel(id);
            }
        }

        for (const auto& [id, channel] : OutputChannelsMap) {
            if (channel.WatermarksMode == NDqProto::EWatermarksMode::WATERMARKS_MODE_DEFAULT) {
                WatermarksTracker.RegisterOutputChannel(id);
            }
        }
    }

    virtual const NYql::NDq::TTaskRunnerStatsBase* GetTaskRunnerStats() = 0;
    virtual const NYql::NDq::TDqMeteringStats* GetMeteringStats() = 0;

    virtual const IDqAsyncOutputBuffer* GetSink(ui64 outputIdx, const TAsyncOutputInfoBase& sinkInfo) const = 0;

public:

    TDuration GetSourceCpuTime() const {
        auto result = TDuration::Zero();
        for (auto& [inputIndex, sourceInfo] : SourcesMap) {
            result += sourceInfo.AsyncInput->GetCpuTime();
        }
        return result;
    }

    TDuration TakeSourceCpuTimeDelta() {
        auto newSourceCpuTime = GetSourceCpuTime();
        auto result = newSourceCpuTime - SourceCpuTime;
        SourceCpuTime = newSourceCpuTime;
        return result;
    }

    void FillStats(NDqProto::TDqComputeActorStats* dst, bool last) {
        if (RuntimeSettings.CollectNone()) {
            return;
        }

        if (last) {
            ReportEventElapsedTime();
        }

        dst->SetCpuTimeUs(CpuTime.MicroSeconds());
        dst->SetMaxMemoryUsage(MemoryLimits.MemoryQuotaManager->GetMaxMemorySize());

        if (auto memProfileStats = GetMemoryProfileStats(); memProfileStats) {
            dst->SetMkqlMaxMemoryUsage(memProfileStats->MkqlMaxUsedMemory);
            dst->SetMkqlExtraMemoryBytes(memProfileStats->MkqlExtraMemoryBytes);
            dst->SetMkqlExtraMemoryRequests(memProfileStats->MkqlExtraMemoryRequests);
        }

        if (Stat) { // for task_runner_actor
            Y_ABORT_UNLESS(!dst->HasExtra());
            NDqProto::TExtraStats extraStats;
            for (const auto& [name, entry]: Stat->Get()) {
                NDqProto::TDqStatsAggr metric;
                metric.SetSum(entry.Sum);
                metric.SetMax(entry.Max);
                metric.SetMin(entry.Min);
                //metric.SetAvg(entry.Avg);
                metric.SetCnt(entry.Count);
                (*extraStats.MutableStats())[name] = metric;
            }
            dst->MutableExtra()->PackFrom(extraStats);
            Stat->Clear();
        } else if (auto* taskStats = GetTaskRunnerStats()) { // for task_runner_actor_local
            auto* protoTask = dst->AddTasks();

            for (auto& [inputIndex, sourceInfo] : SourcesMap) {
                if (auto* source = sourceInfo.AsyncInput) {
                    source->FillExtraStats(protoTask, last, GetMeteringStats());
                }
            }
            FillTaskRunnerStats(Task.GetId(), Task.GetStageId(), *taskStats, protoTask, RuntimeSettings.GetCollectStatsLevel());

            // More accurate cpu time counter:
            if (TDerived::HasAsyncTaskRunner) {
                protoTask->SetCpuTimeUs(CpuTime.MicroSeconds() + taskStats->ComputeCpuTime.MicroSeconds() + taskStats->BuildCpuTime.MicroSeconds());
            }
            protoTask->SetSourceCpuTimeUs(SourceCpuTime.MicroSeconds());

            ui64 ingressBytes = 0;
            ui64 ingressRows = 0;
            ui64 ingressDecompressedBytes = 0;
            auto startTimeMs = protoTask->GetStartTimeMs();

            if (RuntimeSettings.CollectFull()) {
                // in full/profile mode enumerate existing protos
                for (auto& protoSource : *protoTask->MutableSources()) {
                    auto inputIndex = protoSource.GetInputIndex();
                    if (auto* sourceInfoPtr = SourcesMap.FindPtr(inputIndex)) {
                        auto& sourceInfo = *sourceInfoPtr;
                        protoSource.SetIngressName(sourceInfo.Type);
                        const auto& ingressStats = sourceInfo.AsyncInput->GetIngressStats();
                        FillAsyncStats(*protoSource.MutableIngress(), ingressStats);
                        ingressBytes += ingressStats.Bytes;
                        // ingress rows are usually not reported, so we count rows in task runner input
                        ingressRows += ingressStats.Rows ? ingressStats.Rows : taskStats->Sources.at(inputIndex)->GetPopStats().Rows;
                        ingressDecompressedBytes += ingressStats.DecompressedBytes;
                        if (ingressStats.FirstMessageTs) {
                            auto firstMessageMs = ingressStats.FirstMessageTs.MilliSeconds();
                            if (!startTimeMs || startTimeMs > firstMessageMs) {
                                startTimeMs = firstMessageMs;
                            }
                        }
                    }
                }
            } else {
                // in basic mode enum sources directly
                for (auto& [inputIndex, sourceInfo] : SourcesMap) {
                    if (!sourceInfo.AsyncInput)
                        continue;

                    const auto& ingressStats = sourceInfo.AsyncInput->GetIngressStats();
                    ingressBytes += ingressStats.Bytes;
                    // ingress rows are usually not reported, so we count rows in task runner input
                    ingressRows += ingressStats.Rows ? ingressStats.Rows : taskStats->Sources.at(inputIndex)->GetPopStats().Rows;
                    ingressDecompressedBytes += ingressStats.DecompressedBytes;
                }
            }

            if (!startTimeMs) {
                startTimeMs = taskStats->StartTs.MilliSeconds();
            }
            protoTask->SetStartTimeMs(startTimeMs);
            protoTask->SetIngressBytes(ingressBytes);
            protoTask->SetIngressRows(ingressRows);
            protoTask->SetIngressDecompressedBytes(ingressDecompressedBytes);

            ui64 egressBytes = 0;
            ui64 egressRows = 0;
            auto finishTimeMs = protoTask->GetFinishTimeMs();

            for (auto& [outputIndex, sinkInfo] : SinksMap) {
                if (auto* sink = GetSink(outputIndex, sinkInfo)) {
                    if (!sinkInfo.AsyncOutput)
                        continue;

                    const auto& egressStats = sinkInfo.AsyncOutput->GetEgressStats();
                    const auto& pushStats = sink->GetPushStats();
                    if (RuntimeSettings.CollectFull()) {
                        const auto& popStats = sink->GetPopStats();
                        auto& protoSink = *protoTask->AddSinks();
                        protoSink.SetOutputIndex(outputIndex);
                        protoSink.SetEgressName(sinkInfo.Type);
                        FillAsyncStats(*protoSink.MutablePush(), pushStats);
                        FillAsyncStats(*protoSink.MutablePop(), popStats);
                        FillAsyncStats(*protoSink.MutableEgress(), egressStats);
                        protoSink.SetMaxMemoryUsage(popStats.MaxMemoryUsage);
                        protoSink.SetErrorsCount(sinkInfo.IssuesBuffer.GetAllAddedIssuesCount());
                        if (egressStats.LastMessageTs) {
                            auto lastMessageMs = egressStats.LastMessageTs.MilliSeconds();
                            if (!finishTimeMs || finishTimeMs > lastMessageMs) {
                                finishTimeMs = lastMessageMs;
                            }
                        }
                    }
                    egressBytes += egressStats.Bytes;
                    // egress rows are usually not reported, so we count rows in task runner output
                    egressRows += egressStats.Rows ? egressStats.Rows : pushStats.Rows;
                    // p.s. sink == sinkInfo.Buffer
                }
            }

            protoTask->SetFinishTimeMs(finishTimeMs);
            protoTask->SetEgressBytes(egressBytes);
            protoTask->SetEgressRows(egressRows);

            if (startTimeMs && finishTimeMs > startTimeMs) {
                // we may loose precision here a little bit ... rework sometimes
                dst->SetDurationUs((finishTimeMs - startTimeMs) * 1'000);
            }

            for (auto& [inputIndex, transformInfo] : InputTransformsMap) {
                auto* transform = static_cast<TDerived*>(this)->GetInputTransform(inputIndex, transformInfo);
                if (transform && RuntimeSettings.CollectFull()) {
                    // TODO: Ingress clarification
                    auto& protoTransform = *protoTask->AddInputTransforms();
                    protoTransform.SetInputIndex(inputIndex);
                    FillAsyncStats(*protoTransform.MutablePush(), transform->GetPushStats());
                    FillAsyncStats(*protoTransform.MutablePop(), transform->GetPopStats());
                    protoTransform.SetMaxMemoryUsage(transform->PushStats.MaxMemoryUsage);
                }

                if (auto* transform = transformInfo.AsyncInput) {
                    transform->FillExtraStats(protoTask, last, GetMeteringStats());
                }
            }

            if (RuntimeSettings.CollectFull()) {
                for (auto& protoSource : *protoTask->MutableSources()) {
                    if (auto* sourceInfo = SourcesMap.FindPtr(protoSource.GetInputIndex())) {
                        protoSource.SetErrorsCount(sourceInfo->IssuesBuffer.GetAllAddedIssuesCount());
                    }
                }

                for (auto& protoChannel : *protoTask->MutableInputChannels()) {
                    if (auto channelId = protoChannel.GetChannelId()) { // Profile or Full Single
                        if (auto* channelStats = Channels->GetInputChannelStats(channelId)) {
                            protoChannel.SetPollRequests(channelStats->PollRequests);
                            protoChannel.SetResentMessages(channelStats->ResentMessages);
                        }
                    } else if (auto srcStageId = protoChannel.GetSrcStageId()) { // Full Aggregated
                        // TODO Optimize
                        ui64 pollRequests = 0;
                        ui64 resentMessages = 0;
                        for (const auto& [channelId, channel] : InputChannelsMap) {
                            if (channel.SrcStageId == srcStageId) {
                                if (auto* channelStats = Channels->GetInputChannelStats(channelId)) {
                                    pollRequests += channelStats->PollRequests;
                                    resentMessages += channelStats->ResentMessages;
                                }
                            }
                        }
                        if (pollRequests) {
                            protoChannel.SetPollRequests(pollRequests);
                        }
                        if (resentMessages) {
                            protoChannel.SetResentMessages(resentMessages);
                        }
                    }
                }

                for (auto& protoChannel : *protoTask->MutableOutputChannels()) {
                    if (auto channelId = protoChannel.GetChannelId()) { // Profile or Full Single
                        if (auto* channelStats = Channels->GetOutputChannelStats(channelId)) {
                            protoChannel.SetResentMessages(channelStats->ResentMessages);
                        }
                    } else if (auto dstStageId = protoChannel.GetDstStageId()) { // Full Aggregated
                        // TODO Optimize
                        ui64 resentMessages = 0;
                        for (const auto& [channelId, channel] : OutputChannelsMap) {
                            if (channel.DstStageId == dstStageId) {
                                if (auto* channelStats = Channels->GetOutputChannelStats(channelId)) {
                                    resentMessages += channelStats->ResentMessages;
                                }
                            }
                        }
                        if (resentMessages) {
                            protoChannel.SetResentMessages(resentMessages);
                        }
                    }
                }
            }
        }

        static_cast<TDerived*>(this)->FillExtraStats(dst, last);

        if (last && MemoryQuota) {
            MemoryQuota->ResetProfileStats();
        }
    }

protected:
    enum class ESendStats {
        IfPossible,
        IfRequired
    };
    void ReportStats(TInstant now, ESendStats condition) {
        if (!RuntimeSettings.ReportStatsSettings) {
            return;
        }
        auto dT = now - LastSendStatsTime;
        switch(condition) {
            case ESendStats::IfPossible:
                if (dT < RuntimeSettings.ReportStatsSettings->MinInterval) {
                    return;
                }
                break;
            case ESendStats::IfRequired:
                if (dT < RuntimeSettings.ReportStatsSettings->MaxInterval) {
                    return;
                }
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

        if (ComputeActorSpan) {
            ComputeActorSpan.End();
        }

        this->Send(ExecuterId, evState.release(), NActors::IEventHandle::FlagTrackDelivery);

        LastSendStatsTime = now;
    }
private:
    std::shared_ptr<NKikimr::NMiniKQL::TScopedAlloc> Alloc; //must be declared on top to be destroyed after all the rest
protected:
    const NActors::TActorId ExecuterId;
    const TTxId TxId;
    TDqTaskSettings Task;
    TString LogPrefix;
    const TComputeRuntimeSettings RuntimeSettings;
    TComputeMemoryLimits MemoryLimits;
    const bool CanAllocateExtraMemory = false;
    const IDqAsyncIoFactory::TPtr AsyncIoFactory;
    const NKikimr::NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    const NDqProto::ECheckpointingMode CheckpointingMode;
    TDqComputeActorChannels* Channels = nullptr;
    TDqComputeActorCheckpoints* Checkpoints = nullptr;
    THashMap<ui64, TInputChannelInfo> InputChannelsMap; // Channel id -> Channel info
    THashMap<ui64, TAsyncInputHelper> SourcesMap; // Input index -> Source info
    THashMap<ui64, TAsyncInputTransformHelper> InputTransformsMap; // Input index -> Transforms info
    THashMap<ui64, TOutputChannelInfo> OutputChannelsMap; // Channel id -> Channel info
    THashMap<ui64, TAsyncOutputInfoBase> SinksMap; // Output index -> Sink info
    THashMap<ui64, TAsyncOutputTransformInfo> OutputTransformsMap; // Output index -> Transforms info
    bool ResumeEventScheduled = false;
    NDqProto::EComputeState State;
    TIntrusivePtr<NYql::NDq::TRequestContext> RequestContext;
    TDuration CpuTime;

    struct TProcessOutputsState {
        int Inflight = 0;
        bool ChannelsReady = true;
        bool HasDataToSend = false;
        bool DataWasSent = false;
        bool AllOutputsFinished = true;
        ERunStatus LastRunStatus = ERunStatus::PendingInput;
        bool LastPopReturnedNoData = false;
    };
    TProcessOutputsState ProcessOutputsState;

    THolder<TDqMemoryQuota> MemoryQuota;
    TDqComputeActorWatermarks WatermarksTracker;
    ::NMonitoring::TDynamicCounterPtr TaskCounters;
    TDqComputeActorMetrics MetricsReporter;
    NWilson::TSpan ComputeActorSpan;
    TDuration SourceCpuTime;
private:
    bool Running = true;
    TInstant LastSendStatsTime;
    bool PassExceptions = false;
    bool Terminated = false;
protected:
    ::NMonitoring::TDynamicCounters::TCounterPtr MkqlMemoryQuota;
    ::NMonitoring::TDynamicCounters::TCounterPtr OutputChannelSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr SourceCpuTimeMs;
    THolder<NYql::TCounters> Stat;
    TDuration CpuTimeSpent;
};

} // namespace NYql
} // namespace NNq
