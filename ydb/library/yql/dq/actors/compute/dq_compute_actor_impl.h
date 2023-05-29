#pragma once

#include "dq_compute_actor_async_io.h"
#include "dq_compute_actor_channels.h"
#include "dq_compute_actor_checkpoints.h"
#include "dq_compute_actor_metrics.h"
#include "dq_compute_actor_watermarks.h"
#include "dq_compute_actor.h"
#include "dq_compute_issues_buffer.h"
#include "dq_compute_memory_quota.h"

#include <ydb/core/base/kikimr_issue.h>
#include <ydb/core/base/wilson.h>
#include <ydb/core/protos/services.pb.h>

#include <ydb/library/yql/providers/dq/counters/counters.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>
#include <ydb/library/yql/core/issue/yql_issue.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_saveload.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/library/yql/dq/actors/dq.h>

#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/wilson/wilson_span.h>

#include <util/generic/size_literals.h>
#include <util/string/join.h>
#include <util/system/hostname.h>

#include <any>
#include <queue>

#if defined CA_LOG_D || defined CA_LOG_I || defined CA_LOG_E || defined CA_LOG_C
#   error log macro definition clash
#endif

#define CA_LOG_T(s) \
    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define CA_LOG_D(s) \
    LOG_DEBUG_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define CA_LOG_I(s) \
    LOG_INFO_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define CA_LOG_W(s) \
    LOG_WARN_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define CA_LOG_N(s) \
    LOG_NOTICE_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define CA_LOG_E(s) \
    LOG_ERROR_S(*NActors::TlsActivationContext, NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define CA_LOG_C(s) \
    LOG_CRIT_S(*NActors::TlsActivationContext,  NKikimrServices::KQP_COMPUTE, LogPrefix << s)
#define CA_LOG(prio, s) \
    LOG_LOG_S(*NActors::TlsActivationContext, prio, NKikimrServices::KQP_COMPUTE, LogPrefix << s)


namespace NYql {
namespace NDq {

constexpr ui32 IssuesBufferSize = 16;

struct TSinkCallbacks : public IDqComputeActorAsyncOutput::ICallbacks {
    void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override final {
        OnSinkError(outputIndex, issues, fatalCode);
    }

    void OnAsyncOutputStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override final {
        OnSinkStateSaved(std::move(state), outputIndex, checkpoint);
    }

    void OnAsyncOutputFinished(ui64 outputIndex) override final {
        OnSinkFinished(outputIndex);
    }

    virtual void OnSinkError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) = 0;
    virtual void OnSinkStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) = 0;
    virtual void OnSinkFinished(ui64 outputIndex) = 0;
};

struct TOutputTransformCallbacks : public IDqComputeActorAsyncOutput::ICallbacks {
    void OnAsyncOutputError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override final {
        OnOutputTransformError(outputIndex, issues, fatalCode);
    }

    void OnAsyncOutputStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) override final {
        OnTransformStateSaved(std::move(state), outputIndex, checkpoint);
    }

    void OnAsyncOutputFinished(ui64 outputIndex) override final {
        OnTransformFinished(outputIndex);
    }

    virtual void OnOutputTransformError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) = 0;
    virtual void OnTransformStateSaved(NDqProto::TSinkState&& state, ui64 outputIndex, const NDqProto::TCheckpoint& checkpoint) = 0;
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
            LogPrefix = TStringBuilder() << "SelfId: " << this->SelfId() << ", TxId: " << TxId << ", task: " << Task.GetId() << ". ";

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
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        bool ownMemoryQuota = true, bool passExceptions = false,
        const ::NMonitoring::TDynamicCounterPtr& taskCounters = nullptr,
        NWilson::TTraceId traceId = {})
        : ExecuterId(executerId)
        , TxId(txId)
        , Task(std::move(task))
        , RuntimeSettings(settings)
        , MemoryLimits(memoryLimits)
        , CanAllocateExtraMemory(RuntimeSettings.ExtraMemoryAllocationPool != 0)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FunctionRegistry(functionRegistry)
        , CheckpointingMode(GetTaskCheckpointingMode(Task))
        , State(Task.GetCreateSuspended() ? NDqProto::COMPUTE_STATE_UNKNOWN : NDqProto::COMPUTE_STATE_EXECUTING)
        , WatermarksTracker(this->SelfId(), TxId, Task.GetId())
        , TaskCounters(taskCounters)
        , DqComputeActorMetrics(taskCounters)
        , ComputeActorSpan(NKikimr::TWilsonKqp::ComputeActor, std::move(traceId), "ComputeActor")
        , Running(!Task.GetCreateSuspended())
        , PassExceptions(passExceptions)
    {
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            BasicStats = std::make_unique<TBasicStats>();
        }
        InitMonCounters(taskCounters);
        InitializeTask();
        if (ownMemoryQuota) {
            MemoryQuota = InitMemoryQuota();
        }
        InitializeWatermarks();
    }

    TDqComputeActorBase(const NActors::TActorId& executerId, const TTxId& txId, const NDqProto::TDqTask& task,
        IDqAsyncIoFactory::TPtr asyncIoFactory,
        const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        const ::NMonitoring::TDynamicCounterPtr& taskCounters = nullptr,
        NWilson::TTraceId traceId = {})
        : ExecuterId(executerId)
        , TxId(txId)
        , Task(task)
        , RuntimeSettings(settings)
        , MemoryLimits(memoryLimits)
        , CanAllocateExtraMemory(RuntimeSettings.ExtraMemoryAllocationPool != 0)
        , AsyncIoFactory(std::move(asyncIoFactory))
        , FunctionRegistry(functionRegistry)
        , State(Task.GetCreateSuspended() ? NDqProto::COMPUTE_STATE_UNKNOWN : NDqProto::COMPUTE_STATE_EXECUTING)
        , WatermarksTracker(this->SelfId(), TxId, Task.GetId())
        , TaskCounters(taskCounters)
        , DqComputeActorMetrics(taskCounters)
        , ComputeActorSpan(NKikimr::TWilsonKqp::ComputeActor, std::move(traceId), "ComputeActor")
        , Running(!Task.GetCreateSuspended())
    {
        if (RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_BASIC) {
            BasicStats = std::make_unique<TBasicStats>();
        }
        InitMonCounters(taskCounters);
        InitializeTask();
        MemoryQuota = InitMemoryQuota();
        InitializeWatermarks();
    }

    void InitMonCounters(const ::NMonitoring::TDynamicCounterPtr& taskCounters) {
        if (taskCounters) {
            MkqlMemoryQuota = taskCounters->GetCounter("MkqlMemoryQuota");
            OutputChannelSize = taskCounters->GetCounter("OutputChannelSize");
            SourceCpuTimeMs = taskCounters->GetCounter("SourceCpuTimeMs", true);
        }
    }

    void ReportEventElapsedTime() {
        if (BasicStats) {
            ui64 elapsedMicros = NActors::TlsActivationContext->GetCurrentEventTicksAsSeconds() * 1'000'000ull;
            BasicStats->CpuTime += TDuration::MicroSeconds(elapsedMicros);
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

        ReportEventElapsedTime();
    }

    STFUNC(BaseStateFuncBody) {
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
            RuntimeSettings.StatsMode >= NDqProto::DQ_STATS_MODE_PROFILE,
            CanAllocateExtraMemory,
            NActors::TActivationContext::ActorSystem());
    }

    virtual ui64 GetMkqlMemoryLimit() const {
        Y_VERIFY(MemoryQuota);
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

            ReportStats(TInstant::Now());
        }
        if (Terminated) {
            TaskRunner.Reset();
            MemoryQuota.Reset();
            MemoryLimits.MemoryQuotaManager.reset();
        }
    }

    virtual void DoExecuteImpl() {
        auto sourcesState = GetSourcesState();

        PollAsyncInput();
        ERunStatus status = TaskRunner->Run();

        CA_LOG_T("Resume execution, run status: " << status);

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
                    auto peerState = Channels->GetOutputChannelInFlightState(channelId);
                    DrainOutputChannel(outputChannel, peerState);
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
            ContinueExecute();
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
            auto guard = MaybeBindAllocator(); // Source/Sink could destroy mkql values inside PassAway, which requires allocator to be bound

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
        for (auto& [index, input] : InputTransformsMap) {
            if (input.AsyncInput) {
                if (auto data = input.AsyncInput->ExtraData()) {
                    auto* entry = extraData->AddInputTransformsData();
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

        ContinueExecute();
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

    void OnSinkFinished(ui64 outputIndex) override {
        SinksMap.at(outputIndex).FinishIsAcknowledged = true;
        ContinueExecute();
    }

    void OnTransformFinished(ui64 outputIndex) override {
        OutputTransformsMap.at(outputIndex).FinishIsAcknowledged = true;
        ContinueExecute();
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

    void ResumeInputsByCheckpoint() override {
        for (auto& [id, channelInfo] : InputChannelsMap) {
            channelInfo.ResumeByCheckpoint();
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
        const TString LogPrefix;
        ui64 ChannelId;
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
                NDqProto::EWatermarksMode watermarksMode,
                NDqProto::ECheckpointingMode checkpointingMode)
            : LogPrefix(logPrefix)
            , ChannelId(channelId)
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

    struct TAsyncInputInfoBase {
        TString Type;
        const TString LogPrefix;
        ui64 Index;
        IDqAsyncInputBuffer::TPtr Buffer;
        IDqComputeActorAsyncInput* AsyncInput = nullptr;
        NActors::IActor* Actor = nullptr;
        TIssuesBuffer IssuesBuffer;
        bool Finished = false;
        i64 FreeSpace = 1;
        bool PushStarted = false;
        const NDqProto::EWatermarksMode WatermarksMode = NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED;
        TMaybe<TInstant> PendingWatermark = Nothing();

        explicit TAsyncInputInfoBase(
                const TString& logPrefix,
                ui64 index,
                NDqProto::EWatermarksMode watermarksMode)
            : LogPrefix(logPrefix)
            , Index(index)
            , IssuesBuffer(IssuesBufferSize)
            , WatermarksMode(watermarksMode) {}

        bool IsPausedByWatermark() {
            return PendingWatermark.Defined();
        }

        void Pause(TInstant watermark) {
            YQL_ENSURE(WatermarksMode != NDqProto::WATERMARKS_MODE_DISABLED);
            PendingWatermark = watermark;
        }

        void ResumeByWatermark(TInstant watermark) {
            YQL_ENSURE(watermark == PendingWatermark);
            PendingWatermark = Nothing();
        }
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
        NDqProto::EWatermarksMode WatermarksMode = NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED;

        explicit TOutputChannelInfo(ui64 channelId)
            : ChannelId(channelId)
        { }

        struct TStats {
            ui64 BlockedByCapacity = 0;
            ui64 NoDstActorId = 0;
        };
        THolder<TStats> Stats;

        struct TAsyncData { // Is used in case of async compute actor
            TVector<NDqProto::TData> Data;
            TMaybe<NDqProto::TWatermark> Watermark;
            TMaybe<NDqProto::TCheckpoint> Checkpoint;
            bool Finished = false;
            bool Changed = false;
        };
        TMaybe<TAsyncData> AsyncData;
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

    virtual i64 AsyncInputFreeSpace(TAsyncInputInfoBase& source) {
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
        Checkpoints->Receive(handle);
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

        CA_LOG_T("About to drain channelId: " << channelId
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
            ui32 sent = this->SendChannelDataChunk(outputChannel);
            if (sent == 0) {
                break;
            }
            remains -= sent;
        }

        ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
        ProcessOutputsState.AllOutputsFinished &= outputChannel.Finished;
        ProcessOutputsState.DataWasSent |= (!wasFinished && outputChannel.Finished) || remains != toSend;
    }

    ui32 SendChannelDataChunk(TOutputChannelInfo& outputChannel) {
        auto channel = outputChannel.Channel;

        NDqProto::TData data;
        NDqProto::TWatermark watermark;
        NDqProto::TCheckpoint checkpoint;

        bool hasData = channel->Pop(data);
        bool hasWatermark = channel->Pop(watermark);
        bool hasCheckpoint = channel->Pop(checkpoint);
        if (!hasData && !hasWatermark && !hasCheckpoint) {
            if (!channel->IsFinished()) {
                CA_LOG_T("output channelId: " << channel->GetChannelId() << ", nothing to send and is not finished");
                return 0; // channel is empty and not finished yet
            }
        }
        const bool wasFinished = outputChannel.Finished;
        outputChannel.Finished = channel->IsFinished();
        const bool becameFinished = !wasFinished && outputChannel.Finished;

        ui32 dataSize = data.GetRaw().size();
        ui32 watermarkSize = watermark.ByteSize();
        ui32 checkpointSize = checkpoint.ByteSize();

        NDqProto::TChannelData channelData;
        channelData.SetChannelId(channel->GetChannelId());
        channelData.SetFinished(outputChannel.Finished);
        if (hasData) {
            channelData.MutableData()->Swap(&data);
        }
        if (hasWatermark) {
            channelData.MutableWatermark()->Swap(&watermark);
            CA_LOG_I("Resume inputs by watermark");
            // This is excessive, inputs should be resumed after async CA received response with watermark from task runner.
            // But, let it be here, it's better to have the same code as in checkpoints
            ResumeInputsByWatermark(TInstant::MicroSeconds(watermark.GetTimestampUs()));
        }
        if (hasCheckpoint) {
            channelData.MutableCheckpoint()->Swap(&checkpoint);
            CA_LOG_I("Resume inputs by checkpoint");
            ResumeInputsByCheckpoint();
        }

        if (hasData || hasWatermark || hasCheckpoint || becameFinished) {
            Channels->SendChannelData(std::move(channelData));
            return dataSize + watermarkSize + checkpointSize;
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
            Y_VERIFY(inputDesc.HasSource());
            source.Type = inputDesc.GetSource().GetType();
            const ui64 i = inputIndex; // Crutch for clang
            CA_LOG_D("Create source for input " << i << " " << inputDesc);
            try {
                std::tie(source.AsyncInput, source.Actor) = AsyncIoFactory->CreateDqSource(
                    IDqAsyncIoFactory::TSourceArguments {
                        .InputDesc = inputDesc,
                        .InputIndex = inputIndex,
                        .TxId = TxId,
                        .SecureParams = secureParams,
                        .TaskParams = taskParams,
                        .ComputeActorId = this->SelfId(),
                        .TypeEnv = typeEnv,
                        .HolderFactory = holderFactory,
                        .TaskCounters = TaskCounters,
                        .Alloc = TaskRunner ? TaskRunner->GetAllocatorPtr() : nullptr
                    });
            } catch (const std::exception& ex) {
                throw yexception() << "Failed to create source " << inputDesc.GetSource().GetType() << ": " << ex.what();
            }
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
                try {
                    std::tie(transform.AsyncInput, transform.Actor) = AsyncIoFactory->CreateDqInputTransform(
                        IDqAsyncIoFactory::TInputTransformArguments {
                            .InputDesc = inputDesc,
                            .InputIndex = inputIndex,
                            .TxId = TxId,
                            .TaskId = Task.GetId(),
                            .TransformInput = transform.InputBuffer,
                            .SecureParams = secureParams,
                            .TaskParams = taskParams,
                            .ComputeActorId = this->SelfId(),
                            .TypeEnv = typeEnv,
                            .HolderFactory = holderFactory,
                            .ProgramBuilder = *transform.ProgramBuilder,
                            .Alloc = TaskRunner->GetAllocatorPtr()
                        });
                } catch (const std::exception& ex) {
                    throw yexception() << "Failed to create input transform " << inputDesc.GetTransform().GetType() << ": " << ex.what();
                }
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
                try {
                    std::tie(transform.AsyncOutput, transform.Actor) = AsyncIoFactory->CreateDqOutputTransform(
                        IDqAsyncIoFactory::TOutputTransformArguments {
                            .OutputDesc = outputDesc,
                            .OutputIndex = outputIndex,
                            .TxId = TxId,
                            .TransformOutput = transform.OutputBuffer,
                            .Callback = static_cast<TOutputTransformCallbacks*>(this),
                            .SecureParams = secureParams,
                            .TaskParams = taskParams,
                            .TypeEnv = typeEnv,
                            .HolderFactory = holderFactory,
                            .ProgramBuilder = *transform.ProgramBuilder
                        });
                } catch (const std::exception& ex) {
                    throw yexception() << "Failed to create output transform " << outputDesc.GetTransform().GetType() << ": " << ex.what();
                }
                this->RegisterWithSameMailbox(transform.Actor);
            }
        }
        for (auto& [outputIndex, sink] : SinksMap) {
            if (TaskRunner) { sink.Buffer = TaskRunner->GetSink(outputIndex); }
            Y_VERIFY(AsyncIoFactory);
            const auto& outputDesc = Task.GetOutputs(outputIndex);
            Y_VERIFY(outputDesc.HasSink());
            sink.Type = outputDesc.GetSink().GetType();
            const ui64 i = outputIndex; // Crutch for clang
            CA_LOG_D("Create sink for output " << i << " " << outputDesc);
            try {
                std::tie(sink.AsyncOutput, sink.Actor) = AsyncIoFactory->CreateDqSink(
                    IDqAsyncIoFactory::TSinkArguments {
                        .OutputDesc = outputDesc,
                        .OutputIndex = outputIndex,
                        .TxId = TxId,
                        .Callback = static_cast<TSinkCallbacks*>(this),
                        .SecureParams = secureParams,
                        .TaskParams = taskParams,
                        .TypeEnv = typeEnv,
                        .HolderFactory = holderFactory,
                        .RandomProvider = TaskRunner ? TaskRunner->GetRandomProvider() : nullptr
                    });
            } catch (const std::exception& ex) {
                throw yexception() << "Failed to create sink " << outputDesc.GetSink().GetType() << ": " << ex.what();
            }
            this->RegisterWithSameMailbox(sink.Actor);
        }
    }

    void PollAsyncInput(TAsyncInputInfoBase& info, ui64 inputIndex) {
        Y_VERIFY(!TaskRunner || info.Buffer);

        if (info.Finished) {
            CA_LOG_T("Skip polling async input[" << inputIndex << "]: finished");
            return;
        }

        if (info.IsPausedByWatermark()) {
            CA_LOG_T("Skip polling async input[" << inputIndex << "]: paused");
            return;
        }

        const i64 freeSpace = AsyncInputFreeSpace(info);
        if (freeSpace > 0) {
            TMaybe<TInstant> watermark;
            NKikimr::NMiniKQL::TUnboxedValueVector batch;
            Y_VERIFY(info.AsyncInput);
            bool finished = false;
            const i64 space = info.AsyncInput->GetAsyncInputData(batch, watermark, finished, freeSpace);
            CA_LOG_T("Poll async input " << inputIndex
                << ". Buffer free space: " << freeSpace
                << ", read from async input: " << space << " bytes, "
                << batch.size() << " rows, finished: " << finished);

            if (!batch.empty()) {
                // If we have read some data, we must run such reading again
                // to process the case when async input notified us about new data
                // but we haven't read all of it.
                ContinueExecute();
            }

            DqComputeActorMetrics.ReportAsyncInputData(inputIndex, batch.size(), watermark);

            if (watermark) {
                const auto inputWatermarkChanged = WatermarksTracker.NotifyAsyncInputWatermarkReceived(
                    inputIndex,
                    *watermark);

                if (inputWatermarkChanged) {
                    CA_LOG_T("Pause async input " << inputIndex << " because of watermark " << *watermark);
                    info.Pause(*watermark);
                }
            }

            AsyncInputPush(std::move(batch), info, space, finished);
        } else {
            CA_LOG_T("Skip polling async input[" << inputIndex << "]: no free space: " << freeSpace);
            ContinueExecute(); // If there is no free space in buffer, => we have something to process
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
            PollAsyncInput(source, inputIndex);
        }

        CA_LOG_T("Poll inputs");
        for (auto& [inputIndex, transform] : InputTransformsMap) {
            PollAsyncInput(transform, inputIndex);
        }
    }

    void OnNewAsyncInputDataArrived(const IDqComputeActorAsyncInput::TEvNewAsyncInputDataArrived::TPtr& ev) {
        Y_VERIFY(SourcesMap.FindPtr(ev->Get()->InputIndex) || InputTransformsMap.FindPtr(ev->Get()->InputIndex));
        auto cpuTimeDelta = TakeSourceCpuTimeDelta();
        if (SourceCpuTimeMs) {
            SourceCpuTimeMs->Add(cpuTimeDelta.MilliSeconds());
        }
        CpuTimeSpent += cpuTimeDelta;
        ContinueExecute();
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

    void OnSinkError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override {
        if (fatalCode == NYql::NDqProto::StatusIds::UNSPECIFIED) {
            SinksMap.at(outputIndex).IssuesBuffer.Push(issues);
            return;
        }

        CA_LOG_E("Sink[" << outputIndex << "] fatal error: " << issues.ToOneLineString());
        InternalError(fatalCode, issues);
    }

    void OnOutputTransformError(ui64 outputIndex, const TIssues& issues, NYql::NDqProto::StatusIds::StatusCode fatalCode) override {
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
                ui64 index = outputIndex; // Crutch for logging through lambda.
                CA_LOG_D("Waiting finish of sink[" << index << "]");
                return false;
            }
        }
        for (const auto& [outputIndex, transformInfo] : OutputTransformsMap) {
            if (!transformInfo.FinishIsAcknowledged) {
                ui64 index = outputIndex; // Crutch for logging through lambda.
                CA_LOG_D("Waiting finish of transform[" << index << "]");
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

private:
    void InitializeTask() {
        for (ui32 i = 0; i < Task.InputsSize(); ++i) {
            const auto& inputDesc = Task.GetInputs(i);
            Y_VERIFY(!inputDesc.HasSource() || inputDesc.ChannelsSize() == 0); // HasSource => no channels

            if (inputDesc.HasTransform()) {
                auto result = InputTransformsMap.emplace(
                    std::piecewise_construct,
                    std::make_tuple(i),
                    std::make_tuple(LogPrefix, i, NDqProto::WATERMARKS_MODE_DISABLED)
                );
                YQL_ENSURE(result.second);
            }

            if (inputDesc.HasSource()) {
                const auto watermarksMode = inputDesc.GetSource().GetWatermarksMode();
                auto result = SourcesMap.emplace(i, TAsyncInputInfoBase(LogPrefix, i, watermarksMode));
                YQL_ENSURE(result.second);
            } else {
                for (auto& channel : inputDesc.GetChannels()) {
                    auto result = InputChannelsMap.emplace(
                        channel.GetId(),
                        TInputChannelInfo(
                            LogPrefix,
                            channel.GetId(),
                            channel.GetWatermarksMode(),
                            channel.GetCheckpointingMode())
                    );
                    YQL_ENSURE(result.second);
                }
            }
        }

        for (ui32 i = 0; i < Task.OutputsSize(); ++i) {
            const auto& outputDesc = Task.GetOutputs(i);
            Y_VERIFY(!outputDesc.HasSink() || outputDesc.ChannelsSize() == 0); // HasSink => no channels

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
    }

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

    virtual const TDqAsyncInputBufferStats* GetInputTransformStats(ui64 inputIdx, const TAsyncInputTransformInfo& inputTransformInfo) const {
        Y_UNUSED(inputIdx);
        return inputTransformInfo.Buffer ? inputTransformInfo.Buffer->GetStats() : nullptr;
    }

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

        if (Stat) { // for task_runner_actor
            Y_VERIFY(!dst->HasExtra());
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

            THashMap<TString, ui64> Ingress;
            THashMap<TString, ui64> Egress;

            THashMap<ui64, ui64> ingressBytesMap;
            for (auto& [inputIndex, sourceInfo] : SourcesMap) {
                if (auto* source = sourceInfo.AsyncInput) {
                    auto ingressBytes = sourceInfo.AsyncInput->GetIngressBytes();
                    ingressBytesMap.emplace(inputIndex, ingressBytes);
                    Ingress[sourceInfo.Type] = Ingress.Value(sourceInfo.Type, 0) + ingressBytes;
                    // TODO: support async CA
                    source->FillExtraStats(protoTask, last, TaskRunner ? TaskRunner->GetMeteringStats() : nullptr);
                }
            }
            FillTaskRunnerStats(Task.GetId(), Task.GetStageId(), *taskStats, protoTask, (bool) GetProfileStats(), ingressBytesMap);

            // More accurate cpu time counter:
            if (TDerived::HasAsyncTaskRunner) {
                protoTask->SetCpuTimeUs(BasicStats->CpuTime.MicroSeconds() + taskStats->ComputeCpuTime.MicroSeconds() + taskStats->BuildCpuTime.MicroSeconds());
            }
            protoTask->SetSourceCpuTimeUs(SourceCpuTime.MicroSeconds());

            for (auto& [outputIndex, sinkInfo] : SinksMap) {

                ui64 egressBytes = sinkInfo.AsyncOutput ? sinkInfo.AsyncOutput->GetEgressBytes() : 0;

                if (auto* sinkStats = GetSinkStats(outputIndex, sinkInfo)) {
                    protoTask->SetOutputRows(protoTask->GetOutputRows() + sinkStats->RowsIn);
                    protoTask->SetOutputBytes(protoTask->GetOutputBytes() + sinkStats->Bytes);
                    Egress[sinkInfo.Type] = Egress.Value(sinkInfo.Type, 0) + egressBytes;

                    if (GetProfileStats()) {
                        auto* protoSink = protoTask->AddSinks();
                        protoSink->SetOutputIndex(outputIndex);

                        protoSink->SetChunks(sinkStats->Chunks);
                        protoSink->SetBytes(sinkStats->Bytes);
                        protoSink->SetRowsIn(sinkStats->RowsIn);
                        protoSink->SetRowsOut(sinkStats->RowsOut);
                        protoSink->SetEgressBytes(egressBytes);

                        protoSink->SetMaxMemoryUsage(sinkStats->MaxMemoryUsage);
                        protoSink->SetErrorsCount(sinkInfo.IssuesBuffer.GetAllAddedIssuesCount());
                    }
                }
            }

            for (auto& [inputIndex, transformInfo] : InputTransformsMap) {
                auto* transformStats = GetInputTransformStats(inputIndex, transformInfo);
                if (transformStats && GetProfileStats()) {
                    YQL_ENSURE(transformStats);
                    ui64 ingressBytes = transformInfo.AsyncInput ? transformInfo.AsyncInput->GetIngressBytes() : 0;

                    auto* protoTransform = protoTask->AddInputTransforms();
                    protoTransform->SetInputIndex(inputIndex);
                    protoTransform->SetChunks(transformStats->Chunks);
                    protoTransform->SetBytes(transformStats->Bytes);
                    protoTransform->SetRowsIn(transformStats->RowsIn);
                    protoTransform->SetRowsOut(transformStats->RowsOut);
                    protoTransform->SetIngressBytes(ingressBytes);

                    protoTransform->SetMaxMemoryUsage(transformStats->MaxMemoryUsage);
                }

                if (auto* transform = transformInfo.AsyncInput) {
                    // TODO: support async CA
                    transform->FillExtraStats(protoTask, last, TaskRunner ? TaskRunner->GetMeteringStats() : 0);
                }
            }

            for (auto& [name, bytes] : Egress) {
                auto* egressStats = protoTask->AddEgress();
                egressStats->SetName(name);
                egressStats->SetBytes(bytes);
            }
            for (auto& [name, bytes] : Ingress) {
                auto* ingressStats = protoTask->AddIngress();
                ingressStats->SetName(name);
                ingressStats->SetBytes(bytes);
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

        if (ComputeActorSpan) {
            ComputeActorSpan.End();
        }

        this->Send(ExecuterId, evState.release(), NActors::IEventHandle::FlagTrackDelivery);

        LastSendStatsTime = now;
    }

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
        bool LastPopReturnedNoData = false;
    };
    TProcessOutputsState ProcessOutputsState;

    THolder<TDqMemoryQuota> MemoryQuota;
    TDqComputeActorWatermarks WatermarksTracker;
    ::NMonitoring::TDynamicCounterPtr TaskCounters;
    TDqComputeActorMetrics DqComputeActorMetrics;
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
