#include "dq_compute_actor.h"
#include "dq_async_compute_actor.h"
#include "dq_compute_actor_async_input_helper.h"
#include "dq_task_runner_exec_ctx.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_impl.h>

#include <ydb/library/yql/dq/common/dq_common.h>

#include <ydb/core/quoter/public/quoter.h>

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

//Used in Async CA to interact with TaskRunnerActor
struct TComputeActorAsyncInputHelperAsync : public TComputeActorAsyncInputHelper
{
public:
    TComputeActorAsyncInputHelperAsync(
        const TString& logPrefix,
        ui64 index,
        NDqProto::EWatermarksMode watermarksMode,
        ui64& cookie,
        int& inflight
    )
    : TComputeActorAsyncInputHelper(logPrefix, index, watermarksMode)
    , TaskRunnerActor(nullptr)
    , Cookie(cookie)
    , Inflight(inflight)
    , FreeSpace(1)
    , PushStarted(false)
    {}

    i64 GetFreeSpace() const override
    {
        return FreeSpace;
    }

    void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space, bool finished) override
    {
        Inflight++;
        PushStarted = true;
        Finished = finished;
        Y_ABORT_UNLESS(TaskRunnerActor);
        TaskRunnerActor->AsyncInputPush(Cookie++, Index, std::move(batch), space, finished);
    }

    NTaskRunnerActor::ITaskRunnerActor* TaskRunnerActor;
    ui64& Cookie;
    int& Inflight;
    i64 FreeSpace;
    bool PushStarted;
};

class TDqAsyncComputeActor : public TDqComputeActorBase<TDqAsyncComputeActor, TComputeActorAsyncInputHelperAsync>
                           , public NTaskRunnerActor::ITaskRunnerActor::ICallbacks
{
    using TBase = TDqComputeActorBase<TDqAsyncComputeActor, TComputeActorAsyncInputHelperAsync>;
public:
    static constexpr char ActorName[] = "DQ_COMPUTE_ACTOR";

    static constexpr bool HasAsyncTaskRunner = true;

    TDqAsyncComputeActor(const TActorId& executerId, const TTxId& txId, NDqProto::TDqTask* task,
        IDqAsyncIoFactory::TPtr asyncIoFactory, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
        const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
        const NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
        const ::NMonitoring::TDynamicCounterPtr& taskCounters,
        const TActorId& quoterServiceActorId,
        bool ownCounters)
        : TBase(executerId, txId, task, std::move(asyncIoFactory), functionRegistry, settings, memoryLimits, /* ownMemoryQuota = */ false, false, taskCounters)
        , TaskRunnerActorFactory(taskRunnerActorFactory)
        , ReadyToCheckpointFlag(false)
        , SentStatsRequest(false)
        , QuoterServiceActorId(quoterServiceActorId)
    {
        InitializeTask();
        InitExtraMonCounters(taskCounters);
        if (ownCounters) {
            CA_LOG_D("TDqAsyncComputeActor, make stat");
            Stat = MakeHolder<NYql::TCounters>();
        }
    }

    void DoBootstrap() {
        const TActorSystem* actorSystem = TlsActivationContext->ActorSystem();

        TLogFunc logger;
        if (IsDebugLogEnabled(actorSystem)) {
            logger = [actorSystem, txId = GetTxId(), taskId = Task.GetId()] (const TString& message) {
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
            this, TBase::GetAllocatorPtr(), GetTxId(), Task.GetId(), std::move(inputWithDisabledCheckpointing), InitMemoryQuota());
        TaskRunnerActorId = RegisterWithSameMailbox(actor);

        TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = MemoryLimits.ChannelBufferSize;
        limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

        for (auto& [_, source]: SourcesMap) {
            source.TaskRunnerActor = TaskRunnerActor;
        }

        Become(&TDqAsyncComputeActor::StateFuncWrapper<&TDqAsyncComputeActor::StateFuncBody>);

        auto wakeup = [this]{ ContinueExecute(EResumeSource::CABootstrapWakeup); };
        std::shared_ptr<IDqTaskRunnerExecutionContext> execCtx = std::make_shared<TDqTaskRunnerExecutionContext>(TxId, std::move(wakeup));

        Send(TaskRunnerActorId,
            new NTaskRunnerActor::TEvTaskRunnerCreate(
                Task.GetSerializedTask(), limits, RuntimeSettings.StatsMode, execCtx));

        CA_LOG_D("Use CPU quota: " << UseCpuQuota() << ". Rate limiter resource: { \"" << Task.GetRateLimiter() << "\", \"" << Task.GetRateLimiterResource() << "\" }");
    }

    void FillExtraStats(NDqProto::TDqComputeActorStats* /* dst */, bool /* last */) {
    }

    void InitExtraMonCounters(const ::NMonitoring::TDynamicCounterPtr& taskCounters) {
        if (taskCounters && UseCpuQuota()) {
            CpuTimeGetQuotaLatency = taskCounters->GetHistogram("CpuTimeGetQuotaLatencyMs", NMonitoring::ExplicitHistogram({0, 1, 5, 10, 50, 100, 500, 1000}));
            CpuTimeQuotaWaitDelay = taskCounters->GetHistogram("CpuTimeQuotaWaitDelayMs", NMonitoring::ExplicitHistogram({0, 1, 5, 10, 50, 100, 500, 1000}));
            CpuTime = taskCounters->GetCounter("CpuTimeMs", true);
            CpuTime->Add(0);
        }
    }

    TComputeActorAsyncInputHelperAsync CreateInputHelper(const TString& logPrefix,
        ui64 index,
        NDqProto::EWatermarksMode watermarksMode
    ) 
    {
        return TComputeActorAsyncInputHelperAsync(logPrefix, index, watermarksMode, Cookie, ProcessSourcesState.Inflight);
    }

    const IDqAsyncInputBuffer* GetInputTransform(ui64 inputIdx, const TComputeActorAsyncInputHelperSync&) const {
        return TaskRunnerStats.GetInputTransform(inputIdx);
    }

private:
    STFUNC(StateFuncBody) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NTaskRunnerActor::TEvTaskRunFinished, OnRunFinished);
            hFunc(NTaskRunnerActor::TEvSourceDataAck, OnSourceDataAck);
            hFunc(NTaskRunnerActor::TEvOutputChannelData, OnOutputChannelData);
            hFunc(NTaskRunnerActor::TEvTaskRunnerCreateFinished, OnTaskRunnerCreated);
            hFunc(NTaskRunnerActor::TEvInputChannelDataAck, OnInputChannelDataAck);
            hFunc(TEvDqCompute::TEvStateRequest, OnStateRequest);
            hFunc(NTaskRunnerActor::TEvStatistics, OnStatisticsResponse);
            hFunc(NTaskRunnerActor::TEvLoadTaskRunnerFromStateDone, OnTaskRunnerLoaded);
            hFunc(TEvDqCompute::TEvInjectCheckpoint, OnInjectCheckpoint);
            hFunc(TEvDqCompute::TEvRestoreFromCheckpoint, OnRestoreFromCheckpoint);
            hFunc(NKikimr::TEvQuota::TEvClearance, OnCpuQuotaGiven);
            hFunc(NActors::NMon::TEvHttpInfo, OnMonitoringPage)
            default:
                TBase::BaseStateFuncBody(ev);
        };
    };

    void OnMonitoringPage(NActors::NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream html;
        html << "<h3>Common</h3>";
        html << "Cookie: " << Cookie << "<br />";
        html << "MkqlMemoryLimit: " << MkqlMemoryLimit << "<br />";
        html << "SentStatsRequest: " << SentStatsRequest << "<br />";

        html << "<h3>State</h3>";
        html << "<pre>" << ComputeActorState.DebugString() << "</pre>";

        html << "<h3>Watermarks</h3>";
        for (const auto& [time, id]: WatermarkTakeInputChannelDataRequests) {
            html << "WatermarkTakeInputChannelDataRequests: " << time.ToString() << " " << id << "<br />";
        }

        html << "<h3>CPU Quota</h3>";
        html << "QuoterServiceActorId: " << QuoterServiceActorId.ToString() << "<br />";
        if (ContinueRunEvent) {
            html << "ContinueRunEvent.AskFreeSpace: " << ContinueRunEvent->AskFreeSpace << "<br />";
            html << "ContinueRunEvent.AskFreeSpace: " << ContinueRunEvent->CheckpointOnly << "<br />";
            html << "ContinueRunEvent.AskFreeSpace: " << ContinueRunEvent->CheckpointRequest.Defined() << "<br />";
            html << "ContinueRunEvent.AskFreeSpace: " << ContinueRunEvent->WatermarkRequest.Defined() << "<br />";
            html << "ContinueRunEvent.AskFreeSpace: " << ContinueRunEvent->CheckpointOnly << "<br />";
            html << "ContinueRunEvent.AskFreeSpace: " << ContinueRunEvent->MemLimit << "<br />";
            for (const auto& sinkId: ContinueRunEvent->SinkIds) {
                html << "ContinueRunEvent.SinkIds: " << sinkId << "<br />";
            }

            for (const auto& inputTransformId: ContinueRunEvent->InputTransformIds) {
                html << "ContinueRunEvent.InputTransformIds: " << inputTransformId << "<br />";
            }
        }

        html << "ContinueRunStartWaitTime: " << ContinueRunStartWaitTime.ToString() << "<br />";
        html << "ContinueRunInflight: " << ContinueRunInflight << "<br />";
        html << "CpuTimeSpent: " << CpuTimeSpent.ToString() << "<br />";
        html << "CpuTimeQuotaAsked: " << CpuTimeQuotaAsked.ToString() << "<br />";
        html << "UseCpuQuota: " << UseCpuQuota() << "<br />";

        html << "<h3>Checkpoints</h3>";
        html << "ReadyToCheckpoint: " << ReadyToCheckpoint() << "<br />";
        html << "CheckpointRequestedFromTaskRunner: " << CheckpointRequestedFromTaskRunner << "<br />";

        html << "<h3>InputChannels</h3>";
        for (const auto& [id, info]: InputChannelsMap) {
            html << "<h4>Input Channel Id: " << id << "</h4>";
            html << "LogPrefix: " << info.LogPrefix << "<br />";
            html << "ChannelId: " << info.ChannelId << "<br />";
            html << "SrcStageId: " << info.SrcStageId << "<br />";
            html << "HasPeer: " << info.HasPeer << "<br />";
            html << "PendingWatermarks: " << !info.PendingWatermarks.empty() << " " << (info.PendingWatermarks.empty() ? TString{} : info.PendingWatermarks.back().ToString()) << "<br />";
            html << "WatermarksMode: " << NDqProto::EWatermarksMode_Name(info.WatermarksMode) << "<br />";
            html << "PendingCheckpoint: " << info.PendingCheckpoint.has_value() << " " << (info.PendingCheckpoint ? TStringBuilder{} << info.PendingCheckpoint->GetId() << " " << info.PendingCheckpoint->GetGeneration() : TString{}) << "<br />";
            html << "CheckpointingMode: " << NDqProto::ECheckpointingMode_Name(info.CheckpointingMode) << "<br />";
            html << "FreeSpace: " << info.FreeSpace << "<br />";
            html << "IsPaused: " << info.IsPaused() << "<br />";
            if (info.Channel) {
                html << "DqInputChannel.ChannelId: " << info.Channel->GetChannelId() << "<br />";
                html << "DqInputChannel.FreeSpace: " << info.Channel->GetFreeSpace() << "<br />";
                html << "DqInputChannel.StoredBytes: " << info.Channel->GetStoredBytes() << "<br />";
                html << "DqInputChannel.Empty: " << info.Channel->Empty() << "<br />";
                html << "DqInputChannel.InputType: " << (info.Channel->GetInputType() ? info.Channel->GetInputType()->GetKindAsStr() : TString{"unknown"})  << "<br />";
                html << "DqInputChannel.InputWidth: " << (info.Channel->GetInputWidth() ? ToString(*info.Channel->GetInputWidth()) : TString{"unknown"})  << "<br />";
                html << "DqInputChannel.IsFinished: " << info.Channel->IsFinished() << "<br />";

                const auto& pushStats = info.Channel->GetPushStats();
                html << "DqInputChannel.PushStats.ChannelId: " << pushStats.ChannelId << "<br />";
                html << "DqInputChannel.PushStats.SrcStageId: " << pushStats.SrcStageId << "<br />";
                html << "DqInputChannel.PushStats.RowsInMemory: " << pushStats.RowsInMemory << "<br />";
                html << "DqInputChannel.PushStats.MaxMemoryUsage: " << pushStats.MaxMemoryUsage << "<br />";
                html << "DqInputChannel.PushStats.DeserializationTime: " << pushStats.DeserializationTime.ToString() << "<br />";
                html << "DqInputChannel.PushStats.Level: " << static_cast<int>(pushStats.Level) << "<br />";
                html << "DqInputChannel.PushStats.MinWaitDuration: " << pushStats.MinWaitDuration.ToString() << "<br />";
                html << "DqInputChannel.PushStats.CurrentPauseTs: " << (pushStats.CurrentPauseTs ? pushStats.CurrentPauseTs->ToString() : TString{}) << "<br />";
                html << "DqInputChannel.PushStats.MergeWaitPeriod: " << pushStats.MergeWaitPeriod << "<br />";
                html << "DqInputChannel.PushStats.Bytes: " << pushStats.Bytes << "<br />";
                html << "DqInputChannel.PushStats.DecompressedBytes: " << pushStats.DecompressedBytes << "<br />";
                html << "DqInputChannel.PushStats.Rows: " << pushStats.Rows << "<br />";
                html << "DqInputChannel.PushStats.Chunks: " << pushStats.Chunks << "<br />";
                html << "DqInputChannel.PushStats.Splits: " << pushStats.Splits << "<br />";
                html << "DqInputChannel.PushStats.FirstMessageTs: " << pushStats.FirstMessageTs.ToString() << "<br />";
                html << "DqInputChannel.PushStats.PauseMessageTs: " << pushStats.PauseMessageTs.ToString() << "<br />";
                html << "DqInputChannel.PushStats.ResumeMessageTs: " << pushStats.ResumeMessageTs.ToString() << "<br />";
                html << "DqInputChannel.PushStats.LastMessageTs: " << pushStats.LastMessageTs.ToString() << "<br />";
                html << "DqInputChannel.PushStats.WaitTime: " << pushStats.WaitTime.ToString() << "<br />";

                const auto& popStats = info.Channel->GetPopStats();
                html << "DqInputChannel.PopStats.Bytes: " << popStats.Bytes << "<br />";
                html << "DqInputChannel.PopStats.DecompressedBytes: " << popStats.DecompressedBytes << "<br />";
                html << "DqInputChannel.PopStats.Rows: " << popStats.Rows << "<br />";
                html << "DqInputChannel.PopStats.Chunks: " << popStats.Chunks << "<br />";
                html << "DqInputChannel.PopStats.Splits: " << popStats.Splits << "<br />";
                html << "DqInputChannel.PopStats.FirstMessageTs: " << popStats.FirstMessageTs.ToString() << "<br />";
                html << "DqInputChannel.PopStats.PauseMessageTs: " << popStats.PauseMessageTs.ToString() << "<br />";
                html << "DqInputChannel.PopStats.ResumeMessageTs: " << popStats.ResumeMessageTs.ToString() << "<br />";
                html << "DqInputChannel.PopStats.LastMessageTs: " << popStats.LastMessageTs.ToString() << "<br />";
                html << "DqInputChannel.PopStats.WaitTime: " << popStats.WaitTime.ToString() << "<br />";
            }
        }

        html << "<h3>OutputChannels</h3>";
        for (const auto& [id, info]: OutputChannelsMap) {
            html << "<h4>Input Channel Id: " << id << "</h4>";
            html << "ChannelId: " << info.ChannelId << "<br />";
            html << "DstStageId: " << info.DstStageId << "<br />";
            html << "HasPeer: " << info.HasPeer << "<br />";
            html << "Finished: " << info.Finished << "<br />";
            html << "EarlyFinish: " << info.EarlyFinish << "<br />";
            html << "PopStarted: " << info.PopStarted << "<br />";
            html << "IsTransformOutput: " << info.IsTransformOutput << "<br />";
            html << "EWatermarksMode: " << NDqProto::EWatermarksMode_Name(info.WatermarksMode) << "<br />";

            if (info.AsyncData) {
                html << "AsyncData.DataSize: " << info.AsyncData->Data.size() << "<br />";
                html << "AsyncData.Changed: " << info.AsyncData->Changed << "<br />";
                html << "AsyncData.Checkpoint: " << info.AsyncData->Checkpoint << "<br />";
                html << "AsyncData.Finished: " << info.AsyncData->Finished << "<br />";
                html << "AsyncData.Watermark: " << info.AsyncData->Watermark << "<br />";
            }

            if (info.Channel) {
                html << "DqOutputChannel.ChannelId: " << info.Channel->GetChannelId() << "<br />";
                html << "DqOutputChannel.ValuesCount: " << info.Channel->GetValuesCount() << "<br />";
                html << "DqOutputChannel.IsFull: " << info.Channel->IsFull() << "<br />";
                html << "DqOutputChannel.HasData: " << info.Channel->HasData() << "<br />";
                html << "DqOutputChannel.IsFinished: " << info.Channel->IsFinished() << "<br />";
                html << "DqInputChannel.OutputType: " << (info.Channel->GetOutputType() ? info.Channel->GetOutputType()->GetKindAsStr() : TString{"unknown"})  << "<br />";

                const auto& pushStats = info.Channel->GetPushStats();
                html << "DqOutputChannel.PushStats.MaxRowsInMemory: " << pushStats.MaxRowsInMemory << "<br />";
                html << "DqOutputChannel.PushStats.MaxMemoryUsage: " << pushStats.MaxMemoryUsage << "<br />";
                html << "DqOutputChannel.PushStats.Level: " << static_cast<int>(pushStats.Level) << "<br />";
                html << "DqOutputChannel.PushStats.MinWaitDuration: " << pushStats.MinWaitDuration.ToString() << "<br />";
                html << "DqOutputChannel.PushStats.CurrentPauseTs: " << (pushStats.CurrentPauseTs ? pushStats.CurrentPauseTs->ToString() : TString{}) << "<br />";
                html << "DqOutputChannel.PushStats.MergeWaitPeriod: " << pushStats.MergeWaitPeriod << "<br />";
                html << "DqOutputChannel.PushStats.Bytes: " << pushStats.Bytes << "<br />";
                html << "DqOutputChannel.PushStats.Rows: " << pushStats.Rows << "<br />";
                html << "DqOutputChannel.PushStats.Chunks: " << pushStats.Chunks << "<br />";
                html << "DqOutputChannel.PushStats.Splits: " << pushStats.Splits << "<br />";
                html << "DqOutputChannel.PushStats.FirstMessageTs: " << pushStats.FirstMessageTs.ToString() << "<br />";
                html << "DqOutputChannel.PushStats.PauseMessageTs: " << pushStats.PauseMessageTs.ToString() << "<br />";
                html << "DqOutputChannel.PushStats.ResumeMessageTs: " << pushStats.ResumeMessageTs.ToString() << "<br />";
                html << "DqOutputChannel.PushStats.LastMessageTs: " << pushStats.LastMessageTs.ToString() << "<br />";
                html << "DqOutputChannel.PushStats.WaitTime: " << pushStats.WaitTime.ToString() << "<br />";

                const auto& popStats = info.Channel->GetPopStats();
                html << "DqOutputChannel.PopStats.ChannelId: " << popStats.ChannelId << "<br />";
                html << "DqOutputChannel.PopStats.DstStageId: " << popStats.DstStageId << "<br />";
                html << "DqOutputChannel.PopStats.MaxMemoryUsage: " << popStats.MaxMemoryUsage << "<br />";
                html << "DqOutputChannel.PopStats.MaxRowsInMemory: " << popStats.MaxRowsInMemory << "<br />";
                html << "DqOutputChannel.PopStats.SerializationTime: " << popStats.SerializationTime.ToString() << "<br />";
                html << "DqOutputChannel.PopStats.SpilledBytes: " << popStats.SpilledBytes << "<br />";
                html << "DqOutputChannel.PopStats.SpilledRows: " << popStats.SpilledRows << "<br />";
                html << "DqOutputChannel.PopStats.SpilledBlobs: " << popStats.SpilledBlobs << "<br />";
                html << "DqOutputChannel.PopStats.Bytes: " << popStats.Bytes << "<br />";
                html << "DqOutputChannel.PopStats.Rows: " << popStats.Rows << "<br />";
                html << "DqOutputChannel.PopStats.Chunks: " << popStats.Chunks << "<br />";
                html << "DqOutputChannel.PopStats.Splits: " << popStats.Splits << "<br />";
                html << "DqOutputChannel.PopStats.FirstMessageTs: " << popStats.FirstMessageTs.ToString() << "<br />";
                html << "DqOutputChannel.PopStats.PauseMessageTs: " << popStats.PauseMessageTs.ToString() << "<br />";
                html << "DqOutputChannel.PopStats.ResumeMessageTs: " << popStats.ResumeMessageTs.ToString() << "<br />";
                html << "DqOutputChannel.PopStats.LastMessageTs: " << popStats.LastMessageTs.ToString() << "<br />";
                html << "DqOutputChannel.PopStats.WaitTime: " << popStats.WaitTime.ToString() << "<br />";
            }
        }

        Send(ev->Sender, new NActors::NMon::TEvHttpInfoRes(html.Str()));
    }

    void OnStateRequest(TEvDqCompute::TEvStateRequest::TPtr& ev) {
        CA_LOG_T("Got TEvStateRequest from actor " << ev->Sender << " PingCookie: " << ev->Cookie);
        if (!SentStatsRequest) {
            Send(TaskRunnerActorId, new NTaskRunnerActor::TEvStatistics(GetIds(SinksMap), GetIds(InputTransformsMap)));
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
            CA_LOG_T("update task runner stats");
            TaskRunnerStats = std::move(ev->Get()->Stats);
        }
        ComputeActorState = NDqProto::TEvComputeActorState();
        ComputeActorState.SetState(NDqProto::COMPUTE_STATE_EXECUTING);
        ComputeActorState.SetStatusCode(NYql::NDqProto::StatusIds::SUCCESS);
        ComputeActorState.SetTaskId(Task.GetId());
        NYql::TIssues issues;
        FillIssues(issues);

        IssuesToMessage(issues, ComputeActorState.MutableIssues());
        FillStats(ComputeActorState.MutableStats(), /* last */ false);
        for (const auto& [actorId, cookie] : WaitingForStateResponse) {
            auto state = MakeHolder<TEvDqCompute::TEvState>();
            state->Record = ComputeActorState;
            Send(actorId, std::move(state), NActors::IEventHandle::FlagTrackDelivery, cookie);
        }
        WaitingForStateResponse.clear();
    }

    const IDqAsyncOutputBuffer* GetSink(ui64 outputIdx, const TAsyncOutputInfoBase&) const override {
        return TaskRunnerStats.GetSink(outputIdx);
    }

    void DrainOutputChannel(TOutputChannelInfo& outputChannel) override {
        YQL_ENSURE(!outputChannel.Finished || Checkpoints);

        if (outputChannel.PopStarted) {
            return;
        }

        const bool wasFinished = outputChannel.Finished;
        auto channelId = outputChannel.ChannelId;

        const auto& peerState = Channels->GetOutputChannelInFlightState(channelId);

        CA_LOG_T("About to drain channelId: " << channelId
            << ", hasPeer: " << outputChannel.HasPeer
            << ", peerState:(" << peerState.DebugString() << ")"
//            << ", finished: " << outputChannel.Channel->IsFinished());
            );

        const bool shouldSkipData = Channels->ShouldSkipData(outputChannel.ChannelId);
        const bool hasFreeMemory = Channels->HasFreeMemoryInChannel(outputChannel.ChannelId);
        UpdateBlocked(outputChannel, !hasFreeMemory);

        if (!shouldSkipData && !outputChannel.EarlyFinish && !hasFreeMemory) {
            CA_LOG_T("DrainOutputChannel return because No free memory in channel, channel: " << outputChannel.ChannelId);
            ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
            ProcessOutputsState.AllOutputsFinished = !outputChannel.Finished;
            return;
        }

        outputChannel.PopStarted = true;
        ProcessOutputsState.Inflight++;
        Send(TaskRunnerActorId, new NTaskRunnerActor::TEvOutputChannelDataRequest(channelId, wasFinished, peerState.GetFreeMemory()));
    }

    void DrainAsyncOutput(ui64 outputIndex, TAsyncOutputInfoBase& sinkInfo) override {
        if (sinkInfo.Finished && !Checkpoints) {
            return;
        }

        if (sinkInfo.PopStarted) {
            return;
        }

        Y_ABORT_UNLESS(sinkInfo.AsyncOutput);
        Y_ABORT_UNLESS(sinkInfo.Actor);

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
        Send(TaskRunnerActorId, new NTaskRunnerActor::TEvSinkDataRequest(outputIndex, sinkFreeSpaceBeforeSend));
    }

    bool DoHandleChannelsAfterFinishImpl() override {
        Y_ABORT_UNLESS(Checkpoints);
        auto req = GetCheckpointRequest();
        if (!req.Defined()) {
            return true;  // handled channels syncronously
        }
        CA_LOG_D("DoHandleChannelsAfterFinishImpl");
        AskContinueRun(std::move(req), /* checkpointOnly = */ true);
        return false;
    }

    void PeerFinished(ui64 channelId) override {
        // no TaskRunner => no outputChannel.Channel, nothing to Finish
        CA_LOG_I("Peer finished, channel: " << channelId);
        TOutputChannelInfo* outputChannel = OutputChannelsMap.FindPtr(channelId);
        outputChannel->Finished = true;
        outputChannel->EarlyFinish = true;
        YQL_ENSURE(outputChannel, "task: " << Task.GetId() << ", output channelId: " << channelId);

        if (outputChannel->PopStarted) { // There may be another in-flight message here
            return;
        }

        outputChannel->PopStarted = true;
        ProcessOutputsState.Inflight++;
        Send(TaskRunnerActorId, MakeHolder<NTaskRunnerActor::TEvOutputChannelDataRequest>(channelId, /* wasFinished = */ true, 0));  // finish channel
        DoExecute();
    }

    void TakeInputChannelData(TChannelDataOOB&& channelDataOOB, bool ack) override {
        CA_LOG_T("took input");
        NDqProto::TChannelData& channelData = channelDataOOB.Proto;
        TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(channelData.GetChannelId());
        YQL_ENSURE(inputChannel, "task: " << Task.GetId() << ", unknown input channelId: " << channelData.GetChannelId());

        auto finished = channelData.GetFinished();

        TMaybe<TInstant> watermark;
        if (channelData.HasWatermark()) {
            watermark = TInstant::MicroSeconds(channelData.GetWatermark().GetTimestampUs());

            const bool channelWatermarkChanged = WatermarksTracker.NotifyInChannelWatermarkReceived(
                inputChannel->ChannelId,
                *watermark
            );

            if (channelWatermarkChanged) {
                CA_LOG_T("Pause input channel " << channelData.GetChannelId() << " bacause of watermark " << *watermark);
                inputChannel->Pause(*watermark);
            }

            WatermarkTakeInputChannelDataRequests[*watermark]++;
        }

        TDqSerializedBatch batch;
        batch.Proto = std::move(*channelData.MutableData());
        batch.Payload = std::move(channelDataOOB.Payload);

        MetricsReporter.ReportInputChannelWatermark(
            channelData.GetChannelId(),
            batch.RowCount(),
            watermark);

        auto ev = MakeHolder<NTaskRunnerActor::TEvInputChannelData>(
            channelData.GetChannelId(),
            batch.RowCount() ? std::optional{std::move(batch)} : std::nullopt,
            finished,
            channelData.HasCheckpoint()
        );

        Send(TaskRunnerActorId, ev.Release(), 0, Cookie);

        if (channelData.HasCheckpoint()) {
            Y_ABORT_UNLESS(inputChannel->CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
            Y_ABORT_UNLESS(Checkpoints);
            const auto& checkpoint = channelData.GetCheckpoint();
            inputChannel->Pause(checkpoint);
            Checkpoints->RegisterCheckpoint(checkpoint, channelData.GetChannelId());
        }

        TakeInputChannelDataRequests[Cookie++] = TTakeInputChannelData{ack, channelData.GetChannelId(), watermark};
    }

    void PassAway() override {
        if (TaskRunnerActor) {
            TaskRunnerActor->PassAway();
        }
        if (UseCpuQuota() && CpuTimeSpent.MilliSeconds()) {
            if (CpuTime) {
                CpuTime->Add(CpuTimeSpent.MilliSeconds());
            }
            // Send the rest of CPU time that we haven't taken into account
            Send(QuoterServiceActorId,
                new NKikimr::TEvQuota::TEvRequest(
                    NKikimr::TEvQuota::EResourceOperator::And,
                    { NKikimr::TEvQuota::TResourceLeaf(Task.GetRateLimiter(), Task.GetRateLimiterResource(), CpuTimeSpent.MilliSeconds(), true) },
                    TDuration::Max()));
        }
        TBase::PassAway();
    }

    TMaybe<TInstant> GetWatermarkRequest() {
        if (!WatermarksTracker.HasPendingWatermark()) {
            return Nothing();
        }

        const auto pendingWatermark = *WatermarksTracker.GetPendingWatermark();
        if (WatermarkTakeInputChannelDataRequests.contains(pendingWatermark)) {
            // Not all precending to watermark input channels data has been injected
            return Nothing();
        }

        MetricsReporter.ReportInjectedToTaskRunnerWatermark(pendingWatermark);

        return pendingWatermark;
    }

    TMaybe<NDqProto::TCheckpoint> GetCheckpointRequest() {
        if (!CheckpointRequestedFromTaskRunner && Checkpoints && Checkpoints->HasPendingCheckpoint() && !Checkpoints->ComputeActorStateSaved()) {
            CheckpointRequestedFromTaskRunner = true;
            return Checkpoints->GetPendingCheckpoint();
        }
        return Nothing();
    }

    void DoExecuteImpl() override {
        PollAsyncInput();
        if (ProcessSourcesState.Inflight == 0) {
            auto req = GetCheckpointRequest();
            CA_LOG_T("DoExecuteImpl: " << (bool) req);
            AskContinueRun(std::move(req), /* checkpointOnly = */ false);
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

    void OnTaskRunnerCreated(NTaskRunnerActor::TEvTaskRunnerCreateFinished::TPtr& ev) {
        const auto& secureParams = ev->Get()->SecureParams;
        const auto& taskParams = ev->Get()->TaskParams;
        const auto& readRanges = ev->Get()->ReadRanges;
        const auto& typeEnv = ev->Get()->TypeEnv;
        const auto& holderFactory = ev->Get()->HolderFactory;
        if (Stat) {
            Stat->AddCounters2(ev->Get()->Sensors);
        }
        TypeEnv = const_cast<NKikimr::NMiniKQL::TTypeEnvironment*>(&typeEnv);
        for (auto& [inputIndex, transform] : this->InputTransformsMap) {
            std::tie(transform.Input, transform.Buffer) = ev->Get()->InputTransforms.at(inputIndex);
        }
        FillIoMaps(holderFactory, typeEnv, secureParams, taskParams, readRanges, nullptr);

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

        ContinueExecute(EResumeSource::CATaskRunnerCreated);
    }

    bool ReadyToCheckpoint() const override {
        return ReadyToCheckpointFlag;
    }

    void OnRunFinished(NTaskRunnerActor::TEvTaskRunFinished::TPtr& ev) {
        if (Stat) {
            Stat->AddCounters2(ev->Get()->Sensors);
        }
        ContinueRunInflight = false;

        MkqlMemoryLimit = ev->Get()->MkqlMemoryLimit;
        ProfileStats = std::move(ev->Get()->ProfileStats);
        auto status = ev->Get()->RunStatus;

        CA_LOG_T("Resume execution, run status: " << status << " checkpoint: " << (bool) ev->Get()->ProgramState
            << " watermark injected: " << ev->Get()->WatermarkInjectedToOutputs);

        for (const auto& [channelId, freeSpace] : ev->Get()->InputChannelFreeSpace) {
            auto it = InputChannelsMap.find(channelId);
            if (it != InputChannelsMap.end()) {
                it->second.FreeSpace = freeSpace;
            }
        }
        for (const auto& [inputIndex, freeSpace] : ev->Get()->SourcesFreeSpace) {
            auto it = SourcesMap.find(inputIndex);
            if (it != SourcesMap.end()) {
                it->second.FreeSpace = freeSpace;
            }
        }

        if (ev->Get()->WatermarkInjectedToOutputs && !WatermarksTracker.HasOutputChannels()) {
            ResumeInputsByWatermark(*WatermarksTracker.GetPendingWatermark());
            WatermarksTracker.PopPendingWatermark();
        }

        ReadyToCheckpointFlag = (bool) ev->Get()->ProgramState;
        if (ev->Get()->CheckpointRequestedFromTaskRunner) {
            CheckpointRequestedFromTaskRunner = false;
        }
        if (ReadyToCheckpointFlag) {
            Y_ABORT_UNLESS(!ProgramState);
            ProgramState = std::move(ev->Get()->ProgramState);
            Checkpoints->DoCheckpoint();
        }
        ProcessOutputsImpl(status);
        if (status == ERunStatus::Finished) {
            ReportStats(TInstant::Now(), ESendStats::IfPossible);
        }

        if (UseCpuQuota()) {
            CpuTimeSpent += ev->Get()->ComputeTime;
            AskCpuQuota();
            ProcessContinueRun();
        }
    }

    void SaveState(const NDqProto::TCheckpoint& checkpoint, TComputeActorState& state) const override {
        CA_LOG_D("Save state");
        Y_ABORT_UNLESS(ProgramState);
        state.MiniKqlProgram = std::move(*ProgramState);
        ProgramState.Destroy();

        // TODO:(whcrc) maybe save Sources before Program?
        for (auto& [inputIndex, source] : SourcesMap) {
            YQL_ENSURE(source.AsyncInput, "Source[" << inputIndex << "] is not created");
            state.Sources.push_back({});
            TSourceState& sourceState = state.Sources.back();
            source.AsyncInput->SaveState(checkpoint, sourceState);
            sourceState.InputIndex = inputIndex;
        }
    }

    void OnSourceDataAck(NTaskRunnerActor::TEvSourceDataAck::TPtr& ev) {
        auto it = SourcesMap.find(ev->Get()->Index);
        Y_ABORT_UNLESS(it != SourcesMap.end());
        auto& source = it->second;
        source.PushStarted = false;
        source.FreeSpace = ev->Get()->FreeSpaceLeft;
        if (--ProcessSourcesState.Inflight == 0) {
            CA_LOG_T("Send TEvContinueRun on OnAsyncInputPushFinished");
            AskContinueRun(Nothing(), false);
        }
    }

    void OnOutputChannelData(NTaskRunnerActor::TEvOutputChannelData::TPtr& ev) {
        if (Stat) {
            Stat->AddCounters2(ev->Get()->Sensors);
        }
        auto it = OutputChannelsMap.find(ev->Get()->ChannelId);
        Y_ABORT_UNLESS(it != OutputChannelsMap.end());
        TOutputChannelInfo& outputChannel = it->second;
        if (outputChannel.AsyncData) {
            CA_LOG_E("Data was not sent to the output channel in the previous step. Channel: " << outputChannel.ChannelId
            << " Finished: " << outputChannel.Finished
            << " Previous: { DataSize: " << outputChannel.AsyncData->Data.size()
            << ", Watermark: " << outputChannel.AsyncData->Watermark
            << ", Checkpoint: " << outputChannel.AsyncData->Checkpoint
            << ", Finished: " << outputChannel.AsyncData->Finished
            << ", Changed: " << outputChannel.AsyncData->Changed << "}"
            << " Current: { DataSize: " << ev->Get()->Data.size()
            << ", Watermark: " << ev->Get()->Watermark
            << ", Checkpoint: " << ev->Get()->Checkpoint
            << ", Finished: " << ev->Get()->Finished
            << ", Changed: " << ev->Get()->Changed << "} ");
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssue("Data was not sent to the output channel in the previous step"));
            return;
        }
        outputChannel.AsyncData.ConstructInPlace();
        outputChannel.AsyncData->Watermark = std::move(ev->Get()->Watermark);
        outputChannel.AsyncData->Data = std::move(ev->Get()->Data);
        outputChannel.AsyncData->Checkpoint = std::move(ev->Get()->Checkpoint);
        outputChannel.AsyncData->Finished = ev->Get()->Finished;
        outputChannel.AsyncData->Changed = ev->Get()->Changed;

        SendAsyncChannelData(outputChannel);
        CheckRunStatus();
    }

    void SendAsyncChannelData(TOutputChannelInfo& outputChannel) {
        Y_ABORT_UNLESS(outputChannel.AsyncData);

        // If the channel has finished, then the data received after drain is no longer needed
        const bool shouldSkipData = Channels->ShouldSkipData(outputChannel.ChannelId);

        auto& asyncData = *outputChannel.AsyncData;
        outputChannel.Finished = asyncData.Finished || shouldSkipData || outputChannel.EarlyFinish;
        if (outputChannel.Finished) {
            FinishedOutputChannels.insert(outputChannel.ChannelId);
        }

        outputChannel.PopStarted = false;
        ProcessOutputsState.Inflight--;
        ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
        ProcessOutputsState.LastPopReturnedNoData = asyncData.Data.empty();

        if (asyncData.Watermark.Defined()) {
            const auto watermark = TInstant::MicroSeconds(asyncData.Watermark->GetTimestampUs());
            const bool shouldResumeInputs = WatermarksTracker.NotifyOutputChannelWatermarkSent(
                outputChannel.ChannelId,
                watermark
            );

            if (shouldResumeInputs) {
                ResumeInputsByWatermark(watermark);
            }
        }

        if (!shouldSkipData) {
            if (asyncData.Checkpoint.Defined()) {
                ResumeInputsByCheckpoint();
            }

            for (ui32 i = 0; i < asyncData.Data.size(); i++) {
                TDqSerializedBatch& chunk = asyncData.Data[i];
                TChannelDataOOB channelData;
                channelData.Proto.SetChannelId(outputChannel.ChannelId);
                // set finished only for last chunk
                const bool lastChunk = i == asyncData.Data.size() - 1;
                channelData.Proto.SetFinished(asyncData.Finished && lastChunk);
                channelData.Proto.MutableData()->Swap(&chunk.Proto);
                channelData.Payload = std::move(chunk.Payload);
                if (lastChunk && asyncData.Watermark.Defined()) {
                    channelData.Proto.MutableWatermark()->Swap(&*asyncData.Watermark);
                }
                if (lastChunk && asyncData.Checkpoint.Defined()) {
                    channelData.Proto.MutableCheckpoint()->Swap(&*asyncData.Checkpoint);
                }
                Channels->SendChannelData(std::move(channelData), i + 1 == asyncData.Data.size());
            }
            if (asyncData.Data.empty() && asyncData.Changed) {
                TChannelDataOOB channelData;
                channelData.Proto.SetChannelId(outputChannel.ChannelId);
                channelData.Proto.SetFinished(asyncData.Finished);
                if (asyncData.Watermark.Defined()) {
                    channelData.Proto.MutableWatermark()->Swap(&*asyncData.Watermark);
                }
                if (asyncData.Checkpoint.Defined()) {
                    channelData.Proto.MutableCheckpoint()->Swap(&*asyncData.Checkpoint);
                }
                Channels->SendChannelData(std::move(channelData), true);
            }
        }

        ProcessOutputsState.DataWasSent |= asyncData.Changed;

        ProcessOutputsState.AllOutputsFinished =
            FinishedOutputChannels.size() == OutputChannelsMap.size() &&
            FinishedSinks.size() == SinksMap.size();

        outputChannel.AsyncData = Nothing();
    }

    void OnInputChannelDataAck(NTaskRunnerActor::TEvInputChannelDataAck::TPtr& ev) {
        auto it = TakeInputChannelDataRequests.find(ev->Cookie);
        YQL_ENSURE(it != TakeInputChannelDataRequests.end());

        CA_LOG_T("Input data push finished. Cookie: " << ev->Cookie
            << " Watermark: " << it->second.Watermark
            << " Ack: " << it->second.Ack
            << " TakeInputChannelDataRequests: " << TakeInputChannelDataRequests.size()
            << " WatermarkTakeInputChannelDataRequests: " << WatermarkTakeInputChannelDataRequests.size());

        if (it->second.Watermark.Defined()) {
            auto& ct = WatermarkTakeInputChannelDataRequests.at(*it->second.Watermark);
            if (--ct == 0) {
                WatermarkTakeInputChannelDataRequests.erase(*it->second.Watermark);
            }
        }

        TInputChannelInfo* inputChannel = InputChannelsMap.FindPtr(it->second.ChannelId);
        Y_ABORT_UNLESS(inputChannel);
        inputChannel->FreeSpace = ev->Get()->FreeSpace;

        if (it->second.Ack) {
            Channels->SendChannelDataAck(it->second.ChannelId, inputChannel->FreeSpace);
        }

        TakeInputChannelDataRequests.erase(it);

        ResumeExecution(EResumeSource::AsyncPopFinished);
    }

    void SinkSend(ui64 index,
                  NKikimr::NMiniKQL::TUnboxedValueBatch&& batch,
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
        Y_ABORT_UNLESS(it != SinksMap.end());

        TAsyncOutputInfoBase& sinkInfo = it->second;
        sinkInfo.Finished = finished;
        if (finished) {
            FinishedSinks.insert(outputIndex);
        }
        if (checkpoint) {
            CA_LOG_I("Resume inputs");
            ResumeInputsByCheckpoint();
        }

        sinkInfo.PopStarted = false;
        ProcessOutputsState.Inflight--;
        ProcessOutputsState.HasDataToSend |= !sinkInfo.Finished;

        {
            auto guard = BindAllocator();
            NKikimr::NMiniKQL::TUnboxedValueBatch data = std::move(batch);
            sinkInfo.AsyncOutput->SendData(std::move(data), size, std::move(checkpoint), finished);
        }

        Y_ABORT_UNLESS(batch.empty());
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

    const TDqMemoryQuota::TProfileStats* GetMemoryProfileStats() const override {
        return &ProfileStats;
    }

    const NYql::NDq::TTaskRunnerStatsBase* GetTaskRunnerStats() override {
        return TaskRunnerStats.Get();
    }

    const NYql::NDq::TDqMeteringStats* GetMeteringStats() override {
        // TODO: support async CA
        return nullptr;
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
        Y_ABORT_UNLESS(CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
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
        Checkpoints->Receive(*x);
        ev = nullptr;
    }

    void OnInjectCheckpoint(TEvDqCompute::TEvInjectCheckpoint::TPtr& ev) {
        if (TypeEnv) {
            ForwardToCheckpoints(std::move(ev));
        } else {
            Y_ABORT_UNLESS(!DeferredInjectCheckpointEvent);
            DeferredInjectCheckpointEvent = std::move(ev);
        }
    }

    void OnRestoreFromCheckpoint(TEvDqCompute::TEvRestoreFromCheckpoint::TPtr& ev) {
        if (TypeEnv) {
            ForwardToCheckpoints(std::move(ev));
        } else {
            Y_ABORT_UNLESS(!DeferredRestoreFromCheckpointEvent);
            DeferredRestoreFromCheckpointEvent = std::move(ev);
        }
    }

    bool UseCpuQuota() const {
        return QuoterServiceActorId && Task.GetRateLimiter() && Task.GetRateLimiterResource();
    }

    void AskCpuQuota() {
        Y_ABORT_UNLESS(!CpuTimeQuotaAsked);
        if (CpuTimeSpent >= MIN_QUOTED_CPU_TIME) {
            CA_LOG_T("Ask CPU quota: " << CpuTimeSpent.MilliSeconds() << "ms");
            if (CpuTime) {
                CpuTime->Add(CpuTimeSpent.MilliSeconds());
            }
            Send(QuoterServiceActorId,
                new NKikimr::TEvQuota::TEvRequest(
                    NKikimr::TEvQuota::EResourceOperator::And,
                    { NKikimr::TEvQuota::TResourceLeaf(Task.GetRateLimiter(), Task.GetRateLimiterResource(), CpuTimeSpent.MilliSeconds(), true) },
                    TDuration::Max()));
            CpuTimeQuotaAsked = TInstant::Now();
            CpuTimeSpent = TDuration::Zero();
        }
    }

    void AskContinueRun(TMaybe<NDqProto::TCheckpoint>&& checkpointRequest, bool checkpointOnly) {
        Y_ABORT_UNLESS(!checkpointOnly || !ContinueRunEvent);
        if (!ContinueRunEvent) {
            ContinueRunStartWaitTime = TInstant::Now();
            ContinueRunEvent = std::make_unique<NTaskRunnerActor::TEvContinueRun>();
            ContinueRunEvent->SinkIds = GetIds(SinksMap);
            ContinueRunEvent->InputTransformIds = GetIds(InputTransformsMap);
        }
        ContinueRunEvent->CheckpointOnly = checkpointOnly;
        if (TMaybe<TInstant> watermarkRequest = GetWatermarkRequest()) {
            if (!ContinueRunEvent->WatermarkRequest) {
                ContinueRunEvent->WatermarkRequest.ConstructInPlace();
                ContinueRunEvent->WatermarkRequest->Watermark = *watermarkRequest;

                ContinueRunEvent->WatermarkRequest->ChannelIds.reserve(OutputChannelsMap.size());
                for (const auto& [channelId, info] : OutputChannelsMap) {
                    if (info.WatermarksMode != NDqProto::EWatermarksMode::WATERMARKS_MODE_DISABLED) {
                        ContinueRunEvent->WatermarkRequest->ChannelIds.emplace_back(channelId);
                    }
                }
            } else {
                ContinueRunEvent->WatermarkRequest->Watermark = Max(ContinueRunEvent->WatermarkRequest->Watermark, *watermarkRequest);
            }
        }
        if (checkpointRequest) {
            if (!ContinueRunEvent->CheckpointRequest) {
                ContinueRunEvent->CheckpointRequest.ConstructInPlace(GetIds(OutputChannelsMap), GetIds(SinksMap), *checkpointRequest);
            } else {
                Y_ABORT_UNLESS(ContinueRunEvent->CheckpointRequest->Checkpoint.GetGeneration() == checkpointRequest->GetGeneration());
                Y_ABORT_UNLESS(ContinueRunEvent->CheckpointRequest->Checkpoint.GetId() == checkpointRequest->GetId());
            }
        }

        if (!UseCpuQuota()) {
            Send(TaskRunnerActorId, ContinueRunEvent.release());
            return;
        }

        ProcessContinueRun();
    }

    void ProcessContinueRun() {
        if (ContinueRunEvent && !CpuTimeQuotaAsked && !ContinueRunInflight) {
            Send(TaskRunnerActorId, ContinueRunEvent.release());
            if (CpuTimeQuotaWaitDelay) {
                const TDuration quotaWaitDelay = TInstant::Now() - ContinueRunStartWaitTime;
                CpuTimeQuotaWaitDelay->Collect(quotaWaitDelay.MilliSeconds());
            }
            ContinueRunInflight = true;
        }
    }

    void OnCpuQuotaGiven(NKikimr::TEvQuota::TEvClearance::TPtr& ev) {
        const TDuration delay = TInstant::Now() - CpuTimeQuotaAsked;
        CpuTimeQuotaAsked = TInstant::Zero();
        CA_LOG_T("CPU quota delay: " << delay.MilliSeconds() << "ms");
        if (CpuTimeGetQuotaLatency) {
            CpuTimeGetQuotaLatency->Collect(delay.MilliSeconds());
        }

        if (ev->Get()->Result != NKikimr::TEvQuota::TEvClearance::EResult::Success) {
            InternalError(NYql::NDqProto::StatusIds::INTERNAL_ERROR, TIssue("Error getting CPU quota"));
            return;
        }

        ProcessContinueRun();
    }

    void CheckRunStatus() override {
        if (!TakeInputChannelDataRequests.empty()) {
            CA_LOG_T("AsyncCheckRunStatus: TakeInputChannelDataRequests: " << TakeInputChannelDataRequests.size());
            return;
        }
        TBase::CheckRunStatus();
    }

private:
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
        TMaybe<TInstant> Watermark;
    };
    THashMap<ui64, TTakeInputChannelData> TakeInputChannelDataRequests;
    // Watermark should be injected to task runner only after all precending data is injected
    // This hash map will help to track the right moment
    THashMap<TInstant, ui32> WatermarkTakeInputChannelDataRequests;
    ui64 Cookie = 0;
    NDq::TDqTaskRunnerStatsView TaskRunnerStats;
    bool ReadyToCheckpointFlag;
    TVector<std::pair<NActors::TActorId, ui64>> WaitingForStateResponse;
    bool SentStatsRequest;
    mutable THolder<TMiniKqlProgramState> ProgramState;
    ui64 MkqlMemoryLimit = 0;
    TDqMemoryQuota::TProfileStats ProfileStats;
    bool CheckpointRequestedFromTaskRunner = false;

    // Cpu quota
    TActorId QuoterServiceActorId;
    TInstant CpuTimeQuotaAsked;
    std::unique_ptr<NTaskRunnerActor::TEvContinueRun> ContinueRunEvent;
    TInstant ContinueRunStartWaitTime;
    bool ContinueRunInflight = false;
    NMonitoring::THistogramPtr CpuTimeGetQuotaLatency;
    NMonitoring::THistogramPtr CpuTimeQuotaWaitDelay;
    NMonitoring::TDynamicCounters::TCounterPtr CpuTime;
    NDqProto::TEvComputeActorState ComputeActorState;
};


IActor* CreateDqAsyncComputeActor(const TActorId& executerId, const TTxId& txId, NYql::NDqProto::TDqTask* task,
    IDqAsyncIoFactory::TPtr asyncIoFactory, const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TComputeRuntimeSettings& settings, const TComputeMemoryLimits& memoryLimits,
    const NTaskRunnerActor::ITaskRunnerActorFactory::TPtr& taskRunnerActorFactory,
    ::NMonitoring::TDynamicCounterPtr taskCounters,
    const TActorId& quoterServiceActorId,
    bool ownCounters)
{
    return new TDqAsyncComputeActor(executerId, txId, task, std::move(asyncIoFactory),
        functionRegistry, settings, memoryLimits, taskRunnerActorFactory, taskCounters, quoterServiceActorId, ownCounters);
}

} // namespace NDq
} // namespace NYql
