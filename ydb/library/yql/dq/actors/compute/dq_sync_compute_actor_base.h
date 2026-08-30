#pragma once

#include "dq_compute_actor_impl.h"
#include "dq_compute_actor_async_input_helper.h"
#include <ydb/library/yql/dq/actors/spilling/spiller_factory.h>
#include <ydb/library/yql/dq/runtime/dq_input_channel.h>

namespace NYql::NDq {

template<typename TDerived>
class TDqSyncComputeActorBase: public TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperSync>, public IDqInputChannelCallbacks {
    using TBase = TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperSync>;
public:
    using TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperSync>::TDqComputeActorBase;
    static constexpr bool HasAsyncTaskRunner = false;

    TComputeActorAsyncInputHelperSync CreateInputHelper(const TString& logPrefix,
        ui64 index,
        NDqProto::EWatermarksMode watermarksMode,
        TDuration watermarksIdleTimeout
    )
    {
        return TComputeActorAsyncInputHelperSync(logPrefix, index, watermarksMode, watermarksIdleTimeout);
    }

    const IDqAsyncInputBuffer* GetInputTransform(ui64, const TComputeActorAsyncInputHelperSync& inputTransformInfo) const
    {
        return inputTransformInfo.Buffer.Get();
    }
protected:

    void DoExecuteImpl() override{
        auto sourcesState = static_cast<TDerived*>(this)->GetSourcesState();

        auto lastPollResult = TBase::PollAsyncInput();
        ERunStatus status = TaskRunner->Run();

        CA_LOG_T("Resume execution, run status: " << status);

        if (status != ERunStatus::Finished) {
             static_cast<TDerived*>(this)->PollSources(std::move(sourcesState));
        }

        if ((status == ERunStatus::PendingInput || status == ERunStatus::Finished) && this->Checkpoints && this->Checkpoints->HasPendingCheckpoint() && !this->Checkpoints->ComputeActorStateSaved() && ReadyToCheckpoint()) {
            this->Checkpoints->DoCheckpoint();
        }

        TBase::ProcessOutputsImpl(status);

        if (lastPollResult && (*lastPollResult != EResumeSource::CAPollAsyncNoSpace || status == ERunStatus::PendingInput)) {
            // If only reason for continuing was lack on space on all sources,
            // only continue execution when input was consumed;
            // otherwise this may result in busy-poll
            TBase::ContinueExecute(*lastPollResult);
        }
    }

    void DoTerminateImpl() override {
        // we want to log debug output info only for long running (OLAP) tasks
        if (TaskRunner && TBase::State == NDqProto::COMPUTE_STATE_FAILURE && TBase::RuntimeSettings.CollectFull()) {
            auto& stats = *TaskRunner->GetStats();
            if (stats.StartTs && TInstant::Now() - stats.StartTs > TDuration::Seconds(60)) {
                auto taskRunnerDebugString = TaskRunner->GetOutputDebugString();
                if (taskRunnerDebugString) {
                    CA_LOG_E("TaskRunner->Output Debug String: " << taskRunnerDebugString);
                }
            }
        }

        TaskRunner.Reset();
        TBase::DoTerminateImpl();
    }

    void InvalidateMeminfo() override {
        if (TaskRunner) {
            TaskRunner->GetAllocator().InvalidateMemInfo();
            TaskRunner->GetAllocator().DisableStrictAllocationCheck();
        }
    }

    bool DoHandleChannelsAfterFinishImpl() override final {
        Y_ABORT_UNLESS(this->Checkpoints);

        if (this->Checkpoints->HasPendingCheckpoint() && !this->Checkpoints->ComputeActorStateSaved() && ReadyToCheckpoint()) {
            this->Checkpoints->DoCheckpoint();
        }

        // Send checkpoints to output channels.
        CA_LOG_D("Drain outputs after finish");
        TBase::ProcessOutputsImpl(ERunStatus::Finished);
        return true;  // returns true, when channels were handled synchronously
    }

    void ExtraMonitoringInfo(TStringStream& str, const TCgiParameters&) override {
        if (TaskRunner) {
            str << Endl << "TaskRunner" << Endl
                << "  LastFetchTime: " << TaskRunner->LastFetchTime << Endl
                << "  LastFetchStatus: " << TaskRunner->LastFetchStatus << Endl
                << "  OutputDebugString: " << TaskRunner->GetOutputDebugString() << Endl;
        }
    }

protected: //TDqComputeActorChannels::ICalbacks
    i64 GetInputChannelFreeSpace(ui64 channelId) const override final {
        const auto* inputChannel = this->InputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(inputChannel, "task: " << this->Task.GetId() << ", unknown input channelId: " << channelId);

        return inputChannel->Channel->GetFreeSpace();
    }

    // Called only on v1 DQ channels
    void TakeInputChannelData(TChannelDataOOB&& channelData, bool ack) override final {
        const auto channelId = channelData.Proto.GetChannelId();
        typename TBase::TInputChannelInfo* const inputChannel = this->InputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(inputChannel, "task: " << this->Task.GetId() << ", unknown input channelId: " << channelId);

        const auto channel = inputChannel->Channel;

        if (const auto chunkCount = channelData.ChunkCount()) {
            TDqSerializedBatch batch;
            batch.Proto = std::move(*channelData.Proto.MutableData());
            batch.Payload = std::move(channelData.Payload);
            auto guard = TBase::BindAllocator();
            channel->Push(std::move(batch));
            CA_LOG_T("Got data batch from input channel #" << channelId << " with " << chunkCount << " chunks");
        }

        if (channelData.Proto.HasWatermark()) {
            Y_ABORT_UNLESS(inputChannel->WatermarksMode != NDqProto::WATERMARKS_MODE_DISABLED);
            const auto& watermarkRequest = channelData.Proto.GetWatermark();
            const TInstant watermark = TInstant::MicroSeconds(watermarkRequest.GetTimestampUs());
            channel->Push(watermark);
            CA_LOG_T("Got watermark from input channel #" << channelId << ": " << watermark);
        }

        if (channelData.Proto.HasCheckpoint()) {
            Y_ABORT_UNLESS(inputChannel->CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
            Y_ABORT_UNLESS(this->Checkpoints);
            const auto& checkpoint = channelData.Proto.GetCheckpoint();
            auto guard = TBase::BindAllocator();
            inputChannel->Pause(checkpoint);
            this->Checkpoints->RegisterCheckpoint(checkpoint, channelData.Proto.GetChannelId());
            CA_LOG_T("Got checkpoint from input channel #" << channelId << ": " << checkpoint.GetGeneration() << "." << checkpoint.GetId());
        }

        if (channelData.Proto.GetFinished()) {
            channel->Finish();
            CA_LOG_T("Got finished marker from input channel #" << channelId);
        }

        if (ack) {
            const auto freeSpace = channel->GetFreeSpace();
            this->Channels->SendChannelDataAck(channel->GetChannelId(), channel->GetFreeSpace());
            CA_LOG_T("Got ack from input channel #" << channelId << ", send free space: " << freeSpace);
        }

        TBase::ContinueExecute(EResumeSource::CATakeInput);
    }

    void PeerFinished(ui64 channelId) override final {
        auto* outputChannel = this->OutputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(outputChannel, "task: " << this->Task.GetId() << ", output channelId: " << channelId);

        outputChannel->Finished = true;
        outputChannel->Channel->Finish();

        CA_LOG_D("task: " << this->Task.GetId() << ", output channelId: " << channelId << " finished prematurely, "
            << " about to clear buffer");

        {
            auto guard = TBase::BindAllocator();
            ui32 dropRows = outputChannel->Channel->Drop();

            CA_LOG_I("task: " << this->Task.GetId() << ", output channelId: " << channelId << " finished prematurely, "
                << "drop " << dropRows << " rows");
        }

        TBase::DoExecute();
    }

protected: //TDqComputeActorCheckpoints::ICallbacks
    bool ReadyToCheckpoint() const override final {
        for (const auto& [_, sourceInfo] : this->SourcesMap) {
            if (!sourceInfo.Buffer->Empty()) {
                return false;
            }
        }

        for (const auto& [_, channelInfo] : this->InputChannelsMap) {
            if (channelInfo.CheckpointingMode == NDqProto::CHECKPOINTING_MODE_DISABLED) {
                continue;
            }

            // A finished channel may no longer become paused, but its buffer still needs to be drained.
            if (!channelInfo.IsPaused() && !channelInfo.Channel->IsFinished()) {
                return false;
            }

            if (!channelInfo.Channel->Empty()) {
                return false;
            }
        }

        for (const auto& [_, transformInfo] : this->InputTransformsMap) {
            const auto buffer = transformInfo.Buffer;
            if (!buffer->Empty()) {
                return false;
            }

            if (buffer->IsPending()) {
                return false;
            }
        }

        return true;
    }

    void InjectBarrierToOutputs(const NDqProto::TCheckpoint& checkpoint) override final {
        Y_ABORT_UNLESS(this->CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
        CA_LOG_D("Inject barrier to outputs, output channels #" << this->OutputChannelsMap.size() << ", sinks #" << this->SinksMap.size() << ", output transforms #" << this->OutputTransformsMap.size());

        for (const auto& [id, channelInfo] : this->OutputChannelsMap) {
            if (!channelInfo.IsTransformOutput) {
                channelInfo.Channel->Push(NDqProto::TCheckpoint(checkpoint));
            }
        }

        for (const auto& [outputIndex, sink] : this->SinksMap) {
            sink.Buffer->Push(NDqProto::TCheckpoint(checkpoint));
        }

        for (const auto& [outputIndex, transform] : this->OutputTransformsMap) {
            transform.Buffer->Push(NDqProto::TCheckpoint(checkpoint));
        }
    }

    void SaveState(const NDqProto::TCheckpoint& checkpoint, TComputeActorState& state) const override final{
        CA_LOG_D("Save state");
        TMiniKqlProgramState& mkqlProgramState = state.MiniKqlProgram.ConstructInPlace();
        mkqlProgramState.RuntimeVersion = NDqProto::RUNTIME_VERSION_YQL_1_0;
        TStateData& data = mkqlProgramState.Data;
        data.Version = TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion;
        data.Blob = TaskRunner->Save();

        for (auto& [inputIndex, source] : this->SourcesMap) {
            YQL_ENSURE(source.AsyncInput, "Source[" << inputIndex << "] is not created");
            state.Sources.push_back({});
            TSourceState& sourceState = state.Sources.back();
            source.AsyncInput->SaveState(checkpoint, sourceState);
            sourceState.InputIndex = inputIndex;
        }
    }

    TString GetTaskDebugState() const final {
        auto diagnostics = TStringBuilder() << TBase::GetTaskDebugState();

        ui64 emptySources = 0;
        ui64 bytesInSources = 0;
        i64 sourcesFreeSpace = 0;
        for (const auto& [_, sourceInfo] : this->SourcesMap) {
            if (sourceInfo.Buffer) {
                emptySources += sourceInfo.Buffer->Empty();
                bytesInSources += sourceInfo.Buffer->GetStoredBytes();
                sourcesFreeSpace += sourceInfo.Buffer->GetFreeSpace();
            }
        }

        ui64 checkpointedInputChannels = 0;
        ui64 emptyInputChannels = 0;
        ui64 finishedOrPausedInputChannels = 0;
        ui64 bytesInInputChannels = 0;
        i64 inputChannelsFreeSpace = 0;
        for (const auto& [_, channelInfo] : this->InputChannelsMap) {
            if (channelInfo.CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED) {
                checkpointedInputChannels++;

                if (channelInfo.Channel) {
                    emptyInputChannels += channelInfo.Channel->Empty();
                    finishedOrPausedInputChannels += channelInfo.IsPaused() || channelInfo.Channel->IsFinished();
                    bytesInInputChannels += channelInfo.Channel->GetStoredBytes();
                    inputChannelsFreeSpace += channelInfo.Channel->GetFreeSpace();
                }
            }
        }

        ui64 emptyInputTransforms = 0;
        ui64 pendingInputTransforms = 0;
        ui64 bytesInInputTransforms = 0;
        i64 inputTransformsFreeSpace = 0;
        for (const auto& [_, transformInfo] : this->InputTransformsMap) {
            if (const auto buffer = transformInfo.Buffer) {
                emptyInputTransforms += buffer->Empty();
                pendingInputTransforms += buffer->IsPending();
                bytesInInputTransforms += buffer->GetStoredBytes();
                inputTransformsFreeSpace += buffer->GetFreeSpace();
            }
        }

        diagnostics << "Inputs state. ["
            << "Channels paused or finished: " << finishedOrPausedInputChannels << " / " << checkpointedInputChannels
            << ". Channels empty: " << emptyInputChannels << " / " << checkpointedInputChannels << " (stored bytes: " << bytesInInputChannels << ", fs: " << inputChannelsFreeSpace << ")"
            << ". Sources empty: " << emptySources << " / " << this->SourcesMap.size() << " (stored bytes: " << bytesInSources << ", fs: " << sourcesFreeSpace << ")"
            << ". Transforms empty: " << emptyInputTransforms << " / " << this->InputTransformsMap.size() << " (stored bytes: " << bytesInInputTransforms << ", fs: " << inputTransformsFreeSpace << ")"
            << ". Transforms pending: " << pendingInputTransforms << " / " << this->InputTransformsMap.size()
            << "] ";

        const auto getOutputStats = [](const auto& objects, const auto outputInfoExtractor) -> TString {
            ui64 noLimit = 0;
            ui64 softLimit = 0;
            ui64 hardLimit = 0;
            for (const auto& [_, info] : objects) {
                if (const auto& output = outputInfoExtractor(info)) {
                    switch (output->GetFillLevel()) {
                        case NoLimit:
                            noLimit++;
                            break;
                        case SoftLimit:
                            softLimit++;
                            break;
                        case HardLimit:
                            hardLimit++;
                            break;
                    }
                }
            }

            return TStringBuilder() << " no + soft + hard limit: {" << noLimit << " + " << softLimit << " + " << hardLimit << "} / " << objects.size();
        };

        diagnostics
            << "Outputs state. [Channels ready: " << this->ProcessOutputsState.ChannelsReady
            << ". Has data to send: " << this->ProcessOutputsState.HasDataToSend
            << ". Data was sent: " << this->ProcessOutputsState.DataWasSent
            << ". All outputs finished: " << this->ProcessOutputsState.AllOutputsFinished
            << ". Channels: " << getOutputStats(this->OutputChannelsMap, [](const auto& info) { return info.Channel; })
            << ". Sinks: " << getOutputStats(this->SinksMap, [](const auto& info) { return info.Buffer; })
            << ". Transforms: " << getOutputStats(this->OutputTransformsMap, [](const auto& info) { return info.OutputBuffer; })
            << "] ";

        return diagnostics;
    }

protected:
    void DoLoadRunnerState(TString&& blob) override {
        TMaybe<TString> error = Nothing();
        try {
            TaskRunner->Load(blob);
        } catch (const std::exception& e) {
            error = e.what();
        }
        this->Checkpoints->AfterStateLoading(error);
    }

    void SetTaskRunner(const TIntrusivePtr<IDqTaskRunner>& taskRunner) {
        TaskRunner = taskRunner;
    }

    void PrepareTaskRunner(const IDqTaskRunnerExecutionContext& execCtx) {
        YQL_ENSURE(TaskRunner);

        auto guard = TBase::BindAllocator();
        auto* alloc = guard.GetMutex();
        alloc->SetLimit(this->MemoryQuota->GetMkqlMemoryLimit());

        this->MemoryQuota->TrySetIncreaseMemoryLimitCallback(alloc);

        TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = this->MemoryLimits.ChannelBufferSize;
        limits.OutputChunkMaxSize = this->MemoryLimits.OutputChunkMaxSize;
        limits.ChunkSizeLimit = this->MemoryLimits.ChunkSizeLimit;
        limits.ArrayBufferMinFillPercentage = this->MemoryLimits.ArrayBufferMinFillPercentage;
        limits.BufferPageAllocSize = this->MemoryLimits.BufferPageAllocSize;
        limits.ChannelQuotaManager = TBase::MemoryLimits.ChannelQuotaManager;

        if (!limits.OutputChunkMaxSize) {
            limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;
        }

        if (this->Task.GetEnableSpilling()) {
            TaskRunner->SetSpillerFactory(std::make_shared<TDqSpillerFactory>(execCtx.GetTxId(), NActors::TActivationContext::ActorSystem(), execCtx.GetWakeupCallback(), execCtx.GetErrorCallback()));
        }

        this->WatermarksTracker.SetNotifyHandler([this]() {
            // This code is called from TaskRunner (either directly or from input transform/helper code), which is owned by sync CA, so `*this` must be alive at that point
            this->ScheduleIdlenessCheck();
        });
        this->WatermarkGeneratorTracker.SetNotifyHandler([this](TInstant checkTime) {
            this->ScheduleSourceIdlenessCheck(checkTime);
        });

        TaskRunner->Prepare(this->Task, limits, execCtx, &this->WatermarksTracker, &this->WatermarkGeneratorTracker);

        for (auto& [channelId, channel] : this->InputChannelsMap) {
            channel.Channel = TaskRunner->GetInputChannel(channelId);
            channel.Channel->SetCallback(this);
        }

        for (auto& [inputIndex, source] : this->SourcesMap) {
            source.Buffer = TaskRunner->GetSource(inputIndex);
            Y_ABORT_UNLESS(source.Buffer);
        }

        for (auto& [inputIndex, transform] : this->InputTransformsMap) {
            std::tie(transform.Input, transform.Buffer) = *TaskRunner->GetInputTransform(inputIndex);
        }

        for (auto& [channelId, channel] : this->OutputChannelsMap) {
            channel.Channel = TaskRunner->GetOutputChannel(channelId);
            if (this->Task.GetDqChannelVersion() >= 2u && channel.HasPeer) {
                channel.Channel->Bind(this->SelfId(), channel.PeerId);
            }
        }

        for (auto& [outputIndex, transform] : this->OutputTransformsMap) {
            std::tie(transform.Buffer, transform.OutputBuffer) = TaskRunner->GetOutputTransform(outputIndex);
        }

        for (auto& [outputIndex, sink] : this->SinksMap) {
            sink.Buffer = TaskRunner->GetSink(outputIndex);
        }

        TBase::FillIoMaps(
            TaskRunner->GetHolderFactory(),
            TaskRunner->GetTypeEnv(),
            TaskRunner->GetSecureParams(),
            TaskRunner->GetTaskParams(),
            TaskRunner->GetReadRanges(),
            TaskRunner->GetRandomProvider()
        );
    }

    const NYql::NDq::TDqTaskRunnerStats* GetTaskRunnerStats() override {
        return TaskRunner ? TaskRunner->GetStats() : nullptr;
    }

    const NYql::NDq::TDqMeteringStats* GetMeteringStats() override {
        return TaskRunner ? TaskRunner->GetMeteringStats() : nullptr;
    }

    const IDqAsyncOutputBuffer* GetSink(ui64, const typename TBase::TAsyncOutputInfoBase& sinkInfo) const override final {
        return sinkInfo.Buffer.Get();
    }

    TDqComputeActorWatermarks* GetInputTransformWatermarksTracker(ui64 inputId) override {
        return TaskRunner ? TaskRunner->GetInputTransformWatermarksTracker(inputId): nullptr;
    }

protected:
    // Methods that are called via static_cast<TDerived*>(this) and may be overriden by a derived class
    void* GetSourcesState() const {
        return nullptr;
    }

    void PollSources(void* /* state */) {
    }

    virtual const TDqMemoryQuota::TProfileStats* GetMemoryProfileStats() const final {
        Y_ABORT_UNLESS(this->MemoryQuota);
        return this->MemoryQuota->GetProfileStats();
    }

    virtual void DrainOutputChannel(typename TBase::TOutputChannelInfo& outputChannel) final {
        YQL_ENSURE(!outputChannel.Finished || this->Checkpoints);

        const bool wasFinished = outputChannel.Finished;
        const auto channelId = outputChannel.Channel->GetChannelId();
        const bool hasFreeMemoryBeforeDrain = this->Channels->HasFreeMemoryInChannel(channelId);

        CA_LOG_T("About to drain channelId: " << channelId
            << ", Checkpointing mode: " << NDqProto::ECheckpointingMode_Name(outputChannel.CheckpointingMode)
            << ", hasPeer: " << outputChannel.HasPeer
            << ", hasFreeMemory: " << hasFreeMemoryBeforeDrain
            << ", finished: " << outputChannel.Channel->IsFinished());

        this->ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
        this->ProcessOutputsState.AllOutputsFinished &= outputChannel.Finished;

        TBase::UpdateBlocked(outputChannel, !hasFreeMemoryBeforeDrain);

        ui32 sentChunks = 0;
        while ((!outputChannel.Finished || this->Checkpoints) &&
            this->Channels->HasFreeMemoryInChannel(channelId))
        {
            const static ui32 drainPackSize = 16;
            std::vector<typename TBase::TOutputChannelInfo::TDrainedChannelMessage> channelData = outputChannel.DrainChannel(drainPackSize);
            ui32 idx = 0;
            for (auto&& i : channelData) {
                this->Channels->SendChannelData(i.BuildChannelData(channelId), ++idx == channelData.size());
                ++sentChunks;
            }

            if (drainPackSize != channelData.size()) {
                if (!outputChannel.Finished) {
                    CA_LOG_T("Output channelId: " << channelId << ", nothing to send and is not finished (sent #" << sentChunks << " chunks)");
                } else if (sentChunks) {
                    CA_LOG_T("Output channelId: " << channelId << " drained after finish, sent #" << sentChunks << " chunks");
                } else {
                    CA_LOG_T("Output channelId: " << channelId << " drained after finish, nothing to send");
                }
                break;
            }
        }

        this->ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
        this->ProcessOutputsState.AllOutputsFinished &= outputChannel.Finished;
        this->ProcessOutputsState.DataWasSent |= (!wasFinished && outputChannel.Finished) || sentChunks;
    }

    void DrainAsyncOutput(ui64 outputIndex, typename TBase::TAsyncOutputInfoBase& outputInfo) override final {
        this->ProcessOutputsState.AllOutputsFinished &= outputInfo.Finished;
        if (outputInfo.Finished && !this->Checkpoints) {
            return;
        }

        Y_ABORT_UNLESS(outputInfo.Buffer);
        Y_ABORT_UNLESS(outputInfo.AsyncOutput);
        Y_ABORT_UNLESS(outputInfo.Actor);

        const ui32 allowedOvercommit = TBase::AllowedChannelsOvercommit();
        const i64 sinkFreeSpaceBeforeSend = outputInfo.AsyncOutput->GetFreeSpace();

        i64 toSend = sinkFreeSpaceBeforeSend + allowedOvercommit;
        CA_LOG_D("About to drain async output " << outputIndex
            << ". FreeSpace: " << sinkFreeSpaceBeforeSend
            << ", allowedOvercommit: " << allowedOvercommit
            << ", toSend: " << toSend
            << ", finished: " << outputInfo.Buffer->IsFinished());

        i64 sent = 0;
        while (toSend > 0 && (!outputInfo.Finished || this->Checkpoints)) {
            const ui32 sentChunk = TBase::SendDataChunkToAsyncOutput(outputIndex, outputInfo, toSend);
            if (sentChunk == 0) {
                break;
            }
            sent += sentChunk;
            toSend = outputInfo.AsyncOutput->GetFreeSpace() + allowedOvercommit;
        }

        CA_LOG_D("Drain async output " << outputIndex
            << ". Free space decreased: " << (sinkFreeSpaceBeforeSend - outputInfo.AsyncOutput->GetFreeSpace())
            << ", sent data from buffer: " << sent);

        this->ProcessOutputsState.HasDataToSend |= !outputInfo.Finished;
        this->ProcessOutputsState.DataWasSent |= outputInfo.Finished || sent;
    }

    // Called only on v2 DQ channels
    void TakeCheckpoint(const NDqProto::TCheckpoint& checkpoint, ui64 channelId) override {
        CA_LOG_T("Take checkpoint from channelId: " << channelId << ", checkpoint: " << checkpoint.ShortDebugString());
        auto* inputChannel = this->InputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(inputChannel, "task: " << this->Task.GetId() << ", unknown input channelId: " << channelId);
        inputChannel->Pause(checkpoint);
        this->Checkpoints->RegisterCheckpoint(checkpoint, channelId);
    }

protected:
    TIntrusivePtr<IDqTaskRunner> TaskRunner;

};

} //namespace NYql::NDq
