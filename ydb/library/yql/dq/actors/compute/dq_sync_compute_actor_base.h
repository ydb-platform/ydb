#include "dq_compute_actor_impl.h"
#include "dq_compute_actor_async_input_helper.h"

namespace NYql::NDq {

template<typename TDerived>
class TDqSyncComputeActorBase: public TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperSync> {
    using TBase = TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperSync>;
public:
    using TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperSync>::TDqComputeActorBase;
    static constexpr bool HasAsyncTaskRunner = false;

    TComputeActorAsyncInputHelperSync CreateInputHelper(const TString& logPrefix,
        ui64 index,
        NDqProto::EWatermarksMode watermarksMode
    )
    {
        return TComputeActorAsyncInputHelperSync(logPrefix, index, watermarksMode);
    }

    const IDqAsyncInputBuffer* GetInputTransform(ui64, const TComputeActorAsyncInputHelperSync& inputTransformInfo) const
    {
        return inputTransformInfo.Buffer.Get();
    }
protected:

    void DoExecuteImpl() override{
        auto sourcesState = static_cast<TDerived*>(this)->GetSourcesState();

        TBase::PollAsyncInput();
        ERunStatus status = TaskRunner->Run();

        CA_LOG_T("Resume execution, run status: " << status);

        if (status != ERunStatus::Finished) {
             static_cast<TDerived*>(this)->PollSources(std::move(sourcesState));
        }

        if ((status == ERunStatus::PendingInput || status == ERunStatus::Finished) && this->Checkpoints && this->Checkpoints->HasPendingCheckpoint() && !this->Checkpoints->ComputeActorStateSaved() && ReadyToCheckpoint()) {
            this->Checkpoints->DoCheckpoint();
        }

        TBase::ProcessOutputsImpl(status);
    }

    void DoTerminateImpl() override {
        TaskRunner.Reset();
    }

    void InvalidateMeminfo() override {
        if (TaskRunner) {
            TaskRunner->GetAllocator().InvalidateMemInfo();
            TaskRunner->GetAllocator().DisableStrictAllocationCheck();
        }
    }

    bool DoHandleChannelsAfterFinishImpl() override final{ 
        Y_ABORT_UNLESS(this->Checkpoints);

        if (this->Checkpoints->HasPendingCheckpoint() && !this->Checkpoints->ComputeActorStateSaved() && ReadyToCheckpoint()) {
            this->Checkpoints->DoCheckpoint();
        }

        // Send checkpoints to output channels.
        TBase::ProcessOutputsImpl(ERunStatus::Finished);
        return true;  // returns true, when channels were handled synchronously
    }

protected: //TDqComputeActorChannels::ICalbacks
    i64 GetInputChannelFreeSpace(ui64 channelId) const override final {
        const auto* inputChannel = this->InputChannelsMap.FindPtr(channelId);
        YQL_ENSURE(inputChannel, "task: " << this->Task.GetId() << ", unknown input channelId: " << channelId);

        return inputChannel->Channel->GetFreeSpace();
    }

    void TakeInputChannelData(TChannelDataOOB&& channelData, bool ack) override final {
        auto* inputChannel = this->InputChannelsMap.FindPtr(channelData.Proto.GetChannelId());
        YQL_ENSURE(inputChannel, "task: " << this->Task.GetId() << ", unknown input channelId: " << channelData.Proto.GetChannelId());

        auto channel = inputChannel->Channel;

        if (channelData.RowCount()) {
            TDqSerializedBatch batch;
            batch.Proto = std::move(*channelData.Proto.MutableData());
            batch.Payload = std::move(channelData.Payload);
            auto guard = TBase::BindAllocator();
            channel->Push(std::move(batch));
        }

        if (channelData.Proto.HasCheckpoint()) {
            Y_ABORT_UNLESS(inputChannel->CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
            Y_ABORT_UNLESS(this->Checkpoints);
            const auto& checkpoint = channelData.Proto.GetCheckpoint();
            inputChannel->Pause(checkpoint);
            this->Checkpoints->RegisterCheckpoint(checkpoint, channelData.Proto.GetChannelId());
        }

        if (channelData.Proto.GetFinished()) {
            channel->Finish();
        }

        if (ack) {
            this->Channels->SendChannelDataAck(channel->GetChannelId(), channel->GetFreeSpace());
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
        for (auto& [id, channelInfo] : this->InputChannelsMap) {
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

    void InjectBarrierToOutputs(const NDqProto::TCheckpoint& checkpoint) override final {
        Y_ABORT_UNLESS(this->CheckpointingMode != NDqProto::CHECKPOINTING_MODE_DISABLED);
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

        if (!limits.OutputChunkMaxSize) {
            limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;
	}

        TaskRunner->Prepare(this->Task, limits, execCtx);

        for (auto& [channelId, channel] : this->InputChannelsMap) {
            channel.Channel = TaskRunner->GetInputChannel(channelId);
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

    const NYql::NDq::TTaskRunnerStatsBase* GetTaskRunnerStats() override {
        return TaskRunner ? TaskRunner->GetStats() : nullptr;
    }

    const NYql::NDq::TDqMeteringStats* GetMeteringStats() override {
        return TaskRunner ? TaskRunner->GetMeteringStats() : nullptr;
    }

    const IDqAsyncOutputBuffer* GetSink(ui64, const typename TBase::TAsyncOutputInfoBase& sinkInfo) const override final {
        return sinkInfo.Buffer.Get();
    }

protected:
    // methods that are called via static_cast<TDerived*>(this) and may be overriden by a dervied class
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
        auto channelId = outputChannel.Channel->GetChannelId();

        CA_LOG_T("About to drain channelId: " << channelId
            << ", hasPeer: " << outputChannel.HasPeer
            << ", finished: " << outputChannel.Channel->IsFinished());

        this->ProcessOutputsState.HasDataToSend |= !outputChannel.Finished;
        this->ProcessOutputsState.AllOutputsFinished &= outputChannel.Finished;

        TBase::UpdateBlocked(outputChannel, !this->Channels->HasFreeMemoryInChannel(channelId));

        ui32 sentChunks = 0;
        while ((!outputChannel.Finished || this->Checkpoints) &&
            this->Channels->HasFreeMemoryInChannel(outputChannel.ChannelId))
        {
            const static ui32 drainPackSize = 16;
            std::vector<typename TBase::TOutputChannelInfo::TDrainedChannelMessage> channelData = outputChannel.DrainChannel(drainPackSize);
            ui32 idx = 0;
            for (auto&& i : channelData) {
                if (auto* w = i.GetWatermarkOptional()) {
                    CA_LOG_I("Resume inputs by watermark");
                    // This is excessive, inputs should be resumed after async CA received response with watermark from task runner.
                    // But, let it be here, it's better to have the same code as in checkpoints
                    TBase::ResumeInputsByWatermark(TInstant::MicroSeconds(w->GetTimestampUs()));
                }
                if (i.GetCheckpointOptional()) {
                    CA_LOG_I("Resume inputs by checkpoint");
                    TBase::ResumeInputsByCheckpoint();
                }

                this->Channels->SendChannelData(i.BuildChannelData(outputChannel.ChannelId), ++idx == channelData.size());
                ++sentChunks;
            }
            if (drainPackSize != channelData.size()) {
                if (!outputChannel.Finished) {
                    CA_LOG_T("output channelId: " << outputChannel.ChannelId << ", nothing to send and is not finished");
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

protected:
    TIntrusivePtr<IDqTaskRunner> TaskRunner;

};

} //namespace NYql::NDq

