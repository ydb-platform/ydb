#include "dq_compute_actor_impl.h"
#include "dq_compute_actor_async_input_helper.h"

namespace NYql::NDq {

struct TComputeActorAsyncInputHelperForTaskRunner : public TComputeActorAsyncInputHelper
{
public:
    using TComputeActorAsyncInputHelper::TComputeActorAsyncInputHelper;

    void AsyncInputPush(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space, bool finished) override {
        Buffer->Push(std::move(batch), space);
        if (finished) {
            Buffer->Finish();
            Finished = true;
        }
    }
    i64 GetFreeSpace() const override{
        return Buffer->GetFreeSpace();
    }

    IDqAsyncInputBuffer::TPtr Buffer;
};

template<typename TDerived>
class TDqSyncComputeActorBase: public TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperForTaskRunner> {
    using TBase = TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperForTaskRunner>;
public:
    using TDqComputeActorBase<TDerived, TComputeActorAsyncInputHelperForTaskRunner>::TDqComputeActorBase;
    static constexpr bool HasAsyncTaskRunner = false;

    template<typename T>
    requires(std::is_base_of<TComputeActorAsyncInputHelperForTaskRunner, T>::value)
    T CreateInputHelper(const TString& logPrefix,
        ui64 index,
        NDqProto::EWatermarksMode watermarksMode
    )
    {
        return T(logPrefix, index, watermarksMode);
    }

    const IDqAsyncInputBuffer* GetInputTransform(ui64, const TComputeActorAsyncInputHelperForTaskRunner& inputTransformInfo) const
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

        if ((status == ERunStatus::PendingInput || status == ERunStatus::Finished) && this->Checkpoints && this->Checkpoints->HasPendingCheckpoint() && !this->Checkpoints->ComputeActorStateSaved() && TBase::ReadyToCheckpoint()) {
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

    void SaveState(const NDqProto::TCheckpoint& checkpoint, NDqProto::TComputeActorState& state) const override {
        CA_LOG_D("Save state");
        NDqProto::TMiniKqlProgramState& mkqlProgramState = *state.MutableMiniKqlProgram();
        mkqlProgramState.SetRuntimeVersion(NDqProto::RUNTIME_VERSION_YQL_1_0);
        NDqProto::TStateData::TData& data = *mkqlProgramState.MutableData()->MutableStateData();
        data.SetVersion(TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion);
        data.SetBlob(TaskRunner->Save());

        for (auto& [inputIndex, source] : this->SourcesMap) {
            YQL_ENSURE(source.AsyncInput, "Source[" << inputIndex << "] is not created");
            NDqProto::TSourceState& sourceState = *state.AddSources();
            source.AsyncInput->SaveState(checkpoint, sourceState);
            sourceState.SetInputIndex(inputIndex);
        }
    }

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
        limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

        TaskRunner->Prepare(this->Task, limits, execCtx);

        for (auto& [channelId, channel] : this->InputChannelsMap) {
            channel.Channel = TaskRunner->GetInputChannel(channelId);
        }

        for (auto& [inputIndex, source] : this->SourcesMap) {
            source.Buffer = TaskRunner->GetSource(inputIndex);
            Y_ABORT_UNLESS(source.Buffer);
        }

        for (auto& [inputIndex, transform] : this->InputTransformsMap) {
            std::tie(transform.InputBuffer, transform.Buffer) = TaskRunner->GetInputTransform(inputIndex);
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

protected:
    // methods that are called via static_cast<TDerived*>(this) and may be overriden by a dervied class
    void* GetSourcesState() const {
        return nullptr;
    }
    void PollSources(void* /* state */) {
    }

    TIntrusivePtr<IDqTaskRunner> TaskRunner;

};

} //namespace NYql::NDq

