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
    void SaveState(const NDqProto::TCheckpoint& checkpoint, NDqProto::TComputeActorState& state) const override {
        CA_LOG_D("Save state");
        NDqProto::TMiniKqlProgramState& mkqlProgramState = *state.MutableMiniKqlProgram();
        mkqlProgramState.SetRuntimeVersion(NDqProto::RUNTIME_VERSION_YQL_1_0);
        NDqProto::TStateData::TData& data = *mkqlProgramState.MutableData()->MutableStateData();
        data.SetVersion(TDqComputeActorCheckpoints::ComputeActorCurrentStateVersion);
        data.SetBlob(this->TaskRunner->Save());

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
            this->TaskRunner->Load(blob);
        } catch (const std::exception& e) {
            error = e.what();
        }
        this->Checkpoints->AfterStateLoading(error);
    }

    void SetTaskRunner(const TIntrusivePtr<IDqTaskRunner>& taskRunner) {
        this->TaskRunner = taskRunner;
    }

    void PrepareTaskRunner(const IDqTaskRunnerExecutionContext& execCtx) {
        YQL_ENSURE(this->TaskRunner);

        auto guard = TBase::BindAllocator();
        auto* alloc = guard.GetMutex();
        alloc->SetLimit(this->MemoryQuota->GetMkqlMemoryLimit());

        this->MemoryQuota->TrySetIncreaseMemoryLimitCallback(alloc);

        TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = this->MemoryLimits.ChannelBufferSize;
        limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

        this->TaskRunner->Prepare(this->Task, limits, execCtx);

        for (auto& [channelId, channel] : this->InputChannelsMap) {
            channel.Channel = this->TaskRunner->GetInputChannel(channelId);
        }

        for (auto& [inputIndex, source] : this->SourcesMap) {
            source.Buffer = this->TaskRunner->GetSource(inputIndex);
            Y_ABORT_UNLESS(source.Buffer);
        }

        for (auto& [inputIndex, transform] : this->InputTransformsMap) {
            std::tie(transform.InputBuffer, transform.Buffer) = this->TaskRunner->GetInputTransform(inputIndex);
        }

        for (auto& [channelId, channel] : this->OutputChannelsMap) {
            channel.Channel = this->TaskRunner->GetOutputChannel(channelId);
        }

        for (auto& [outputIndex, transform] : this->OutputTransformsMap) {
            std::tie(transform.Buffer, transform.OutputBuffer) = this->TaskRunner->GetOutputTransform(outputIndex);
        }

        for (auto& [outputIndex, sink] : this->SinksMap) {
            sink.Buffer = this->TaskRunner->GetSink(outputIndex);
        }

        TBase::FillIoMaps(
            this->TaskRunner->GetHolderFactory(),
            this->TaskRunner->GetTypeEnv(),
            this->TaskRunner->GetSecureParams(),
            this->TaskRunner->GetTaskParams(),
            this->TaskRunner->GetReadRanges(),
            this->TaskRunner->GetRandomProvider()
        );
    }
};

} //namespace NYql::NDq

