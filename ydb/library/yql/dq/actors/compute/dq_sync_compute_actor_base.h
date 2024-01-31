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
    void SetTaskRunner(const TIntrusivePtr<IDqTaskRunner>& taskRunner) {
        this->TaskRunner = taskRunner;
    }

    void PrepareTaskRunner(const IDqTaskRunnerExecutionContext& execCtx) {
        YQL_ENSURE(this->TaskRunner);

        auto guard = this->TaskRunner->BindAllocator(this->MemoryQuota->GetMkqlMemoryLimit());
        auto* alloc = guard.GetMutex();

        this->MemoryQuota->TrySetIncreaseMemoryLimitCallback(alloc);

        TDqTaskRunnerMemoryLimits limits;
        limits.ChannelBufferSize = this->MemoryLimits.ChannelBufferSize;
        limits.OutputChunkMaxSize = GetDqExecutionSettings().FlowControl.MaxOutputChunkSize;

        this->TaskRunner->Prepare(this->Task, limits, execCtx);

        TBase::FillIoMaps(
                this->TaskRunner->GetHolderFactory(),
                this->TaskRunner->GetTypeEnv(),
                this->TaskRunner->GetSecureParams(),
                this->TaskRunner->GetTaskParams(),
                this->TaskRunner->GetReadRanges());
    }
};

} //namespace NYql::NDq

