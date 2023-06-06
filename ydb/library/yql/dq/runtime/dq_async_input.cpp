#include "dq_async_input.h"
#include "dq_input_impl.h"

namespace NYql::NDq {

class TDqAsyncInputBuffer : public TDqInputImpl<TDqAsyncInputBuffer, IDqAsyncInputBuffer> {
    using TBaseImpl = TDqInputImpl<TDqAsyncInputBuffer, IDqAsyncInputBuffer>;
    friend TBaseImpl;
public:
    TDqAsyncInputBuffer(ui64 inputIndex, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, bool collectProfileStats)
        : TBaseImpl(inputType, maxBufferBytes)
        , InputIndex(inputIndex)
        , BasicStats(InputIndex)
        , ProfileStats(collectProfileStats ? &BasicStats : nullptr) {}

    ui64 GetInputIndex() const override {
        return InputIndex;
    }

    void Push(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space) override {
        Y_VERIFY(!batch.empty() || !space);
        if (!batch.empty()) {
            AddBatch(std::move(batch), space);
        }
    }

    const TDqAsyncInputBufferStats* GetStats() const override {
        return &BasicStats;
    }

private:
    const ui64 InputIndex;
    TDqAsyncInputBufferStats BasicStats;
    TDqAsyncInputBufferStats* ProfileStats = nullptr;
};

IDqAsyncInputBuffer::TPtr CreateDqAsyncInputBuffer(
    ui64 inputIndex, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, bool collectStats)
{
    return new TDqAsyncInputBuffer(inputIndex, inputType, maxBufferBytes, collectStats);
}

} // namespace NYql::NDq
