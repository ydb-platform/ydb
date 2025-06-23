#include "dq_async_input.h"
#include "dq_input_impl.h"

namespace NYql::NDq {

class TDqAsyncInputBuffer : public TDqInputImpl<TDqAsyncInputBuffer, IDqAsyncInputBuffer> {
    using TBaseImpl = TDqInputImpl<TDqAsyncInputBuffer, IDqAsyncInputBuffer>;
    friend TBaseImpl;
    bool Pending = false;
public:
    TDqAsyncInputBufferStats PushStats;
    TDqInputStats PopStats;

    TDqAsyncInputBuffer(ui64 inputIndex, const TString& type, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, TCollectStatsLevel level)
        : TBaseImpl(inputType, maxBufferBytes)
    {
        PopStats.Level = level;
        PushStats.Level = level;
        PushStats.InputIndex = inputIndex;
        PushStats.Type = type;
    }

    ui64 GetInputIndex() const override {
        return PushStats.InputIndex;
    }

    const TDqAsyncInputBufferStats& GetPushStats() const override {
        return PushStats;
    }

    const TDqInputStats& GetPopStats() const override {
        return PopStats;
    }

    void Push(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space) override {
        Pending = space != 0;
        if (!batch.empty()) {
            auto rows = AddBatch(std::move(batch), space);

            if (PushStats.CollectBasic()) {
                PushStats.Bytes += space;
                PushStats.Rows += rows;
                PushStats.Chunks++;
                PushStats.Resume();
                if (PushStats.CollectFull()) {
                    PushStats.MaxMemoryUsage = std::max(PushStats.MaxMemoryUsage, StoredBytes);
                }
            }

            if (GetFreeSpace() < 0) {
                PopStats.TryPause();
            }
        }
    }

    virtual void Push(TDqSerializedBatch&&, i64) override {
        YQL_ENSURE(!"Unimplemented");
    }

    bool IsPending() const override {
        return Pending;
    }
};

IDqAsyncInputBuffer::TPtr CreateDqAsyncInputBuffer(
    ui64 inputIndex, const TString& type, NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes, TCollectStatsLevel level)
{
    return new TDqAsyncInputBuffer(inputIndex, type, inputType, maxBufferBytes, level);
}

} // namespace NYql::NDq
