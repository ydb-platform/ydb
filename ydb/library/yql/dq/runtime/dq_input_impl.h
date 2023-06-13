#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

namespace NYql::NDq {

template <class TDerived, class IInputInterface>
class TDqInputImpl : public IInputInterface {
public:
    TDqInputImpl(NKikimr::NMiniKQL::TType* inputType, ui64 maxBufferBytes)
        : InputType(inputType)
        , Width(inputType->IsMulti() ? static_cast<const NKikimr::NMiniKQL::TMultiType*>(inputType)->GetElementsCount() : TMaybe<ui32>{})
        , MaxBufferBytes(maxBufferBytes)
    {
    }

    i64 GetFreeSpace() const override {
        return (i64) MaxBufferBytes - StoredBytes;
    }

    ui64 GetStoredBytes() const override {
        return StoredBytes;
    }

    [[nodiscard]]
    bool Empty() const override {
        return Batches.empty() || (IsPaused() && GetBatchesBeforePause() == 0);
    }

    void AddBatch(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space) {
        Y_VERIFY(batch.Width() == GetWidth());

        StoredBytes += space;
        StoredRows += batch.RowCount();

        auto& stats = MutableBasicStats();
        stats.Chunks++;
        stats.Bytes += space;
        stats.RowsIn += batch.RowCount();
        if (!stats.FirstRowTs) {
            stats.FirstRowTs = TInstant::Now();
        }

        if (auto* profile = MutableProfileStats()) {
            profile->MaxMemoryUsage = std::max(profile->MaxMemoryUsage, StoredBytes);
        }

        Batches.emplace_back(std::move(batch));
    }

    [[nodiscard]]
    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        Y_VERIFY(batch.Width() == GetWidth());
        if (Empty()) {
            return false;
        }

        batch.clear();

        if (IsPaused()) {
            ui64 batchesCount = GetBatchesBeforePause();
            Y_VERIFY(batchesCount <= Batches.size());

            if (batch.IsWide()) {
                while (batchesCount--) {
                    auto& part = Batches.front();
                    part.ForEachRowWide([&batch](NUdf::TUnboxedValue* values, ui32 width) {
                        batch.PushRow(values, width);
                    });
                    Batches.pop_front();
                }
            } else {
                while (batchesCount--) {
                    auto& part = Batches.front();
                    part.ForEachRow([&batch](NUdf::TUnboxedValue& value) {
                        batch.emplace_back(std::move(value));
                    });
                    Batches.pop_front();
                }
            }

            BatchesBeforePause = PauseMask;
            Y_VERIFY(GetBatchesBeforePause() == 0);
            StoredBytes -= StoredBytesBeforePause;
            StoredRows -= StoredRowsBeforePause;
            StoredBytesBeforePause = 0;
            StoredRowsBeforePause = 0;
        } else {
            if (batch.IsWide()) {
                for (auto&& part : Batches) {
                    part.ForEachRowWide([&batch](NUdf::TUnboxedValue* values, ui32 width) {
                        batch.PushRow(values, width);
                    });
                }
            } else {
                for (auto&& part : Batches) {
                    part.ForEachRow([&batch](NUdf::TUnboxedValue& value) {
                        batch.emplace_back(std::move(value));
                    });
                }
            }

            StoredBytes = 0;
            StoredRows = 0;
            Batches.clear();
        }

        MutableBasicStats().RowsOut += batch.RowCount();
        return true;
    }

    void Finish() override {
        Finished = true;
    }

    bool IsFinished() const override {
        return Finished && (!IsPaused() || Batches.empty());
    }

    NKikimr::NMiniKQL::TType* GetInputType() const override {
        return InputType;
    }

    auto& MutableBasicStats() {
        return static_cast<TDerived*>(this)->BasicStats;
    }

    auto* MutableProfileStats() {
        return static_cast<TDerived*>(this)->ProfileStats;
    }

    void Pause() override {
        Y_VERIFY(!IsPaused());
        if (!Finished) {
            BatchesBeforePause = Batches.size() | PauseMask;
            StoredRowsBeforePause = StoredRows;
            StoredBytesBeforePause = StoredBytes;
        }
    }

    void Resume() override {
        StoredBytesBeforePause = StoredRowsBeforePause = BatchesBeforePause = 0;
        Y_VERIFY(!IsPaused());
    }

    bool IsPaused() const override {
        return BatchesBeforePause;
    }

protected:
    ui64 GetBatchesBeforePause() const {
        return BatchesBeforePause & ~PauseMask;
    }

    TMaybe<ui32> GetWidth() const {
        return Width;
    }

protected:
    NKikimr::NMiniKQL::TType* const InputType = nullptr;
    const TMaybe<ui32> Width;
    const ui64 MaxBufferBytes = 0;
    TList<NKikimr::NMiniKQL::TUnboxedValueBatch, NKikimr::NMiniKQL::TMKQLAllocator<NKikimr::NMiniKQL::TUnboxedValueBatch>> Batches;
    ui64 StoredBytes = 0;
    ui64 StoredRows = 0;
    bool Finished = false;
    ui64 BatchesBeforePause = 0;
    ui64 StoredBytesBeforePause = 0;
    ui64 StoredRowsBeforePause = 0;
    static constexpr ui64 PauseMask = 1llu << 63llu;
};

} // namespace NYql::NDq
