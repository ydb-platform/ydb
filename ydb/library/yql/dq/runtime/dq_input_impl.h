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
        Y_ABORT_UNLESS(batch.Width() == GetWidth());

        StoredBytes += space;
        StoredRows += batch.RowCount();

        if (static_cast<TDerived*>(this)->PushStats.CollectBasic()) {
            static_cast<TDerived*>(this)->PushStats.Bytes += space;
            static_cast<TDerived*>(this)->PushStats.Rows += batch.RowCount();
            static_cast<TDerived*>(this)->PushStats.Chunks++;
            static_cast<TDerived*>(this)->PushStats.Resume();
            if (static_cast<TDerived*>(this)->PushStats.CollectFull()) {
                static_cast<TDerived*>(this)->PushStats.MaxMemoryUsage = std::max(static_cast<TDerived*>(this)->PushStats.MaxMemoryUsage, StoredBytes);
            }
        }

        if (GetFreeSpace() < 0) {
            static_cast<TDerived*>(this)->PopStats.TryPause();
        }

        Batches.emplace_back(std::move(batch));
    }

    [[nodiscard]]
    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        Y_ABORT_UNLESS(batch.Width() == GetWidth());
        if (Empty()) {
            static_cast<TDerived*>(this)->PushStats.TryPause();
            return false;
        }

        batch.clear();

        static_cast<TDerived*>(this)->PopStats.Resume(); //save timing before processing
        ui64 popBytes = 0;

        if (IsPaused()) {
            ui64 batchesCount = GetBatchesBeforePause();
            Y_ABORT_UNLESS(batchesCount <= Batches.size());

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

            popBytes = StoredBytesBeforePause;

            BatchesBeforePause = PauseMask;
            Y_ABORT_UNLESS(GetBatchesBeforePause() == 0);
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

            popBytes = StoredBytes;

            StoredBytes = 0;
            StoredRows = 0;
            Batches.clear();
        }

        if (static_cast<TDerived*>(this)->PopStats.CollectBasic()) {
            static_cast<TDerived*>(this)->PopStats.Bytes += popBytes;
            static_cast<TDerived*>(this)->PopStats.Rows += batch.RowCount(); // may do not match to pushed row count
            static_cast<TDerived*>(this)->PopStats.Chunks++;
        }

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

    void Pause() override {
        Y_ABORT_UNLESS(!IsPaused());
        if (!Finished) {
            BatchesBeforePause = Batches.size() | PauseMask;
            StoredRowsBeforePause = StoredRows;
            StoredBytesBeforePause = StoredBytes;
        }
    }

    void Resume() override {
        StoredBytesBeforePause = StoredRowsBeforePause = BatchesBeforePause = 0;
        Y_ABORT_UNLESS(!IsPaused());
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
