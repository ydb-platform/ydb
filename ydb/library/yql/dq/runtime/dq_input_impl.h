#pragma once

#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node.h>

namespace NYql::NDq {

// remove LEGACY* support after upgrade S3/Generic Sources to use modern format

enum TInputChannelFormat {
    FORMAT_UNKNOWN,
    SIMPLE_SCALAR,
    SIMPLE_WIDE,
    BLOCK_WIDE,
    LEGACY_CH,
    LEGACY_SIMPLE_BLOCK,
    LEGACY_TUPLED_BLOCK
};

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

    bool IsLegacySimpleBlock(NKikimr::NMiniKQL::TStructType* structType, ui32& blockLengthIndex) {
        auto index = structType->FindMemberIndex(BlockLengthColumnName);
        if (index) {
            for (ui32 i = 0; i < structType->GetMembersCount(); i++) {
                auto type = structType->GetMemberType(i);
                if (!type->IsBlock()) {
                    return false;
                }
            }
            blockLengthIndex = *index;
            return true;
        } else {
            return false;
        }
    }

    TInputChannelFormat GetFormat() {
        if (Width) {
            switch (InputType->GetKind()) {
                case NKikimr::NMiniKQL::TTypeBase::EKind::Struct: {
                    auto structType = static_cast<NKikimr::NMiniKQL::TStructType*>(InputType);
                    for (ui32 i = 0; i < structType->GetMembersCount(); i++) {
                        if (structType->GetMemberType(i)->IsBlock()) {
                            return BLOCK_WIDE;
                        }
                    }
                    break;
                }
                case NKikimr::NMiniKQL::TTypeBase::EKind::Tuple: {
                    auto tupleType = static_cast<NKikimr::NMiniKQL::TTupleType*>(InputType);
                    for (ui32 i = 0; i < tupleType->GetElementsCount(); i++) {
                        if (tupleType->GetElementType(i)->IsBlock()) {
                            return BLOCK_WIDE;
                        }
                    }
                    break;
                }
                case NKikimr::NMiniKQL::TTypeBase::EKind::Multi: {
                    auto multiType = static_cast<NKikimr::NMiniKQL::TMultiType*>(InputType);
                    for (ui32 i = 0; i < multiType->GetElementsCount(); i++) {
                        if (multiType->GetElementType(i)->IsBlock()) {
                            return BLOCK_WIDE;
                        }
                    }
                    break;
                }
                default:
                    break;
            }
            return SIMPLE_WIDE;
        }

        if (InputType->IsStruct()) {
            return IsLegacySimpleBlock(static_cast<NKikimr::NMiniKQL::TStructType*>(InputType), LegacyBlockLengthIndex) ? LEGACY_SIMPLE_BLOCK : SIMPLE_SCALAR;
        } else if (InputType->IsResource()) {
            if (static_cast<NKikimr::NMiniKQL::TResourceType*>(InputType)->GetTag() == "ClickHouseClient.Block") {
                return LEGACY_CH;
            }
        } else if (InputType->IsTuple()) {
            auto tupleType= static_cast<NKikimr::NMiniKQL::TTupleType*>(InputType);
            if (tupleType->GetElementsCount() == 2) {
                auto type = tupleType->GetElementType(0);
                if (type->IsStruct()) {
                    return IsLegacySimpleBlock(static_cast<NKikimr::NMiniKQL::TStructType*>(type), LegacyBlockLengthIndex) ? LEGACY_TUPLED_BLOCK : SIMPLE_SCALAR;
                } else if (InputType->IsResource()) {
                    if (static_cast<NKikimr::NMiniKQL::TResourceType*>(InputType)->GetTag() == "ClickHouseClient.Block") {
                        return LEGACY_CH;
                    }
                }
            }
        }

        return SIMPLE_SCALAR;
    }

    ui64 GetRowsCount(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) {
        if (Y_UNLIKELY(Format == FORMAT_UNKNOWN)) {
            Format = GetFormat();
        }

        switch (Format) {
            case BLOCK_WIDE: {
                ui64 result = 0;
                batch.ForEachRowWide([&](NUdf::TUnboxedValue* values, ui32 width) {
                    const auto& blockLength = values[width - 1];
                    result += NKikimr::NMiniKQL::TArrowBlock::From(blockLength).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case LEGACY_CH:
                // can't count rows inside CH UDF resource
                return 0;
            case LEGACY_SIMPLE_BLOCK: {
                ui64 result = 0;
                batch.ForEachRow([&](NUdf::TUnboxedValue& value) {
                    const auto& blockLength = value.GetElement(LegacyBlockLengthIndex);
                    result += NKikimr::NMiniKQL::TArrowBlock::From(blockLength).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case LEGACY_TUPLED_BLOCK: {
                ui64 result = 0;
                batch.ForEachRow([&](NUdf::TUnboxedValue& value) {
                    auto value0 = value.GetElement(0);
                    const auto& blockLength = value0.GetElement(LegacyBlockLengthIndex);
                    result += NKikimr::NMiniKQL::TArrowBlock::From(blockLength).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case SIMPLE_SCALAR:
            case SIMPLE_WIDE:
            default:
                return batch.RowCount();
        }
    }

    ui64 AddBatch(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space) {
        Y_ABORT_UNLESS(batch.Width() == GetWidth());

        ui64 rows = GetRowsCount(batch);
        StoredBytes += space;
        StoredRows += rows;

        if (GetFreeSpace() < 0) {
            static_cast<TDerived*>(this)->PopStats.TryPause();
        }

        Batches.emplace_back(std::move(batch));
        AddBatchCounts(space, rows);

        return rows;
    }

    [[nodiscard]]
    bool Pop(NKikimr::NMiniKQL::TUnboxedValueBatch& batch) override {
        Y_ABORT_UNLESS(batch.Width() == GetWidth());
        if (Empty()) {
            static_cast<TDerived*>(this)->PushStats.TryPause();
            return false;
        }

        batch.clear();

        auto& popStats = static_cast<TDerived*>(this)->PopStats;

        popStats.Resume(); //save timing before processing

        auto [popBytes, popRows, batchesCount] = PopReadyCounts();
        Y_ABORT_UNLESS(batchesCount > 0);
        Y_ABORT_UNLESS(batchesCount <= Batches.size());

        if (batchesCount != Batches.size()) {

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

            Batches.clear();
        }

        StoredBytes -= popBytes;
        StoredRows -= popRows;

        if (popStats.CollectBasic()) {
            popStats.Bytes += popBytes;
            popStats.Rows += popRows;
            popStats.Chunks++;
        }

        Y_ABORT_UNLESS(!batch.empty());
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

    [[nodiscard]]
    bool Empty() const override {
        return Batches.empty() || IsPaused() && BeforeBarrier.Batches == 0;
    }

private:
    void AddBatchCounts(ui64 space, ui64 rows) {
        auto& barrier = PendingBarriers.empty() ? BeforeBarrier : PendingBarriers.back();
        barrier.Batches ++;
        barrier.Bytes += space;
        barrier.Rows += rows;
    }

    std::tuple<ui64, ui64, ui64> PopReadyCounts() {
        if (!PendingBarriers.empty() && !IsPaused()) {
            // There were watermarks, but channel is not paused
            // Process data anyway and move watermarks behind
            auto lastBarrier = PendingBarriers.back().Barrier;
            for (const auto& barrier : PendingBarriers) {
                Y_ENSURE(!barrier.IsCheckpoint());
                BeforeBarrier += barrier;
            }
            PendingBarriers.clear();
            PendingBarriers.emplace_back(TBarrier { .Barrier = lastBarrier });
        }
        auto popBatches = BeforeBarrier.Batches;
        auto popBytes = BeforeBarrier.Bytes;
        auto popRows = BeforeBarrier.Rows;

        BeforeBarrier.Clear();
        return std::make_tuple(popBytes, popRows, popBatches);
    }

public:
    bool IsPaused() const {
        return IsPausedByWatermark() || IsPausedByCheckpoint();
    }

private:
    void SkipWatermarksBeforeBarrier() {
        // Drop watermarks before current barrier
        while (!PendingBarriers.empty()) {
            auto& barrier = PendingBarriers.front();
            if (barrier.Barrier >= PauseBarrier) {
                break;
            }
            BeforeBarrier.Batches += barrier.Batches;
            BeforeBarrier.Rows += barrier.Rows;
            BeforeBarrier.Bytes += barrier.Bytes;
            PendingBarriers.pop_front();
        }
    }

public:
    void PauseByWatermark(TInstant watermark) override {
        Y_ENSURE(PauseBarrier <= watermark);
        PauseBarrier = watermark;
        if (IsPausedByCheckpoint()) {
            return;
        }
        if (IsFinished()) {
            return;
        }
        SkipWatermarksBeforeBarrier();
        Y_ENSURE(!PendingBarriers.empty());
        Y_ENSURE(PendingBarriers.front().Barrier >= watermark);
    }

    void PauseByCheckpoint() override {
        Y_ENSURE(!IsPausedByCheckpoint());
        if (PauseBarrier != TBarrier::NoBarrier) {
            Y_ENSURE(!PendingBarriers.empty());
            if (PendingBarriers.front().Barrier > PauseBarrier) {
                // (1.BeforeBarrier) (3.Watermark > PauseBarrier) (4.Some data and watermarks) | (5.Here will be checkpoint)
                // ->
                // (1.BeforeBarrier) (3.Fake watermark == PauseBarrier with data from 3 & 4 behind) (Checkpoint with empty data behind) (Max watermark from 3 & 4 with empty data behind)
                auto lastWatermark = PendingBarriers.back().Barrier;
                TBarrier fakeWatermark(PauseBarrier);
                for (auto& barrier: PendingBarriers) {
                    fakeWatermark += barrier;
                }
                PendingBarriers.clear();
                PendingBarriers.emplace_back(fakeWatermark);
                PendingBarriers.emplace_back(); // CheckpointBarrier
                PendingBarriers.emplace_back(TBarrier { .Barrier = lastWatermark });
            } else {
                Y_ENSURE(PendingBarriers.front().Barrier == PauseBarrier);
                // (1.BeforeBarrier) (3.Watermark == PauseBarrier) (4.Some data and watermarks) | (5.Here will be checkpoint)
                // ->
                // (1.BeforeBarrier) (3.Watermark == PauseBarriers with all data from 3 & 4 behind) (Checkpoint with empty data behind) (Max watermark from 4 with empty data behind)
                auto lastWatermark = PendingBarriers.size() > 1 ? PendingBarriers.back().Barrier : TBarrier::NoBarrier;
                for (auto& firstWatermark = PendingBarriers.front(); PendingBarriers.size() > 1; PendingBarriers.pop_back()) {
                    firstWatermark += PendingBarriers.back();
                }
                PendingBarriers.emplace_back(); // CheckpointBarrier
                if (lastWatermark != TBarrier::NoBarrier) {
                    PendingBarriers.emplace_back(TBarrier { .Barrier = lastWatermark });
                }
            }
        } else if (PendingBarriers.empty()) {
            PendingBarriers.emplace_front(); // CheckpointBarrier
        } else {
            // (1.BeforeBarrier) (4.Some data and watermarks) | (5.Here will be checkpoint)
            // ->
            // (1.BeforeBarrier + all data from 4) (5.Checkpoint with empty data behind) (Max watermark from 4 if any with empty data behind)
            auto lastWatermark = PendingBarriers.back().Barrier;
            for (auto& barrier: PendingBarriers) { // Move all collected data before checkpoint
                BeforeBarrier += barrier;
            }
            PendingBarriers.clear();
            PendingBarriers.emplace_back(); // CheckpointBarrier
            PendingBarriers.emplace_back(TBarrier { .Barrier = lastWatermark });
        }
    }

    void AddWatermark(TInstant watermark) override {
        if (!PendingBarriers.empty() && PendingBarriers.back().Batches == 0 && !PendingBarriers.back().IsCheckpoint()) {
            Y_ENSURE(PendingBarriers.back().Rows == 0);
            Y_ENSURE(PendingBarriers.back().Bytes == 0);
            PendingBarriers.back().Barrier = watermark;
        } else {
            PendingBarriers.emplace_back(TBarrier { .Barrier = watermark });
        }
    }

    bool IsPausedByWatermark() const override {
        return !IsPausedByCheckpoint() && PauseBarrier != TBarrier::NoBarrier;
    }

    bool IsPausedByCheckpoint() const override {
        return !PendingBarriers.empty() && PendingBarriers.front().IsCheckpoint();
    }

    void ResumeByWatermark(TInstant watermark) override {
        Y_ENSURE(Empty());
        Y_ENSURE(PauseBarrier == watermark);
        PauseBarrier = TBarrier::NoBarrier;
        if (IsFinished()) {
            return;
        }
        Y_ENSURE(!PendingBarriers.empty());
        Y_ENSURE(PendingBarriers.front().Barrier >= watermark);
        if (PendingBarriers.front().Barrier == watermark) {
            BeforeBarrier = PendingBarriers.front();
            PendingBarriers.pop_front();
        }
        Y_ENSURE(PendingBarriers.empty() || PendingBarriers.front().Barrier > watermark);
    }

    void ResumeByCheckpoint() override {
        Y_ENSURE(IsPausedByCheckpoint());
        Y_ENSURE(Empty());
        BeforeBarrier = PendingBarriers.front();
        PendingBarriers.pop_front();
        // There can be watermarks before current barrier exposed by checkpoint removal
        SkipWatermarksBeforeBarrier();
    }

protected:

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
    TInputChannelFormat Format = FORMAT_UNKNOWN;
    ui32 LegacyBlockLengthIndex = 0;

    struct TBarrier {
        static constexpr TInstant NoBarrier = TInstant::Zero();
        static constexpr TInstant CheckpointBarrier = TInstant::Max();
        TInstant Barrier = CheckpointBarrier;
        ui64 Batches = 0;
        ui64 Bytes = 0;
        ui64 Rows = 0;
        // watermark (!= TInstant::Max()) or checkpoint (TInstant::Max())
        bool IsCheckpoint() const {
            return Barrier == CheckpointBarrier;
        }
        TBarrier& operator+= (const TBarrier& other) {
            Batches += other.Batches;
            Bytes += other.Bytes;
            Rows += other.Rows;
            return *this;
        }
        void Clear() {
            Batches = 0;
            Bytes = 0;
            Rows = 0;
        }
    };
    std::deque<TBarrier> PendingBarriers; // barrier and counts after barrier
    TBarrier BeforeBarrier; // counts before barrier
    TInstant PauseBarrier; // Watermark barrier or TBarrier::NoBarrier
};

} // namespace NYql::NDq
