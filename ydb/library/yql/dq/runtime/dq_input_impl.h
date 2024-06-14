#pragma once

#include <ydb/library/yql/core/yql_expr_type_annotation.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node.h>

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

    [[nodiscard]]
    bool Empty() const override {
        return Batches.empty() || (IsPaused() && GetBatchesBeforePause() == 0);
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
            if (InputType->IsStruct()) {
                auto structType = static_cast<NKikimr::NMiniKQL::TStructType*>(InputType);
                for (ui32 i = 0; i < structType->GetMembersCount(); i++) {
                    if (structType->GetMemberType(i)->IsBlock()) {
                        return BLOCK_WIDE;
                    }
                }
            } else if (InputType->IsTuple()) {
                auto tupleType= static_cast<NKikimr::NMiniKQL::TTupleType*>(InputType);
                for (ui32 i = 0; i < tupleType->GetElementsCount(); i++) {
                    if (tupleType->GetElementType(i)->IsBlock()) {
                        return BLOCK_WIDE;
                    }
                }
            } else {
                return SIMPLE_WIDE;
            }
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
                    result += NKikimr::NMiniKQL::TArrowBlock::From(values[width - 1]).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case LEGACY_CH:
                // can't count rows inside CH UDF resource
                return 0;
            case LEGACY_SIMPLE_BLOCK: {
                ui64 result = 0;
                batch.ForEachRow([&](NUdf::TUnboxedValue& value) {
                    result += NKikimr::NMiniKQL::TArrowBlock::From(value.GetElement(LegacyBlockLengthIndex)).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case LEGACY_TUPLED_BLOCK: {
                ui64 result = 0;
                batch.ForEachRow([&](NUdf::TUnboxedValue& value) {
                    auto value0 = value.GetElement(0);
                    result += NKikimr::NMiniKQL::TArrowBlock::From(value0.GetElement(LegacyBlockLengthIndex)).GetDatum().scalar_as<arrow::UInt64Scalar>().value;
                });
                return result;
            }
            case SIMPLE_SCALAR:
            case SIMPLE_WIDE:
            default:
                return batch.RowCount();
        }
    }

    void AddBatch(NKikimr::NMiniKQL::TUnboxedValueBatch&& batch, i64 space) {
        Y_ABORT_UNLESS(batch.Width() == GetWidth());

        StoredBytes += space;
        StoredRows += batch.RowCount();
        auto& pushStats = static_cast<TDerived*>(this)->PushStats;

        if (pushStats.CollectBasic()) {
            pushStats.Bytes += space;
            pushStats.Rows += GetRowsCount(batch);
            pushStats.Chunks++;
            pushStats.Resume();
            if (pushStats.CollectFull()) {
                pushStats.MaxMemoryUsage = std::max(pushStats.MaxMemoryUsage, StoredBytes);
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

        auto& popStats = static_cast<TDerived*>(this)->PopStats;

        popStats.Resume(); //save timing before processing
        ui64 popBytes = 0;

        if (IsPaused()) {
            ui64 batchesCount = GetBatchesBeforePause();
            Y_ABORT_UNLESS(batchesCount > 0);
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

        if (popStats.CollectBasic()) {
            popStats.Bytes += popBytes;
            popStats.Rows += GetRowsCount(batch);
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

    void Pause() override {
        Y_ABORT_UNLESS(!IsPaused());
        BatchesBeforePause = Batches.size() | PauseMask;
        StoredRowsBeforePause = StoredRows;
        StoredBytesBeforePause = StoredBytes;
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
    TInputChannelFormat Format = FORMAT_UNKNOWN;
    ui32 LegacyBlockLengthIndex = 0;
};

} // namespace NYql::NDq
