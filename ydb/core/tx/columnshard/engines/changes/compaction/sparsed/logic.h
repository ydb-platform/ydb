#pragma once
#include <ydb/core/formats/arrow/accessor/abstract/accessor.h>
#include <ydb/core/formats/arrow/accessor/common/const.h>
#include <ydb/core/formats/arrow/accessor/sparsed/accessor.h>
#include <ydb/core/tx/columnshard/engines/changes/compaction/abstract/merger.h>

namespace NKikimr::NOlap::NCompaction {

class TSparsedMerger: public IColumnMerger {
private:
    static inline auto Registrator = TFactory::TRegistrator<TSparsedMerger>(NArrow::NAccessor::TGlobalConst::SparsedDataAccessorName);

    using TBase = IColumnMerger;
    class TWriter: public TColumnPortionResult {
    private:
        using TBase = TColumnPortionResult;
        const std::shared_ptr<arrow::DataType> DataType;
        std::unique_ptr<arrow::ArrayBuilder> IndexBuilder;
        std::unique_ptr<arrow::ArrayBuilder> ValueBuilder;
        arrow::UInt32Builder* IndexBuilderImpl = nullptr;
        const TColumnMergeContext& Context;
        ui32 UsefulRecordsCount = 0;
        std::optional<ui32> LastRecordIdx;
    public:
        bool AddRecord(const arrow::Array& colValue, const ui32 idx, const ui32 globalRecordIdx) {
            AFL_VERIFY(NArrow::Append(*ValueBuilder, colValue, idx));
            if (LastRecordIdx) {
                AFL_VERIFY(*LastRecordIdx < globalRecordIdx);
            }
            LastRecordIdx = globalRecordIdx;
            NArrow::TStatusValidator::Validate(IndexBuilderImpl->Append(globalRecordIdx));
            ++UsefulRecordsCount;
            return true;
        }

        TWriter(const TColumnMergeContext& context);
        TColumnPortionResult Flush(const ui32 recordsCount);
    };

    class TSparsedChunkCursor: public TBaseIterator<NArrow::NAccessor::TSparsedArray> {
    private:
        const ui32 CursorIdx;
        using TBase = TBaseIterator<NArrow::NAccessor::TSparsedArray>;
        std::optional<ui32> GlobalResultOffset;
        const TSourceReverseRemap* RemapToGlobalResult = nullptr;
        ui32 ScanIndex = 0;
        [[nodiscard]] bool MoveToSignificant(const std::optional<ui32> lowerBound = std::nullopt);

    public:
        ui32 GetCursorIdx() const {
            return CursorIdx;
        }

        TString DebugString() const {
            TStringBuilder sb;
            AFL_VERIFY(GlobalResultOffset);
            AFL_VERIFY(RemapToGlobalResult);
            sb << "{offset=" << *GlobalResultOffset << ";remap=" << RemapToGlobalResult->DebugString()
               << ";pos=" << GetGlobalResultIndexImpl().value_or(Max<i64>()) << "}";
            return sb;
        }
        [[nodiscard]] std::optional<i64> GetGlobalResultIndexImpl() const;

        struct THeapComparator {
            bool operator()(const TSparsedChunkCursor* const item1, const TSparsedChunkCursor* const item2) const {
                return item2->GetGlobalResultIndexVerified() < item1->GetGlobalResultIndexVerified();
            }
        };

        [[nodiscard]] bool InitGlobalRemapping(
            const TSourceReverseRemap& remapToGlobalResult, const ui32 globalResultOffset, const ui32 globalResultSize);

        [[nodiscard]] ui32 GetGlobalResultIndexVerified() const {
            std::optional<i64> result = GetGlobalResultIndexImpl();
            AFL_VERIFY(result);
            AFL_VERIFY(*result >= 0)("result", result);
            return *result;
        }

        bool Next() {
            ++ScanIndex;
            return MoveToSignificant();
        }

        bool AddIndexTo(TWriter& writer);
        TSparsedChunkCursor(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& input, const std::shared_ptr<TColumnLoader>& loader, const ui32 cursorIdx)
            : TBase(input, loader)
            , CursorIdx(cursorIdx)
        {
        }
    };

    std::vector<TSparsedChunkCursor> Cursors;
    std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>> Inputs;

    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergeContext) override;

    virtual TColumnPortionResult DoExecute(const TChunkMergeContext& context, TMergingContext& mergeContext) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction
