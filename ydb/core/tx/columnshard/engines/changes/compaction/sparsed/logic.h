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
        const TColumnMergeContext& Context;
        std::unique_ptr<arrow::ArrayBuilder> IndexBuilder;
        std::unique_ptr<arrow::ArrayBuilder> ValueBuilder;
        arrow::UInt32Builder* IndexBuilderImpl = nullptr;
        ui32 CurrentRecordIdx = 0;
        ui32 UsefulRecordsCount = 0;

    public:
        TWriter(const TColumnMergeContext& context);

        bool HasData() const {
            return CurrentRecordIdx;
        }

        bool HasUsefulData() const {
            return UsefulRecordsCount;
        }

        ui32 AddPosition() {
            return ++CurrentRecordIdx;
        }

        void AddRealData(const std::shared_ptr<arrow::Array>& arr, const ui32 index);

        TColumnPortionResult Flush();
    };

    class TCursor {
    private:
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> Array;
        std::optional<NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress> CurrentOwnedArray;
        std::shared_ptr<NArrow::NAccessor::TSparsedArray> CurrentSparsedArray;
        ui32 NextGlobalPosition = 0;
        ui32 NextLocalPosition = 0;
        ui32 CommonShift = 0;
        std::optional<NArrow::NAccessor::TSparsedArrayChunk> Chunk;
        const TColumnMergeContext& Context;
        void InitArrays(const ui32 position);

    public:
        TCursor(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& array, const TColumnMergeContext& context)
            : Array(array)
            , Context(context) {
            AFL_VERIFY(Array->GetRecordsCount());
            InitArrays(0);
        }

        bool AddIndexTo(const ui32 index, TWriter& writer);
    };

    std::vector<TCursor> Cursors;
    virtual void DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input) override;

    virtual std::vector<TColumnPortionResult> DoExecute(
        const TChunkMergeContext& context, const arrow::UInt16Array& pIdxArray, const arrow::UInt32Array& pRecordIdxArray) override;

public:
    using TBase::TBase;
};

}   // namespace NKikimr::NOlap::NCompaction
