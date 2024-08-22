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

    class TPlainChunkCursor {
    private:
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> CurrentChunkedArray;
        std::optional<NArrow::NAccessor::IChunkedArray::TFullDataAddress> ChunkAddress;
        const NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress* CurrentOwnedArray;
        ui32 ChunkStartPosition = 0;
        ui32 ChunkFinishPosition = 0;

        void InitArrays(const ui32 position) {
            AFL_VERIFY(!ChunkAddress || ChunkFinishPosition <= position);
            ChunkAddress = CurrentChunkedArray->GetChunk(ChunkAddress, position);
            AFL_VERIFY(ChunkAddress);
            ChunkStartPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + ChunkAddress->GetAddress().GetGlobalStartPosition();
            ChunkFinishPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + ChunkAddress->GetAddress().GetGlobalFinishPosition();
        }

    public:
        TPlainChunkCursor(const std::shared_ptr<NArrow::NAccessor::IChunkedArray>& chunked,
            const NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress* currentOwnedArray)
            : CurrentChunkedArray(chunked)
            , CurrentOwnedArray(currentOwnedArray)
        {
            AFL_VERIFY(CurrentChunkedArray);
            AFL_VERIFY(CurrentOwnedArray);
            InitArrays(CurrentOwnedArray->GetAddress().GetGlobalStartPosition());
        }
        bool AddIndexTo(const ui32 index, TWriter& writer, const TColumnMergeContext& context);
    };

    class TSparsedChunkCursor {
    private:
        std::shared_ptr<NArrow::NAccessor::TSparsedArray> CurrentSparsedArray;
        const NArrow::NAccessor::TSparsedArrayChunk* Chunk = nullptr;
        const NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress* CurrentOwnedArray;
        ui32 ChunkStartGlobalPosition = 0;
        ui32 NextGlobalPosition = 0;
        ui32 NextLocalPosition = 0;
        ui32 FinishGlobalPosition = 0;
        void InitArrays(const ui32 position) {
            AFL_VERIFY(!Chunk || CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetFinishPosition() <= position);
            Chunk = &CurrentSparsedArray->GetSparsedChunk(CurrentOwnedArray->GetAddress().GetLocalIndex(position));
            AFL_VERIFY(Chunk->GetRecordsCount());
            AFL_VERIFY(CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetStartPosition() <= position && 
                    position < CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetFinishPosition())
            ("pos", position)("start", Chunk->GetStartPosition())("finish", Chunk->GetFinishPosition())(
                "shift", CurrentOwnedArray->GetAddress().GetGlobalStartPosition());
            ChunkStartGlobalPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetStartPosition();
            NextGlobalPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetFirstIndexNotDefault();
            NextLocalPosition = 0;
            FinishGlobalPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + Chunk->GetFinishPosition();
        }
    public:
        bool AddIndexTo(const ui32 index, TWriter& writer, const TColumnMergeContext& context);
        TSparsedChunkCursor(const std::shared_ptr<NArrow::NAccessor::TSparsedArray>& sparsed,
            const NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress* currentOwnedArray)
            : CurrentSparsedArray(sparsed)
            , CurrentOwnedArray(currentOwnedArray) {
            AFL_VERIFY(sparsed);
            AFL_VERIFY(currentOwnedArray);
            InitArrays(CurrentOwnedArray->GetAddress().GetGlobalStartPosition());
        }
    };

    class TCursor {
    private:
        std::shared_ptr<NArrow::NAccessor::IChunkedArray> Array;
        std::optional<NArrow::NAccessor::IChunkedArray::TFullChunkedArrayAddress> CurrentOwnedArray;
        std::shared_ptr<TSparsedChunkCursor> SparsedCursor;
        std::shared_ptr<TPlainChunkCursor> PlainCursor;
        ui32 FinishGlobalPosition = 0;
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
