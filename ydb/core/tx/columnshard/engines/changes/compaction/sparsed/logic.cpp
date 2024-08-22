#include "logic.h"
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction {

void TSparsedMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input) {
    for (auto&& p : input) {
        Cursors.emplace_back(p, Context);
    }
}

std::vector<TColumnPortionResult> TSparsedMerger::DoExecute(
    const TChunkMergeContext& chunkContext, const arrow::UInt16Array& pIdxArray, const arrow::UInt32Array& pRecordIdxArray) {
    std::vector<TColumnPortionResult> result;
    std::shared_ptr<TWriter> writer = std::make_shared<TWriter>(Context);
    for (ui32 idx = 0; idx < pIdxArray.length(); ++idx) {
        const ui16 portionIdx = pIdxArray.Value(idx);
        const ui32 portionRecordIdx = pRecordIdxArray.Value(idx);
        auto& cursor = Cursors[portionIdx];

        cursor.AddIndexTo(portionRecordIdx, *writer);
        if (writer->AddPosition() == chunkContext.GetPortionRowsCountLimit()) {
            result.emplace_back(writer->Flush());
            writer = std::make_shared<TWriter>(Context);
        }
    }
    if (writer->HasData()) {
        result.emplace_back(writer->Flush());
    }
    return result;
}

void TSparsedMerger::TWriter::AddRealData(const std::shared_ptr<arrow::Array>& arr, const ui32 index) {
    AFL_VERIFY(arr);
    AFL_VERIFY(NArrow::Append(*ValueBuilder, *arr, index));
    NArrow::TStatusValidator::Validate(IndexBuilderImpl->Append(CurrentRecordIdx));
    ++UsefulRecordsCount;
}

TColumnPortionResult TSparsedMerger::TWriter::Flush() {
    std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("index", arrow::uint32()),
        std::make_shared<arrow::Field>("value", DataType) };
    auto schema = std::make_shared<arrow::Schema>(fields);
    std::vector<std::shared_ptr<arrow::Array>> columns = { NArrow::TStatusValidator::GetValid(IndexBuilder->Finish()),
        NArrow::TStatusValidator::GetValid(ValueBuilder->Finish()) };

    auto recordBatch = arrow::RecordBatch::Make(schema, UsefulRecordsCount, columns);
    NArrow::NAccessor::TSparsedArray::TBuilder builder(
        Context.GetIndexInfo().GetColumnFeaturesVerified(Context.GetColumnId()).GetDefaultValue().GetValue(), Context.GetResultField()->type());
    builder.AddChunk(CurrentRecordIdx, recordBatch);
    Chunks.emplace_back(std::make_shared<NChunks::TChunkPreparation>(Context.GetSaver().Apply(recordBatch), builder.Finish(),
        TChunkAddress(ColumnId, 0), Context.GetIndexInfo().GetColumnFeaturesVerified(ColumnId)));
    return *this;
}

TSparsedMerger::TWriter::TWriter(const TColumnMergeContext& context)
    : TBase(context.GetColumnId())
    , DataType(context.GetResultField()->type())
    , Context(context) {
    IndexBuilder = NArrow::MakeBuilder(arrow::uint32());
    ValueBuilder = NArrow::MakeBuilder(DataType);
    IndexBuilderImpl = (arrow::UInt32Builder*)(IndexBuilder.get());
}

bool TSparsedMerger::TPlainChunkCursor::AddIndexTo(const ui32 index, TWriter& writer, const TColumnMergeContext& context) {
    if (ChunkFinishPosition <= index) {
        InitArrays(index);
    }
    AFL_VERIFY(ChunkStartPosition <= index);
    if (NArrow::ColumnEqualsScalar(ChunkAddress->GetArray(), index - ChunkStartPosition, context.GetLoader()->GetDefaultValue())) {
        return false;
    } else {
        writer.AddRealData(ChunkAddress->GetArray(), index - ChunkStartPosition);
        return true;
    }
}

bool TSparsedMerger::TSparsedChunkCursor::AddIndexTo(const ui32 index, TWriter& writer, const TColumnMergeContext& /*context*/) {
    AFL_VERIFY(ChunkStartGlobalPosition <= index);
    if (index < NextGlobalPosition) {
        return false;
    } else {
        if (FinishGlobalPosition <= index) {
            InitArrays(index);
        }
        if (index == NextGlobalPosition) {
            writer.AddRealData(Chunk->GetColValue(), NextLocalPosition);
            if (++NextLocalPosition < Chunk->GetNotDefaultRecordsCount()) {
                NextGlobalPosition = ChunkStartGlobalPosition + Chunk->GetIndexUnsafeFast(NextLocalPosition);
                return true;
            } else {
                NextGlobalPosition = ChunkStartGlobalPosition + Chunk->GetRecordsCount();
                return false;
            }
        } else {
            bool found = false;
            for (; NextLocalPosition < Chunk->GetNotDefaultRecordsCount(); ++NextLocalPosition) {
                NextGlobalPosition = ChunkStartGlobalPosition + Chunk->GetIndexUnsafeFast(NextLocalPosition);
                if (NextGlobalPosition == index) {
                    writer.AddRealData(Chunk->GetColValue(), NextLocalPosition);
                    found = true;
                } else if (index < NextGlobalPosition) {
                    return found;
                }
            }
            NextGlobalPosition = ChunkStartGlobalPosition + Chunk->GetRecordsCount();
            return false;
        }
    }
}

bool TSparsedMerger::TCursor::AddIndexTo(const ui32 index, TWriter& writer) {
    if (FinishGlobalPosition <= index) {
        InitArrays(index);
    }
    if (SparsedCursor) {
        return SparsedCursor->AddIndexTo(index, writer, Context);
    } else {
        return PlainCursor->AddIndexTo(index, writer, Context);
    }
}

void TSparsedMerger::TCursor::InitArrays(const ui32 position) {
    AFL_VERIFY(!CurrentOwnedArray || !CurrentOwnedArray->GetAddress().Contains(position));
    CurrentOwnedArray = Array->GetArray(CurrentOwnedArray, position, Array);
    if (CurrentOwnedArray->GetArray()->GetType() == NArrow::NAccessor::IChunkedArray::EType::SparsedArray) {
        auto sparsedArray = static_pointer_cast<NArrow::NAccessor::TSparsedArray>(CurrentOwnedArray->GetArray());
        SparsedCursor = std::make_shared<TSparsedChunkCursor>(sparsedArray, &*CurrentOwnedArray);
        PlainCursor = nullptr;
    } else {
        PlainCursor = make_shared<TPlainChunkCursor>(CurrentOwnedArray->GetArray(), &*CurrentOwnedArray);
        SparsedCursor = nullptr;
    }
    FinishGlobalPosition = CurrentOwnedArray->GetAddress().GetGlobalStartPosition() + CurrentOwnedArray->GetArray()->GetRecordsCount();
}

}   // namespace NKikimr::NOlap::NCompaction
