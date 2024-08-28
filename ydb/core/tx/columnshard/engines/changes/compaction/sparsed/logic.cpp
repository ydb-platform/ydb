#include "logic.h"

#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction {

void TSparsedMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergingContext) {
    ui32 idx = 0;
    for (auto&& p : input) {
        if (p) {
            Cursors.emplace_back(p, Context);
            if (mergingContext.HasRemapInfo(idx)) {
                CursorPositions.emplace_back(TCursorPosition(&Cursors.back(), mergingContext.GetRemapPortionIndexToResultIndex(idx)));
                if (CursorPositions.back().IsFinished()) {
                    CursorPositions.pop_back();
                }
            }
        }
        ++idx;
    }
}

std::vector<TColumnPortionResult> TSparsedMerger::DoExecute(const TChunkMergeContext& chunkContext, TMergingContext& /*mergeContext*/) {
    std::vector<TColumnPortionResult> result;
    std::shared_ptr<TWriter> writer = std::make_shared<TWriter>(Context);
    const auto addSkipsToWriter = [&](i64 delta) {
        if (!delta) {
            return;
        }
        AFL_VERIFY(delta >= 0);
        if (chunkContext.GetPortionRowsCountLimit() <= writer->GetCurrentSize() + delta) {
            const i64 diff = chunkContext.GetPortionRowsCountLimit() - writer->GetCurrentSize();
            writer->AddPositions(diff);
            result.emplace_back(writer->Flush());
            writer = std::make_shared<TWriter>(Context);
            delta -= diff;
        }
        while (chunkContext.GetPortionRowsCountLimit() <= delta) {
            writer->AddPositions(chunkContext.GetPortionRowsCountLimit());
            result.emplace_back(writer->Flush());
            writer = std::make_shared<TWriter>(Context);
            delta -= chunkContext.GetPortionRowsCountLimit();
        }
        if (delta) {
            writer->AddPositions(delta);
        }
    };

    std::vector<TCursorPosition> heap;
    for (auto it = CursorPositions.begin(); it != CursorPositions.end();) {
        AFL_VERIFY(chunkContext.GetBatchIdx() <= it->GetCurrentGlobalChunkIdx());
        if (it->GetCurrentGlobalChunkIdx() == chunkContext.GetBatchIdx()) {
            heap.emplace_back(std::move(*it));
            it = CursorPositions.erase(it);
        } else {
            ++it;
        }
    }
    std::make_heap(heap.begin(), heap.end());
    ui32 nextGlobalPosition = 0;
    while (heap.size()) {
        std::pop_heap(heap.begin(), heap.end());
        while (heap.size() == 1 || (heap.size() > 1 && heap.front() < heap.back())) {
            AFL_VERIFY(nextGlobalPosition <= heap.back().GetCurrentGlobalPosition());
            addSkipsToWriter(heap.back().GetCurrentGlobalPosition() - nextGlobalPosition);

            heap.back().AddIndexTo(*writer);
            if (chunkContext.GetPortionRowsCountLimit() == writer->GetCurrentSize()) {
                result.emplace_back(writer->Flush());
                writer = std::make_shared<TWriter>(Context);
            }
            nextGlobalPosition = heap.back().GetCurrentGlobalPosition() + 1;
            if (!heap.back().Next()) {
                heap.pop_back();
                break;
            } else if (heap.back().GetCurrentGlobalChunkIdx() != chunkContext.GetBatchIdx()) {
                CursorPositions.emplace_back(std::move(heap.back()));
                heap.pop_back();
                break;
            }
        }
        std::push_heap(heap.begin(), heap.end());
    }
    AFL_VERIFY(nextGlobalPosition <= chunkContext.GetRecordsCount());
    addSkipsToWriter(chunkContext.GetRecordsCount() - nextGlobalPosition);
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
    ++CurrentRecordIdx;
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

bool TSparsedMerger::TPlainChunkCursor::AddIndexTo(const ui32 index, TWriter& writer) {
    AFL_VERIFY(ChunkStartPosition <= index);
    writer.AddRealData(ChunkAddress->GetArray(), index - ChunkStartPosition);
    return true;
}

bool TSparsedMerger::TSparsedChunkCursor::AddIndexTo(const ui32 index, TWriter& writer) {
    AFL_VERIFY(ChunkStartGlobalPosition <= index);
    AFL_VERIFY(index == NextGlobalPosition);
    writer.AddRealData(Chunk->GetColValue(), NextLocalPosition);
    return true;
}

bool TSparsedMerger::TCursor::AddIndexTo(const ui32 index, TWriter& writer) {
    if (FinishGlobalPosition <= index) {
        InitArrays(index);
    }
    if (SparsedCursor) {
        return SparsedCursor->AddIndexTo(index, writer);
    } else if (PlainCursor) {
        return PlainCursor->AddIndexTo(index, writer);
    } else {
        return false;
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
