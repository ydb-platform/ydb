#include "logic.h"

#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

namespace NKikimr::NOlap::NCompaction {

void TSparsedMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& /*mergingContext*/) {
    ui32 idx = 0;
    for (auto&& p : input) {
        Cursors.emplace_back(TSparsedChunkCursor(p, Context.GetLoader(), idx++));
    }
    Inputs = input;
}

TColumnPortionResult TSparsedMerger::DoExecute(const TChunkMergeContext& chunkContext, TMergingContext& /*mergeContext*/) {
    std::shared_ptr<TWriter> writer = std::make_shared<TWriter>(Context);
    chunkContext.GetRemapper().InitReverseIndexes(Inputs);
    std::vector<TSparsedChunkCursor*> heap;
    for (ui32 idx = 0; idx < Cursors.size(); ++idx) {
        if (!Cursors[idx].IsValid()) {
            continue;
        }
        if (!Cursors[idx].InitGlobalRemapping(chunkContext.GetRemapper().GetReverseIndexes(idx), chunkContext.GetRemapper().GetOffset(),
                chunkContext.GetRemapper().GetSize())) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "skip_source")("idx", idx);
            continue;
        }
        if (chunkContext.GetRemapper().GetRecordsCount() <= Cursors[idx].GetGlobalResultIndexVerified()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "skip_source")("idx", idx);
            continue;
        }
        heap.emplace_back(&Cursors[idx]);
    }

    TSparsedChunkCursor::THeapComparator heapComparator;
    std::make_heap(heap.begin(), heap.end(), heapComparator);
    while (heap.size()) {
        std::pop_heap(heap.begin(), heap.end(), heapComparator);
        AFL_VERIFY(heap.back()->IsValid());
        AFL_VERIFY(heap.back()->GetGlobalResultIndexVerified() < chunkContext.GetRemapper().GetRecordsCount())(
                                                                   "cursor", heap.back()->DebugString())(
                                                                   "context", chunkContext.DebugString());
        AFL_VERIFY(heap.back()->AddIndexTo(*writer));
        if (!heap.back()->Next()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "stopped_source")("idx", heap.back()->GetCursorIdx());
            heap.pop_back();
        } else if (!heap.back()->GetGlobalResultIndexImpl()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "stopped_source")("idx", heap.back()->GetCursorIdx());
            heap.pop_back();
        } else if (chunkContext.GetRemapper().GetRecordsCount() <= heap.back()->GetGlobalResultIndexVerified()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "stopped_source")("idx", heap.back()->GetCursorIdx());
            heap.pop_back();
        } else {
            std::push_heap(heap.begin(), heap.end(), heapComparator);
        }
    }
    return writer->Flush(chunkContext.GetRemapper().GetRecordsCount());
}

TColumnPortionResult TSparsedMerger::TWriter::Flush(const ui32 recordsCount) {
    std::vector<std::shared_ptr<arrow::Field>> fields = { std::make_shared<arrow::Field>("index", arrow::uint32()),
        std::make_shared<arrow::Field>("value", DataType) };
    auto schema = std::make_shared<arrow::Schema>(fields);
    std::vector<std::shared_ptr<arrow::Array>> columns = { NArrow::TStatusValidator::GetValid(IndexBuilder->Finish()),
        NArrow::TStatusValidator::GetValid(ValueBuilder->Finish()) };
    AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "sparsed_flush")("count", recordsCount)("useful", UsefulRecordsCount);
    auto recordBatch = arrow::RecordBatch::Make(schema, UsefulRecordsCount, columns);
    NArrow::NAccessor::TSparsedArray::TBuilder builder(
        Context.GetIndexInfo().GetColumnFeaturesVerified(Context.GetColumnId()).GetDefaultValue().GetValue(), Context.GetResultField()->type());
    builder.AddChunk(recordsCount, recordBatch);
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

bool TSparsedMerger::TSparsedChunkCursor::AddIndexTo(TWriter& writer) {
    return writer.AddRecord(*GetCurrentDataChunk().GetSparsedChunk().GetColValue(), ScanIndex, GetGlobalResultIndexVerified());
}

bool TSparsedMerger::TSparsedChunkCursor::MoveToSignificant(const std::optional<ui32> sourceLowerBound) {
    if (ScanIndex < GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->length()) {
        AFL_VERIFY(MoveToPosition(TBase::GetGlobalPosition(GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->Value(ScanIndex))));
        if (GetGlobalResultIndexImpl().value_or(0) >= 0) {
            if (!sourceLowerBound || *sourceLowerBound <= GetGlobalPosition()) {
                return true;
            }
        }
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "skip_record")("idx", ScanIndex)("cursor_idx", CursorIdx)(
            "record_idx", GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->Value(ScanIndex))("lb", sourceLowerBound);
        ++ScanIndex;
        while (ScanIndex < GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->length()) {
            AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "skip_record")("idx", ScanIndex)("cursor_idx", CursorIdx)(
                "record_idx", GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->Value(ScanIndex))("lb", sourceLowerBound);
            AFL_VERIFY(MoveToPosition(TBase::GetGlobalPosition(GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->Value(ScanIndex))));
            if (GetGlobalResultIndexImpl().value_or(0) >= 0) {
                if (!sourceLowerBound || *sourceLowerBound <= GetGlobalPosition()) {
                    return true;
                }
            }
            ++ScanIndex;
        }
    }
    AFL_VERIFY(ScanIndex == GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->length());
    while (TBase::IsValid() && ScanIndex == GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->length()) {
        Y_UNUSED(TBase::MoveToPosition(TBase::GetGlobalPosition(GetCurrentDataChunk().GetRecordsCount())));
        ScanIndex = 0;
    }
    if (TBase::IsValid()) {
        return MoveToSignificant(sourceLowerBound);
    } else {
        return false;
    }
}

std::optional<i64> TSparsedMerger::TSparsedChunkCursor::GetGlobalResultIndexImpl() const {
    AFL_VERIFY(TBase::IsValid());
    AFL_VERIFY(RemapToGlobalResult);
    std::optional<i64> result = RemapToGlobalResult->RemapSourceIndex(GetGlobalPosition());
    if (!result) {
        return result;
    }
    if (*result < 0) {
        return result;
    }
    AFL_VERIFY(*GlobalResultOffset <= *result)("result", result)("offset", GlobalResultOffset);
    return *result - *GlobalResultOffset;
}

bool TSparsedMerger::TSparsedChunkCursor::InitGlobalRemapping(
    const TSourceReverseRemap& remapToGlobalResult, const ui32 globalResultOffset, const ui32 globalResultSize) {
    if (remapToGlobalResult.IsEmpty()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "skip_source")("reason", "empty")("idx", GetCursorIdx());
        return false;
    }
    if (globalResultOffset + globalResultSize <= remapToGlobalResult.GetMinResultIndex()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "skip_source")("reason", "too_early")("idx", GetCursorIdx());
        return false;
    }
    AFL_VERIFY(IsValid());
    GlobalResultOffset = globalResultOffset;
    RemapToGlobalResult = &remapToGlobalResult;
    {
        Y_UNUSED(MoveToSignificant(RemapToGlobalResult->GetMinSourceIndex()));
        AFL_VERIFY(RemapToGlobalResult->GetMinSourceIndex() <= GetGlobalPosition());
        if (!IsValid()) {
            return false;
        }
    }
    if (!GetGlobalResultIndexImpl()) {
        AFL_TRACE(NKikimrServices::TX_COLUMNSHARD)("event", "skip_source")("reason", "not_index")("idx", GetCursorIdx())(
            "offset", globalResultOffset)("size", globalResultSize)("debug", remapToGlobalResult.DebugString())("pos", GetGlobalPosition());
        return false;
    }
    Y_UNUSED(GetGlobalResultIndexVerified());
    return true;
}

}   // namespace NKikimr::NOlap::NCompaction
