#include "logic.h"

#include <ydb/core/formats/arrow/accessor/composite/accessor.h>
#include <ydb/core/formats/arrow/accessor/plain/accessor.h>
#include <ydb/core/formats/arrow/switch/switch_type.h>
#include <ydb/core/tx/columnshard/engines/storage/chunks/column.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap::NCompaction {

void TSparsedMerger::DoStart(const std::vector<std::shared_ptr<NArrow::NAccessor::IChunkedArray>>& input, TMergingContext& mergingContext) {
    Inputs = input;
    ui32 idx = 0;
    const auto outputDefault = Context.GetIndexInfo().GetColumnFeaturesVerified(Context.GetColumnId()).GetDefaultValue().GetValue();
    for (auto&& p : Inputs) {
        std::shared_ptr<arrow::Scalar> fill = Context.GetDefaultValue();
        const auto& portionSchemas = mergingContext.GetPortionSchemas();
        const bool hasPortionSchema = idx < portionSchemas.size() && portionSchemas[idx] && Context.GetSchemaInfo();
        if (hasPortionSchema) {
            fill = ISnapshotSchema::GetAbsentColumnFill(*portionSchemas[idx], *Context.GetSchemaInfo(), Context.GetColumnId());
        }
        if (!p) {
            if (!NArrow::ScalarEqualsNullable(fill, outputDefault)) {
                const auto& containers = mergingContext.GetInputContainers();
                const ui32 rows = idx < containers.size() && containers[idx] ? containers[idx]->num_rows() : 0;
                if (rows) {
                    p = ISnapshotSchema::BuildAbsentColumnArray(Context.GetResultField()->type(), fill, rows);
                }
            }
        }
        const auto materializeMismatched = [&](std::shared_ptr<NArrow::NAccessor::IChunkedArray>& item) {
            const auto expand = [&](auto&& self, std::shared_ptr<NArrow::NAccessor::IChunkedArray>& current) -> void {
                if (!current) {
                    return;
                }
                using EType = NArrow::NAccessor::IChunkedArray::EType;
                if (current->GetType() == EType::CompositeChunkedArray) {
                    const auto composite = std::static_pointer_cast<NArrow::NAccessor::TCompositeChunkedArray>(current);
                    auto chunks = composite->GetChunks();
                    bool changed = false;
                    for (auto& chunk : chunks) {
                        const auto before = chunk;
                        self(self, chunk);
                        changed = changed || chunk != before;
                    }
                    if (changed) {
                        if (chunks.size() == 1) {
                            current = std::move(chunks.front());
                        } else {
                            current = std::make_shared<NArrow::NAccessor::TCompositeChunkedArray>(
                                std::move(chunks), current->GetRecordsCount(), current->GetDataType());
                        }
                    }
                    return;
                }
                if (current->GetType() == EType::SerializedChunkedArray) {
                    current = current->GetArraySlow(0, current).GetArray();
                    self(self, current);
                    return;
                }
                if (current->GetType() == EType::SparsedArray) {
                    const auto sparsed = std::static_pointer_cast<NArrow::NAccessor::TSparsedArray>(current);
                    if (!NArrow::ScalarEqualsNullable(sparsed->GetDefaultValue(), outputDefault)) {
                        current = std::make_shared<NArrow::NAccessor::TTrivialChunkedArray>(sparsed->GetChunkedArray());
                    }
                }
            };
            expand(expand, item);
        };
        if (!hasPortionSchema || !NArrow::ScalarEqualsNullable(fill, outputDefault)) {
            materializeMismatched(p);
        }
        Cursors.emplace_back(TSparsedChunkCursor(p, Context.GetLoader(), idx++));
    }
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
            YDB_LOG_TRACE("",
                {"event", "skip_source"},
                {"idx", idx});
            continue;
        }
        if (chunkContext.GetRemapper().GetRecordsCount() <= Cursors[idx].GetGlobalResultIndexVerified()) {
            YDB_LOG_TRACE("",
                {"event", "skip_source"},
                {"idx", idx});
            continue;
        }
        heap.emplace_back(&Cursors[idx]);
    }

    TSparsedChunkCursor::THeapComparator heapComparator;
    std::make_heap(heap.begin(), heap.end(), heapComparator);
    while (heap.size()) {
        std::pop_heap(heap.begin(), heap.end(), heapComparator);
        YDB_LOG_VERIFY(heap.back()->IsValid());
        YDB_LOG_VERIFY(heap.back()->GetGlobalResultIndexVerified() < chunkContext.GetRemapper().GetRecordsCount(),
            {"cursor", heap.back()->DebugString()},
            {"context", chunkContext.DebugString()});
        YDB_LOG_VERIFY(heap.back()->AddIndexTo(*writer));
        if (!heap.back()->Next()) {
            YDB_LOG_TRACE("",
                {"event", "stopped_source"},
                {"idx", heap.back()->GetCursorIdx()});
            heap.pop_back();
        } else if (!heap.back()->GetGlobalResultIndexImpl()) {
            YDB_LOG_TRACE("",
                {"event", "stopped_source"},
                {"idx", heap.back()->GetCursorIdx()});
            heap.pop_back();
        } else if (chunkContext.GetRemapper().GetRecordsCount() <= heap.back()->GetGlobalResultIndexVerified()) {
            YDB_LOG_TRACE("",
                {"event", "stopped_source"},
                {"idx", heap.back()->GetCursorIdx()});
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
    YDB_LOG_TRACE("",
        {"event", "sparsed_flush"},
        {"count", recordsCount},
        {"useful", UsefulRecordsCount});
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
    , Context(context)
{
    IndexBuilder = NArrow::MakeBuilder(arrow::uint32());
    ValueBuilder = NArrow::MakeBuilder(DataType);
    IndexBuilderImpl = (arrow::UInt32Builder*)(IndexBuilder.get());
}

bool TSparsedMerger::TSparsedChunkCursor::AddIndexTo(TWriter& writer) {
    return writer.AddRecord(*GetCurrentDataChunk().GetSparsedChunk().GetColValue(), ScanIndex, GetGlobalResultIndexVerified());
}

bool TSparsedMerger::TSparsedChunkCursor::MoveToSignificant(const std::optional<ui32> sourceLowerBound) {
    if (ScanIndex < GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->length()) {
        YDB_LOG_VERIFY(MoveToPosition(TBase::GetGlobalPosition(GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->Value(ScanIndex))));
        if (GetGlobalResultIndexImpl().value_or(0) >= 0) {
            if (!sourceLowerBound || *sourceLowerBound <= GetGlobalPosition()) {
                return true;
            }
        }
        YDB_LOG_TRACE("",
            {"event", "skip_record"},
            {"idx", ScanIndex},
            {"cursorIdx", CursorIdx},
            {"recordIdx", GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->Value(ScanIndex)},
            {"lb", sourceLowerBound});
        ++ScanIndex;
        while (ScanIndex < GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->length()) {
            YDB_LOG_TRACE("",
                {"event", "skip_record"},
                {"idx", ScanIndex},
                {"cursorIdx", CursorIdx},
                {"recordIdx", GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->Value(ScanIndex)},
                {"lb", sourceLowerBound});
            YDB_LOG_VERIFY(MoveToPosition(TBase::GetGlobalPosition(GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->Value(ScanIndex))));
            if (GetGlobalResultIndexImpl().value_or(0) >= 0) {
                if (!sourceLowerBound || *sourceLowerBound <= GetGlobalPosition()) {
                    return true;
                }
            }
            ++ScanIndex;
        }
    }
    YDB_LOG_VERIFY(ScanIndex == GetCurrentDataChunk().GetSparsedChunk().GetUI32ColIndex()->length());
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
    YDB_LOG_VERIFY(TBase::IsValid());
    YDB_LOG_VERIFY(RemapToGlobalResult);
    std::optional<i64> result = RemapToGlobalResult->RemapSourceIndex(GetGlobalPosition());
    if (!result) {
        return result;
    }
    if (*result < 0) {
        return result;
    }
    YDB_LOG_VERIFY(*GlobalResultOffset <= *result,
        {"result", result},
        {"offset", GlobalResultOffset});
    return *result - *GlobalResultOffset;
}

bool TSparsedMerger::TSparsedChunkCursor::InitGlobalRemapping(
    const TSourceReverseRemap& remapToGlobalResult, const ui32 globalResultOffset, const ui32 globalResultSize) {
    if (remapToGlobalResult.IsEmpty()) {
        YDB_LOG_TRACE("",
            {"event", "skip_source"},
            {"reason", "empty"},
            {"idx", GetCursorIdx()});
        return false;
    }
    if (globalResultOffset + globalResultSize <= remapToGlobalResult.GetMinResultIndex()) {
        YDB_LOG_TRACE("",
            {"event", "skip_source"},
            {"reason", "too_early"},
            {"idx", GetCursorIdx()});
        return false;
    }
    YDB_LOG_VERIFY(IsValid());
    GlobalResultOffset = globalResultOffset;
    RemapToGlobalResult = &remapToGlobalResult;
    {
        Y_UNUSED(MoveToSignificant(RemapToGlobalResult->GetMinSourceIndex()));
        YDB_LOG_VERIFY(RemapToGlobalResult->GetMinSourceIndex() <= GetGlobalPosition());
        if (!IsValid()) {
            return false;
        }
    }
    if (!GetGlobalResultIndexImpl()) {
        YDB_LOG_TRACE("",
            {"event", "skip_source"},
            {"reason", "not_index"},
            {"idx", GetCursorIdx()},
            {"offset", globalResultOffset},
            {"size", globalResultSize},
            {"debug", remapToGlobalResult.DebugString()},
            {"pos", GetGlobalPosition()});
        return false;
    }
    Y_UNUSED(GetGlobalResultIndexVerified());
    return true;
}

}   // namespace NKikimr::NOlap::NCompaction
