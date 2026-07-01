#include "sort_merger.h"

#include "abstract/merger.h"

#include <ydb/core/tx/columnshard/engines/scheme/abstract/index_info.h>

#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/c/bridge.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/array/array_base.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/builder.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/c/bridge.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/chunked_array.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/api_vector.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/exec.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/compute/function.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/record_batch.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/table.h>
#include <contrib/libs/apache/arrow_next/cpp/src/arrow/type.h>

namespace NKikimr::NOlap::NCompaction {

namespace {

// Bridge a single old-Arrow array to Arrow 20 via the C Data Interface (same pattern as
// NArrow::NAccessor::TMinMax::Compute in accessor/abstract/minmax_utils.cpp).
std::shared_ptr<arrow20::Array> ToArrow20(const std::shared_ptr<arrow::Array>& arr) {
    struct ArrowArray cArray;
    struct ArrowSchema cSchema;
    auto status = arrow::ExportArray(*arr, &cArray, &cSchema);
    AFL_VERIFY(status.ok())("error", status.ToString());
    return arrow20::ImportArray(&cArray, &cSchema).ValueOrDie();
}

std::shared_ptr<arrow::Array> ToArrow5(const std::shared_ptr<arrow20::Array>& arr) {
    struct ArrowArray cArray;
    struct ArrowSchema cSchema;
    auto status = arrow20::ExportArray(*arr, &cArray, &cSchema);
    AFL_VERIFY(status.ok())("error", status.ToString());
    return arrow::ImportArray(&cArray, &cSchema).ValueOrDie();
}

// Per-source accept bitmap (old Arrow): replicates TBatchIterator::IsDeleted semantics — a row that
// the filter rejects must not become a surviving record. We carry it as a column and drop the rejected
// rows only *after* deduplication, so a rejected max-version winner removes the whole PK (rather than
// letting a lower version take its place).
std::shared_ptr<arrow::Array> BuildAcceptArray(const std::shared_ptr<NArrow::TColumnFilter>& filter, const ui32 numRows) {
    if (!filter || filter->IsTotalAllowFilter()) {
        return arrow::MakeArrayFromScalar(arrow::BooleanScalar(true), numRows).ValueOrDie();
    }
    if (filter->IsTotalDenyFilter()) {
        return arrow::MakeArrayFromScalar(arrow::BooleanScalar(false), numRows).ValueOrDie();
    }
    return filter->BuildArrowFilter(numRows);
}

std::shared_ptr<arrow::RecordBatch> ProjectToIndexFields(
    const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::shared_ptr<arrow::Field>>& indexFields) {
    std::vector<std::shared_ptr<arrow::Array>> columns;
    columns.reserve(indexFields.size());
    for (auto&& f : indexFields) {
        auto column = batch->GetColumnByName(f->name());
        AFL_VERIFY(column)("field", f->name());
        columns.emplace_back(column);
    }
    return arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(indexFields), batch->num_rows(), std::move(columns));
}

}   // namespace

std::vector<std::shared_ptr<arrow::RecordBatch>> TSortIndicesMerger::BuildRemapper(
    const std::vector<std::shared_ptr<NArrow::TGeneralContainer>>& batches, const std::vector<std::shared_ptr<NArrow::TColumnFilter>>& filters,
    const std::shared_ptr<arrow::Schema>& replaceKey, const std::vector<std::string>& snapshotColumnNames,
    const std::vector<std::shared_ptr<arrow::Field>>& indexFields, const NArrow::NMerger::TIntervalPositions& checkPoints) {
    AFL_VERIFY(batches.size() == filters.size());

    static const std::string AcceptFieldName = "$$__sort_merge_accept";
    const std::string portionIdName(IColumnMerger::PortionIdFieldName.data(), IColumnMerger::PortionIdFieldName.size());
    const std::string portionRecordIdxName(IColumnMerger::PortionRecordIndexFieldName.data(), IColumnMerger::PortionRecordIndexFieldName.size());
    const std::vector<std::string> pkNames = replaceKey->field_names();

    std::set<std::string> neededColumns(pkNames.begin(), pkNames.end());
    neededColumns.insert(snapshotColumnNames.begin(), snapshotColumnNames.end());
    neededColumns.insert(portionIdName);
    neededColumns.insert(portionRecordIdxName);
    for (auto&& f : indexFields) {
        if (f->name() == IIndexInfo::GetDeleteFlagColumnName()) {
            neededColumns.insert(f->name());
        }
    }

    // 1) materialize the needed columns of each source as a single-chunk Arrow 20 record batch, with the
    //    per-source accept bitmap appended as the last column.
    std::vector<std::shared_ptr<arrow20::RecordBatch>> sources20;
    sources20.reserve(batches.size());
    for (ui32 idx = 0; idx < batches.size(); ++idx) {
        const auto& container = batches[idx];
        const ui32 numRows = container->num_rows();
        if (numRows == 0) {
            continue;
        }
        auto table = container->BuildTableVerified(NArrow::TGeneralContainer::TTableConstructionContext(neededColumns));
        auto batch5 = NArrow::ToBatch(table);

        std::vector<std::shared_ptr<arrow20::Array>> columns20;
        std::vector<std::shared_ptr<arrow20::Field>> fields20;
        columns20.reserve(batch5->num_columns() + 1);
        fields20.reserve(batch5->num_columns() + 1);
        for (int c = 0; c < batch5->num_columns(); ++c) {
            auto column20 = ToArrow20(batch5->column(c));
            fields20.emplace_back(arrow20::field(batch5->schema()->field(c)->name(), column20->type()));
            columns20.emplace_back(std::move(column20));
        }
        auto accept20 = ToArrow20(BuildAcceptArray(filters[idx], numRows));
        fields20.emplace_back(arrow20::field(AcceptFieldName, accept20->type()));
        columns20.emplace_back(std::move(accept20));

        sources20.emplace_back(arrow20::RecordBatch::Make(arrow20::schema(fields20), numRows, std::move(columns20)));
    }
    if (sources20.empty()) {
        return {};
    }

    // 2) concat to a single record batch (multi-chunk Table sort is unreliable in this build) + sort_indices + take.
    auto combined = arrow20::Table::FromRecordBatches(sources20).ValueOrDie();
    if (combined->num_rows() == 0) {
        return {};
    }

    std::vector<arrow20::compute::SortKey> sortKeys;
    // pkNames.
    for (auto&& name : pkNames) {
        sortKeys.emplace_back(arrow20::FieldRef(name), arrow20::compute::SortOrder::Ascending);
    }
    for (auto&& name : snapshotColumnNames) {
        sortKeys.emplace_back(arrow20::FieldRef(name), arrow20::compute::SortOrder::Descending);
    }
    // deterministic tie-break for rows equal on PK and version (allowed by PossibleSameVersion semantics).
    sortKeys.emplace_back(arrow20::FieldRef(portionIdName), arrow20::compute::SortOrder::Ascending);
    sortKeys.emplace_back(arrow20::FieldRef(portionRecordIdxName), arrow20::compute::SortOrder::Ascending);
    arrow20::compute::SortOptions sortOptions(sortKeys);

    auto indices = arrow20::compute::SortIndices(arrow20::Datum(combined), sortOptions).ValueOrDie();
    auto sortedTable = arrow20::compute::Take(arrow20::Datum(combined), arrow20::Datum(indices)).ValueOrDie().table();
    // auto sortedTable = arrow20::Table::FromRecordBatches(sorted->schema(), { sorted }).ValueOrDie();
    arrow20::TableBatchReader reader(*sortedTable);
    std::shared_ptr<arrow20::RecordBatch> sortedBatch;

    // Read the first batch
    auto status = reader.ReadNext(&sortedBatch);
    AFL_VERIFY(status.ok());
    AFL_VERIFY(sortedBatch->num_rows() == sortedTable->num_rows());

    // auto sortedBatch = sorted->chunks(0);

    // 3) deduplicate: keep the first row of each PK group (== max version winner).
    if (sortedBatch->num_rows() > 1) {
        auto allButFirst = sortedBatch->Slice(1);
        auto allButLast = sortedBatch->Slice(0, sortedBatch->num_rows() - 1);
        arrow20::Datum changed;
        for (ui32 i = 0; i < pkNames.size(); ++i) {
            auto keyChanged = arrow20::compute::CallFunction(
                "not_equal", { allButFirst->GetColumnByName(pkNames[i]), allButLast->GetColumnByName(pkNames[i]) })
                                  .ValueOrDie();
            changed = (i == 0) ? keyChanged : arrow20::compute::CallFunction("or", { changed, keyChanged }).ValueOrDie();
        }
        arrow20::BooleanBuilder headBuilder;
        AFL_VERIFY(headBuilder.Append(true).ok());
        std::shared_ptr<arrow20::Array> head;
        AFL_VERIFY(headBuilder.Finish(&head).ok());
        std::vector<std::shared_ptr<arrow20::Array>> chunks;
        chunks.push_back(head);
        // CallFunction over Array inputs (RecordBatch::GetColumnByName yields Arrays, not ChunkedArrays)
        // produces an Array Datum, so handle both kinds rather than assuming a chunked_array().
        if (changed.is_array()) {
            chunks.push_back(changed.make_array());
        } else {
            for (auto&& chunk : changed.chunked_array()->chunks()) {
                chunks.push_back(chunk);
            }
        }
        auto keepBitmap = arrow20::ChunkedArray::Make(chunks).ValueOrDie();
        auto keepIndices = arrow20::compute::CallFunction("indices_nonzero", { keepBitmap }).ValueOrDie();
        sortedBatch = arrow20::compute::Take(arrow20::Datum(sortedBatch), keepIndices).ValueOrDie().record_batch();
    }

    // 4) drop rows rejected by their source filter (the surviving winner of a PK). Filter the deduplicated
    //    batch from step 3 (not the original sorted table), otherwise the deduplication would be discarded.
    auto deduped = arrow20::compute::Filter(arrow20::Datum(sortedBatch), arrow20::Datum(sortedBatch->GetColumnByName(AcceptFieldName)))
                       .ValueOrDie()
                       .record_batch();
    if (deduped->num_rows() == 0) {
        return {};
    }

    // 5) convert back to a single old-Arrow record batch (carries PK columns, used for checkpoint cutting).
    std::vector<std::shared_ptr<arrow::Array>> backColumns;
    std::vector<std::shared_ptr<arrow::Field>> backFields;
    backColumns.reserve(deduped->num_columns());
    backFields.reserve(deduped->num_columns());
    for (int c = 0; c < deduped->num_columns(); ++c) {
        auto column5 = ToArrow5(deduped->column(c));
        backFields.emplace_back(arrow::field(deduped->schema()->field(c)->name(), column5->type()));
        backColumns.emplace_back(std::move(column5));
    }
    auto resultBatch = arrow::RecordBatch::Make(std::make_shared<arrow::Schema>(backFields), deduped->num_rows(), std::move(backColumns));

    // 6) split at checkpoint PK boundaries, projecting each slice to exactly indexFields (matches DrainAllParts).
    std::vector<std::shared_ptr<arrow::RecordBatch>> result;
    const ui32 totalRows = resultBatch->num_rows();
    ui32 prevCut = 0;
    const auto emit = [&](const ui32 from, const ui32 to) {
        if (to <= from) {
            return;
        }
        result.emplace_back(ProjectToIndexFields(resultBatch->Slice(from, to - from), indexFields));
    };
    for (auto&& intervalPosition : checkPoints) {
        if (prevCut >= totalRows) {
            break;
        }
        auto found = NArrow::NMerger::TSortableBatchPosition::FindBound(
            resultBatch, intervalPosition.GetPosition(), intervalPosition.IsIncludedToLeftInterval(), prevCut);
        const ui32 cut = found ? found->GetPosition() : totalRows;
        emit(prevCut, cut);
        prevCut = cut;
    }
    emit(prevCut, totalRows);
    return result;
}

}   // namespace NKikimr::NOlap::NCompaction
