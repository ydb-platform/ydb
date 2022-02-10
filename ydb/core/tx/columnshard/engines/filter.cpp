#include "defs.h"
#include "filter.h"
#include "indexed_read_data.h"
#include <ydb/core/formats/arrow_helpers.h>

namespace NKikimr::NOlap {

std::vector<bool> MakeSnapshotFilter(std::shared_ptr<arrow::Table> table,
                                     std::shared_ptr<arrow::Schema> snapSchema,
                                     ui64 planStep, ui64 txId) {
    Y_VERIFY(table);
    Y_VERIFY(snapSchema);
    Y_VERIFY(snapSchema->num_fields() == 2);

    std::vector<std::shared_ptr<arrow::ChunkedArray>> snapColumns;
    snapColumns.reserve(snapSchema->num_fields());
    for (auto& field : snapSchema->fields()) {
        snapColumns.push_back(table->GetColumnByName(field->name()));
        Y_VERIFY(snapColumns.back());
    }

    bool alwaysTrue = true;
    std::vector<bool> bits;
    bits.reserve(snapColumns[0]->length());

    for (int ch = 0; ch < snapColumns[0]->num_chunks(); ++ch) {
        auto steps = snapColumns[0]->chunk(ch);
        auto ids = snapColumns[1]->chunk(ch);
        Y_VERIFY(steps->length() == ids->length());

        const auto* rawSteps = std::static_pointer_cast<arrow::UInt64Array>(steps)->raw_values();
        const auto* rawIds = std::static_pointer_cast<arrow::UInt64Array>(ids)->raw_values();

        for (int i = 0; i < steps->length(); ++i) {
            bool value = snapLessOrEqual(rawSteps[i], rawIds[i], planStep, txId);
            alwaysTrue = alwaysTrue && value;
            bits.push_back(value);
        }
    }

    // Optimization: do not need filter if it's const true.
    if (alwaysTrue) {
        return {};
    }
    return bits;
}

std::vector<bool> MakeReplaceFilter(std::shared_ptr<arrow::RecordBatch> batch,
                                    THashSet<NArrow::TReplaceKey>& keys) {
    bool alwaysTrue = true;
    std::vector<bool> bits;
    bits.reserve(batch->num_rows());

    auto columns = std::make_shared<NArrow::TArrayVec>(batch->columns());

    for (int i = 0; i < batch->num_rows(); ++i) {
        NArrow::TReplaceKey key(columns, i);
        bool keep = !keys.count(key);
        if (keep) {
            keys.emplace(key);
        }

        bits.push_back(keep);
        alwaysTrue = alwaysTrue && keep;
    }

    // Optimization: do not need filter if it's const true.
    if (alwaysTrue) {
        return {};
    }
    return bits;
}

std::vector<bool> MakeReplaceFilterLastWins(std::shared_ptr<arrow::RecordBatch> batch,
                                            THashSet<NArrow::TReplaceKey>& keys) {
    if (!batch->num_rows()) {
        return {};
    }

    bool alwaysTrue = true;
    std::vector<bool> bits;
    bits.resize(batch->num_rows());

    auto columns = std::make_shared<NArrow::TArrayVec>(batch->columns());

    for (int i = batch->num_rows() - 1; i >= 0; --i) {
        NArrow::TReplaceKey key(columns, i);
        bool keep = !keys.count(key);
        if (keep) {
            keys.emplace(key);
        }

        bits[i] = keep;
        alwaysTrue = alwaysTrue && keep;
    }

    // Optimization: do not need filter if it's const true.
    if (alwaysTrue) {
        return {};
    }
    return bits;
}

std::shared_ptr<arrow::RecordBatch> FilterPortion(std::shared_ptr<arrow::Table> portion,
                                                  const TReadMetadata& readMetadata) {
    Y_VERIFY(portion);
    std::vector<bool> snapFilter;
    if (readMetadata.PlanStep) {
        auto snapSchema = TIndexInfo::ArrowSchemaSnapshot();
        snapFilter = MakeSnapshotFilter(portion, snapSchema, readMetadata.PlanStep, readMetadata.TxId);
    }

    std::vector<bool> less;
    if (readMetadata.LessPredicate) {
        auto cmpType = readMetadata.LessPredicate->Inclusive ?
            NArrow::ECompareType::LESS_OR_EQUAL : NArrow::ECompareType::LESS;
        less = NArrow::MakePredicateFilter(portion, readMetadata.LessPredicate->Batch, cmpType);
    }

    std::vector<bool> greater;
    if (readMetadata.GreaterPredicate) {
        auto cmpType = readMetadata.GreaterPredicate->Inclusive ?
            NArrow::ECompareType::GREATER_OR_EQUAL : NArrow::ECompareType::GREATER;
        greater = NArrow::MakePredicateFilter(portion, readMetadata.GreaterPredicate->Batch, cmpType);
    }

    std::vector<bool> bits = NArrow::CombineFilters(std::move(snapFilter),
                                                    NArrow::CombineFilters(std::move(less), std::move(greater)));
    if (bits.size()) {
        auto res = arrow::compute::Filter(portion, NArrow::MakeFilter(bits));
        Y_VERIFY_S(res.ok(), res.status().message());
        Y_VERIFY((*res).kind() == arrow::Datum::TABLE);
        portion = (*res).table();
    }

    Y_VERIFY(portion);
    if (!portion->num_rows()) {
        // TableBatchReader return nullptr in case of empty table. We need a valid batch with 0 rows.
        return NArrow::MakeEmptyBatch(portion->schema());
    }

    auto res = portion->CombineChunks();
    Y_VERIFY(res.ok());

    arrow::TableBatchReader reader(*portion);
    auto result = reader.Next();
    Y_VERIFY(result.ok());
    auto batch = *result;
    result = reader.Next();
    Y_VERIFY(result.ok() && !(*result));
    return batch;
}

void ReplaceDupKeys(std::shared_ptr<arrow::RecordBatch>& batch,
                    const std::shared_ptr<arrow::Schema>& replaceSchema, bool lastWins) {
    THashSet<NArrow::TReplaceKey> replaces;

    auto keyBatch = NArrow::ExtractColumns(batch, replaceSchema);

    std::vector<bool> bits;
    if (lastWins) {
        bits = MakeReplaceFilterLastWins(keyBatch, replaces);
    } else {
        bits = MakeReplaceFilter(keyBatch, replaces);
    }
    if (!bits.empty()) {
        auto res = arrow::compute::Filter(batch, NArrow::MakeFilter(bits));
        Y_VERIFY_S(res.ok(), res.status().message());
        Y_VERIFY((*res).kind() == arrow::Datum::RECORD_BATCH);
        batch = (*res).record_batch();
        Y_VERIFY(batch);
    }
}

}
