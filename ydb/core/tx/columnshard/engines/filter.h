#pragma once

#include <ydb/core/formats/program.h>
#include <ydb/core/formats/replace_key.h>

namespace NKikimr::NOlap {

struct TFilteredBatch {
    std::shared_ptr<arrow::RecordBatch> Batch;
    std::vector<bool> Filter;

    bool Valid() const {
        if (Batch) {
            return Filter.empty() || (Filter.size() == (size_t)Batch->num_rows());
        }
        return false;
    }

    void ApplyFilter();
};

std::vector<bool> MakeSnapshotFilter(const std::shared_ptr<arrow::RecordBatch>& batch,
                                     const std::shared_ptr<arrow::Schema>& snapSchema,
                                     ui64 planStep, ui64 txId);

std::vector<bool> MakeReplaceFilter(const std::shared_ptr<arrow::RecordBatch>& batch, THashSet<NArrow::TReplaceKey>& keys);
std::vector<bool> MakeReplaceFilterLastWins(const std::shared_ptr<arrow::RecordBatch>& batch, THashSet<NArrow::TReplaceKey>& keys);

void ReplaceDupKeys(std::shared_ptr<arrow::RecordBatch>& batch,
                    const std::shared_ptr<arrow::Schema>& replaceSchema, bool lastWins = false);

struct TReadMetadata;
TFilteredBatch FilterPortion(const std::shared_ptr<arrow::RecordBatch>& batch, const TReadMetadata& readMetadata);
TFilteredBatch FilterNotIndexed(const std::shared_ptr<arrow::RecordBatch>& batch, const TReadMetadata& readMetadata);
TFilteredBatch EarlyFilter(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<NSsa::TProgram> ssa);

} // namespace NKikimr::NOlap
