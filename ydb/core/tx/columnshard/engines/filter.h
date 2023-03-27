#pragma once
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

std::vector<bool> MakeSnapshotFilter(std::shared_ptr<arrow::RecordBatch> batch,
                                     std::shared_ptr<arrow::Schema> snapSchema,
                                     ui64 planStep, ui64 txId);

std::vector<bool> MakeReplaceFilter(std::shared_ptr<arrow::RecordBatch> batch, THashSet<NArrow::TReplaceKey>& keys);
std::vector<bool> MakeReplaceFilterLastWins(std::shared_ptr<arrow::RecordBatch> batch, THashSet<NArrow::TReplaceKey>& keys);
#if 0
std::vector<bool> MakeReplaceFilter(std::shared_ptr<arrow::RecordBatch> batch,
                                    const THashSet<NArrow::TReplaceKey>& staticKeys,
                                    THashSet<NArrow::TReplaceKey>& keys);
#endif

void ReplaceDupKeys(std::shared_ptr<arrow::RecordBatch>& batch,
                    const std::shared_ptr<arrow::Schema>& replaceSchema, bool lastWins = false);

struct TReadMetadata;
TFilteredBatch FilterPortion(const std::shared_ptr<arrow::RecordBatch>& batch, const TReadMetadata& readMetadata);
TFilteredBatch FilterNotIndexed(const std::shared_ptr<arrow::RecordBatch>& batch, const TReadMetadata& readMetadata);
TFilteredBatch EarlyFilter(const std::shared_ptr<arrow::RecordBatch>& batch, std::shared_ptr<NSsa::TProgram> ssa);

}
