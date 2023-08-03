#include "granule_preparation.h"

namespace NKikimr::NOlap::NIndexedReader {

std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> TTaskGranulePreparation::GroupInKeyRanges(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, const TIndexInfo& indexInfo) {
    std::vector<std::vector<std::shared_ptr<arrow::RecordBatch>>> rangesSlices; // rangesSlices[rangeNo][sliceNo]
    rangesSlices.reserve(batches.size());
    {
        TMap<NArrow::TReplaceKey, std::vector<std::shared_ptr<arrow::RecordBatch>>> points;

        for (auto& batch : batches) {
            auto compositeKey = NArrow::ExtractColumns(batch, indexInfo.GetReplaceKey());
            Y_VERIFY(compositeKey && compositeKey->num_rows() > 0);
            auto keyColumns = std::make_shared<NArrow::TArrayVec>(compositeKey->columns());

            NArrow::TReplaceKey min(keyColumns, 0);
            NArrow::TReplaceKey max(keyColumns, compositeKey->num_rows() - 1);

            points[min].push_back(batch); // insert start
            points[max].push_back({}); // insert end
        }

        int sum = 0;
        for (auto& [key, vec] : points) {
            if (!sum) { // count(start) == count(end), start new range
                rangesSlices.push_back({});
                rangesSlices.back().reserve(batches.size());
            }

            for (auto& batch : vec) {
                if (batch) {
                    ++sum;
                    rangesSlices.back().push_back(batch);
                } else {
                    --sum;
                }
            }
        }
    }
    return rangesSlices;
}

std::vector<std::shared_ptr<arrow::RecordBatch>> TTaskGranulePreparation::SpecialMergeSorted(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches, const TIndexInfo& indexInfo, const std::shared_ptr<NArrow::TSortDescription>& description, const THashSet<const void*>& batchesToDedup) {
    auto rangesSlices = GroupInKeyRanges(batches, indexInfo);

    // Merge slices in ranges
    std::vector<std::shared_ptr<arrow::RecordBatch>> out;
    out.reserve(rangesSlices.size());
    for (auto& slices : rangesSlices) {
        if (slices.empty()) {
            continue;
        }

        // Do not merge slice if it's alone in its key range
        if (slices.size() == 1) {
            auto batch = slices[0];
            if (batchesToDedup.count(batch.get())) {
                NArrow::DedupSortedBatch(batch, description->ReplaceKey, out);
            } else {
                Y_VERIFY_DEBUG(NArrow::IsSortedAndUnique(batch, description->ReplaceKey));
                out.push_back(batch);
            }
            continue;
        }
        auto batch = NArrow::CombineSortedBatches(slices, description);
        out.push_back(batch);
    }

    return out;
}

bool TTaskGranulePreparation::DoApply(NOlap::NIndexedReader::TGranulesFillingContext& indexedDataRead) const {
    indexedDataRead.GetGranuleVerified(GranuleId)->OnGranuleDataPrepared(std::move(BatchesInGranule));
    return true;
}

bool TTaskGranulePreparation::DoExecuteImpl() {
    auto& indexInfo = ReadMetadata->GetIndexInfo();
    for (auto& batch : BatchesInGranule) {
        Y_VERIFY(batch->num_rows());
        Y_VERIFY_DEBUG(NArrow::IsSorted(batch, indexInfo.SortReplaceDescription()->ReplaceKey));
    }
    BatchesInGranule = SpecialMergeSorted(BatchesInGranule, indexInfo, indexInfo.SortReplaceDescription(), BatchesToDedup);
    return true;
}

}
