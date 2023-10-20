#include "mark_granules.h"

namespace NKikimr::NOlap {
TMarksGranules::TMarksGranules(std::vector<TPair>&& marks) noexcept
    : Marks(std::move(marks)) {
    Y_DEBUG_ABORT_UNLESS(std::is_sorted(Marks.begin(), Marks.end()));
}

TMarksGranules::TMarksGranules(std::vector<TMark>&& points) {
    std::sort(points.begin(), points.end());

    Marks.reserve(points.size());

    for (size_t i = 0, end = points.size(); i != end; ++i) {
        Marks.emplace_back(std::move(points[i]), i + 1);
    }
}

TMarksGranules::TMarksGranules(const TSelectInfo& selectInfo) {
    Marks.reserve(selectInfo.Granules.size());

    for (const auto& rec : selectInfo.Granules) {
        Marks.emplace_back(std::make_pair(rec.Mark, rec.Granule));
    }

    std::sort(Marks.begin(), Marks.end(), [](const TPair& a, const TPair& b) {
        return a.first < b.first;
        });
}

bool TMarksGranules::MakePrecedingMark(const TIndexInfo& indexInfo) {
    ui64 minGranule = 0;
    TMark minMark(TMark::MinBorder(indexInfo.GetIndexKey()));
    if (Marks.empty()) {
        Marks.emplace_back(std::move(minMark), minGranule);
        return true;
    }

    if (minMark < Marks[0].first) {
        std::vector<TPair> copy;
        copy.reserve(Marks.size() + 1);
        copy.emplace_back(std::move(minMark), minGranule);
        for (auto&& [mark, granule] : Marks) {
            copy.emplace_back(std::move(mark), granule);
        }
        Marks.swap(copy);
        return true;
    }
    return false;
}

THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> TMarksGranules::SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch, const std::map<NArrow::TReplaceKey, ui64>& granules, const TIndexInfo& indexInfo) {
    Y_ABORT_UNLESS(batch);
    if (batch->num_rows() == 0) {
        return {};
    }
    THashMap<ui64, std::vector<std::shared_ptr<arrow::RecordBatch>>> out;

    if (granules.size() == 1) {
        out[granules.begin()->second].emplace_back(batch);
    } else {
        const auto effKey = GetEffectiveKey(batch, indexInfo);
        Y_ABORT_UNLESS(effKey->num_columns() && effKey->num_rows());

        std::vector<NArrow::TRawReplaceKey> keys;
        {
            const auto& columns = effKey->columns();
            keys.reserve(effKey->num_rows());
            for (i64 i = 0; i < effKey->num_rows(); ++i) {
                keys.emplace_back(NArrow::TRawReplaceKey(&columns, i));
            }
        }

        i64 offset = 0;
        auto itNext = granules.begin();
        ++itNext;
        for (auto it = granules.begin(); it != granules.end() && offset < effKey->num_rows(); ++it) {
            const i64 end = (itNext == granules.end())
                // Just take the number of elements in the key column for the last granule.
                ? effKey->num_rows()
                // Locate position of the next granule in the key.
                : NArrow::TReplaceKeyHelper::LowerBound(keys, itNext->first, offset);

            if (const i64 size = end - offset) {
                out[it->second].emplace_back(batch->Slice(offset, size));
            }

            offset = end;
            if (itNext != granules.end()) {
                ++itNext;
            }
        }
    }
    return out;
}

std::shared_ptr<arrow::RecordBatch> TMarksGranules::GetEffectiveKey(const std::shared_ptr<arrow::RecordBatch>& batch, const TIndexInfo& indexInfo) {
    const auto& key = indexInfo.GetIndexKey();
    auto resBatch = NArrow::ExtractColumns(batch, key);
    Y_VERIFY_S(resBatch, "Cannot extract effective key " << key->ToString()
        << " from batch " << batch->schema()->ToString());
    return resBatch;
}

}
