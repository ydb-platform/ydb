#include "mark_granules.h"

namespace NKikimr::NOlap {
TMarksGranules::TMarksGranules(std::vector<TPair>&& marks) noexcept
    : Marks(std::move(marks)) {
    Y_VERIFY_DEBUG(std::is_sorted(Marks.begin(), Marks.end()));
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

THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> TMarksGranules::SliceIntoGranules(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const TIndexInfo& indexInfo)
{
    return SliceIntoGranules(batch, Marks, indexInfo);
}

THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> TMarksGranules::SliceIntoGranules(const std::shared_ptr<arrow::RecordBatch>& batch, const std::vector<std::pair<TMark, ui64>>& granules, const TIndexInfo& indexInfo) {
    Y_VERIFY(batch);
    if (batch->num_rows() == 0) {
        return {};
    }

    THashMap<ui64, std::shared_ptr<arrow::RecordBatch>> out;

    if (granules.size() == 1) {
        out.emplace(granules[0].second, batch);
    } else {
        const auto effKey = GetEffectiveKey(batch, indexInfo);
        Y_VERIFY(effKey->num_columns() && effKey->num_rows());

        std::vector<NArrow::TRawReplaceKey> keys;
        {
            const auto& columns = effKey->columns();
            keys.reserve(effKey->num_rows());
            for (i64 i = 0; i < effKey->num_rows(); ++i) {
                keys.emplace_back(NArrow::TRawReplaceKey(&columns, i));
            }
        }

        i64 offset = 0;
        for (size_t i = 0; i < granules.size() && offset < effKey->num_rows(); ++i) {
            const i64 end = (i + 1 == granules.size())
                // Just take the number of elements in the key column for the last granule.
                ? effKey->num_rows()
                // Locate position of the next granule in the key.
                : NArrow::LowerBound(keys, granules[i + 1].first.GetBorder(), offset);

            if (const i64 size = end - offset) {
                Y_VERIFY(out.emplace(granules[i].second, batch->Slice(offset, size)).second);
            }

            offset = end;
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
