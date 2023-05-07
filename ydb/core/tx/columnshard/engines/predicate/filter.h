#pragma once
#include "range.h"
#include <deque>

namespace NKikimr::NOlap {

class TPKRangesFilter {
private:
    bool NotFakeRanges = false;
    std::deque<TPKRangeFilter> SortedRanges;
    bool ReverseFlag = false;
public:
    TPKRangesFilter(const bool reverse);

    bool IsReverse() const {
        return ReverseFlag;
    }

    const TPKRangeFilter& Front() const {
        Y_VERIFY(Size());
        return SortedRanges.front();
    }

    size_t Size() const {
        return SortedRanges.size();
    }

    std::deque<TPKRangeFilter>::const_iterator begin() const {
        return SortedRanges.begin();
    }

    std::deque<TPKRangeFilter>::const_iterator end() const {
        return SortedRanges.end();
    }

    bool IsPortionInUsage(const TPortionInfo& info, const TIndexInfo& indexInfo) const;

    NArrow::TColumnFilter BuildFilter(std::shared_ptr<arrow::RecordBatch> data) const;

    bool Add(std::shared_ptr<NOlap::TPredicate> f, std::shared_ptr<NOlap::TPredicate> t, const TIndexInfo* indexInfo) Y_WARN_UNUSED_RESULT;

    std::set<std::string> GetColumnNames() const {
        std::set<std::string> result;
        for (auto&& i : SortedRanges) {
            for (auto&& c : i.GetColumnNames()) {
                result.emplace(c);
            }
        }
        return result;
    }

    TString DebugString() const;

    std::set<ui32> GetColumnIds(const TIndexInfo& indexInfo) const;
};

}
