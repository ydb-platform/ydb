#pragma once
#include "range.h"
#include <deque>

namespace NKikimr::NOlap {

class TPKRangesFilter {
private:
    bool FakeRanges = true;
    std::deque<TPKRangeFilter> SortedRanges;
    bool ReverseFlag = false;
public:
    TPKRangesFilter(const bool reverse);

    bool IsEmpty() const {
        return SortedRanges.empty() || FakeRanges;
    }

    bool IsReverse() const {
        return ReverseFlag;
    }

    const TPKRangeFilter& Front() const {
        Y_ABORT_UNLESS(Size());
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
    bool IsPortionInPartialUsage(const NArrow::TReplaceKey& start, const NArrow::TReplaceKey& end, const TIndexInfo& indexInfo) const;

    NArrow::TColumnFilter BuildFilter(const arrow::Datum& data) const;

    [[nodiscard]] bool Add(std::shared_ptr<NOlap::TPredicate> f, std::shared_ptr<NOlap::TPredicate> t, const TIndexInfo* indexInfo);

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
