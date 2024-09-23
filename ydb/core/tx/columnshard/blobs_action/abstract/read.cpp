#include "read.h"
#include <ydb/library/actors/core/log.h>
#include <util/string/join.h>

namespace NKikimr::NOlap {

void IBlobsReadingAction::StartReading(std::vector<TBlobRange>&& ranges) {
    AFL_VERIFY(ranges.size());
    AFL_VERIFY(Counters);
    for (auto&& i : ranges) {
        Counters->OnRequest(i.Size);
    }
    THashSet<TBlobRange> result;
    Groups = GroupBlobsForOptimization(std::move(ranges));
    for (auto&& [range, _] :Groups) {
        result.emplace(range);
    }
    return DoStartReading(std::move(result));
}

void IBlobsReadingAction::Start(const THashSet<TBlobRange>& rangesInProgress) {
    Y_ABORT_UNLESS(!Started);
    Started = true;

    Y_ABORT_UNLESS(RangesForRead.size() + RangesForResult.size());
    StartWaitingRanges = TMonotonic::Now();
    WaitingRangesCount = RangesForRead.size();
    std::vector<TBlobRange> rangesFiltered;
    if (rangesInProgress.empty()) {
        rangesFiltered.insert(rangesFiltered.end(), RangesForRead.begin(), RangesForRead.end());
    } else {
        for (auto&& r : RangesForRead) {
            if (!rangesInProgress.contains(r)) {
                rangesFiltered.emplace_back(r);
            }
        }
    }
    if (rangesFiltered.size()) {
        StartReading(std::move(rangesFiltered));
    }
    for (auto&& i : RangesForResult) {
        AFL_VERIFY(i.second.size() == i.first.Size);
        AFL_VERIFY(Replies.emplace(i.first, i.second).second);
    }
}

void IBlobsReadingAction::OnReadResult(const TBlobRange& range, const TString& data) {
    auto it = Groups.find(range);
    AFL_VERIFY(it != Groups.end());
    AFL_VERIFY(Counters);
    WaitingRangesCount -= it->second.size();
    AFL_VERIFY(WaitingRangesCount >= 0);
    Counters->OnReply(range.Size, TMonotonic::Now() - StartWaitingRanges);
    AFL_VERIFY(data.size() == range.Size);
    for (auto&& i : it->second) {
        AFL_VERIFY(i.Offset + i.GetBlobSize() <= range.Offset + data.size());
        AFL_VERIFY(range.Offset <= i.Offset);
        Replies.emplace(i, data.substr(i.Offset - range.Offset, i.GetBlobSize()));
    }
    Groups.erase(it);
}

void IBlobsReadingAction::OnReadError(const TBlobRange& range, const TErrorStatus& replyStatus) {
    auto it = Groups.find(range);
    AFL_VERIFY(it != Groups.end());

    AFL_VERIFY(Counters);
    WaitingRangesCount -= it->second.size();
    AFL_VERIFY(WaitingRangesCount >= 0);
    Counters->OnFail(range.Size, TMonotonic::Now() - StartWaitingRanges);
    for (auto&& i : it->second) {
        Fails.emplace(i, replyStatus);
    }
    Groups.erase(it);
}

void IBlobsReadingAction::AddRange(const TBlobRange& range, const std::optional<TString>& result /*= {}*/) {
    Y_ABORT_UNLESS(!Started);
    if (!result) {
        AFL_VERIFY(!RangesForResult.contains(range));
        AFL_VERIFY(RangesForRead.emplace(range).second)("range", range.ToString());
    } else {
        AFL_VERIFY(result->size() == range.Size);
        AFL_VERIFY(RangesForResult.emplace(range, *result).second)("range", range.ToString());
    }
}

TString TActionReadBlobs::DebugString() const {
    THashSet<TBlobRange> ranges;
    for (auto&& i : Blobs) {
        ranges.emplace(i.first);
    }
    return JoinSeq(",", ranges);
}

}
