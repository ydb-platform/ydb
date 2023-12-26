#include "read.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NOlap {

void IBlobsReadingAction::StartReading(THashMap<TUnifiedBlobId, THashSet<TBlobRange>>&& ranges) {
    AFL_VERIFY(ranges.size());
    AFL_VERIFY(Counters);
    for (auto&& i : ranges) {
        AFL_VERIFY(i.second.size());
        for (auto&& br : i.second) {
            Counters->OnRequest(br.Size);
        }
    }
    return DoStartReading(ranges);
}

void IBlobsReadingAction::ExtractBlobsDataTo(THashMap<TBlobRange, TString>& result) {
    AFL_VERIFY(Started);
    if (result.empty()) {
        std::swap(result, Replies);
    } else {
        for (auto&& i : Replies) {
            AFL_VERIFY(result.emplace(i.first, std::move(i.second)).second);
        }
        Replies.clear();
    }
    RangesForResult.clear();
}

void IBlobsReadingAction::Start(const THashSet<TBlobRange>& rangesInProgress) {
    Y_ABORT_UNLESS(!Started);
    Y_ABORT_UNLESS(RangesForRead.size() + RangesForResult.size());
    StartWaitingRanges = TMonotonic::Now();
    for (auto&& i : RangesForRead) {
        WaitingRangesCount += i.second.size();
    }
    THashMap<TUnifiedBlobId, THashSet<TBlobRange>> rangesFiltered;
    if (rangesInProgress.empty()) {
        rangesFiltered = RangesForRead;
    } else {
        for (auto&& i : RangesForRead) {
            for (auto&& r : i.second) {
                if (!rangesInProgress.contains(r)) {
                    rangesFiltered[r.BlobId].emplace(r);
                }
            }
        }
    }
    if (rangesFiltered.size()) {
        StartReading(std::move(rangesFiltered));
    }
    Started = true;
    for (auto&& i : RangesForResult) {
        AFL_VERIFY(Replies.emplace(i.first, i.second).second);
    }
}

void IBlobsReadingAction::OnReadResult(const TBlobRange& range, const TString& data) {
    AFL_VERIFY(Counters);
    AFL_VERIFY(--WaitingRangesCount >= 0);
    Counters->OnReply(range.Size, TMonotonic::Now() - StartWaitingRanges);
    Replies.emplace(range, data);
}

void IBlobsReadingAction::OnReadError(const TBlobRange& range, const TErrorStatus& replyStatus) {
    AFL_VERIFY(Counters);
    AFL_VERIFY(--WaitingRangesCount >= 0);
    Counters->OnFail(range.Size, TMonotonic::Now() - StartWaitingRanges);
    Fails.emplace(range, replyStatus);
}

void IBlobsReadingAction::AddRange(const TBlobRange& range, const TString& result /*= Default<TString>()*/) {
    Y_ABORT_UNLESS(!Started);
    if (!result) {
        AFL_VERIFY(RangesForRead[range.BlobId].emplace(range).second)("range", range.ToString());
    } else {
        AFL_VERIFY(RangesForResult.emplace(range, result).second)("range", range.ToString());
    }
}

}
