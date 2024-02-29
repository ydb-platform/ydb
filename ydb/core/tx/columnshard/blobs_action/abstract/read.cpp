#include "read.h"
#include <ydb/library/actors/core/log.h>
#include <util/string/join.h>

namespace NKikimr::NOlap {

void IBlobsReadingAction::StartReading(THashSet<TBlobRange>&& ranges) {
    AFL_VERIFY(ranges.size());
    AFL_VERIFY(Counters);
    for (auto&& i : ranges) {
        Counters->OnRequest(i.Size);
    }
    return DoStartReading(std::move(ranges));
}

void IBlobsReadingAction::Start(const THashSet<TBlobRange>& rangesInProgress) {
    Y_ABORT_UNLESS(!Started);
    Started = true;

    Y_ABORT_UNLESS(RangesForRead.size() + RangesForResult.size());
    StartWaitingRanges = TMonotonic::Now();
    WaitingRangesCount = RangesForRead.size();
    THashSet<TBlobRange> rangesFiltered;
    if (rangesInProgress.empty()) {
        rangesFiltered = RangesForRead;
    } else {
        for (auto&& r : RangesForRead) {
            if (!rangesInProgress.contains(r)) {
                rangesFiltered.emplace(r);
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
    AFL_VERIFY(Counters);
    AFL_VERIFY(--WaitingRangesCount >= 0);
    Counters->OnReply(range.Size, TMonotonic::Now() - StartWaitingRanges);
    AFL_VERIFY(data.size() == range.Size);
    Replies.emplace(range, data);
}

void IBlobsReadingAction::OnReadError(const TBlobRange& range, const TErrorStatus& replyStatus) {
    AFL_VERIFY(Counters);
    AFL_VERIFY(--WaitingRangesCount >= 0);
    Counters->OnFail(range.Size, TMonotonic::Now() - StartWaitingRanges);
    Fails.emplace(range, replyStatus);
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
