#pragma once
#include "common.h"
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/protos/base.pb.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {

class IBlobsReadingAction: public ICommonBlobsAction {
private:
    using TBase = ICommonBlobsAction;
    THashMap<TUnifiedBlobId, THashSet<TBlobRange>> RangesForRead;
    THashSet<TBlobRange> WaitingRanges;
    THashMap<TBlobRange, TString> Replies;
    THashMap<TBlobRange, NKikimrProto::EReplyStatus> Fails;
    bool Started = false;
protected:
    virtual void DoStartReading(const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& range) = 0;
    void StartReading(THashMap<TUnifiedBlobId, THashSet<TBlobRange>>&& ranges);
public:
    IBlobsReadingAction(const TString& storageId)
        : TBase(storageId)
    {

    }

    ui64 GetExpectedBlobsSize() const {
        ui64 result = 0;
        for (auto&& i : RangesForRead) {
            for (auto&& b : i.second) {
                result += b.Size;
            }
        }
        return result;
    }

    ui64 GetExpectedBlobsCount() const {
        ui64 result = 0;
        for (auto&& i : RangesForRead) {
            result += i.second.size();
        }
        return result;
    }

    void FillExpectedRanges(THashSet<TBlobRange>& ranges) const {
        for (auto&& i : RangesForRead) {
            for (auto&& b : i.second) {
                Y_VERIFY(ranges.emplace(b).second);
            }
        }
    }

    const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& GetRangesForRead() const {
        return RangesForRead;
    }

    void AddRange(const TBlobRange& range) {
        Y_VERIFY(!Started);
        Y_VERIFY(RangesForRead[range.BlobId].emplace(range).second);
    }

    void Start(const THashSet<TBlobRange>& rangesInProgress) {
        Y_VERIFY(!Started);
        Y_VERIFY(RangesForRead.size());
        for (auto&& i : RangesForRead) {
            for (auto&& r : i.second) {
                WaitingRanges.emplace(r);
            }
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
    }

    void OnReadResult(const TBlobRange& range, const TString& data) {
        Y_VERIFY(WaitingRanges.erase(range));
        Replies.emplace(range, data);
    }

    void OnReadError(const TBlobRange& range, const NKikimrProto::EReplyStatus replyStatus) {
        Y_VERIFY(WaitingRanges.erase(range));
        Fails.emplace(range, replyStatus);
    }

    bool HasFails() const {
        return Fails.size();
    }

    bool IsFinished() const {
        return WaitingRanges.size() == 0;
    }
};

}
