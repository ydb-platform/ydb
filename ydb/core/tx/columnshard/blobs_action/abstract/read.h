#pragma once
#include "common.h"
#include <ydb/core/tx/columnshard/blobs_action/counters/read.h>
#include <ydb/core/tx/columnshard/blob.h>
#include <ydb/core/protos/base.pb.h>
#include <ydb/library/conclusion/status.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NOlap {

class IBlobsReadingAction: public ICommonBlobsAction {
public:
    using TErrorStatus = TConclusionSpecialStatus<NKikimrProto::EReplyStatus, NKikimrProto::EReplyStatus::OK, NKikimrProto::EReplyStatus::ERROR>;
private:
    using TBase = ICommonBlobsAction;

    THashMap<TUnifiedBlobId, THashSet<TBlobRange>> RangesForRead;
    THashMap<TBlobRange, TString> RangesForResult;
    THashMap<TBlobRange, TMonotonic> WaitingRanges;
    THashMap<TBlobRange, TString> Replies;
    THashMap<TBlobRange, TErrorStatus> Fails;
    std::shared_ptr<NBlobOperations::TReadCounters> Counters;
    bool Started = false;
    YDB_ACCESSOR(bool, IsBackgroundProcess, true);
protected:
    virtual void DoStartReading(const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& range) = 0;
    void StartReading(THashMap<TUnifiedBlobId, THashSet<TBlobRange>>&& ranges);
public:

    void SetCounters(std::shared_ptr<NBlobOperations::TReadCounters> counters) {
        Counters = counters;
    }

    IBlobsReadingAction(const TString& storageId)
        : TBase(storageId)
    {

    }

    void ExtractBlobsDataTo(THashMap<TBlobRange, TString>& result);

    ui64 GetExpectedBlobsSize() const {
        ui64 result = 0;
        for (auto&& i : RangesForRead) {
            for (auto&& b : i.second) {
                result += b.Size;
            }
        }
        for (auto&& i : RangesForResult) {
            result += i.first.Size;
        }
        return result;
    }

    ui64 GetExpectedBlobsCount() const {
        ui64 result = 0;
        for (auto&& i : RangesForRead) {
            result += i.second.size();
        }
        return result + RangesForResult.size();
    }

    void FillExpectedRanges(THashSet<TBlobRange>& ranges) const {
        for (auto&& i : RangesForRead) {
            for (auto&& b : i.second) {
                Y_ABORT_UNLESS(ranges.emplace(b).second);
            }
        }
        for (auto&& i : RangesForResult) {
            Y_ABORT_UNLESS(ranges.emplace(i.first).second);
        }
    }

    const THashMap<TUnifiedBlobId, THashSet<TBlobRange>>& GetRangesForRead() const {
        return RangesForRead;
    }

    void AddRange(const TBlobRange& range, const TString& result = Default<TString>());

    void Start(const THashSet<TBlobRange>& rangesInProgress);
    void OnReadResult(const TBlobRange& range, const TString& data);
    void OnReadError(const TBlobRange& range, const TErrorStatus& replyStatus);

    bool HasFails() const {
        return Fails.size();
    }

    bool IsFinished() const {
        return WaitingRanges.size() == 0;
    }
};

}
