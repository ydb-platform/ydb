#pragma once

#include "partition.h"

namespace NKafka {

    static const ui64 MAX_SEQNO_DIFFERENCE_UNTIL_OUT_OF_ORDER = std::numeric_limits<i32>::max() / 2;
    enum class ECheckDeduplicationResult {
        OK,
        INVALID_PRODUCER_EPOCH,
        OUT_OF_ORDER_SEQUENCE_NUMBER,
        DUPLICATE_SEQUENCE_NUMBER
    };

    // IsDuplicate is only needed for Kafka protocol deduplication.
    // baseSequence field in Kafka protocol has type int32 and the numbers loop back from the maximum possible value back to 0.
    // I.e. the next seqno after int32max is 0.
    // To decide if we got a duplicate seqno or an out of order seqno,
    // we are comparing the difference between maxSeqNo and seqNo with MAX_SEQNO_DIFFERENCE_UNTIL_OUT_OF_ORDER.
    // The value of MAX_SEQNO_DIFFERENCE_UNTIL_OUT_OF_ORDER is half of the int32 range.
    bool IsDuplicate(ui64 maxSeqNo, ui64 seqNo);

    bool InSequence(ui64 maxSeqNo, ui64 seqNo);

    ECheckDeduplicationResult CheckDeduplication(i16 lastEpoch, ui64 lastSeqNo, i16 messageEpoch, ui64 messageSeqNo);

    std::pair<NPersQueue::NErrorCode::EErrorCode, TString> MakeDeduplicationError(
        ECheckDeduplicationResult res, const TString& topicName, ui32 partitionId, const TString& sourceId, ui64 poffset,
        i16 lastEpoch, ui64 lastSeqNo, i16 messageEpoch, ui64 messageSeqNo);
}

namespace NKikimr::NPQ {

class TKeyLevel {
public:
    friend IOutputStream& operator <<(IOutputStream& out, const TKeyLevel& value);

    TKeyLevel(ui32 border)
    : Border_(border)
    , Sum_(0)
    , RecsCount_(0)
    , InternalPartsCount_(0) {}

    void Clear() {
        Keys_.clear();
        Sum_ = 0;
        RecsCount_ = 0;
        InternalPartsCount_ = 0;
    }

    ui32 KeysCount() const {
        return Keys_.size();
    }

    ui32 RecsCount() const {
        return RecsCount_;
    }

    ui16 InternalPartsCount() const {
        return InternalPartsCount_;
    }

    bool NeedCompaction() const {
        return Sum_ >= Border_;
    }

    std::pair<TKey, ui32> Compact() {
        Y_ABORT_UNLESS(!Keys_.empty());
        TKey tmp(Keys_.front().first);
        tmp.SetCount(RecsCount_);
        tmp.SetInternalPartsCount(InternalPartsCount_);
        std::pair<TKey, ui32> res(tmp, Sum_);
        Clear();
        return res;
    }

    std::pair<TKey, ui32> PopFront() {
        Y_ABORT_UNLESS(!Keys_.empty());
        Sum_ -= Keys_.front().second;
        RecsCount_ -= Keys_.front().first.GetCount();
        InternalPartsCount_ -= Keys_.front().first.GetInternalPartsCount();
        auto res = Keys_.front();
        Keys_.pop_front();
        return res;
    }

    std::pair<TKey, ui32> PopBack() {
        Y_ABORT_UNLESS(!Keys_.empty());
        Sum_ -= Keys_.back().second;
        RecsCount_ -= Keys_.back().first.GetCount();
        InternalPartsCount_ -= Keys_.back().first.GetInternalPartsCount();
        auto res = Keys_.back();
        Keys_.pop_back();
        return res;
    }

    ui32 Sum() const {
        return Sum_;
    }

    const TKey& GetKey(const ui32 pos) const {
        Y_ABORT_UNLESS(pos < Keys_.size());
        return Keys_[pos].first;
    }

    const ui32& GetSize(const ui32 pos) const {
        Y_ABORT_UNLESS(pos < Keys_.size());
        return Keys_[pos].second;
    }

    void PushKeyToFront(const TKey& key, ui32 size) {
        Sum_ += size;
        RecsCount_ += key.GetCount();
        InternalPartsCount_ += key.GetInternalPartsCount();
        Keys_.push_front(std::make_pair(key, size));
    }

    void AddKey(const TKey& key, ui32 size) {
        Sum_ += size;
        RecsCount_ += key.GetCount();
        InternalPartsCount_ += key.GetInternalPartsCount();
        Keys_.push_back(std::make_pair(key, size));
    }

    ui32 Border() const {
        return Border_;
    }

private:
    const ui32 Border_;
    std::deque<std::pair<TKey, ui32>> Keys_;
    ui32 Sum_;
    ui32 RecsCount_;
    ui16 InternalPartsCount_;
};

struct TPartition::THasDataReq {
    ui64 Num;
    ui64 Offset;
    TActorId Sender;
    TMaybe<ui64> Cookie;
    TString ClientId;
    TMaybe<TInstant> ReadTimestamp;

    bool operator < (const THasDataReq& req) const {
        return std::tuple(Offset, Num) < std::tuple(req.Offset, req.Num);
    }
};

struct TPartition::THasDataDeadline {
    TInstant Deadline;
    TPartition::THasDataReq Request;

    bool operator < (const THasDataDeadline& dl) const {
        return Deadline < dl.Deadline || Deadline == dl.Deadline && Request < dl.Request;
    }
};

void AddCheckDiskRequest(TEvKeyValue::TEvRequest *request, ui32 numChannels);
NKikimrClient::TKeyValueRequest::EStorageChannel GetChannel(ui32 i);
bool IsQuotingEnabled(const NKikimrPQ::TPQConfig& pqConfig,
                      bool isLocalDC);
void AddCmdDeleteRange(TEvKeyValue::TEvRequest& request,
                       TKeyPrefix::EType c,
                       const TPartitionId& partitionId);

bool SeqnoViolation(TMaybe<i16> lastEpoch, ui64 lastSeqNo, TMaybe<i16> messageEpoch, ui64 messageSeqNo);

} // namespace NKikimr::NPQ
