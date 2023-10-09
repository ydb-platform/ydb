#pragma once

#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <util/datetime/base.h>

namespace NKikimr::NDataStreams::V1 {

const i64 TIMESTAMP_DELTA_ALLOWED_MS = 10'000;

class TShardIterator {
using TPartitionOffset =
    std::invoke_result_t<decltype(&NKikimrClient::TCmdReadResult_TResult::GetOffset),
                         NKikimrClient::TCmdReadResult_TResult>;
using TYdsSeqNo =
    std::invoke_result_t<decltype(&NKikimrPQ::TYdsShardIterator::GetSequenceNumber),
                         NKikimrPQ::TYdsShardIterator>;
static_assert(std::is_same<TPartitionOffset, TYdsSeqNo>::value,
              "Types of partition message offset and yds record sequence number should match");

using TCreationTimestamp =
    std::invoke_result_t<decltype(&NKikimrClient::TCmdReadResult_TResult::GetCreateTimestampMS),
                         NKikimrClient::TCmdReadResult_TResult>;
using TYdsTimestamp =
    std::invoke_result_t<decltype(&NKikimrPQ::TYdsShardIterator::GetReadTimestampMs),
                         NKikimrPQ::TYdsShardIterator>;
static_assert(std::is_same<TCreationTimestamp, TYdsTimestamp>::value,
              "Types of partition message creation timestamp and yds record timestamp should match");

public:
static constexpr ui64 LIFETIME_MS = TDuration::Minutes(5).MilliSeconds();

TShardIterator(const TString& iteratorStr) : Expired{false}, Valid{true} {
    try {
        TString decoded;
        Base64StrictDecode(iteratorStr, decoded);
        Valid = Proto.ParseFromString(decoded) && IsAlive(TInstant::Now().MilliSeconds());
        Expired = !IsAlive(TInstant::Now().MilliSeconds());
    } catch (std::exception&) {
        Valid = false;
    }
}

TShardIterator(const TString& streamName, const TString& streamArn,
                ui32 shardId, ui64 readTimestamp, ui64 sequenceNumber,
                NKikimrPQ::TYdsShardIterator::ETopicKind kind = NKikimrPQ::TYdsShardIterator::KIND_COMMON)
    : Expired{false}, Valid{true} {
    Proto.SetStreamName(streamName);
    Proto.SetStreamArn(streamArn);
    Proto.SetShardId(shardId);
    Proto.SetReadTimestampMs(readTimestamp);
    Proto.SetSequenceNumber(sequenceNumber);
    Proto.SetCreationTimestampMs(TInstant::Now().MilliSeconds());
    Proto.SetKind(kind);
}

TShardIterator(const TShardIterator& other) : TShardIterator(
    other.GetStreamName(),
    other.GetStreamArn(),
    other.GetShardId(),
    other.GetReadTimestamp(),
    other.GetSequenceNumber(),
    other.GetKind()
) {}

static TShardIterator Common(const TString& streamName, const TString& streamArn,
                ui32 shardId, ui64 readTimestamp, ui64 sequenceNumber) {
    return TShardIterator(streamName, streamArn, shardId, readTimestamp, sequenceNumber);
}

static TShardIterator Cdc(const TString& streamName, const TString& streamArn,
                ui32 shardId, ui64 readTimestamp, ui64 sequenceNumber) {
    return TShardIterator(streamName, streamArn, shardId, readTimestamp, sequenceNumber,
            NKikimrPQ::TYdsShardIterator::KIND_CDC);
}

TString Serialize() const {
    TString data;
    bool result = Proto.SerializeToString(&data);
    Y_ABORT_UNLESS(result);
    TString encoded;
    Base64Encode(data, encoded);
    return encoded;
}

TString GetStreamName() const {
    return Proto.GetStreamName();
}

TString GetStreamArn() const {
    return Proto.GetStreamArn();
}

ui32 GetShardId() const {
    return Proto.GetShardId();
}

ui64 GetReadTimestamp() const {
    return Proto.GetReadTimestampMs();
}

void SetReadTimestamp(ui64 ts) {
    Proto.SetReadTimestampMs(ts);
}

ui64 GetSequenceNumber() const {
    return Proto.GetSequenceNumber();
}

void SetSequenceNumber(ui64 seqno) {
    Proto.SetSequenceNumber(seqno);
}

bool IsAlive(ui64 now) const {
    return now + TIMESTAMP_DELTA_ALLOWED_MS >= Proto.GetCreationTimestampMs() && now -
        Proto.GetCreationTimestampMs() < LIFETIME_MS;
}

NKikimrPQ::TYdsShardIterator::ETopicKind GetKind() const {
    return Proto.GetKind();
}

bool IsCdcTopic() const {
    return Proto.GetKind() == NKikimrPQ::TYdsShardIterator::KIND_CDC;
}

bool IsValid() const {
    return Valid;
}

bool IsExpired() const {
    return Expired;
}

private:
bool Expired;
bool Valid;
NKikimrPQ::TYdsShardIterator Proto;
};

} // namespace NKikimr::NDataStreams::V1
