#include "message_id_deduplicator.h"
#include "partition.h"

#include <ydb/core/persqueue/public/mlp/mlp_message_attributes.h>

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

namespace {

static constexpr size_t BucketSize = TDuration::MilliSeconds(100).MilliSeconds();

TInstant trim(TInstant value) {
    return TInstant::MilliSeconds(value.MilliSeconds() / BucketSize * BucketSize + BucketSize);
}

}

TMessageIdDeduplicator::TMessageIdDeduplicator(TIntrusivePtr<ITimeProvider> timeProvider, TDuration deduplicationWindow)
    : TimeProvider(timeProvider)
    , DeduplicationWindow(deduplicationWindow)
{
}

TMessageIdDeduplicator::~TMessageIdDeduplicator() {
}

const TDuration& TMessageIdDeduplicator::GetDeduplicationWindow() const {
    return DeduplicationWindow;
}

TInstant TMessageIdDeduplicator::GetExpirationTime() const {
    return trim(TimeProvider->Now()) + DeduplicationWindow;
}

bool TMessageIdDeduplicator::AddMessage(const TString& deduplicationId) {
    if (Messages.contains(deduplicationId)) {
        return false;
    }

    const auto now = trim(TimeProvider->Now());
    const auto expirationTime = now + DeduplicationWindow;

    if (!CurrentBucket.StartTime) {
        CurrentBucket.StartTime = now;
        CurrentBucket.StartMessageIndex = Queue.size();
    }

    HasChanges = true;
    Queue.emplace_back(deduplicationId, expirationTime);
    Messages.insert(deduplicationId);

    return true;
}

size_t TMessageIdDeduplicator::Compact() {
    const auto now = TimeProvider->Now();
    size_t removed = 0;

    while (!Queue.empty()) {
        const auto& message = Queue.front();
        if (message.ExpirationTime > now) {
            break;
        }
        Messages.erase(message.DeduplicationId);
        Queue.pop_front();
        ++removed;
    }

    auto normalize = [&](size_t value) {
        return value > removed ? value - removed : 0;
    };

    CurrentBucket.StartMessageIndex = normalize(CurrentBucket.StartMessageIndex);
    CurrentBucket.LastWrittenMessageIndex = normalize(CurrentBucket.LastWrittenMessageIndex);
    CurrentBucket.StartTime = Queue.empty() ? TInstant::Zero() : Queue.front().ExpirationTime;

    return removed;
}

void TMessageIdDeduplicator::Commit() {
    if (PendingBucket) {
        CurrentBucket = std::move(*PendingBucket);
        PendingBucket.reset();
        HasChanges = false;
    }
}

void TMessageIdDeduplicator::Rollback() {
    if (PendingBucket) {
        PendingBucket.reset();
        HasChanges = true;
    }
}

bool TMessageIdDeduplicator::ApplyWAL(NKikimrPQ::MessageDeduplicationIdWAL&& wal) {
    CurrentBucket.StartMessageIndex = Queue.size();

    auto expirationTime = TInstant::MilliSeconds(wal.GetExpirationTimestampMilliseconds());
    for (auto& deduplicationId : *wal.MutableDeduplicationId()) {
        Queue.emplace_back(deduplicationId, expirationTime);
        Messages.insert(std::move(deduplicationId));
    }

    CurrentBucket.LastWrittenMessageIndex = Queue.size();
    CurrentBucket.StartTime = Queue.empty() ? TInstant::Zero() : Queue.back().ExpirationTime;

    return true;
}

bool TMessageIdDeduplicator::SerializeTo(NKikimrPQ::MessageDeduplicationIdWAL& wal) {
    if (Queue.empty() || !HasChanges) {
        return false;
    }

    const auto expirationTime = Queue.back().ExpirationTime;
    const bool sameBucket = CurrentBucket.StartTime == expirationTime;
    size_t startIndex = sameBucket ? CurrentBucket.StartMessageIndex : CurrentBucket.LastWrittenMessageIndex;
    if (startIndex == Queue.size()) {
        return false;
    }

    wal.SetExpirationTimestampMilliseconds(expirationTime.MilliSeconds());
    for (size_t i = startIndex; i < Queue.size(); ++i) {
        Queue[i].ExpirationTime = expirationTime;
        wal.AddDeduplicationId(Queue[i].DeduplicationId);
    }

    PendingBucket = {
        .StartTime = expirationTime,
        .StartMessageIndex = startIndex,
        .LastWrittenMessageIndex = Queue.size(),
    };

    return true;
}

const std::deque<TMessageIdDeduplicator::TMessage>& TMessageIdDeduplicator::GetQueue() const {
    return Queue;
}

TString MakeDeduplicatorWALKey(ui32 partitionId, const TInstant& expirationTime) {
    static constexpr char WALSeparator = '|';

    TKeyPrefix ikey(TKeyPrefix::EType::TypeDeduplicator, TPartitionId(partitionId));
    ikey.Append(WALSeparator);

    auto bucket = Sprintf("%.16llX", expirationTime.MilliSeconds() / BucketSize);
    ikey.Append(bucket.data(), bucket.size());

    return ikey.ToString();
}

void TPartition::AddMessageDeduplicatorKeys(TEvKeyValue::TEvRequest* request) {
    MessageIdDeduplicator.Compact();

    NKikimrPQ::MessageDeduplicationIdWAL wal;
    if (MessageIdDeduplicator.SerializeTo(wal)) {
        auto expirationTime = TInstant::MilliSeconds(wal.GetExpirationTimestampMilliseconds());

        auto* writeWAL = request->Record.AddCmdWrite();
        writeWAL->SetKey(MakeDeduplicatorWALKey(Partition.OriginalPartitionId, expirationTime));
        writeWAL->SetValue(wal.SerializeAsString());
    }

    auto* deleteExpired = request->Record.AddCmdDeleteRange();
    deleteExpired->MutableRange()->SetFrom(MakeDeduplicatorWALKey(Partition.OriginalPartitionId, TInstant::Zero()));
    deleteExpired->MutableRange()->SetTo(MakeDeduplicatorWALKey(Partition.OriginalPartitionId, MessageIdDeduplicator.GetExpirationTime()));
    deleteExpired->MutableRange()->SetIncludeFrom(true);
    deleteExpired->MutableRange()->SetIncludeTo(true);
}

bool TPartition::DeduplicateByMessageId(const TEvPQ::TEvWrite::TMsg& msg) {
    if (!Config.GetEnableDeduplicationByMessageDeduplicationId()) {
        return true;
    }
    if (msg.TotalParts > 1) {
        // Working with messages consisting of several parts is not supported.
        return false;
    }

    NKikimrPQClient::TDataChunk proto;
    bool res = proto.ParseFromString(msg.Data);
    AFL_ENSURE(res)("o", msg.SeqNo);

    std::optional<TString> deduplicationId;
    for (auto& attr : *proto.MutableMessageMeta()) {
        if (attr.key() == NMLP::NMessageConsts::MessageDeduplicationId) {
            deduplicationId = attr.value();
            break;
        }
    }

    if (!deduplicationId) {
        return true;
    }

    return MessageIdDeduplicator.AddMessage(*deduplicationId);
}

} // namespace NKikimr::NPQ