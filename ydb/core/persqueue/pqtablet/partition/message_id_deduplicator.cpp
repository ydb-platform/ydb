#include "message_id_deduplicator.h"
#include "partition.h"

#include <ydb/core/persqueue/public/mlp/mlp_message_attributes.h>

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

namespace {

constexpr TDuration BucketSize = TDuration::MilliSeconds(100);

TInstant Trim(TInstant value) {
    return TInstant::MilliSeconds(value.MilliSeconds() / BucketSize.MilliSeconds() * BucketSize.MilliSeconds() + BucketSize.MilliSeconds());
}

}

TMessageIdDeduplicator::TMessageIdDeduplicator(TIntrusivePtr<ITimeProvider> timeProvider, TDuration deduplicationWindow)
    : TimeProvider(timeProvider)
    , DeduplicationWindow(deduplicationWindow)
{
}

TMessageIdDeduplicator::~TMessageIdDeduplicator() = default;

TDuration TMessageIdDeduplicator::GetDeduplicationWindow() const {
    return DeduplicationWindow;
}

TInstant TMessageIdDeduplicator::GetExpirationTime() const {
    return Trim(TimeProvider->Now()) + DeduplicationWindow;
}

std::optional<ui64> TMessageIdDeduplicator::AddMessage(const TString& deduplicationId, const ui64 offset) {
    auto it = Messages.find(deduplicationId);
    if (it != Messages.end()) {
        return it->second;
    }

    const auto now = Trim(TimeProvider->Now());
    const auto expirationTime = now + DeduplicationWindow;

    if (!CurrentBucket.StartTime) {
        CurrentBucket.StartTime = now;
        CurrentBucket.StartMessageIndex = Queue.size();
    }

    HasChanges = true;
    Queue.emplace_back(deduplicationId, expirationTime, offset);
    Messages.emplace(deduplicationId, offset);

    return std::nullopt;
}

size_t TMessageIdDeduplicator::Compact() {
    const auto now = TimeProvider->Now();
    size_t removed = 0;

    while (!Queue.empty()) {
        const auto& message = Queue.front();
        if (message.ExpirationTime > now) {
            break;
        }
        auto it = Messages.find(message.DeduplicationId);
        if (it != Messages.end() && it->second <= message.Offset) {
            Messages.erase(it);
        }
        Queue.pop_front();
        ++removed;
    }

    auto normalize = [&](size_t value) {
        return value > removed ? value - removed : 0;
    };

    auto compactBucket = [&](TBucket& bucket) {
        bucket.StartMessageIndex = normalize(bucket.StartMessageIndex);
        bucket.LastWrittenMessageIndex = normalize(bucket.LastWrittenMessageIndex);
        bucket.StartTime = Queue.empty() ? TInstant::Zero() : Queue.front().ExpirationTime;
    };

    compactBucket(CurrentBucket);
    if (PendingBucket) {
        compactBucket(*PendingBucket);
    }

    return removed;
}

void TMessageIdDeduplicator::Commit() {
    if (PendingBucket) {
        CurrentBucket = std::move(*PendingBucket);
        PendingBucket.reset();
        HasChanges = false;
    }
}

bool TMessageIdDeduplicator::ApplyWAL(TString&& key, NKikimrPQ::TMessageDeduplicationIdWAL&& wal) {
    CurrentBucket.StartMessageIndex = Queue.size();

    size_t offset = 0;
    TInstant expirationTime = TInstant::Zero();

    for (auto& message : *wal.MutableMessage()) {
        offset += message.GetOffsetDelta();
        expirationTime += TDuration::MilliSeconds(message.GetExpirationTimestampMillisecondsDelta());

        auto it = Messages.find(message.GetDeduplicationId());
        if (it != Messages.end() && it->second >= offset) {
            continue;
        }
        Queue.emplace_back(message.GetDeduplicationId(), expirationTime, offset);
        Messages[std::move(*message.MutableDeduplicationId())] = offset;
    }

    CurrentBucket.LastWrittenMessageIndex = Queue.size();
    CurrentBucket.StartTime = Queue.empty() ? TInstant::Zero() : Queue.back().ExpirationTime;

    WALKeys.emplace_back(std::move(key), expirationTime);

    return true;
}

bool TMessageIdDeduplicator::SerializeTo(NKikimrPQ::TMessageDeduplicationIdWAL& wal) {
    if (Queue.empty() || !HasChanges) {
        return false;
    }

    const auto expirationTime = Queue.back().ExpirationTime;
    const bool sameBucket = CurrentBucket.StartTime == expirationTime;
    size_t startIndex = sameBucket ? CurrentBucket.StartMessageIndex : CurrentBucket.LastWrittenMessageIndex;
    if (startIndex == Queue.size()) {
        return false;
    }

    ui64 lastOffset = 0;
    TInstant lastExpirationTime = TInstant::Zero();

    for (size_t i = startIndex; i < Queue.size(); ++i) {
        auto* message = wal.AddMessage();
        message->SetOffsetDelta(Queue[i].Offset - lastOffset);
        message->SetDeduplicationId(Queue[i].DeduplicationId);
        message->SetExpirationTimestampMillisecondsDelta(Queue[i].ExpirationTime.MilliSeconds() - lastExpirationTime.MilliSeconds());

        lastOffset = Queue[i].Offset;
        lastExpirationTime = Queue[i].ExpirationTime;
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

    auto bucket = Sprintf("%.16llX", expirationTime.MilliSeconds() / BucketSize.MilliSeconds());
    ikey.Append(bucket.data(), bucket.size());

    return ikey.ToString();
}

void TPartition::AddMessageDeduplicatorKeys(TEvKeyValue::TEvRequest* request) {
    if (MirroringEnabled(Config) || Partition.IsSupportivePartition()) {
        return;
    }

    MessageIdDeduplicator.Compact();

    NKikimrPQ::TMessageDeduplicationIdWAL wal;
    if (MessageIdDeduplicator.SerializeTo(wal)) {
        auto expirationTime = TInstant::MilliSeconds(wal.GetExpirationTimestampMilliseconds());

        auto* writeWAL = request->Record.AddCmdWrite();
        writeWAL->SetKey(MakeDeduplicatorWALKey(Partition.OriginalPartitionId, expirationTime));
        writeWAL->SetValue(wal.SerializeAsString());
        if (writeWAL->GetValue().size() < 1000) {
            writeWAL->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
        }
    }

    auto* deleteExpired = request->Record.AddCmdDeleteRange();
    deleteExpired->MutableRange()->SetFrom(MakeDeduplicatorWALKey(Partition.OriginalPartitionId, TInstant::Zero()));
    deleteExpired->MutableRange()->SetTo(MakeDeduplicatorWALKey(Partition.OriginalPartitionId, MessageIdDeduplicator.GetExpirationTime()));
    deleteExpired->MutableRange()->SetIncludeFrom(true);
    deleteExpired->MutableRange()->SetIncludeTo(true);
}

std::optional<ui64> TPartition::DeduplicateByMessageId(const TEvPQ::TEvWrite::TMsg& msg, const ui64 offset) {
    if (msg.TotalParts > 1) {
        // Working with messages consisting of several parts is not supported.
        return std::nullopt;
    }

    NKikimrPQClient::TDataChunk proto;
    bool res = proto.ParseFromString(msg.Data);
    if (!res) {
        return std::nullopt;
    }

    std::optional<TString> deduplicationId;
    for (auto& attr : *proto.MutableMessageMeta()) {
        if (attr.key() == NMLP::NMessageConsts::MessageDeduplicationId) {
            deduplicationId = attr.value();
            break;
        }
    }

    if (!deduplicationId) {
        return std::nullopt;
    }

    return MessageIdDeduplicator.AddMessage(*deduplicationId, offset);
}

} // namespace NKikimr::NPQ
