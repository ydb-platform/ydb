#include "message_id_deduplicator.h"
#include "partition.h"

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

namespace {

constexpr TDuration MaxDeduplicationWindow = TDuration::Minutes(5);
constexpr size_t MaxDeduplicationRPS = 1000;
constexpr TDuration BucketSize = TDuration::Seconds(1);
constexpr size_t MaxBucketCount = MaxDeduplicationWindow.MilliSeconds() / BucketSize.MilliSeconds();
constexpr ui64 MaxDeduplicationIDs = MaxDeduplicationWindow.Seconds() * MaxDeduplicationRPS;
constexpr size_t MaxMessageIdInBucketCount = MaxDeduplicationIDs / MaxBucketCount;

TInstant Trim(TInstant value) {
    return TInstant::MilliSeconds(value.MilliSeconds() / BucketSize.MilliSeconds() * BucketSize.MilliSeconds());
}

}

TMessageIdDeduplicator::TMessageIdDeduplicator(const TPartitionId& partitionId, TIntrusivePtr<ITimeProvider> timeProvider, TDuration deduplicationWindow)
    : PartitionId(partitionId)
    , TimeProvider(timeProvider)
    , DeduplicationWindow(deduplicationWindow)
{
}

TMessageIdDeduplicator::~TMessageIdDeduplicator() = default;

TDuration TMessageIdDeduplicator::GetDeduplicationWindow() const {
    return DeduplicationWindow;
}

std::optional<ui64> TMessageIdDeduplicator::AddMessage(const TString& deduplicationId, const ui64 offset) {
    auto it = Messages.find(deduplicationId);
    if (it != Messages.end()) {
        return it->second;
    }

    const auto now = TimeProvider->Now();
    const auto expirationTime = now + DeduplicationWindow;

    if (!CurrentBucket.StartTime) {
        CurrentBucket.StartTime = Trim(now) + DeduplicationWindow;
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
        if (Queue.size() <= MaxDeduplicationIDs && message.ExpirationTime > now) {
            break;
        }
        auto it = Messages.find(message.DeduplicationId);
        if (it != Messages.end() && it->second <= message.Offset) {
            Messages.erase(it);
        }
        Queue.pop_front();
        ++removed;
    }

    while (!WALKeys.empty() && (WALKeys.front().ExpirationTime <= now || WALKeys.size() > MaxBucketCount)) {
        WALKeys.pop_front();
    }

    auto normalize = [&](size_t value) {
        return value > removed ? value - removed : 0;
    };

    auto compactBucket = [&](TBucket& bucket) {
        bucket.StartMessageIndex = normalize(bucket.StartMessageIndex);
        bucket.LastWrittenMessageIndex = normalize(bucket.LastWrittenMessageIndex);
        bucket.StartTime = Queue.empty() ? TInstant::Zero() : Trim(Queue.front().ExpirationTime);
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
    const auto now = TimeProvider->Now();
    CurrentBucket.StartMessageIndex = Queue.size();

    size_t offset = 0;
    TInstant expirationTime = TInstant::Zero();

    for (auto& message : *wal.MutableMessage()) {
        offset += message.GetOffsetDelta();
        expirationTime += TDuration::MilliSeconds(message.GetExpirationTimestampMillisecondsDelta());
        if (expirationTime <= now) {
            continue;
        }

        auto it = Messages.find(message.GetDeduplicationId());
        if (it != Messages.end() && it->second >= offset) {
            continue;
        }
        Queue.emplace_back(message.GetDeduplicationId(), expirationTime, offset);
        Messages[std::move(*message.MutableDeduplicationId())] = offset;
    }

    CurrentBucket.LastWrittenMessageIndex = Queue.size();
    CurrentBucket.StartTime = Queue.empty() ? TInstant::Zero() : Trim(Queue.back().ExpirationTime);

    WALKeys.emplace_back(std::move(key), expirationTime);

    return true;
}

std::optional<TString> TMessageIdDeduplicator::SerializeTo(NKikimrPQ::TMessageDeduplicationIdWAL& wal) {
    if (Queue.empty() || !HasChanges) {
        return std::nullopt;
    }

    const bool sameBucket = Queue.size() - CurrentBucket.StartMessageIndex <= MaxMessageIdInBucketCount;
    size_t startIndex = sameBucket ? CurrentBucket.StartMessageIndex : CurrentBucket.LastWrittenMessageIndex;
    if (startIndex == Queue.size()) {
        return std::nullopt;
    }

    ui64 lastOffset = 0;
    TInstant lastExpirationTime = TInstant::Zero();

    for (size_t i = startIndex; i < Queue.size(); ++i) {
        auto messageTime = std::max(Queue[i].ExpirationTime, lastExpirationTime);

        auto* message = wal.AddMessage();
        message->SetOffsetDelta(Queue[i].Offset - lastOffset);
        message->SetDeduplicationId(Queue[i].DeduplicationId);
        message->SetExpirationTimestampMillisecondsDelta(messageTime.MilliSeconds() - lastExpirationTime.MilliSeconds());

        lastOffset = Queue[i].Offset;
        lastExpirationTime = messageTime;
    }

    PendingBucket = {
        .StartTime = Trim(lastExpirationTime),
        .StartMessageIndex = startIndex,
        .LastWrittenMessageIndex = Queue.size(),
    };

    if (WALKeys.empty() || !sameBucket) {
        WALKeys.emplace_back(MakeDeduplicatorWALKey(PartitionId.OriginalPartitionId, NextMessageIdDeduplicatorWAL), lastExpirationTime);
        NextMessageIdDeduplicatorWAL++;
    } else {
        WALKeys.back().ExpirationTime = lastExpirationTime;
    }

    return WALKeys.back().Key;
}

TString TMessageIdDeduplicator::GetFirstActualWAL() const {
    if (WALKeys.empty()) {
        return MakeDeduplicatorWALKey(PartitionId.OriginalPartitionId, NextMessageIdDeduplicatorWAL);
    }
    return WALKeys.front().Key;
}

const std::deque<TMessageIdDeduplicator::TMessage>& TMessageIdDeduplicator::GetQueue() const {
    return Queue;
}

TString MakeDeduplicatorWALKey(ui32 partitionId, ui64 id) {
    static constexpr char WALSeparator = '|';

    TKeyPrefix ikey(TKeyPrefix::EType::TypeDeduplicator, TPartitionId(partitionId));
    ikey.Append(WALSeparator);

    auto bucket = Sprintf("%.16llX", id);
    ikey.Append(bucket.data(), bucket.size());

    return ikey.ToString();
}

bool TPartition::AddMessageDeduplicatorKeys(TEvKeyValue::TEvRequest* request) {
    if (MirroringEnabled(Config) || Partition.IsSupportivePartition()) {
        return false;
    }

    MessageIdDeduplicator.Compact();

    bool hasChanges = false;

    NKikimrPQ::TMessageDeduplicationIdWAL wal;
    if (auto key = MessageIdDeduplicator.SerializeTo(wal); key) {
        auto* writeWAL = request->Record.AddCmdWrite();
        writeWAL->SetKey(key.value());
        writeWAL->SetValue(wal.SerializeAsString());
        if (writeWAL->GetValue().size() < 1000) {
            writeWAL->SetStorageChannel(NKikimrClient::TKeyValueRequest::INLINE);
        }
        hasChanges = true;
    }

    auto* deleteExpired = request->Record.AddCmdDeleteRange();
    deleteExpired->MutableRange()->SetFrom(MakeDeduplicatorWALKey(Partition.OriginalPartitionId, 0));
    deleteExpired->MutableRange()->SetTo(MessageIdDeduplicator.GetFirstActualWAL());
    deleteExpired->MutableRange()->SetIncludeFrom(true);
    deleteExpired->MutableRange()->SetIncludeTo(false);

    return hasChanges;
}

std::optional<ui64> TPartition::DeduplicateByMessageId(const TEvPQ::TEvWrite::TMsg& msg, const ui64 offset) {
    if (msg.TotalParts > 1) {
        // Working with messages consisting of several parts is not supported.
        return std::nullopt;
    }

    if (!msg.MessageDeduplicationId) {
        return std::nullopt;
    }

    return MessageIdDeduplicator.AddMessage(*msg.MessageDeduplicationId, offset);
}

} // namespace NKikimr::NPQ
