#include "message_id_deduplicator.h"

#include <ydb/core/protos/pqconfig.pb.h>

namespace NKikimr::NPQ {

namespace {

TInstant trim(TInstant value) {
    return TInstant::MilliSeconds(value.MilliSeconds() / 100 * 100 + 100);
}

}

TMessageIdDeduplicator::TMessageIdDeduplicator(TIntrusivePtr<ITimeProvider> timeProvider, TDuration deduplicationWindow)
    : TimeProvider(timeProvider)
    , DeduplicationWindow(deduplicationWindow)
{
}

TMessageIdDeduplicator::~TMessageIdDeduplicator() {
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
    CurrentBucket = std::move(*PendingBucket);
    PendingBucket.reset();
}

void TMessageIdDeduplicator::Rollback() {
    PendingBucket.reset();
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
    if (Queue.empty()) {
        return false;
    }

    wal.SetExpirationTimestampMilliseconds(Queue.back().ExpirationTime.MilliSeconds());

    const bool sameBucket = CurrentBucket.StartTime == Queue.back().ExpirationTime;
    size_t startIndex = sameBucket ? CurrentBucket.StartMessageIndex : CurrentBucket.LastWrittenMessageIndex;

    for (size_t i = startIndex; i < Queue.size(); ++i) {
        wal.AddDeduplicationId(Queue[i].DeduplicationId);
    }

    PendingBucket = {
        .StartTime = Queue.back().ExpirationTime,
        .StartMessageIndex = startIndex,
        .LastWrittenMessageIndex = Queue.size(),
    };

    return true;
}

} // namespace NKikimr::NPQ