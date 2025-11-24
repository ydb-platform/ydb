#pragma once

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <library/cpp/time_provider/time_provider.h>

#include <deque>

namespace NKikimrPQ {
class MessageDeduplicationIdWAL;
}

namespace NKikimr::NPQ {

class TMessageIdDeduplicator {
public:
    struct TMessage {
        TString DeduplicationId;
        TInstant ExpirationTime;

        bool operator==(const TMessage& other) const = default;
    };

    TMessageIdDeduplicator(TIntrusivePtr<ITimeProvider> timeProvider = CreateDefaultTimeProvider(), TDuration deduplicationWindow = TDuration::Minutes(5));
    ~TMessageIdDeduplicator();

    bool AddMessage(const TString& deduplicationId);
    size_t Compact();

    void Commit();
    void Rollback();

    bool ApplyWAL(NKikimrPQ::MessageDeduplicationIdWAL&& wal);
    bool SerializeTo(NKikimrPQ::MessageDeduplicationIdWAL& wal);

    const std::deque<TMessage>& GetQueue() const;

private:
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TDuration DeduplicationWindow;

    std::deque<TMessage> Queue;
    absl::flat_hash_set<TString> Messages;

    struct TBucket {
        TInstant StartTime;
        size_t StartMessageIndex = 0;
        size_t LastWrittenMessageIndex = 0;
    };

    TBucket CurrentBucket;
    std::optional<TBucket> PendingBucket;
};

} // namespace NKikimr::NPQ
