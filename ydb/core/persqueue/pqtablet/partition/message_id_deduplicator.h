#pragma once

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/time_provider/time_provider.h>

#include <deque>

namespace NKikimrPQ {
class TMessageDeduplicationIdWAL;
}

namespace NKikimr::NPQ {

TString MakeDeduplicatorWALKey(ui32 partitionId, const TInstant& expirationTime);

class TMessageIdDeduplicator {
public:
    struct TMessage {
        TString DeduplicationId;
        TInstant ExpirationTime;
        ui64 Offset;

        bool operator==(const TMessage& other) const = default;
    };

    TMessageIdDeduplicator(TIntrusivePtr<ITimeProvider> timeProvider = CreateDefaultTimeProvider(), TDuration deduplicationWindow = TDuration::Minutes(5));
    ~TMessageIdDeduplicator();

    TDuration GetDeduplicationWindow() const;
    TInstant GetExpirationTime() const;

    std::optional<ui64> AddMessage(const TString& deduplicationId, const ui64 offset);
    size_t Compact();

    void Commit();

    bool ApplyWAL(TString&& key, NKikimrPQ::TMessageDeduplicationIdWAL&& wal);
    bool SerializeTo(NKikimrPQ::TMessageDeduplicationIdWAL& wal);

    const std::deque<TMessage>& GetQueue() const;

private:
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TDuration DeduplicationWindow;

    bool HasChanges = false;
    std::deque<TMessage> Queue;
    absl::flat_hash_map<TString, ui64> Messages;

    struct TBucket {
        TInstant StartTime;
        size_t StartMessageIndex = 0;
        size_t LastWrittenMessageIndex = 0;
    };
    TBucket CurrentBucket;
    std::optional<TBucket> PendingBucket;

    ui64 NextMessageIdDeduplicatorWAL = 0;
    struct WALKey {
        TString Key;
        TInstant ExpirationTime;
    };
    std::deque<WALKey> WALKeys;
};

} // namespace NKikimr::NPQ
