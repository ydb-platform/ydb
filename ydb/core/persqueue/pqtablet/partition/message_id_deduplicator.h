#pragma once

#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <library/cpp/time_provider/time_provider.h>

#include <deque>

namespace NKikimr::NPQ {

class TMessageIdDeduplicator {
public:
    class TBatch {
        bool SerializeTo(TString& res) const;
    };

    TMessageIdDeduplicator(TIntrusivePtr<ITimeProvider> timeProvider = CreateDefaultTimeProvider(), TDuration deduplicationWindow = TDuration::Minutes(5));
    ~TMessageIdDeduplicator();

    bool AddMessage(const TString& deduplicationId);
    size_t Compact();

    bool ApplyWAL();

private:
    TIntrusivePtr<ITimeProvider> TimeProvider;
    TDuration DeduplicationWindow;

    struct TMessage {
        TString DeduplicationId;
        TInstant ExpirationTime;
    };
    std::deque<TMessage> Queue;
    absl::flat_hash_set<TString> Messages;

    TInstant BucketStartTime;
    size_t LastWrittenMessageIndex = 0;
    size_t LastBucketIndex = 0;
};

} // namespace NKikimr::NPQ
