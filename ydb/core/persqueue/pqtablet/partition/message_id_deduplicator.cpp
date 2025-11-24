#include "message_id_deduplicator.h"

namespace NKikimr::NPQ {

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

    const auto now = TimeProvider->Now();
    const auto expirationTime = now + DeduplicationWindow;
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

    return removed;
}

} // namespace NKikimr::NPQ