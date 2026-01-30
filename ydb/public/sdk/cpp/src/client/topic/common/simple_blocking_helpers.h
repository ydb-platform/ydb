#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/write_events.h>

#include <atomic>
#include <optional>

namespace NYdb::inline Dev::NTopic::NDetail {

// Common helper for blocking write sessions to wait for a continuation token.
// Used by both TSimpleBlockingWriteSession and TSimpleBlockingFederatedWriteSession.
template <typename TWriter>
std::optional<TContinuationToken> WaitForToken(
    TWriter& writer,
    std::atomic_bool& closed,
    const TDuration& timeout
) {
    TInstant startTime = TInstant::Now();
    TDuration remainingTime = timeout;

    std::optional<TContinuationToken> token;

    while (!closed.load() && remainingTime > TDuration::Zero()) {
        writer.WaitEvent().Wait(remainingTime);

        for (auto event : writer.GetEvents(false, std::nullopt)) {
            if (auto* readyEvent = std::get_if<TWriteSessionEvent::TReadyToAcceptEvent>(&event)) {
                Y_ABORT_UNLESS(!token.has_value());
                token = std::move(readyEvent->ContinuationToken);
            } else if (std::get_if<TWriteSessionEvent::TAcksEvent>(&event)) {
                // discard (maybe log?)
            } else if (std::get_if<TSessionClosedEvent>(&event)) {
                closed.store(true);
                return std::nullopt;
            }
        }

        if (token.has_value()) {
            return token;
        }

        remainingTime = timeout - (TInstant::Now() - startTime);
    }

    return std::nullopt;
}

}  // namespace NYdb::NTopic::NDetail
