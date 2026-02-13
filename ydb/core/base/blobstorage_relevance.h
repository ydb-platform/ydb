#pragma once

#include <memory>
#include <optional>

namespace NKikimr {

struct TMessageRelevanceTracker {};
using TMessageRelevanceOwner = std::shared_ptr<TMessageRelevanceTracker>;
using TMessageRelevanceWatcher = std::weak_ptr<TMessageRelevanceTracker>;

class TMessageRelevance {
public:
    TMessageRelevance() = default;
    TMessageRelevance(const TMessageRelevanceOwner& owner,
            std::optional<TMessageRelevanceWatcher> external = std::nullopt);
    bool IsRelevant() const;

private:
    // tracks request actor state and cancels request when actor dies
    TMessageRelevanceWatcher InternalWatcher;
    // can be passed as request parameter to cancel request on demand
    std::optional<TMessageRelevanceWatcher> ExternalWatcher;
};

} // namespace NKikimr
