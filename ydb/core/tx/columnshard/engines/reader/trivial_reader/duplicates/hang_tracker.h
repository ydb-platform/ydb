#pragma once

#include <ydb/library/actors/core/monotonic.h>

#include <util/datetime/base.h>

#include <optional>

namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering {

class THangTracker {
public:
    static constexpr TDuration DefaultTimeout = TDuration::Minutes(15);

private:
    const TDuration Timeout;
    const TDuration CheckInterval;
    std::optional<TMonotonic> LastProgressInstant;
    bool Scheduled = false;

public:
    explicit THangTracker(const TDuration timeout = DefaultTimeout);

    TDuration GetTimeout() const {
        return Timeout;
    }

    TDuration GetCheckInterval() const {
        return CheckInterval;
    }

    /// Marks progress. Returns check interval if a wakeup must be scheduled.
    std::optional<TDuration> OnProgress(const TMonotonic now);

    struct TWakeupResult {
        bool TimedOut = false;
        std::optional<TDuration> RescheduleAfter;
    };

    /// Handles watchdog wakeup. Uses actor-system monotonic time from caller.
    TWakeupResult OnWakeup(const bool isActive, const TMonotonic now);
};

}   // namespace NKikimr::NOlap::NReader::NTrivial::NDuplicateFiltering
