#pragma once

#include <util/datetime/base.h>

#include <chrono>

namespace NYdb::inline Dev {

class TDeadline {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using Duration = Clock::duration;

    TDeadline() = default;

    static TDeadline Now();

    static TDeadline Max();

    static TDeadline AfterDuration(const TDuration& after);

    static TDeadline AfterDuration(const Duration& after);

    template <typename Rep, typename Period>
    static TDeadline AfterDuration(const std::chrono::duration<Rep, Period>& after) {
        return AfterDuration(SafeDurationCast(after));
    }

    static Duration SafeDurationCast(const TDuration& duration);

    template <typename Rep, typename Period>
    static Duration SafeDurationCast(const std::chrono::duration<Rep, Period>& duration) {
        using FromDuration = std::chrono::duration<Rep, Period>;

        // Require resolution of 'Duration' higher than 'FromDuration',
        // to safely cast 'Duration::max' value to 'FromDuration'
        static_assert(std::is_constructible_v<Duration, FromDuration>);

        if (std::chrono::duration_cast<FromDuration>(Duration::max()) < duration) {
            return Duration::max();
        }

        if constexpr (std::is_signed_v<Rep>) {
            if (duration < std::chrono::duration_cast<FromDuration>(Duration::min())) {
                return Duration::min();
            }
        }

        return std::chrono::duration_cast<Duration>(duration);
    }

    TimePoint GetTimePoint() const noexcept;

    bool operator<(const TDeadline& other) const noexcept;
    bool operator==(const TDeadline& other) const noexcept;

    TDeadline operator+(const Duration& duration) const noexcept;
    TDeadline operator-(const Duration& duration) const noexcept;

private:
    explicit TDeadline(TimePoint timePoint) noexcept
        : TimePoint_(timePoint)
    {}

    static TimePoint SafeSum(TimePoint timePoint, Duration duration) noexcept;

    TimePoint TimePoint_;
};

} // namespace NYdb
