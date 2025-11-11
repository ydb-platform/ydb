#include "time.h"

namespace NYdb::inline Dev {

namespace {
    TDeadline::TimePoint SafeSum(TDeadline::TimePoint timePoint, TDeadline::Duration duration) {
        if (duration > TDeadline::TimePoint::max() - timePoint) {
            return TDeadline::TimePoint::max();
        }
        return timePoint + duration;
    }
}

TDeadline TDeadline::Now() {
    return TDeadline(Clock::now());
}

TDeadline TDeadline::Max() {
    return TDeadline(TimePoint::max());
}

TDeadline TDeadline::AfterDuration(const TDuration& after) {
    return AfterDuration(SafeDurationCast(after));
}

TDeadline TDeadline::AfterDuration(const Duration& after) {
    return TDeadline(SafeSum(Clock::now(), after));
}

TDeadline::Duration TDeadline::SafeDurationCast(const TDuration& after) {
    if (after.MicroSeconds() > static_cast<std::uint64_t>(std::chrono::microseconds::max().count())) {
        return Duration::max();
    }
    std::chrono::microseconds afterMicros(after.MicroSeconds());
    return SafeDurationCast(afterMicros);
}

TDeadline::TimePoint TDeadline::GetTimePoint() const noexcept {
    return TimePoint_;
}

bool TDeadline::operator<(const TDeadline& other) const noexcept {
    return TimePoint_ < other.TimePoint_;
}

bool TDeadline::operator==(const TDeadline& other) const noexcept {
    return TimePoint_ == other.TimePoint_;
}

} // namespace NYdb
