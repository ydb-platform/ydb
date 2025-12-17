#include "time.h"

namespace NYdb::inline Dev {

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

bool TDeadline::operator<=(const TDeadline& other) const noexcept {
    return TimePoint_ <= other.TimePoint_;
}

bool TDeadline::operator==(const TDeadline& other) const noexcept {
    return TimePoint_ == other.TimePoint_;
}

TDeadline TDeadline::operator+(const Duration& duration) const noexcept {
    return TDeadline(SafeSum(TimePoint_, duration));
}

TDeadline TDeadline::operator-(const Duration& duration) const noexcept {
    return TDeadline(SafeSum(TimePoint_, -duration));
}

TDeadline::TimePoint TDeadline::SafeSum(TDeadline::TimePoint timePoint, TDeadline::Duration duration) noexcept {
    if (duration > TDeadline::Duration::zero()) {
        if (timePoint > TDeadline::TimePoint::max() - duration) {
            return TDeadline::TimePoint::max();
        }
    } else {
        if (TDeadline::TimePoint::min() - duration > timePoint) {
            return TDeadline::TimePoint::min();
        }
    }
    return timePoint + duration;
}

} // namespace NYdb
