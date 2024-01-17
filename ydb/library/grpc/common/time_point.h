#pragma once

#include <contrib/libs/grpc/include/grpcpp/support/time.h>

#include <util/datetime/base.h>

#include <chrono>

namespace grpc {
// Specialization of TimePoint for TInstant
template <>
class TimePoint<TInstant> : public TimePoint<std::chrono::system_clock::time_point> {
    using TChronoDuration = std::chrono::duration<TDuration::TValue, std::micro>;

public:
    TimePoint(const TInstant& time)
        : TimePoint<std::chrono::system_clock::time_point>(
              std::chrono::system_clock::time_point(
                  std::chrono::duration_cast<std::chrono::system_clock::duration>(
                      TChronoDuration(time.GetValue())))) {
    }
};
} // namespace grpc
