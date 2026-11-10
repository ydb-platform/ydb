#pragma once

#include <library/cpp/time_provider/monotonic.h>

#include <optional>

namespace NBridgeSkipper {

inline std::optional<ui64> AgeMilliseconds(TMonotonic sentAt, TMonotonic now) {
    if (!sentAt) {
        return std::nullopt;
    }
    return now > sentAt ? (now - sentAt).MilliSeconds() : 0;
}

} // namespace NBridgeSkipper
