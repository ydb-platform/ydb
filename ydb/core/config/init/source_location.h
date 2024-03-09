#pragma once

#if __has_builtin(__builtin_source_location)
#include <source_location>

namespace NKikimr::NCompat {

using TSourceLocation = std::source_location;

} // namespace NCompat
#else
namespace NKikimr::NCompat {

// dummy implementation for older compilers
// as far as we use it only for debugging purposes
// it is totally okay to lose this information
struct TSourceLocation {
    static constexpr TSourceLocation current() noexcept {
        return {};
    }

    constexpr const char* file_name() const noexcept {
        return "";
    }

    constexpr uint_least32_t line() const noexcept {
        return 0;
    }
};

} // namespace NCompat
#endif
