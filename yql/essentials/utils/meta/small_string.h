#pragma once

#include "ensure.h"

#include <util/generic/strbuf.h>

#include <cstddef>
#include <array>
#include <string_view>

namespace NYql {

template <size_t Capacity>
struct TSmallString final {
    template <size_t N> // NOLINTNEXTLINE(modernize-avoid-c-arrays, google-explicit-constructor)
    consteval TSmallString(const char (&s)[N]) {
        static_assert(N <= Capacity);

        Ensure(s[N - 1] == '\0');
        Size = N - 1;
        for (size_t i = 0; i < N; ++i) {
            Data[i] = s[i];
        }
    }

    constexpr char* data() noexcept {
        return Data.data();
    }

    constexpr const char* data() const noexcept {
        return Data.data();
    }

    constexpr char& operator[](size_t i) {
        Ensure(i < Size);
        return data()[i];
    }

    constexpr bool empty() const noexcept {
        return Size == 0;
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr operator std::string_view() const noexcept {
        return std::string_view(data(), Size);
    }

    // NOLINTNEXTLINE(google-explicit-constructor)
    constexpr operator TStringBuf() const noexcept {
        return TStringBuf(data(), Size);
    }

    template <size_t M>
    constexpr bool operator==(const TSmallString<M>& rhs) const noexcept {
        return static_cast<TStringBuf>(*this) == static_cast<TStringBuf>(rhs);
    }

    // Public to use it as a template argument.
    std::array<char, Capacity> Data = {};
    size_t Size = 0;
};

template <size_t N> // NOLINTNEXTLINE(modernize-avoid-c-arrays)
TSmallString(const char (&)[N]) -> TSmallString<N>;

} // namespace NYql
