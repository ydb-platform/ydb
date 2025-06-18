#pragma once

#include <cstddef>
#include <type_traits>

#include <util/generic/vector.h>
#include <util/generic/string.h>

namespace NSQLComplete {

    template <class T>
    struct TByteSize;

    template <class T>
        requires std::is_fundamental_v<T>
    struct TByteSize<T> {
        size_t operator()(const T& x) const noexcept {
            return sizeof(x);
        }
    };

    template <class T>
    struct TByteSize<TVector<T>> {
        size_t operator()(const TVector<T>& x) const noexcept {
            size_t bytes = sizeof(x);
            bytes = Accumulate(x, bytes, [](size_t acc, const T& x) {
                return acc + TByteSize<T>()(x);
            });
            bytes += x.capacity() * sizeof(T);
            return bytes;
        }
    };

    template <>
    struct TByteSize<TString> {
        size_t operator()(const TString& x) const noexcept {
            return std::max(sizeof(x), sizeof(x) + x.capacity());
        }
    };

    template <class T>
    concept CByteSized = requires(const T& x) {
        { TByteSize<T>()(x) } -> std::convertible_to<std::size_t>;
    };

} // namespace NSQLComplete
