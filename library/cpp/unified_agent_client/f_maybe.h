#pragma once

#include <util/generic/maybe.h>

namespace NUnifiedAgent {
    template <typename T>
    using TFMaybe = TMaybe<T, ::NMaybe::TPolicyUndefinedFail>;

    template <class T>
    inline constexpr TFMaybe<std::decay_t<T>> MakeFMaybe(T&& value) {
        return TMaybe<std::decay_t<T>, ::NMaybe::TPolicyUndefinedFail>(std::forward<T>(value));
    }

    template <class T, class... TArgs>
    inline constexpr TFMaybe<T> MakeFMaybe(TArgs&&... args) {
        return TFMaybe<T>(typename TFMaybe<T>::TInPlace{}, std::forward<TArgs>(args)...);
    }

    template <class T>
    inline constexpr TFMaybe<std::decay_t<T>> MakeFMaybe(const TMaybe<T>& source) {
        return source.Defined() ? MakeFMaybe(*source) : Nothing();
    }
}
