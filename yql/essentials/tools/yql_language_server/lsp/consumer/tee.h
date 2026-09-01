#pragma once

#include "map.h"

namespace NLsp {

template <typename T, std::invocable<const T&> F>
IConsumer<T>::TPtr Tee(F f, typename IConsumer<T>::TPtr consumer) {
    return Map<T, T>([f = std::move(f)](T value) -> T {
        f(static_cast<const T&>(value));
        return value;
    }, std::move(consumer));
}

} // namespace NLsp
