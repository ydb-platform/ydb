#pragma once
#include <util/generic/ptr.h>

/// Similar to MakeHolder, but also fills Record field of T with first argument. Useful for events.
template <class T>
[[nodiscard]] THolder<T> MakeHolderWithRecord(auto&& record, auto&& ... args) {
    THolder<T> holder = MakeHolder<T>(std::forward<decltype(args)>(args)...);
    holder->Record = std::forward<decltype(record)>(record);
    return holder;
}
