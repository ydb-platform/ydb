#ifndef PROPAGATING_STORAGE_INL_H_
#error "Direct inclusion of this file is not allowed, include propagating_storage.h"
// For the sake of sane code completion.
#include "propagating_storage.h"
#endif
#undef PROPAGATING_STORAGE_INL_H_

namespace NYT::NConcurrency {

////////////////////////////////////////////////////////////////////////////////

template <class T>
bool TPropagatingStorage::Has() const
{
    return FindRaw(typeid(T)) != nullptr;
}

template <class T>
const T& TPropagatingStorage::GetOrCrash() const
{
    const auto* result = Find<T>();
    YT_VERIFY(result);
    return *result;
}

template <class T>
const T* TPropagatingStorage::Find() const
{
    const auto* result = FindRaw(typeid(T));
    return result ? std::any_cast<T>(result) : nullptr;
}

template <class T>
std::optional<T> TPropagatingStorage::Exchange(T value)
{
    auto result = ExchangeRaw(std::make_any<T>(std::move(value)));
    return result ? std::make_optional<T>(std::any_cast<T>(std::move(*result))) : std::nullopt;
}

template <class T>
std::optional<T> TPropagatingStorage::Remove()
{
    auto result = RemoveRaw(typeid(T));
    return result ? std::make_optional<T>(std::any_cast<T>(std::move(*result))) : std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TPropagatingValueGuard<T>::TPropagatingValueGuard(T value)
{
    auto& storage = GetCurrentPropagatingStorage();
    OldValue_ = storage.Exchange<T>(std::move(value));
}

template <class T>
TPropagatingValueGuard<T>::~TPropagatingValueGuard()
{
    auto& storage = GetCurrentPropagatingStorage();
    if (OldValue_) {
        storage.Exchange<T>(std::move(*OldValue_));
    } else {
        storage.Remove<T>();
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NConcurrency
