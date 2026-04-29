#ifndef SENTINEL_OPTIONAL_INL_H_
#error "Direct inclusion of this file is not allowed, include sentinel_optional.h"
// For the sake of sane code completion.
#include "sentinel_optional.h"
#endif

#include <library/cpp/yt/assert/assert.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, class TSentinel>
constexpr TSentinelOptional<T, TSentinel>::TSentinelOptional(std::nullopt_t) noexcept
    : Value_(TSentinel::Sentinel)
{ }

template <class T, class TSentinel>
constexpr TSentinelOptional<T, TSentinel>::TSentinelOptional(T value) noexcept
    : Value_(value)
{ }

template <class T, class TSentinel>
constexpr TSentinelOptional<T, TSentinel>::TSentinelOptional(std::optional<T> opt) noexcept
    : Value_(opt ? *opt : TSentinel::Sentinel)
{ }

template <class T, class TSentinel>
constexpr TSentinelOptional<T, TSentinel>& TSentinelOptional<T, TSentinel>::operator=(std::nullopt_t) noexcept
{
    Value_ = TSentinel::Sentinel;
    return *this;
}

template <class T, class TSentinel>
constexpr TSentinelOptional<T, TSentinel>& TSentinelOptional<T, TSentinel>::operator=(T value) noexcept
{
    Value_ = value;
    return *this;
}

template <class T, class TSentinel>
constexpr bool TSentinelOptional<T, TSentinel>::has_value() const noexcept
{
    return Value_ != TSentinel::Sentinel;
}

template <class T, class TSentinel>
constexpr TSentinelOptional<T, TSentinel>::operator bool() const noexcept
{
    return has_value();
}

template <class T, class TSentinel>
constexpr T& TSentinelOptional<T, TSentinel>::operator*() noexcept
{
    return Value_;
}

template <class T, class TSentinel>
constexpr const T& TSentinelOptional<T, TSentinel>::operator*() const noexcept
{
    return Value_;
}

template <class T, class TSentinel>
constexpr T* TSentinelOptional<T, TSentinel>::operator->() noexcept
{
    return &Value_;
}

template <class T, class TSentinel>
constexpr const T* TSentinelOptional<T, TSentinel>::operator->() const noexcept
{
    return &Value_;
}

template <class T, class TSentinel>
T& TSentinelOptional<T, TSentinel>::value() noexcept
{
    YT_ASSERT(has_value());
    return Value_;
}

template <class T, class TSentinel>
const T& TSentinelOptional<T, TSentinel>::value() const noexcept
{
    YT_ASSERT(has_value());
    return Value_;
}

template <class T, class TSentinel>
constexpr T TSentinelOptional<T, TSentinel>::value_or(T default_value) const noexcept
{
    return has_value() ? Value_ : default_value;
}

template <class T, class TSentinel>
constexpr void TSentinelOptional<T, TSentinel>::reset() noexcept
{
    Value_ = TSentinel::Sentinel;
}

template <class T, class TSentinel>
constexpr TSentinelOptional<T, TSentinel>::operator std::optional<T>() const noexcept
{
    return has_value() ? std::optional<T>(Value_) : std::nullopt;
}

template <class T, class TSentinel>
constexpr bool TSentinelOptional<T, TSentinel>::operator==(std::nullopt_t) const noexcept
{
    return !has_value();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
