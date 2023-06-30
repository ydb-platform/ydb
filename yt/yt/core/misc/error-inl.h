#ifndef ERROR_INL_H_
#error "Direct inclusion of this file is not allowed, include error.h"
// For the sake of sane code completion.
#include "error.h"
#endif

#include <library/cpp/yt/string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline constexpr TErrorCode::TErrorCode()
    : Value_(static_cast<int>(NYT::EErrorCode::OK))
{ }

inline constexpr TErrorCode::TErrorCode(int value)
    : Value_(value)
{ }

template <class E>
requires std::is_enum_v<E>
constexpr TErrorCode::TErrorCode(E value)
    : Value_(static_cast<int>(value))
{ }

inline constexpr TErrorCode::operator int() const
{
    return Value_;
}

template <class E>
requires std::is_enum_v<E>
constexpr bool operator == (TErrorCode lhs, E rhs)
{
    return static_cast<int>(lhs) == static_cast<int>(rhs);
}

constexpr inline bool operator == (TErrorCode lhs, TErrorCode rhs)
{
    return static_cast<int>(lhs) == static_cast<int>(rhs);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <size_t Length, class... TArgs>
TString FormatErrorMessage(const char (&format)[Length], TArgs&&... args)
{
    return Format(format, std::forward<TArgs>(args)...);
}

template <size_t Length>
TString FormatErrorMessage(const char (&message)[Length])
{
    return TString(message);
}

} // namespace NDetail

template <size_t Length, class... TArgs>
TError::TErrorOr(const char (&messageOrFormat)[Length], TArgs&&... args)
    : TErrorOr(NYT::EErrorCode::Generic, NYT::NDetail::FormatErrorMessage(messageOrFormat, std::forward<TArgs>(args)...))
{ }

template <size_t Length, class... TArgs>
TError::TErrorOr(TErrorCode code, const char (&messageOrFormat)[Length], TArgs&&... args)
    : TErrorOr(code, NYT::NDetail::FormatErrorMessage(messageOrFormat, std::forward<TArgs>(args)...))
{ }

template <class... TArgs>
TError TError::Wrap(TArgs&&... args) const
{
    return TError(std::forward<TArgs>(args)...) << *this;
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
TErrorOr<T>::TErrorOr()
{
    Value_.emplace();
}

template <class T>
TErrorOr<T>::TErrorOr(T&& value) noexcept
    : Value_(std::move(value))
{ }

template <class T>
TErrorOr<T>::TErrorOr(const T& value)
    : Value_(value)
{ }

template <class T>
TErrorOr<T>::TErrorOr(const TError& other)
    : TError(other)
{
    YT_VERIFY(!IsOK());
}

template <class T>
TErrorOr<T>::TErrorOr(TError&& other) noexcept
    : TError(std::move(other))
{
    YT_VERIFY(!IsOK());
}

template <class T>
TErrorOr<T>::TErrorOr(const TErrorOr<T>& other)
    : TError(other)
{
    if (IsOK()) {
        Value_ = other.Value();
    }
}

template <class T>
TErrorOr<T>::TErrorOr(TErrorOr<T>&& other) noexcept
    : TError(std::move(other))
{
    if (IsOK()) {
        Value_ = std::move(other.Value());
    }
}

template <class T>
template <class U>
TErrorOr<T>::TErrorOr(const TErrorOr<U>& other)
    : TError(other)
{
    if (IsOK()) {
        Value_ = other.Value();
    }
}

template <class T>
template <class U>
TErrorOr<T>::TErrorOr(TErrorOr<U>&& other) noexcept
    : TError(other)
{
    if (IsOK()) {
        Value_ = std::move(other.Value());
    }
}

template <class T>
TErrorOr<T>::TErrorOr(const std::exception& ex)
    : TError(ex)
{ }

template <class T>
TErrorOr<T>& TErrorOr<T>::operator = (const TErrorOr<T>& other)
{
    static_cast<TError&>(*this) = static_cast<const TError&>(other);
    Value_ = other.Value_;
    return *this;
}

template <class T>
TErrorOr<T>& TErrorOr<T>::operator = (TErrorOr<T>&& other) noexcept
{
    static_cast<TError&>(*this) = std::move(other);
    Value_ = std::move(other.Value_);
    return *this;
}

template <class T>
T&& TErrorOr<T>::ValueOrThrow() &&
{
    if (!IsOK()) {
        THROW_ERROR std::move(*this);
    }
    return std::move(*Value_);
}

template <class T>
T& TErrorOr<T>::ValueOrThrow() &
{
    if (!IsOK()) {
        THROW_ERROR *this;
    }
    return *Value_;
}

template <class T>
const T& TErrorOr<T>::ValueOrThrow() const &
{
    if (!IsOK()) {
        THROW_ERROR *this;
    }
    return *Value_;
}

template <class T>
T&& TErrorOr<T>::Value() &&
{
    YT_ASSERT(IsOK());
    return std::move(*Value_);
}

template <class T>
T& TErrorOr<T>::Value() &
{
    YT_ASSERT(IsOK());
    return *Value_;
}

template <class T>
const T& TErrorOr<T>::Value() const &
{
    YT_ASSERT(IsOK());
    return *Value_;
}

template <class T>
const T& TErrorOr<T>::ValueOrDefault(const T& defaultValue) const &
{
    return IsOK() ? *Value_ : defaultValue;
}

template <class T>
T& TErrorOr<T>::ValueOrDefault(T& defaultValue) &
{
    return IsOK() ? *Value_ : defaultValue;
}

template <class T>
constexpr T TErrorOr<T>::ValueOrDefault(T&& defaultValue) const &
{
    return IsOK()
        ? *Value_
        : std::forward<T>(defaultValue);
}

template <class T>
constexpr T TErrorOr<T>::ValueOrDefault(T&& defaultValue) &&
{
    return IsOK()
        ? std::move(*Value_)
        : std::forward<T>(defaultValue);
}

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(TStringBuilderBase* builder, const TErrorOr<T>& error, TStringBuf spec)
{
    FormatValue(builder, static_cast<const TError&>(error), spec);
}

template <class T>
TString ToString(const TErrorOr<T>& valueOrError)
{
    return ToString(TError(valueOrError));
}

////////////////////////////////////////////////////////////////////////////////

template <class TException>
TException&& operator <<= (TException&& ex, const TError& error)
{
    YT_VERIFY(!error.IsOK());
    ex.Error() = error;
    return std::move(ex);
}

template <class TException>
TException&& operator <<= (TException&& ex, TError&& error)
{
    YT_VERIFY(!error.IsOK());
    ex.Error() = std::move(error);
    return std::move(ex);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
