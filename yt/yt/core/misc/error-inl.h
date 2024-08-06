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
constexpr TErrorCode::operator E() const
{
    return static_cast<E>(Value_);
}

template <class E>
    requires std::is_enum_v<E>
constexpr bool TErrorCode::operator == (E rhs) const
{
    return Value_ == static_cast<int>(rhs);
}

constexpr bool TErrorCode::operator == (TErrorCode rhs) const
{
    return Value_ == static_cast<int>(rhs);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <size_t Length, class... TArgs>
TString FormatErrorMessage(const char (&format)[Length], TArgs&&... args)
{
    return Format(TRuntimeFormat{format}, std::forward<TArgs>(args)...);
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

template <CInvocable<bool(const TError&)> TFilter>
std::optional<TError> TError::FindMatching(const TFilter& filter) const
{
    if (!Impl_) {
        return {};
    }

    if (filter(*this)) {
        return *this;
    }

    for (const auto& innerError : InnerErrors()) {
        if (auto innerResult = innerError.FindMatching(filter)) {
            return innerResult;
        }
    }

    return {};
}

template <CInvocable<bool(TErrorCode)> TFilter>
std::optional<TError> TError::FindMatching(const TFilter& filter) const
{
    return FindMatching([&] (const TError& error) { return filter(error.GetCode()); });
}

template <class... TArgs>
    requires std::constructible_from<TError, TArgs...>
TError TError::Wrap(TArgs&&... args) const &
{
    return TError(std::forward<TArgs>(args)...) << *this;
}

template <class... TArgs>
    requires std::constructible_from<TError, TArgs...>
TError TError::Wrap(TArgs&&... args) &&
{
    return TError(std::forward<TArgs>(args)...) << std::move(*this);
}

template <CErrorNestable TValue>
TError&& TError::operator << (TValue&& rhs) &&
{
    return std::move(*this <<= std::forward<TValue>(rhs));
}

template <CErrorNestable TValue>
TError TError::operator << (TValue&& rhs) const &
{
    return TError(*this) << std::forward<TValue>(rhs);
}

template <CErrorNestable TValue>
TError&& TError::operator << (const std::optional<TValue>& rhs) &&
{
    if (rhs) {
        return std::move(*this <<= *rhs);
    } else {
        return std::move(*this);
    }
}

template <CErrorNestable TValue>
TError TError::operator << (const std::optional<TValue>& rhs) const &
{
    if (rhs) {
        return TError(*this) << *rhs;
    } else {
        return *this;
    }
}

template <class... TArgs>
void TError::ThrowOnError(TArgs&&... args) const
{
    if (!IsOK()) {
        THROW_ERROR std::move(*this).Wrap(std::forward<TArgs>(args)...);
    }
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
        Value_.emplace(other.Value());
    }
}

template <class T>
TErrorOr<T>::TErrorOr(TErrorOr<T>&& other) noexcept
    : TError(std::move(other))
{
    if (IsOK()) {
        Value_.emplace(std::move(other.Value()));
    }
}

template <class T>
template <class U>
TErrorOr<T>::TErrorOr(const TErrorOr<U>& other)
    : TError(other)
{
    if (IsOK()) {
        Value_.emplace(other.Value());
    }
}

template <class T>
template <class U>
TErrorOr<T>::TErrorOr(TErrorOr<U>&& other) noexcept
    : TError(other)
{
    if (IsOK()) {
        Value_.emplace(std::move(other.Value()));
    }
}

template <class T>
TErrorOr<T>::TErrorOr(const std::exception& ex)
    : TError(ex)
{ }

template <class T>
TErrorOr<T>& TErrorOr<T>::operator = (const TErrorOr<T>& other)
    requires std::is_copy_assignable_v<T>
{
    static_cast<TError&>(*this) = static_cast<const TError&>(other);
    Value_ = other.Value_;
    return *this;
}

template <class T>
TErrorOr<T>& TErrorOr<T>::operator = (TErrorOr<T>&& other) noexcept
    requires std::is_nothrow_move_assignable_v<T>
{
    static_cast<TError&>(*this) = std::move(other);
    Value_ = std::move(other.Value_);
    return *this;
}

template <class T>
template <class... TArgs>
T&& TErrorOr<T>::ValueOrThrow(TArgs&&... args) && Y_LIFETIME_BOUND
{
    if (!IsOK()) {
        THROW_ERROR std::move(*this).Wrap(std::forward<TArgs>(args)...);
    }
    return std::move(*Value_);
}

template <class T>
template <class... TArgs>
T& TErrorOr<T>::ValueOrThrow(TArgs&&... args) & Y_LIFETIME_BOUND
{
    if (!IsOK()) {
        THROW_ERROR Wrap(std::forward<TArgs>(args)...);
    }
    return *Value_;
}

template <class T>
template <class... TArgs>
const T& TErrorOr<T>::ValueOrThrow(TArgs&&... args) const & Y_LIFETIME_BOUND
{
    if (!IsOK()) {
        THROW_ERROR Wrap(std::forward<TArgs>(args)...);
    }
    return *Value_;
}

template <class T>
T&& TErrorOr<T>::Value() && Y_LIFETIME_BOUND
{
    YT_ASSERT(IsOK());
    return std::move(*Value_);
}

template <class T>
T& TErrorOr<T>::Value() & Y_LIFETIME_BOUND
{
    YT_ASSERT(IsOK());
    return *Value_;
}

template <class T>
const T& TErrorOr<T>::Value() const & Y_LIFETIME_BOUND
{
    YT_ASSERT(IsOK());
    return *Value_;
}

template <class T>
const T& TErrorOr<T>::ValueOrDefault(const T& defaultValue Y_LIFETIME_BOUND) const & Y_LIFETIME_BOUND
{
    return IsOK() ? *Value_ : defaultValue;
}

template <class T>
T& TErrorOr<T>::ValueOrDefault(T& defaultValue Y_LIFETIME_BOUND) & Y_LIFETIME_BOUND
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

////////////////////////////////////////////////////////////////////////////////

template <class TException>
    requires std::derived_from<std::remove_cvref_t<TException>, TErrorException>
TException&& operator <<= (TException&& ex, const TError& error)
{
    YT_VERIFY(!error.IsOK());
    ex.Error() = error;
    return std::move(ex);
}

template <class TException>
    requires std::derived_from<std::remove_cvref_t<TException>, TErrorException>
TException&& operator <<= (TException&& ex, TError&& error)
{
    YT_VERIFY(!error.IsOK());
    ex.Error() = std::move(error);
    return std::move(ex);
}

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

template <class TArg>
    requires std::constructible_from<TError, TArg>
TError TErrorAdaptor::operator << (TArg&& rhs) const
{
    return TError(std::forward<TArg>(rhs));
}

template <class TArg>
    requires
        std::constructible_from<TError, TArg> &&
        std::derived_from<std::remove_cvref_t<TArg>, TError>
TArg&& TErrorAdaptor::operator << (TArg&& rhs) const
{
    return std::forward<TArg>(rhs);
}

template <class TErrorLike, class... TArgs>
    requires
        std::derived_from<std::remove_cvref_t<TErrorLike>, TError> &&
        std::constructible_from<TError, TArgs...>
void ThrowErrorExceptionIfFailed(TErrorLike&& error, TArgs&&... args)
{
    if (!error.IsOK()) {
        THROW_ERROR std::move(error).Wrap(std::forward<TArgs>(args)...);
    }
}

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
