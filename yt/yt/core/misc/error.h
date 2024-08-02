#pragma once

#include "public.h"

#include <yt/yt/core/yson/public.h>

#include <yt/yt/core/ytree/public.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/threading/public.h>

#include <library/cpp/yt/yson/public.h>

#include <library/cpp/yt/yson_string/convert.h>
#include <library/cpp/yt/yson_string/string.h>

#include <library/cpp/yt/misc/property.h>

#include <util/system/compiler.h>
#include <util/system/getpid.h>

#include <util/generic/size_literals.h>

#include <type_traits>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! An opaque wrapper around |int| value capable of conversions from |int|s and
//! arbitrary enum types.
class TErrorCode
{
public:
    using TUnderlying = int;

    constexpr TErrorCode();
    explicit constexpr TErrorCode(int value);
    template <class E>
        requires std::is_enum_v<E>
    constexpr TErrorCode(E value);

    constexpr operator int() const;

    template <class E>
        requires std::is_enum_v<E>
    constexpr operator E() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    template <class E>
        requires std::is_enum_v<E>
    constexpr bool operator == (E rhs) const;

    constexpr bool operator == (TErrorCode rhs) const;

private:
    int Value_;
};

void FormatValue(TStringBuilderBase* builder, TErrorCode code, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

constexpr int ErrorSerializationDepthLimit = 16;

////////////////////////////////////////////////////////////////////////////////

//! When this guard is set, newly created errors do not have non-deterministic
//! system attributes and have "datetime" and "host" attributes overridden with a given values.
class TErrorSanitizerGuard
    : public TNonCopyable
{
public:
    TErrorSanitizerGuard(TInstant datetimeOverride, TSharedRef localHostNameOverride);
    ~TErrorSanitizerGuard();

private:
    const bool SavedEnabled_;
    const TInstant SavedDatetimeOverride_;
    const TSharedRef SavedLocalHostNameOverride_;
};

////////////////////////////////////////////////////////////////////////////////

struct TErrorAttribute
{
    template <class T>
    TErrorAttribute(const TString& key, const T& value)
        : Key(key)
        , Value(NYson::ConvertToYsonString(value))
    { }

    TErrorAttribute(const TString& key, const NYson::TYsonString& value)
        : Key(key)
        , Value(value)
    { }

    TString Key;
    NYson::TYsonString Value;
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
concept CErrorNestable = requires (TError& error, TValue&& operand)
{
    { error <<= std::forward<TValue>(operand) } -> std::same_as<TError&>;
};

template <>
class [[nodiscard]] TErrorOr<void>
{
public:
    TErrorOr();
    ~TErrorOr();

    TErrorOr(const TError& other);
    TErrorOr(TError&& other) noexcept;

    TErrorOr(const std::exception& ex);

    explicit TErrorOr(TString message);
    TErrorOr(TErrorCode code, TString message);

    template <size_t Length, class... TArgs>
    explicit TErrorOr(
        const char (&messageOrFormat)[Length],
        TArgs&&... arg);

    template <size_t Length, class... TArgs>
    TErrorOr(
        TErrorCode code,
        const char (&messageOrFormat)[Length],
        TArgs&&... args);

    TError& operator = (const TError& other);
    TError& operator = (TError&& other) noexcept;

    static TError FromSystem();
    static TError FromSystem(int error);
    static TError FromSystem(const TSystemError& error);

    TErrorCode GetCode() const;
    TError& SetCode(TErrorCode code);

    TErrorCode GetNonTrivialCode() const;
    THashSet<TErrorCode> GetDistinctNonTrivialErrorCodes() const;

    const TString& GetMessage() const;
    TError& SetMessage(TString message);

    bool HasHost() const;
    TStringBuf GetHost() const;

    bool HasOriginAttributes() const;
    TProcessId GetPid() const;
    TStringBuf GetThreadName() const;
    NThreading::TThreadId GetTid() const;
    NConcurrency::TFiberId GetFid() const;

    bool HasDatetime() const;
    TInstant GetDatetime() const;

    bool HasTracingAttributes() const;
    void SetTracingAttributes(NTracing::TTracingAttributes tracingAttributes);
    NTracing::TTraceId GetTraceId() const;
    NTracing::TSpanId GetSpanId() const;

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary* MutableAttributes();

    const std::vector<TError>& InnerErrors() const;
    std::vector<TError>* MutableInnerErrors();

    TError Truncate(
        int maxInnerErrorCount = 2,
        i64 stringLimit = 16_KB,
        const THashSet<TStringBuf>& attributeWhitelist = {}) const &;
    TError Truncate(
        int maxInnerErrorCount = 2,
        i64 stringLimit = 16_KB,
        const THashSet<TStringBuf>& attributeWhitelist = {}) &&;

    bool IsOK() const;

    template <class... TArgs>
    void ThrowOnError(TArgs&&... args) const;

    template <CInvocable<bool(const TError&)> TFilter>
    std::optional<TError> FindMatching(const TFilter& filter) const;
    template <CInvocable<bool(TErrorCode)> TFilter>
    std::optional<TError> FindMatching(const TFilter& filter) const;
    std::optional<TError> FindMatching(TErrorCode code) const;
    std::optional<TError> FindMatching(const THashSet<TErrorCode>& codes) const;

    template <class... TArgs>
        requires std::constructible_from<TError, TArgs...>
    TError Wrap(TArgs&&... args) const &;
    TError Wrap() const &;

    template <class... TArgs>
        requires std::constructible_from<TError, TArgs...>
    TError Wrap(TArgs&&... args) &&;
    TError Wrap() &&;

    //! Perform recursive aggregation of error codes and messages over the error tree.
    //! Result of this aggregation is suitable for error clustering in groups of
    //! "similar" errors. Refer to yt/yt/library/error_skeleton/skeleton_ut.cpp for examples.
    //!
    //! This method builds skeleton from scratch by doing complete error tree traversal,
    //! so calling it in computationally hot code is discouraged.
    //!
    //! In order to prevent core -> re2 dependency, implementation belongs to a separate library
    //! yt/yt/library/error_skeleton. Calling this method without PEERDIR'ing implementation
    //! results in an exception.
    TString GetSkeleton() const;

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

    TError& operator <<= (const TErrorAttribute& attribute) &;
    TError& operator <<= (const std::vector<TErrorAttribute>& attributes) &;
    TError& operator <<= (const TError& innerError) &;
    TError& operator <<= (TError&& innerError) &;
    TError& operator <<= (const std::vector<TError>& innerErrors) &;
    TError& operator <<= (std::vector<TError>&& innerErrors) &;
    TError& operator <<= (const NYTree::IAttributeDictionary& attributes) &;

    template <CErrorNestable TValue>
    TError&& operator << (TValue&& operand) &&;
    template <CErrorNestable TValue>
    TError operator << (TValue&& operand) const &;

    template <CErrorNestable TValue>
    TError&& operator << (const std::optional<TValue>& operand) &&;
    template <CErrorNestable TValue>
    TError operator << (const std::optional<TValue>& operand) const &;

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    explicit TErrorOr(std::unique_ptr<TImpl> impl);

    void MakeMutable();

    friend bool operator == (const TError& lhs, const TError& rhs);

    friend void ToProto(NProto::TError* protoError, const TError& error);
    friend void FromProto(TError* error, const NProto::TError& protoError);

    friend void Serialize(
        const TError& error,
        NYson::IYsonConsumer* consumer,
        const std::function<void(NYson::IYsonConsumer*)>* valueProducer,
        int depth);
    friend void Deserialize(TError& error, const NYTree::INodePtr& node);
    friend void Deserialize(TError& error, NYson::TYsonPullParserCursor* cursor);
};

bool operator == (const TError& lhs, const TError& rhs);

void ToProto(NProto::TError* protoError, const TError& error);
void FromProto(TError* error, const NProto::TError& protoError);

using TErrorVisitor = std::function<void(const TError&, int depth)>;

//! Traverses the error tree in DFS order.
void TraverseError(
    const TError& error,
    const TErrorVisitor& visitor,
    int depth = 0);

void Serialize(
    const TError& error,
    NYson::IYsonConsumer* consumer,
    const std::function<void(NYson::IYsonConsumer*)>* valueProducer = nullptr,
    int depth = 0);
void Deserialize(
    TError& error,
    const NYTree::INodePtr& node);

void FormatValue(TStringBuilderBase* builder, const TError& error, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

template <class T>
struct TErrorTraits
{
    using TWrapped = TErrorOr<T>;
    using TUnwrapped = T;
};

template <class T>
struct TErrorTraits<TErrorOr<T>>
{
    using TUnderlying = T;
    using TWrapped = TErrorOr<T>;
    using TUnwrapped = T;
};

////////////////////////////////////////////////////////////////////////////////

class TErrorException
    : public std::exception
{
public:
    DEFINE_BYREF_RW_PROPERTY(TError, Error);

public:
    TErrorException() = default;
    TErrorException(const TErrorException& other) = default;
    TErrorException(TErrorException&& other) = default;

    const char* what() const noexcept override;

private:
    mutable TString CachedWhat_;
};

// Make these templates to avoid type erasure during throw.
template <class TException>
    requires std::derived_from<std::remove_cvref_t<TException>, TErrorException>
TException&& operator <<= (TException&& ex, const TError& error);
template <class TException>
    requires std::derived_from<std::remove_cvref_t<TException>, TErrorException>
TException&& operator <<= (TException&& ex, TError&& error);

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

struct TErrorAdaptor
{
    template <class TArg>
        requires std::constructible_from<TError, TArg>
    TError operator << (TArg&& rhs) const;

    template <class TArg>
        requires
            std::constructible_from<TError, TArg> &&
            std::derived_from<std::remove_cvref_t<TArg>, TError>
    TArg&& operator << (TArg&& rhs) const;
};

// Make these to correctly forward TError to Wrap call.
template <class TErrorLike, class... TArgs>
    requires
        std::derived_from<std::remove_cvref_t<TErrorLike>, TError> &&
        std::constructible_from<TError, TArgs...>
void ThrowErrorExceptionIfFailed(TErrorLike&& error, TArgs&&... args);

} // namespace NDetail

////////////////////////////////////////////////////////////////////////////////

#define THROW_ERROR \
    throw ::NYT::TErrorException() <<= ::NYT::NDetail::TErrorAdaptor() <<

#define THROW_ERROR_EXCEPTION(head, ...) \
    THROW_ERROR ::NYT::TError(head __VA_OPT__(,) __VA_ARGS__)

#define THROW_ERROR_EXCEPTION_IF_FAILED(error, ...) \
    ::NYT::NDetail::ThrowErrorExceptionIfFailed((error) __VA_OPT__(,) __VA_ARGS__); \

#define THROW_ERROR_EXCEPTION_UNLESS(condition, head, ...) \
    if ((condition)) {\
    } else { \
        THROW_ERROR ::NYT::TError(head __VA_OPT__(,) __VA_ARGS__); \
    }

#define THROW_ERROR_EXCEPTION_IF(condition, head, ...) \
    THROW_ERROR_EXCEPTION_UNLESS(!(condition), head, __VA_ARGS__)

////////////////////////////////////////////////////////////////////////////////

template <class T>
class [[nodiscard]] TErrorOr
    : public TError
{
public:
    TErrorOr();

    TErrorOr(const T& value);
    TErrorOr(T&& value) noexcept;

    TErrorOr(const TErrorOr<T>& other);
    TErrorOr(TErrorOr<T>&& other) noexcept;

    TErrorOr(const TError& other);
    TErrorOr(TError&& other) noexcept;

    TErrorOr(const std::exception& ex);

    template <class U>
    TErrorOr(const TErrorOr<U>& other);
    template <class U>
    TErrorOr(TErrorOr<U>&& other) noexcept;

    TErrorOr<T>& operator = (const TErrorOr<T>& other)
        requires std::is_copy_assignable_v<T>;
    TErrorOr<T>& operator = (TErrorOr<T>&& other) noexcept
        requires std::is_nothrow_move_assignable_v<T>;

    const T& Value() const & Y_LIFETIME_BOUND;
    T& Value() & Y_LIFETIME_BOUND;
    T&& Value() && Y_LIFETIME_BOUND;

    template <class... TArgs>
    const T& ValueOrThrow(TArgs&&... args) const & Y_LIFETIME_BOUND;

    template <class... TArgs>
    T& ValueOrThrow(TArgs&&... args) & Y_LIFETIME_BOUND;

    template <class... TArgs>
    T&& ValueOrThrow(TArgs&&... args) && Y_LIFETIME_BOUND;

    const T& ValueOrDefault(const T& defaultValue Y_LIFETIME_BOUND) const & Y_LIFETIME_BOUND;
    T& ValueOrDefault(T& defaultValue Y_LIFETIME_BOUND) & Y_LIFETIME_BOUND;
    constexpr T ValueOrDefault(T&& defaultValue) const &;
    constexpr T ValueOrDefault(T&& defaultValue) &&;

private:
    std::optional<T> Value_;
};

template <class T>
void FormatValue(TStringBuilderBase* builder, const TErrorOr<T>& error, TStringBuf spec);

////////////////////////////////////////////////////////////////////////////////

template <class F, class... As>
auto RunNoExcept(F&& functor, As&&... args) noexcept -> decltype(functor(std::forward<As>(args)...))
{
    return functor(std::forward<As>(args)...);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ERROR_INL_H_
#include "error-inl.h"
#undef ERROR_INL_H_
