#pragma once

#include "public.h"
#include "property.h"
#include "optional.h"

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/attributes.h>

#include <yt/yt/core/tracing/public.h>

#include <yt/yt/core/concurrency/public.h>

#include <yt/yt/core/threading/public.h>

#include <library/cpp/yt/yson_string/convert.h>

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

    void Save(TStreamSaveContext& context) const;
    void Load(TStreamLoadContext& context);

private:
    int Value_;
};

void FormatValue(TStringBuilderBase* builder, TErrorCode code, TStringBuf spec);
TString ToString(TErrorCode code);

template <class E>
requires std::is_enum_v<E>
constexpr bool operator == (TErrorCode lhs, E rhs);

constexpr bool operator == (TErrorCode lhs, TErrorCode rhs);

////////////////////////////////////////////////////////////////////////////////

constexpr int ErrorSerializationDepthLimit = 16;

////////////////////////////////////////////////////////////////////////////////

//! When this guard is set, newly created errors do not have non-deterministic
//! system attributes and have "datetime" attribute overridden with a given value.
class TErrorSanitizerGuard
    : public TNonCopyable
{
public:
    explicit TErrorSanitizerGuard(TInstant datetimeOverride);
    ~TErrorSanitizerGuard();

private:
    const bool SavedEnabled_;
    const TInstant SavedDatetimeOverride_;
};

////////////////////////////////////////////////////////////////////////////////

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

    bool HasOriginAttributes() const;
    TStringBuf GetHost() const;
    TProcessId GetPid() const;
    TStringBuf GetThreadName() const;
    NThreading::TThreadId GetTid() const;
    NConcurrency::TFiberId GetFid() const;

    bool HasDatetime() const;
    TInstant GetDatetime() const;

    bool HasTracingAttributes() const;
    NTracing::TTraceId GetTraceId() const;
    NTracing::TSpanId GetSpanId() const;

    const NYTree::IAttributeDictionary& Attributes() const;
    NYTree::IAttributeDictionary* MutableAttributes();

    const std::vector<TError>& InnerErrors() const;
    std::vector<TError>* MutableInnerErrors();

    // TODO(gritukan): This method is used only outside of YT. Remove it.
    TError Sanitize() const;

    TError Truncate(int maxInnerErrorCount = 2, i64 stringLimit = 16_KB) const;

    bool IsOK() const;

    void ThrowOnError() const;

    std::optional<TError> FindMatching(TErrorCode code) const;

    template <class... TArgs>
    TError Wrap(TArgs&&... args) const;
    TError Wrap() const;

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

private:
    class TImpl;
    std::unique_ptr<TImpl> Impl_;

    explicit TErrorOr(std::unique_ptr<TImpl> impl);

    void MakeMutable();

    friend bool operator == (const TError& lhs, const TError& rhs);
    friend bool operator != (const TError& lhs, const TError& rhs);

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
bool operator != (const TError& lhs, const TError& rhs);

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
TString ToString(const TError& error);

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

TError operator << (TError error, const TErrorAttribute& attribute);
TError operator << (TError error, const std::vector<TErrorAttribute>& attributes);
TError operator << (TError error, const TError& innerError);
TError operator << (TError error, TError&& innerError);
TError operator << (TError error, const std::vector<TError>& innerErrors);
TError operator << (TError error, std::vector<TError>&& innerErrors);
TError operator << (TError error, const NYTree::IAttributeDictionary& attributes);

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
TException&& operator <<= (TException&& ex, const TError& error);
template <class TException>
TException&& operator <<= (TException&& ex, TError&& error);

////////////////////////////////////////////////////////////////////////////////

#define THROW_ERROR \
    throw ::NYT::TErrorException() <<=

#define THROW_ERROR_EXCEPTION(head, ...) \
    THROW_ERROR ::NYT::TError(head __VA_OPT__(,) __VA_ARGS__)

#define THROW_ERROR_EXCEPTION_IF_FAILED(error, ...) \
    if (const auto& error__ ## __LINE__  = (error); error__ ## __LINE__ .IsOK()) { \
    } else { \
        THROW_ERROR error__ ## __LINE__.Wrap(__VA_ARGS__); \
    }

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

    TErrorOr<T>& operator = (const TErrorOr<T>& other);
    TErrorOr<T>& operator = (TErrorOr<T>&& other) noexcept;

    const T& Value() const &;
    T& Value() &;
    T&& Value() &&;

    const T& ValueOrThrow() const &;
    T& ValueOrThrow() &;
    T&& ValueOrThrow() &&;

    const T& ValueOrDefault(const T& defaultValue) const &;
    T& ValueOrDefault(T& defaultValue) &;
    constexpr T ValueOrDefault(T&& defaultValue) const &;
    constexpr T ValueOrDefault(T&& defaultValue) &&;

private:
    std::optional<T> Value_;
};

template <class T>
void FormatValue(TStringBuilderBase* builder, const TErrorOr<T>& error, TStringBuf spec);

template <class T>
TString ToString(const TErrorOr<T>& valueOrError);

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
