#ifndef TAG_INL_H_
#error "Direct inclusion of this file is not allowed, include tag.h"
// For the sake of sane code completion.
#include "tag.h"
#endif

#include <library/cpp/yt/string/string_builder.h>

#include <utility>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

class TLoggingTagSpec
{
public:
    template <size_t N>
    consteval TLoggingTagSpec(const char (&spec)[N])
        : Spec_(spec + 1, N - 2)
    {
        static_assert(N >= 2, "Logging tag format spec must be a non-empty string literal");
        if (spec[0] != '%') {
            throw "Logging tag format spec must start with '%'";
        }
    }

    TStringBuf Get() const
    {
        return Spec_;
    }

private:
    const TStringBuf Spec_;
};

////////////////////////////////////////////////////////////////////////////////

class TLoggingTagKey
{
public:
    template <size_t N>
    consteval TLoggingTagKey(const char (&key)[N])
        : Key_(key, N - 1)
    {
        static_assert(N >= 2, "Logging tag key must be a non-empty string literal");
        for (size_t index = 0; index + 1 < N; ++index) {
            if (key[index] == '%' || key[index] == ':') {
                throw "Logging tag key must not contain '%' or ':'";
            }
        }
    }

    //! Escape hatch for the few call sites that compose a key at run time.
    static TLoggingTagKey FromRuntime(TStringBuf key)
    {
        return TLoggingTagKey(key);
    }

    TStringBuf Get() const
    {
        return Key_;
    }

private:
    const TStringBuf Key_;

    explicit TLoggingTagKey(TStringBuf key)
        : Key_(key)
    { }
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TLoggingTagList& TLoggingTagList::Add(TLoggingTagKey key, const TValue& value)
{
    DoAdd(key, value, "v"_sb);
    return *this;
}

template <class TValue>
TLoggingTagList& TLoggingTagList::Add(TLoggingTagKey key, const TValue& value, TLoggingTagSpec spec)
{
    DoAdd(key, value, spec.Get());
    return *this;
}

template <class... TArgs>
TLoggingTagList& TLoggingTagList::AddFormat(TLoggingTagKey key, TFormatString<TArgs...> format, TArgs&&... args)
{
    TStringBuilder builder;
    Format(&builder, format, std::forward<TArgs>(args)...);
    DoAdd(key, builder.GetBuffer());
    return *this;
}

template <class TValue>
TLoggingTagList TLoggingTagList::With(TLoggingTagKey key, const TValue& value) const &
{
    auto result = *this;
    result.Add(key, value);
    return result;
}

template <class TValue>
TLoggingTagList TLoggingTagList::With(TLoggingTagKey key, const TValue& value) &&
{
    Add(key, value);
    return std::move(*this);
}

template <class TValue>
TLoggingTagList TLoggingTagList::With(TLoggingTagKey key, const TValue& value, TLoggingTagSpec spec) const &
{
    auto result = *this;
    result.Add(key, value, spec);
    return result;
}

template <class TValue>
TLoggingTagList TLoggingTagList::With(TLoggingTagKey key, const TValue& value, TLoggingTagSpec spec) &&
{
    Add(key, value, spec);
    return std::move(*this);
}

template <class... TArgs>
TLoggingTagList TLoggingTagList::WithFormat(TLoggingTagKey key, TFormatString<TArgs...> format, TArgs&&... args) const &
{
    auto result = *this;
    result.AddFormat(key, format, std::forward<TArgs>(args)...);
    return result;
}

template <class... TArgs>
TLoggingTagList TLoggingTagList::WithFormat(TLoggingTagKey key, TFormatString<TArgs...> format, TArgs&&... args) &&
{
    AddFormat(key, format, std::forward<TArgs>(args)...);
    return std::move(*this);
}

inline TLoggingTagList::TLoggingTagList(TLoggingTagListPayload payload)
    : Payload_(std::move(payload))
{ }

inline TLoggingTagList& TLoggingTagList::Add(const TLoggingTagList& other)
{
    Payload_.Underlying() += other.Payload_.Underlying();
    return *this;
}

inline bool TLoggingTagList::IsEmpty() const
{
    return Payload_.Underlying().empty();
}

inline const TLoggingTagListPayload& TLoggingTagList::GetPayload() const
{
    return Payload_;
}

template <class TValue>
void TLoggingTagList::DoAdd(TLoggingTagKey key, const TValue& value, TStringBuf spec)
{
    TStringBuilder builder;
    FormatValue(&builder, value, spec);
    DoAdd(key, builder.GetBuffer());
}

////////////////////////////////////////////////////////////////////////////////

inline TLoggingTagListPayloadView AsView(const TLoggingTagListPayload& payload)
{
    return TLoggingTagListPayloadView(payload.Underlying());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
