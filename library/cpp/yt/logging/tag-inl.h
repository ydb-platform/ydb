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
            TheLoggingTagFormatSpecMustStartWithPercentSign();
        }
    }

    TStringBuf Get() const
    {
        return Spec_;
    }

private:
    const TStringBuf Spec_;

    // Undefined on purpose: calling it from the |consteval| ctor turns a missing
    // leading |%| into a compile error that names the violated rule.
    static void TheLoggingTagFormatSpecMustStartWithPercentSign();
};

////////////////////////////////////////////////////////////////////////////////

template <class TValue>
TLoggingTagList& TLoggingTagList::With(TStringBuf key, const TValue& value) &
{
    DoWith(key, value, "v"_sb);
    return *this;
}

template <class TValue>
TLoggingTagList& TLoggingTagList::With(TStringBuf key, const TValue& value, TLoggingTagSpec spec) &
{
    DoWith(key, value, spec.Get());
    return *this;
}

template <class... TArgs>
TLoggingTagList& TLoggingTagList::WithFormat(TStringBuf key, TFormatString<TArgs...> format, TArgs&&... args) &
{
    TStringBuilder builder;
    Format(&builder, format, std::forward<TArgs>(args)...);
    AppendTag(key, builder.GetBuffer());
    return *this;
}

template <class TValue>
TLoggingTagList&& TLoggingTagList::With(TStringBuf key, const TValue& value) &&
{
    DoWith(key, value, "v"_sb);
    return std::move(*this);
}

template <class TValue>
TLoggingTagList&& TLoggingTagList::With(TStringBuf key, const TValue& value, TLoggingTagSpec spec) &&
{
    DoWith(key, value, spec.Get());
    return std::move(*this);
}

template <class... TArgs>
TLoggingTagList&& TLoggingTagList::WithFormat(TStringBuf key, TFormatString<TArgs...> format, TArgs&&... args) &&
{
    TStringBuilder builder;
    Format(&builder, format, std::forward<TArgs>(args)...);
    AppendTag(key, builder.GetBuffer());
    return std::move(*this);
}

inline bool TLoggingTagList::IsEmpty() const
{
    return Payload_.empty();
}

inline TStringBuf TLoggingTagList::GetPayload() const
{
    return Payload_;
}

template <class TValue>
void TLoggingTagList::DoWith(TStringBuf key, const TValue& value, TStringBuf spec)
{
    TStringBuilder builder;
    FormatValue(&builder, value, spec);
    AppendTag(key, builder.GetBuffer());
}

inline void TLoggingTagList::AppendTag(TStringBuf key, TStringBuf value)
{
    TTaggedPayloadWriter::AppendTag(&Payload_, key, value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging
