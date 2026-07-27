#pragma once

#include "tagged_payload.h"

#include <library/cpp/yt/string/format.h>

#include <util/generic/strbuf.h>

#include <string>

namespace NYT::NLogging {

////////////////////////////////////////////////////////////////////////////////

//! Wraps the format spec passed to a tag-appending |With| and validates at compile time
//! that it is a |%|-prefixed string literal (e.g. |"%v"|, |"%08x"|).
class TLoggingTagSpec;

////////////////////////////////////////////////////////////////////////////////

//! An opaque, pre-serialized list of logging tags.
class TLoggingTagList
{
public:
    TLoggingTagList() = default;

    template <class TValue>
    TLoggingTagList& With(TStringBuf key, const TValue& value) &;
    template <class TValue>
    TLoggingTagList& With(TStringBuf key, const TValue& value, TLoggingTagSpec spec) &;
    template <class... TArgs>
    TLoggingTagList& WithFormat(TStringBuf key, TFormatString<TArgs...> format, TArgs&&... args) &;

    template <class TValue>
    TLoggingTagList&& With(TStringBuf key, const TValue& value) &&;
    template <class TValue>
    TLoggingTagList&& With(TStringBuf key, const TValue& value, TLoggingTagSpec spec) &&;
    template <class... TArgs>
    TLoggingTagList&& WithFormat(TStringBuf key, TFormatString<TArgs...> format, TArgs&&... args) &&;

    bool IsEmpty() const;

    //! The serialized tag section, spliced verbatim by #TTaggedPayloadWriter::AppendTags.
    TStringBuf GetPayload() const;

private:
    std::string Payload_;

    template <class TValue>
    void DoWith(TStringBuf key, const TValue& value, TStringBuf spec);
    void AppendTag(TStringBuf key, TStringBuf value);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NLogging

#define TAG_INL_H_
#include "tag-inl.h"
#undef TAG_INL_H_
