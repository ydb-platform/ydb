#pragma once

#include <library/cpp/yt/misc/tag_invoke_cpo.h>

// TODO(arkady-e1ppa): Eliminate.
#include <library/cpp/yt/yson_string/string.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NToAttributeValueImpl {

struct TFn
    : public TTagInvokeCpoBase<TFn>
{ };

} // namespace NToAttributeValueImpl

////////////////////////////////////////////////////////////////////////////////

inline constexpr NToAttributeValueImpl::TFn ToAttributeValue = {};

////////////////////////////////////////////////////////////////////////////////

template <class T>
concept CConvertibleToAttributeValue = CTagInvocableS<
    TTagInvokeTag<ToAttributeValue>,
    NYson::TYsonString(const T&)>;

////////////////////////////////////////////////////////////////////////////////

struct TErrorAttribute
{
    // NB(arkady-e1ppa): Switch to std::string is quite possible
    // however it requires patching IAttributeDictionary or
    // switching it to the std::string first for interop reasons.
    // Do that later.
    using TKey = TString;
    // TODO(arkady-e1ppa): Use ConvertToYsonString(value, Format::Text)
    // here for complex values. Write manual implementations as ToString
    // for primitive types (e.g. integral types, guid, string, time).
    using TValue = NYson::TYsonString;

    template <CConvertibleToAttributeValue T>
    TErrorAttribute(const TKey& key, const T& value)
        : Key(key)
        , Value(NYT::ToAttributeValue(value))
    { }

    TKey Key;
    TValue Value;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
