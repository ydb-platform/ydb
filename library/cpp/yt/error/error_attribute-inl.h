#ifndef ERROR_ATTRIBUTE_INL_H_
#error "Direct inclusion of this file is not allowed, include error_attribute.h"
// For the sake of sane code completion.
#include "error_attribute.h"
#endif

#include <library/cpp/yt/yson_string/convert.h>
#include <library/cpp/yt/yson_string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

namespace NAttributeValueConversionImpl {

template <CPrimitiveConvertible T>
NYson::TYsonString TagInvoke(TTagInvokeTag<ToErrorAttributeValue>, const T& value)
{
    if constexpr (std::constructible_from<TStringBuf, const T&>) {
        return NYson::ConvertToTextYsonString(TStringBuf(value));
    } else {
        return NYson::ConvertToTextYsonString(value);
    }
}

////////////////////////////////////////////////////////////////////////////////

inline bool IsBinaryYson(const NYson::TYsonString& yson)
{
    using namespace NYson::NDetail;

    auto view = yson.AsStringBuf();
    return
        std::ssize(view) != 0 &&
        (view.front() == EntitySymbol ||
         view.front() == StringMarker ||
         view.front() == Int64Marker ||
         view.front() == DoubleMarker ||
         view.front() == FalseMarker ||
         view.front() == TrueMarker ||
         view.front() == Uint64Marker);
}

////////////////////////////////////////////////////////////////////////////////

template <CPrimitiveConvertible T>
T TagInvoke(TFrom<T>, const NYson::TYsonString& value)
{
    YT_VERIFY(!IsBinaryYson(value));
    return NYson::ConvertFromTextYsonString<T>(value);
}

} // namespace NAttributeValueConversionImpl

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
