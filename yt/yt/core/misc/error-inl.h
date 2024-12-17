#ifndef ERROR_INL_H_
#error "Direct inclusion of this file is not allowed, include error.h"
// For the sake of sane code completion.
#include "error.h"
#endif

namespace NYT::NToAttributeValueImpl {

////////////////////////////////////////////////////////////////////////////////

template <class T>
NYson::TYsonString TagInvoke(TTagInvokeTag<ToAttributeValue>, const T& value)
{
    return NYson::ConvertToYsonString(value);
}

inline NYson::TYsonString TagInvoke(TTagInvokeTag<ToAttributeValue>, const NYson::TYsonString& value)
{
    return value;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NToAttributeValueImpl
