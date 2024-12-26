#ifndef ERROR_INL_H_
#error "Direct inclusion of this file is not allowed, include error.h"
// For the sake of sane code completion.
#include "error.h"
#endif

namespace NYT::NAttributeValueConversionImpl {

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires (!CPrimitiveConvertible<T>)
NYson::TYsonString TagInvoke(TTagInvokeTag<ToErrorAttributeValue>, const T& value)
{
    return NYson::ConvertToYsonString(value, NYson::EYsonFormat::Text);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAttributeValueConversionImpl
