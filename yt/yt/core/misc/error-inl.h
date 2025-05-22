#ifndef ERROR_INL_H_
#error "Direct inclusion of this file is not allowed, include error.h"
// For the sake of sane code completion.
#include "error.h"
#endif

namespace NYT::NAttributeValueConversionImpl {

////////////////////////////////////////////////////////////////////////////////

template <class T>
    requires (!CPrimitiveConvertible<T>)
std::string TagInvoke(TTagInvokeTag<ToErrorAttributeValue>, const T& value)
{
    return std::string(NYson::ConvertToYsonString(value, NYson::EYsonFormat::Text).ToString());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NAttributeValueConversionImpl
