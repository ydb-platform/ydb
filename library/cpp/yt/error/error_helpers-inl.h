#ifndef ERROR_HELPERS_INL_H_
#error "Direct inclusion of this file is not allowed, include error_helpers.h"
// For the sake of sane code completion.
#include "error_helpers.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
typename TOptionalTraits<T>::TOptional FindAttribute(const TError& error, TStringBuf key)
{
    return error.Attributes().Find<T>(key);
}

template <class T>
typename TOptionalTraits<T>::TOptional FindAttributeRecursive(const TError& error, TStringBuf key)
{
    auto attr = FindAttribute<T>(error, key);
    if (TOptionalTraits<T>::HasValue(attr)) {
        return attr;
    }

    for (const auto& inner : error.InnerErrors()) {
        attr = FindAttribute<T>(inner, key);
        if (TOptionalTraits<T>::HasValue(attr)) {
            return attr;
        }
    }

    return TOptionalTraits<T>::Empty();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
