#pragma once

#include "error.h"

#include <yt/yt/core/ytree/attributes.h>

#include <library/cpp/yt/misc/optional.h>

namespace NYT {

// NB: Methods below are listed in a separate file and not in error.h to prevent
// circular includes cause by the fact that attributes include error.
////////////////////////////////////////////////////////////////////////////////

template <class T>
typename TOptionalTraits<T>::TOptional FindAttribute(const TError& error, TStringBuf key);

template <class T>
typename TOptionalTraits<T>::TOptional FindAttributeRecursive(const TError& error, TStringBuf key);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define ERROR_HELPERS_INL_H_
#include "error_helpers-inl.h"
#undef ERROR_HELPERS_INL_H_
