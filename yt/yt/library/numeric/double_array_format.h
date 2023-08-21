#pragma once

#include "vector_format.h"
#include "double_array.h"

#include <util/string/builder.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

// This overload is important for readable output in tests.
template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
std::ostream& operator<<(std::ostream& os, const TDerived& vec)
{
    os << ToString(vec);
    return os;
}

////////////////////////////////////////////////////////////////////////////////

}; // namespace NYT::NDetail
