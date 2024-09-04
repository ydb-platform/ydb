#pragma once

#include "double_array.h"

#include <library/cpp/yt/string/format.h>

namespace NYT::NDetail {

////////////////////////////////////////////////////////////////////////////////

// This overload is important for readable output in tests.
template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
std::ostream& operator<<(std::ostream& os, const TDerived& vec)
{
    os << NYT::ToString(vec);
    return os;
}

////////////////////////////////////////////////////////////////////////////////

}; // namespace NYT::NDetail
