#pragma once

#include <yt/yt/library/numeric/double_array.h>

#include <library/cpp/yt/string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class TDerived>
    requires (IsDoubleArray<TDerived>)
void FormatValue(TStringBuilderBase* builder, const TDerived& vec, TStringBuf spec)
{
    builder->AppendChar('[');
    FormatValue(builder, vec[0], spec);
    for (size_t i = 1; i < TDerived::Size; i++) {
        builder->AppendChar(' ');
        FormatValue(builder, vec[i], spec);
    }
    builder->AppendChar(']');
}

// TODO(ignat)
// template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
// IOutputStream& operator<<(IOutputStream& os, const TDerived& vec)
// {
//     return os << Format("%v", vec);
// }

template <class TDerived, class = std::enable_if_t<IsDoubleArray<TDerived>>>
std::ostream& operator<<(std::ostream& os, const TDerived& vec)
{
    return os << Format("%v", vec);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
