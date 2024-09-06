#ifndef RANGE_FORMATTERS_INL_H_
#error "Direct inclusion of this file is not allowed, include range_formatters.h"
// For the sake of sane code completion.
#include "range_formatters.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(TStringBuilderBase* builder, const TRange<T>& collection, TStringBuf /*spec*/)
{
    NYT::FormatRange(builder, collection, TDefaultFormatter());
}

template <class T>
void FormatValue(TStringBuilderBase* builder, const TSharedRange<T>& collection, TStringBuf /*spec*/)
{
    NYT::FormatRange(builder, collection, TDefaultFormatter());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
