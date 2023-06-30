#pragma once

#include "range.h"
#include "shared_range.h"

#include <library/cpp/yt/string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

// TRange formatter
template <class T>
struct TValueFormatter<TRange<T>, void>
{
    static void Do(TStringBuilderBase* builder, TRange<T> range, TStringBuf /*format*/)
    {
        FormatRange(builder, range, TDefaultFormatter());
    }
};

// TSharedRange formatter
template <class T>
struct TValueFormatter<TSharedRange<T>>
{
    static void Do(TStringBuilderBase* builder, const TSharedRange<T>& range, TStringBuf /*format*/)
    {
        FormatRange(builder, range, TDefaultFormatter());
    }
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
