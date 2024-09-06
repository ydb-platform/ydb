#pragma once

#include "range.h"
#include "shared_range.h"

#include <library/cpp/yt/string/format.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T>
void FormatValue(TStringBuilderBase* builder, const TRange<T>& collection, TStringBuf /*spec*/);

template <class T>
void FormatValue(TStringBuilderBase* builder, const TSharedRange<T>& collection, TStringBuf /*spec*/);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define RANGE_FORMATTERS_INL_H_
#include "range_formatters-inl.h"
#undef RANGE_FORMATTERS_INL_H_
