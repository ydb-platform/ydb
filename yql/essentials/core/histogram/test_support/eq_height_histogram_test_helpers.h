#pragma once

#include <yql/essentials/core/histogram/eq_height_histogram.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NKikimr::NHistogramTest {

inline constexpr ui64 DEFAULT_MAX_STATE_BYTES = 4ULL << 20;

TString MakeKey(i32 value);
TEqHeightHistogramBuilder::TParams MakeParams(ui32 numBuckets, ui64 maxStateBytes = DEFAULT_MAX_STATE_BYTES);
TVector<TString> MakeSortedStream(ui32 numKeys, ui32 perKey);
TEqHeightHistogramBuilder BuildFromStream(const TVector<TString>& keys, const TEqHeightHistogramBuilder::TParams& params);

} // namespace NKikimr::NHistogramTest
