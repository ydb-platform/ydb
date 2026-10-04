#pragma once

#include "public.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

const TVector<TPercentileDesc>& GetDefaultPercentiles();
const TVector<TString>& GetDefaultPercentileNames();

////////////////////////////////////////////////////////////////////////////////

TVector<double> CalculateWeightedPercentiles(
    const TVector<TBucketInfo>& buckets,
    const TVector<TPercentileDesc>& percentiles);

}   // namespace NYdb::NBS
