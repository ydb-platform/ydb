#include "histogram_types.h"

#include <util/string/builder.h>

namespace NYdb::NBS {

namespace {

////////////////////////////////////////////////////////////////////////////////

template <size_t BucketsCount>
const TVector<TString> MakeNames(
    const std::array<double, BucketsCount>& limits,
    TStringBuf suffix)
{
    TVector<TString> names;
    for (ui32 i = 0; i < limits.size(); ++i) {
        if (i != limits.size() - 1) {
            names.emplace_back(TStringBuilder() << limits[i] << suffix);
        } else {
            names.emplace_back("Inf");
        }
    }
    return names;
}

}   // namespace

////////////////////////////////////////////////////////////////////////////////

TVector<TString> TRequestUsTimeBuckets::MakeNames()
{
    static const TVector<TString> names = NYdb::NBS::MakeNames(Buckets, "");
    return names;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TString> TRequestUsTimeBucketsLowResolution::MakeNames()
{
    static const TVector<TString> names = NYdb::NBS::MakeNames(Buckets, "");
    return names;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TString> TRequestMsTimeBuckets::MakeNames()
{
    static const TVector<TString> names = NYdb::NBS::MakeNames(Buckets, "ms");
    return names;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TString> TQueueSizeBuckets::MakeNames()
{
    static const TVector<TString> names = NYdb::NBS::MakeNames(Buckets, "");
    return names;
}

////////////////////////////////////////////////////////////////////////////////

TVector<TString> TKbSizeBuckets::MakeNames()
{
    static const TVector<TString> names = NYdb::NBS::MakeNames(Buckets, "KB");
    return names;
}

}   // namespace NYdb::NBS
