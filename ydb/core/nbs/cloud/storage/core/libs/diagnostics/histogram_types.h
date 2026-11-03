#pragma once

#include "public.h"

#include <util/generic/vector.h>
#include <util/string/cast.h>

#include <array>

namespace NYdb::NBS {

////////////////////////////////////////////////////////////////////////////////

struct TRequestUsTimeBuckets
{
    static constexpr size_t BUCKETS_COUNT = 25;

    static constexpr std::array<double, BUCKETS_COUNT> Buckets = {
        {1,       100,      200,      300,          400,    500,     600,
         700,     800,      900,      1000,         2000,   5000,    10000,
         20000,   50000,    100000,   200000,       500000, 1000000, 2000000,
         5000000, 10000000, 35000000, Max<double>()}};

    static constexpr TStringBuf Units = "usec";
    static constexpr double PercentileMultiplier = 1.0;

    static TVector<TString> MakeNames();

    static const TString& GetBucketName(TDuration duration)
    {
        static const auto Names = MakeNames();

        auto micros = duration.MicroSeconds();
        auto it = LowerBound(Buckets.begin(), Buckets.end(), micros);
        auto idx = std::distance(Buckets.begin(), it);
        return Names[idx];
    }
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestUsTimeBucketsLowResolution
{
    static constexpr size_t BUCKETS_COUNT = 15;

    static constexpr std::array<double, BUCKETS_COUNT> Buckets = {
        {1000,
         2000,
         5000,
         10000,
         20000,
         50000,
         100000,
         200000,
         500000,
         1000000,
         2000000,
         5000000,
         10000000,
         35000000,
         Max<double>()}};

    static constexpr TStringBuf Units = "usec";
    static constexpr double PercentileMultiplier = 1.0;

    static TVector<TString> MakeNames();
};

////////////////////////////////////////////////////////////////////////////////

struct TRequestMsTimeBuckets
{
    static constexpr size_t BUCKETS_COUNT =
        TRequestUsTimeBuckets::BUCKETS_COUNT;
    static constexpr TStringBuf Units = "";
    // Uses buckets in milliseconds for the histogram, but reports values in
    // microseconds for percentiles.
    static constexpr double PercentileMultiplier = 1000.0;

    static constexpr auto MakeArray =
        [](const std::array<double, BUCKETS_COUNT>& array)
    {
        std::array<double, BUCKETS_COUNT> result;
        for (size_t i = 0; i + 1 < array.size(); ++i) {
            result[i] = array[i] / PercentileMultiplier;
        }
        result.back() = std::numeric_limits<double>::max();
        return result;
    };

    static constexpr std::array<double, BUCKETS_COUNT> Buckets =
        MakeArray(TRequestUsTimeBuckets::Buckets);

    static TVector<TString> MakeNames();
};

////////////////////////////////////////////////////////////////////////////////

struct TQueueSizeBuckets
{
    static constexpr size_t BUCKETS_COUNT = 15;

    static constexpr std::array<double, BUCKETS_COUNT> Buckets = {
        {1,
         2,
         5,
         10,
         20,
         50,
         100,
         200,
         500,
         1000,
         2000,
         5000,
         10000,
         35000,
         Max<double>()}};

    static constexpr TStringBuf Units = "";
    static constexpr double PercentileMultiplier = 1.0;

    static TVector<TString> MakeNames();
};

////////////////////////////////////////////////////////////////////////////////

struct TKbSizeBuckets
{
    static constexpr size_t BUCKETS_COUNT = 12;

    static constexpr std::array<double, BUCKETS_COUNT> Buckets = {
        {4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, Max<double>()}};

    static constexpr TStringBuf Units = "KB";
    // NOTE: maybe should be using multiplier of 1024 to increase the
    // percentiles resolution.
    static constexpr double PercentileMultiplier = 1.0;

    static TVector<TString> MakeNames();
};

template <class TBucketsType>
inline TVector<double> ConvertToHistBounds(const TBucketsType& buckets)
{
    return {buckets.begin(), std::prev(buckets.end())};
}

}   // namespace NYdb::NBS
