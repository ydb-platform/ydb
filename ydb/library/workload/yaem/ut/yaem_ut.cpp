#include <ydb/library/workload/yaem/yaem.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/random/mersenne.h>

#include <cmath>

using namespace NYdbWorkload;
using namespace NYdbWorkload::NYaem;

namespace {

constexpr i64 Second = 1000 * 1000;
constexpr i64 Minute = 60 * Second;
constexpr i64 Hour = 60 * Minute;
constexpr i64 Day = 24 * Hour;

size_t FindBucket(const TLagDistribution& lag, i64 lagUs) {
    const auto& buckets = lag.GetBuckets();
    for (size_t i = 0; i < buckets.size(); ++i) {
        if (buckets[i].FromUs <= lagUs && lagUs < buckets[i].ToUs) {
            return i;
        }
    }
    UNIT_FAIL("lag " << lagUs << " us is outside every bucket");
    return buckets.size();
}

} // namespace

Y_UNIT_TEST_SUITE(YaemWorkload) {
    Y_UNIT_TEST(DefaultLagSpecParses) {
        const auto lag = TLagDistribution::Parse(TLagDistribution::DefaultSpec());
        const auto& buckets = lag.GetBuckets();
        UNIT_ASSERT_VALUES_EQUAL(buckets.size(), 19);

        UNIT_ASSERT_VALUES_EQUAL(buckets[0].Name, "future");
        UNIT_ASSERT_VALUES_EQUAL(buckets[0].FromUs, -Minute);
        UNIT_ASSERT_VALUES_EQUAL(buckets[0].ToUs, 0);
        UNIT_ASSERT_DOUBLES_EQUAL(buckets[0].Weight, 0.023, 1e-12);

        UNIT_ASSERT_VALUES_EQUAL(buckets[1].Name, "0-1m");
        UNIT_ASSERT_VALUES_EQUAL(buckets[1].FromUs, 0);
        UNIT_ASSERT_VALUES_EQUAL(buckets[1].ToUs, Minute);
        UNIT_ASSERT_DOUBLES_EQUAL(buckets[1].Weight, 58.426, 1e-12);

        UNIT_ASSERT_VALUES_EQUAL(buckets[11].Name, "90-120m");
        UNIT_ASSERT_VALUES_EQUAL(buckets[11].FromUs, 90 * Minute);
        UNIT_ASSERT_VALUES_EQUAL(buckets[11].ToUs, 120 * Minute);

        UNIT_ASSERT_VALUES_EQUAL(buckets[12].Name, "2-6h");
        UNIT_ASSERT_VALUES_EQUAL(buckets[12].FromUs, 2 * Hour);
        UNIT_ASSERT_VALUES_EQUAL(buckets[12].ToUs, 6 * Hour);

        UNIT_ASSERT_VALUES_EQUAL(buckets[15].Name, "1-3d");
        UNIT_ASSERT_VALUES_EQUAL(buckets[15].FromUs, Day);
        UNIT_ASSERT_VALUES_EQUAL(buckets[15].ToUs, 3 * Day);

        UNIT_ASSERT_VALUES_EQUAL(buckets[17].Name, "7-30d");
        UNIT_ASSERT_VALUES_EQUAL(buckets[17].Weight, 0);

        UNIT_ASSERT_VALUES_EQUAL(buckets[18].Name, ">30d");
        UNIT_ASSERT_VALUES_EQUAL(buckets[18].FromUs, 30 * Day);
        UNIT_ASSERT_VALUES_EQUAL(buckets[18].ToUs, 60 * Day);
        UNIT_ASSERT_VALUES_EQUAL(buckets[18].Weight, 0);

        double total = 0;
        for (const auto& bucket : buckets) {
            UNIT_ASSERT_LT(bucket.FromUs, bucket.ToUs);
            total += bucket.Weight;
        }
        UNIT_ASSERT_DOUBLES_EQUAL(total, 100.0, 0.01);

        UNIT_ASSERT_VALUES_EQUAL(lag.ToString(), TLagDistribution::DefaultSpec());
    }

    Y_UNIT_TEST(LagSpecGrammar) {
        const auto lag = TLagDistribution::Parse(" 30s-2m : 10 , 1.5h-1d:30, 1w-2w:60");
        const auto& buckets = lag.GetBuckets();
        UNIT_ASSERT_VALUES_EQUAL(buckets.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(buckets[0].FromUs, 30 * Second);
        UNIT_ASSERT_VALUES_EQUAL(buckets[0].ToUs, 2 * Minute);
        UNIT_ASSERT_VALUES_EQUAL(buckets[1].FromUs, 90 * Minute);
        UNIT_ASSERT_VALUES_EQUAL(buckets[1].ToUs, Day);
        UNIT_ASSERT_VALUES_EQUAL(buckets[2].FromUs, 7 * Day);
        UNIT_ASSERT_VALUES_EQUAL(buckets[2].ToUs, 14 * Day);

        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse(""), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse("0-1m"), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse("0-1m:-5"), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse("0-1m:abc"), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse("1m-0:5"), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse("1m-1m:5"), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse("0-1x:5"), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse("1m:5"), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse(">0m:5"), yexception);
        UNIT_ASSERT_EXCEPTION(TLagDistribution::Parse("0-1m:0,1-2m:0"), yexception);
    }

    Y_UNIT_TEST(LagSampleBoundaries) {
        const auto lag = TLagDistribution::Parse(TLagDistribution::DefaultSpec());
        const double futureShare = 0.023 / 100;

        UNIT_ASSERT_VALUES_EQUAL(lag.SampleLagUs(0.0), -Minute);
        UNIT_ASSERT_LT(lag.SampleLagUs(futureShare * 0.999), 0);
        UNIT_ASSERT_GE(lag.SampleLagUs(futureShare * 1.001), 0);
        UNIT_ASSERT_LT(lag.SampleLagUs(futureShare * 1.001), Minute);

        // The two trailing zero-weight buckets are never chosen.
        const i64 top = lag.SampleLagUs(std::nextafter(1.0, 0.0));
        UNIT_ASSERT_GE(top, 3 * Day);
        UNIT_ASSERT_LT(top, 7 * Day);
        UNIT_ASSERT_EQUAL(FindBucket(lag, lag.SampleLagUs(1.0)), 16);
        UNIT_ASSERT_EQUAL(FindBucket(lag, lag.SampleLagUs(2.0)), 16);
        UNIT_ASSERT_EQUAL(FindBucket(lag, lag.SampleLagUs(-1.0)), 0);
    }

    Y_UNIT_TEST(LagSamplingFollowsWeights) {
        const auto lag = TLagDistribution::Parse(TLagDistribution::DefaultSpec());
        const auto& buckets = lag.GetBuckets();
        const ui64 samples = 2'000'000;
        TMersenne<ui64> rng(20261106);
        TVector<ui64> hits(buckets.size());
        for (ui64 i = 0; i < samples; ++i) {
            ++hits[FindBucket(lag, lag.SampleLagUs(rng.GenRandReal2()))];
        }
        double total = 0;
        for (const auto& bucket : buckets) {
            total += bucket.Weight;
        }
        for (size_t i = 0; i < buckets.size(); ++i) {
            const double expected = buckets[i].Weight / total * samples;
            const double tolerance = 5 * std::sqrt(expected) + 1;
            UNIT_ASSERT_C(std::abs(static_cast<double>(hits[i]) - expected) <= tolerance,
                "bucket " << buckets[i].Name << ": " << hits[i] << " hits, expected " << expected << " +- " << tolerance);
        }
        UNIT_ASSERT_VALUES_EQUAL(hits[17], 0);
        UNIT_ASSERT_VALUES_EQUAL(hits[18], 0);
    }

    Y_UNIT_TEST(RowsPerShardPercentiles) {
        const auto rows = TPercentileDistribution::Parse(TYaemWorkloadParams::DefaultRowsPerShardSpec());
        const auto& points = rows.GetPoints();
        UNIT_ASSERT_VALUES_EQUAL(points.size(), 9);
        UNIT_ASSERT_VALUES_EQUAL(points.front().Percent, 0);
        UNIT_ASSERT_VALUES_EQUAL(points.front().Value, 0); // extrapolated below zero, clamped
        UNIT_ASSERT_VALUES_EQUAL(points.back().Percent, 100);
        UNIT_ASSERT_DOUBLES_EQUAL(points.back().Value, 124.13 + (124.13 - 108.65) / 4, 1e-9);

        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.10), 7.99, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.40), 41.28, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.50), 46.51, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.70), 56.96, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.90), 89.30, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.95), 108.65, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.99), 124.13, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.30), 7.99 + (41.28 - 7.99) * 20 / 30, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(0.05), 7.99 / 2, 1e-9);
        UNIT_ASSERT_VALUES_EQUAL(rows.Sample(0.0), 0);
        UNIT_ASSERT_DOUBLES_EQUAL(rows.Sample(1.0), points.back().Value, 1e-9);

        double previous = -1;
        for (int i = 0; i <= 1000; ++i) {
            const double value = rows.Sample(i / 1000.0);
            UNIT_ASSERT_GE(value, previous);
            previous = value;
        }
    }

    Y_UNIT_TEST(PercentileSpecGrammar) {
        const auto constant = TPercentileDistribution::Parse("p50:46");
        UNIT_ASSERT_VALUES_EQUAL(constant.Sample(0.0), 46);
        UNIT_ASSERT_VALUES_EQUAL(constant.Sample(0.5), 46);
        UNIT_ASSERT_VALUES_EQUAL(constant.Sample(0.999), 46);

        const auto explicitEnds = TPercentileDistribution::Parse("0:1, 50:10, 100:100");
        UNIT_ASSERT_VALUES_EQUAL(explicitEnds.GetPoints().size(), 3);
        UNIT_ASSERT_DOUBLES_EQUAL(explicitEnds.Sample(0.25), 5.5, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(explicitEnds.Sample(0.75), 55, 1e-9);

        UNIT_ASSERT_EXCEPTION(TPercentileDistribution::Parse(""), yexception);
        UNIT_ASSERT_EXCEPTION(TPercentileDistribution::Parse("50"), yexception);
        UNIT_ASSERT_EXCEPTION(TPercentileDistribution::Parse("50:10,40:20"), yexception);
        UNIT_ASSERT_EXCEPTION(TPercentileDistribution::Parse("50:10,50:20"), yexception);
        UNIT_ASSERT_EXCEPTION(TPercentileDistribution::Parse("50:10,60:5"), yexception);
        UNIT_ASSERT_EXCEPTION(TPercentileDistribution::Parse("150:10"), yexception);
        UNIT_ASSERT_EXCEPTION(TPercentileDistribution::Parse("50:-1"), yexception);
        UNIT_ASSERT_EXCEPTION(TPercentileDistribution::Parse("x:1"), yexception);
    }

    Y_UNIT_TEST(RowsPerWriteFollowsDistribution) {
        TYaemWorkloadParams params;
        params.Validate(TWorkloadParams::ECommandType::Run, static_cast<int>(TYaemGenerator::EType::BulkUpsert));
        UNIT_ASSERT_VALUES_EQUAL(params.Shards, 128);

        const size_t samples = 100'000;
        TVector<ui64> sizes;
        sizes.reserve(samples);
        for (size_t i = 0; i < samples; ++i) {
            sizes.push_back(params.SampleRowsPerWrite());
        }
        std::sort(sizes.begin(), sizes.end());
        const auto percentile = [&](double p) {
            return static_cast<double>(sizes[std::min<size_t>(samples - 1, static_cast<size_t>(p / 100 * samples))]);
        };
        UNIT_ASSERT_GE(sizes.front(), 1);
        UNIT_ASSERT_LE(sizes.back(), static_cast<ui64>(std::llround(128 * (124.13 + (124.13 - 108.65) / 4))));
        UNIT_ASSERT_DOUBLES_EQUAL(percentile(10), 7.99 * 128, 0.03 * 7.99 * 128);
        UNIT_ASSERT_DOUBLES_EQUAL(percentile(50), 46.51 * 128, 0.02 * 46.51 * 128);
        UNIT_ASSERT_DOUBLES_EQUAL(percentile(90), 89.30 * 128, 0.02 * 89.30 * 128);
        UNIT_ASSERT_DOUBLES_EQUAL(percentile(99), 124.13 * 128, 0.02 * 124.13 * 128);
    }

    Y_UNIT_TEST(GeneratedWriteHasYaemTimestamps) {
        TYaemWorkloadParams params;
        params.Shards = 16;
        params.IntColumnsCnt = 2;
        params.StrColumnsCnt = 1;
        params.KeyColumnsCnt = 1;
        params.Validate(TWorkloadParams::ECommandType::Run, static_cast<int>(TYaemGenerator::EType::BulkUpsert));
        TYaemGenerator generator(&params);

        const TInstant now = TInstant::Now();
        ui64 rowsTotal = 0;
        ui64 within1m = 0;
        ui64 future = 0;
        for (size_t write = 0; write < 200; ++write) {
            const auto rows = generator.GenerateWrite(now);
            UNIT_ASSERT_GE(rows.size(), 1);
            UNIT_ASSERT_LE(rows.size(), 16 * 129);
            for (const auto& row : rows) {
                UNIT_ASSERT(!row.LogId.empty());
                UNIT_ASSERT_VALUES_EQUAL(row.IngestedAt, now);
                UNIT_ASSERT(row.Ts < now + TDuration::Minutes(1));
                UNIT_ASSERT(row.Ts >= now - TDuration::Days(7));
                UNIT_ASSERT_VALUES_EQUAL(row.Ints.size(), 2);
                UNIT_ASSERT_VALUES_EQUAL(row.Strings.size(), 1);
                UNIT_ASSERT(row.Ints[0] != 0); // key column is never null
                ++rowsTotal;
                if (row.Ts > now) {
                    ++future;
                } else if (now - row.Ts < TDuration::Minutes(1)) {
                    ++within1m;
                }
            }
        }
        UNIT_ASSERT_GT(rowsTotal, 100'000);
        const double within1mShare = 100.0 * within1m / rowsTotal;
        UNIT_ASSERT_C(std::abs(within1mShare - 58.426) < 1.5, "share of rows within a minute: " << within1mShare);
        UNIT_ASSERT_C(100.0 * future / rowsTotal < 0.2, "share of future rows: " << 100.0 * future / rowsTotal);
    }

    Y_UNIT_TEST(WorkloadTypesAndQueries) {
        TYaemWorkloadParams params;
        params.DbPath = "/Root/db";
        params.Validate(TWorkloadParams::ECommandType::Run, static_cast<int>(TYaemGenerator::EType::BulkUpsert));
        UNIT_ASSERT_VALUES_EQUAL(params.GetWorkloadName(), "Yaem");
        UNIT_ASSERT(params.CreateDataInitializers().empty());

        auto generator = params.CreateGenerator();
        const auto types = generator->GetSupportedWorkloadTypes();
        UNIT_ASSERT_VALUES_EQUAL(types.size(), 4);
        UNIT_ASSERT_VALUES_EQUAL(types[2].CommandName, "bulk-upsert");

        const auto bulk = generator->GetWorkload(static_cast<int>(TYaemGenerator::EType::BulkUpsert));
        UNIT_ASSERT_VALUES_EQUAL(bulk.size(), 1);
        UNIT_ASSERT(bulk.front().TableOperation);

        const auto upsert = generator->GetWorkload(static_cast<int>(TYaemGenerator::EType::Upsert));
        UNIT_ASSERT_VALUES_EQUAL(upsert.size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(upsert.front().Query, "UPSERT INTO `yaem`");

        UNIT_ASSERT_STRING_CONTAINS(generator->GetDDLQueries(), "CREATE TABLE `/Root/db/yaem`");
        UNIT_ASSERT(!generator->GetWorkload(static_cast<int>(TYaemGenerator::EType::Select)).empty());
    }
}
