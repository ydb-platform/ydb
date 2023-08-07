#include "histogram_snapshot.h"

#include <library/cpp/testing/unittest/registar.h>

#include <random>

using namespace NMonitoring;

Y_UNIT_TEST_SUITE(THistogramSnapshotTest) {
    struct Buckets {
        TBucketBounds Bounds;
        TBucketValues Values;
    };

    Buckets MakeBuckets(uint32_t nBackets) {
        Buckets result;
        for (uint32_t i = 0; i < nBackets; ++i) {
            result.Bounds.push_back(i + 1);
            result.Values.push_back(i + 1);
        }
        return result;
    }

    Y_UNIT_TEST(SimpleTest) {
        for (uint32_t nBuckets = HISTOGRAM_MAX_BUCKETS_COUNT; nBuckets <= 3 * HISTOGRAM_MAX_BUCKETS_COUNT; ++nBuckets) {
            auto buckets = MakeBuckets(nBuckets);
            auto snapshot = ExplicitHistogramSnapshot(buckets.Bounds, buckets.Values, true);
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Count(), std::min(HISTOGRAM_MAX_BUCKETS_COUNT, nBuckets));
            uint64_t sumValues{0};
            for (uint32_t i = 0; i < snapshot->Count(); ++i) {
                sumValues += snapshot->Value(i);
            }
            UNIT_ASSERT_VALUES_EQUAL(sumValues, nBuckets * (nBuckets + 1) / 2);
        }

        auto backets = MakeBuckets(HISTOGRAM_MAX_BUCKETS_COUNT + 10);
        UNIT_ASSERT_EXCEPTION(ExplicitHistogramSnapshot(backets.Bounds, backets.Values, false), yexception);
    }

    Y_UNIT_TEST(InfSimpleTest) {
        for (uint32_t nBuckets = 1; nBuckets <= 3 * HISTOGRAM_MAX_BUCKETS_COUNT; ++nBuckets) {
            auto buckets = MakeBuckets(nBuckets);
            buckets.Bounds.back() = Max<TBucketBound>();
            auto snapshot = ExplicitHistogramSnapshot(buckets.Bounds, buckets.Values, true);

            auto nBucketsReal = std::min(HISTOGRAM_MAX_BUCKETS_COUNT, nBuckets);
            UNIT_ASSERT_VALUES_EQUAL(snapshot->Count(), nBucketsReal);
            UNIT_ASSERT_DOUBLES_EQUAL(snapshot->UpperBound(nBucketsReal - 1), Max<TBucketBound>(), 1e-6);
            uint64_t sumValues{0};
            for (uint32_t i = 0; i < snapshot->Count(); ++i) {
                sumValues += snapshot->Value(i);
            }
            UNIT_ASSERT_VALUES_EQUAL(sumValues, nBuckets * (nBuckets + 1) / 2);
        }
    }

    Y_UNIT_TEST(BacketsTest) {
        for (uint32_t nBuckets = HISTOGRAM_MAX_BUCKETS_COUNT; nBuckets <= 3 * HISTOGRAM_MAX_BUCKETS_COUNT; ++nBuckets) {
            auto overlap = nBuckets % HISTOGRAM_MAX_BUCKETS_COUNT;
            auto divided = nBuckets / HISTOGRAM_MAX_BUCKETS_COUNT;
            auto buckets = MakeBuckets(nBuckets);
            auto snapshot = ExplicitHistogramSnapshot(buckets.Bounds, buckets.Values, true);
            UNIT_ASSERT_DOUBLES_EQUAL(snapshot->UpperBound(HISTOGRAM_MAX_BUCKETS_COUNT - 1), nBuckets, 1e-6);

            uint64_t sumBuckets{0};
            uint64_t sumSnapshot{0};
            size_t idx{0};

            for (uint32_t i = 0; i < HISTOGRAM_MAX_BUCKETS_COUNT; ++i) {
                sumSnapshot += snapshot->Value(i);
                auto delta = (i < HISTOGRAM_MAX_BUCKETS_COUNT - overlap) ? 0ull : (i - (HISTOGRAM_MAX_BUCKETS_COUNT - overlap) + 1);
                auto endIdx = divided * (i + 1) + delta;
                UNIT_ASSERT_VALUES_EQUAL(snapshot->UpperBound(i), endIdx);
                while (idx < endIdx) {
                    sumBuckets += buckets.Values[idx];
                    ++idx;
                }
                UNIT_ASSERT_VALUES_EQUAL(sumBuckets, sumSnapshot);
            }
        }
    }

    Y_UNIT_TEST(CompareHistTest) {
        uint32_t K = 4;
        uint32_t N = K * HISTOGRAM_MAX_BUCKETS_COUNT;
        Buckets bucketsBig;
        Buckets bucketsSmall;
        for (uint32_t i = 1; i <= N; ++i) {
            if (i % K == 0) {
                bucketsSmall.Bounds.push_back(i);
                bucketsSmall.Values.push_back(0);
            }
            bucketsBig.Bounds.push_back(i);
            bucketsBig.Values.push_back(0);
        }

        UNIT_ASSERT_VALUES_EQUAL(bucketsBig.Values.size(), N);
        UNIT_ASSERT_VALUES_EQUAL(bucketsSmall.Values.size(), N / K);
        UNIT_ASSERT_VALUES_EQUAL(bucketsBig.Bounds.back(), N);
        UNIT_ASSERT_VALUES_EQUAL(bucketsSmall.Bounds.back(), N);

        std::random_device rd;
        std::mt19937 gen(rd());
        std::uniform_int_distribution<> distrib(1, N);

        for (int i = 0; i < 1000; ++i) {
            auto rndValue = distrib(gen);
            ++bucketsBig.Values[rndValue - 1];
            ++bucketsSmall.Values[(rndValue - 1) / K];
        }

        UNIT_ASSERT_VALUES_EQUAL(bucketsSmall.Bounds.back(), N);
        UNIT_ASSERT_VALUES_EQUAL(bucketsBig.Bounds.back(), N);
        auto snapshotBig = ExplicitHistogramSnapshot(bucketsBig.Bounds, bucketsBig.Values, true);
        auto snapshotSmall = ExplicitHistogramSnapshot(bucketsSmall.Bounds, bucketsSmall.Values, true);
        UNIT_ASSERT_VALUES_EQUAL(snapshotBig->Count(), snapshotSmall->Count());

        for (uint32_t i = 0; i < snapshotSmall->Count(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(snapshotSmall->Value(i), snapshotBig->Value(i));
        }
    }
}
