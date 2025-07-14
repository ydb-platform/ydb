#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <vector>
#include <random>

#include <util/system/fs.h>
#include <util/system/compiler.h>
#include <util/stream/null.h>
#include <util/system/mem_info.h>

#include <ydb/library/yql/minikql/comp_nodes/packed_tuple/cardinality.h>

#include <arrow/util/bit_util.h>
#include <util/digest/numeric.h>

namespace NKikimr {
namespace NMiniKQL {
namespace NPackedTuple {

using namespace std::chrono_literals;

static volatile bool IsVerbose = false;
#define CTEST (IsVerbose ? Cerr : Cnull)

Y_UNIT_TEST_SUITE(Cardinality) {

/**
 * Lower selectivity -> higher chance to pick disjoint sets in samples
 * More buckets -> hisher chance to put one or less elements per one bucket
 *
 * Therefore, estimation tends to zero tuples, if number of buckets is big.
 * 
 * If number of buckets is small, we will receive value close to PK-FK join result.
 */
Y_UNIT_TEST(EstimateErrorPKFK) {
    std::mt19937 rng(std::random_device{}());

    TVector<ui64> build(100'000, 0);
    std::iota(build.begin(), build.end(), 1);
    TVector<ui64> probe(500'000, 0);
    std::iota(probe.begin(), probe.end(), build.size() + 1);

    for (size_t i = 0; i < probe.size() / 10; ++i) { // 10% selectivity
        auto bidx = rng() % build.size();
        probe[i] = build[bidx];
    }
    for (size_t i = 0; i < probe.size() / 2; ++i) {
        std::swap(probe[rng() % probe.size()], probe[rng() % probe.size()]);
    }

    ui64 answer = 0;
    std::unordered_map<ui64, ui64> buildMap;
    for (auto val: build) {
        buildMap[val]++;
    }
    for (auto val: probe) {
        if (buildMap.count(val)) {
            answer += buildMap[val];
        }
    }

    for (auto& val: build) {
        val = ::IntHash(val);
    }
    for (auto& val: probe) {
        val = ::IntHash(val);
    }

    TVector<ui64> buildSample(build.size() / 100);
    for (size_t i = 0, j = 0; i < buildSample.size(); ++i, j += 100) {
        buildSample[i] = build[j];
    }
    TVector<ui64> probeSample(probe.size() / 100);
    for (size_t i = 0, j = 0; i < probeSample.size(); ++i, j += 100) {
        probeSample[i] = probe[j];
    }

    ui64 step = 1;
    CTEST << "EstimateErrorPKFK:" << Endl;
    for (ui64 buckets = 1; buckets < 32; buckets += step) {
        CardinalityEstimator est{buckets};
        auto got = est.Estimate(probe.size(), probeSample, build.size(), buildSample);
        double err = std::fabs(100.0 - 100.0 * (double)got / (double)answer);
        CTEST << "  Buckets: " << buckets << ", got: " << got << ", expected: " << answer << ", abs. err: " << err << Endl;
    }

    CTEST << "Good enough:" << Endl;
    auto buckets = probeSample.size() / 2000; // 1/20 (5%) * 1/100 (step) -> 2000
    CardinalityEstimator est{buckets};
    auto got = est.Estimate(probe.size(), probeSample, build.size(), buildSample);
    double err = std::fabs(100.0 - 100.0 * (double)got / (double)answer);
    CTEST << "  Buckets: " << buckets << ", got: " << got << ", expected: " << answer << ", abs. err: " << err << Endl;
} // Y_UNIT_TEST(EstimateErrorPKFK)
#if 0
Y_UNIT_TEST(EstimateErrorNormal) {
    std::mt19937 rng(std::random_device{}());

    TVector<ui64> build(100'000, 0);
    std::lognormal_distribution<> distrib(0, 10'000);
    for (auto& val: build) {
        val = distrib(rng);
    }
    TVector<ui64> probe(500'000, 0);
    std::iota(probe.begin(), probe.end(), build.size() + 1);

    for (size_t i = 0; i < probe.size() / 10; ++i) { // 10% selectivity
        auto bidx = rng() % build.size();
        auto pidx = rng() % probe.size();
        probe[pidx] = build[bidx];
    }

    ui64 answer = 0;
    std::unordered_map<ui64, ui64> buildMap;
    for (auto val: build) {
        buildMap[val]++;
    }
    for (auto val: probe) {
        if (buildMap.count(val)) {
            answer += buildMap[val];
        }
    }

    for (auto& val: build) {
        val = ::IntHash(val);
    }
    for (auto& val: probe) {
        val = ::IntHash(val);
    }

    TVector<ui64> buildSample(build.size() / 100);
    for (size_t i = 0; i < buildSample.size(); ++i) {
        buildSample[i] = build[rng() % build.size()];
    }
    TVector<ui64> probeSample(probe.size() / 100);
    for (size_t i = 0; i < probeSample.size(); ++i) {
        probeSample[i] = probe[rng() % probe.size()];
    }

    CTEST << "EstimateErrorNormal:" << Endl;
    for (ui64 buckets = 64; buckets < 2046; buckets += 128) {
        CardinalityEstimator est{buckets};
        auto got = est.Estimate(probe.size(), probeSample, build.size(), buildSample);
        double err = std::fabs(100.0 - 100.0 * (double)got / (double)answer);
        CTEST << "  Buckets: " << buckets << ", got: " << got << ", expected: " << answer << ", abs. err: " << err << Endl;
    }
} // Y_UNIT_TEST(EstimateErrorNormal)
#endif
} // Y_UNIT_TEST_SUITE(Cardinality)


}
} // namespace NMiniKQL
} // namespace NKikimr
