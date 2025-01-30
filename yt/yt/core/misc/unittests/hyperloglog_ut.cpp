#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/hyperloglog.h>
#include <yt/yt/core/misc/random.h>

#include <library/cpp/yt/farmhash/farm_hash.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

class THyperLogLogTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<int, int, int, int>>
{ };

std::pair<THyperLogLog<8>, int> GenerateHyperLogLog(
    TRandomGenerator& rng,
    int size,
    int targetCardinality)
{
    auto hll = THyperLogLog<8>();

    int cardinality = 1;
    ui64 n = 0;
    hll.Add(FarmFingerprint(n));
    for (int i = 0; i < size; i++) {
        if (static_cast<int>(rng.Generate<ui64>() % size) < targetCardinality) {
            cardinality++;
            n += 1 + (rng.Generate<ui32>() % 100);
        }

        hll.Add(FarmFingerprint(n));
    }

    return std::pair(hll, cardinality);
}

TEST_P(THyperLogLogTest, Random)
{
    auto param = GetParam();
    auto seed = std::get<0>(param);
    auto size = std::get<1>(param);
    auto targetCardinality = std::get<2>(param);
    auto iterations = std::get<3>(param);
    auto rng = TRandomGenerator(seed);

    auto error = 0.0;

    for (int i = 0; i < iterations; i++) {
        auto hll = GenerateHyperLogLog(
            rng,
            size,
            targetCardinality);

        THyperLogLog<8> hllClone(hll.first.Data());
        EXPECT_EQ(hll.first.EstimateCardinality(), hllClone.EstimateCardinality());

        auto err = ((double)hll.first.EstimateCardinality() - hll.second) / hll.second;
        error += err;
    }

    auto meanError = error / iterations;

    EXPECT_NEAR(meanError, 0, 0.05);
}

INSTANTIATE_TEST_SUITE_P(
    HyperLogLogTest,
    THyperLogLogTest,
    ::testing::Values(
        std::tuple<int, int, int, int>(123, 100000, 200, 10),
        std::tuple<int, int, int, int>(324, 100000, 1000, 10),
        std::tuple<int, int, int, int>(653, 100000, 5000, 10),
        std::tuple<int, int, int, int>(890, 100000, 10000, 10),
        std::tuple<int, int, int, int>(278, 100000, 50000, 10)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
