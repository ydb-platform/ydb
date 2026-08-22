#include <library/cpp/testing/unittest/registar.h>
#include <yql/essentials/udfs/common/reservoir_sampling/lib/reservoir.h>

Y_UNIT_TEST_SUITE(TReservoir) {
Y_UNIT_TEST(CheckProbability) {
    constexpr size_t k = 5;
    constexpr size_t runs = 5'000'000;
    constexpr size_t diffThreshold = 80; // in 0.01% units
    std::array<size_t, 3> sizes = {4, 10, 16};
    using T = uint8_t; // slightly faster
    size_t cnt = Accumulate(sizes, 0);
    std::vector<size_t> apperanceCount(cnt);
    auto rnd = CreateDefaultRandomProvider();
    for (size_t run = 0; run < runs; ++run) {
        T curr = 0;
        // randomly permute sizes
        for (size_t i = 1; i < sizes.size(); ++i) {
            std::swap(sizes[i], sizes[rnd->GenRand64() % (i + 1)]);
        }
        NReservoirSampling::TReservoir<T> r1(curr++, k);
        for (size_t i = 1; i < sizes[0]; ++i) {
            r1.Add(*rnd, curr++);
        }
        for (size_t i = 1; i < sizes.size(); ++i) {
            NReservoirSampling::TReservoir<T> r2(curr++, k);
            for (size_t j = 1; j < sizes[i]; ++j) {
                r2.Add(*rnd, curr++);
            }
            if (r1.Merge(*rnd, r2)) {
                std::swap(r1, r2);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(r1.GetItems().size(), k);
        UNIT_ASSERT_VALUES_EQUAL(r1.GetCount(), cnt);
        for (auto& e : r1.GetItems()) {
            ++apperanceCount[e];
        }
    }
    // expected = Runs * (K / cnt)
    // diff (%): abs(actual / expected - 1) = abs (actual * cnt / (Runs * k) - 1) = abs ((Runs * k - actual * cnt) / (Runs * k))
    // (f1 - f2) / f1

    for (auto& e : apperanceCount) {
        auto f1 = runs * k;
        auto f2 = e * cnt;
        auto diff = 10000 * (std::max(f2, f1) - std::min(f1, f2)) / f1; // in 0.01%
        Cerr << "Expected diff=" << diffThreshold << ", got " << diff << " (in 0.01% units). Runs=" << runs << ", cnt=" << cnt << ", appearedCnt=" << e << "\n";
        UNIT_ASSERT(diff < diffThreshold);
    }
}
} // Y_UNIT_TEST_SUITE(TReservoir)
