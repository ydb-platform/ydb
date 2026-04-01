#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NOlap::NStorageOptimizer::NTiling {

struct TLevelParameters {
    ui8 MaxHeight = 9;
    ui8 OverloadHeight = 11;
};

TLevelParameters BuildLevelParameters(const ui8 levelIdx);
bool IsLevelOverloaded(
    const ui64 totalBlobBytes,
    const ui64 maxBlobBytes,
    const ui64 totalRecordsCount,
    const ui64 maxRecordsCount,
    const i32 intersectionsCount,
    const ui8 overloadHeight,
    const ui64 smallPortionsTotalBlobBytes,
    const ui64 smallPortionsOverloadBlobBytes);
ui8 CalculateLevelsCount(
    const ui64 totalBlobBytes,
    const ui64 totalRecordsCount,
    const ui64 initialBlobBytes,
    const ui64 initialRecordsCount,
    const ui64 lastLevelBytes,
    const ui64 lastLevelRecordsCount,
    const ui8 k);

Y_UNIT_TEST_SUITE(TilingPlannerHelpers) {

    Y_UNIT_TEST(Level1_Config_IsSpecialized) {
        const auto params = BuildLevelParameters(1);

        UNIT_ASSERT_VALUES_EQUAL(params.MaxHeight, 1);
        UNIT_ASSERT_VALUES_EQUAL(params.OverloadHeight, 3);
    }

    Y_UNIT_TEST(HigherLevels_UseDefaultHeights) {
        const auto params = BuildLevelParameters(2);

        UNIT_ASSERT_VALUES_EQUAL(params.MaxHeight, 9);
        UNIT_ASSERT_VALUES_EQUAL(params.OverloadHeight, 11);
    }

    Y_UNIT_TEST(IsLevelOverloaded_ByBlobBytes) {
        UNIT_ASSERT(IsLevelOverloaded(101, 100, 10, 10, 0, 3, 0, 100));
    }

    Y_UNIT_TEST(IsLevelOverloaded_ByRecordCount) {
        UNIT_ASSERT(IsLevelOverloaded(10, 100, 11, 10, 0, 3, 0, 100));
    }

    Y_UNIT_TEST(IsLevelOverloaded_ByIntersectionDepth) {
        UNIT_ASSERT(IsLevelOverloaded(10, 100, 10, 100, 4, 3, 0, 100));
    }

    Y_UNIT_TEST(IsLevelOverloaded_BySmallPortionsBytes) {
        UNIT_ASSERT(IsLevelOverloaded(10, 100, 10, 100, 0, 3, 101, 100));
    }

    Y_UNIT_TEST(IsLevelOverloaded_FalseWithinLimits) {
        UNIT_ASSERT(!IsLevelOverloaded(10, 100, 10, 100, 3, 3, 100, 100));
    }

    Y_UNIT_TEST(CalculateLevelsCount_InitialStateCreatesThreeLevels) {
        const auto levels = CalculateLevelsCount(
            0,
            0,
            1024ull * 1024ull * 1024ull,
            1'000'000,
            128ull * 1024ull * 1024ull,
            100'000,
            10);

        UNIT_ASSERT_VALUES_EQUAL(levels, 3);
    }

    Y_UNIT_TEST(CalculateLevelsCount_LargeDataCreatesFourLevels) {
        const auto levels = CalculateLevelsCount(
            100ull * 1024ull * 1024ull * 1024ull,
            1'000'000,
            1024ull * 1024ull * 1024ull,
            1'000'000,
            128ull * 1024ull * 1024ull,
            100'000,
            10);

        UNIT_ASSERT_VALUES_EQUAL(levels, 4);
    }

    Y_UNIT_TEST(CalculateLevelsCount_RecordThresholdAlsoDrivesGrowth) {
        const auto levels = CalculateLevelsCount(
            1,
            100'000'000,
            1024ull * 1024ull * 1024ull,
            1'000'000,
            128ull * 1024ull * 1024ull,
            100'000,
            10);

        UNIT_ASSERT_VALUES_EQUAL(levels, 4);
    }
}

} // namespace NKikimr::NOlap::NStorageOptimizer::NTiling
