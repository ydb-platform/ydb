#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/table_client/unversioned_row.h>
#include <yt/yt/client/table_client/unversioned_value.h>

namespace NYT::NTableClient {
namespace {

////////////////////////////////////////////////////////////////////////////////

// The goal is to provide a sanity check for stability of FarmHash and FarmFingerprint functions
// for TUnversionedRow and TUnversionedValue.
class TFarmHashTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<
        std::tuple<TUnversionedValue, TUnversionedValue, ui64, ui64, ui64>>
{ };

TEST_P(TFarmHashTest, TFarmHashUnversionedValueTest)
{
    const auto& params = GetParam();

    std::array<TUnversionedValue, 2> values{std::get<0>(params), std::get<1>(params)};
    auto valueRange = TRange(values);

    auto expected1 = std::get<2>(params);
    auto expected2 = std::get<3>(params);
    auto expected3 = std::get<4>(params);

    static_assert(std::is_same_v<ui64, decltype(TDefaultUnversionedValueHash()(values[0]))>);
    EXPECT_EQ(expected1, TDefaultUnversionedValueHash()(values[0]));
    EXPECT_EQ(expected2, TDefaultUnversionedValueHash()(values[1]));

    static_assert(std::is_same_v<ui64, decltype(GetFarmFingerprint(values[0]))>);
    EXPECT_EQ(expected1, GetFarmFingerprint(values[0]));
    EXPECT_EQ(expected2, GetFarmFingerprint(values[1]));

    static_assert(std::is_same_v<ui64, decltype(GetFarmFingerprint(valueRange))>);
    EXPECT_EQ(expected3, GetFarmFingerprint(valueRange));
}

TEST_P(TFarmHashTest, TFarmHashUnversionedRowTest)
{
    const auto& params = GetParam();

    char buf[sizeof(TUnversionedRowHeader) + 2 * sizeof(TUnversionedValue)];
    *reinterpret_cast<TUnversionedRowHeader*>(buf) = TUnversionedRowHeader{2, 2};
    *reinterpret_cast<TUnversionedValue*>(buf + sizeof(TUnversionedRowHeader)) = std::get<0>(params);
    *reinterpret_cast<TUnversionedValue*>(buf + sizeof(TUnversionedRowHeader) + sizeof(TUnversionedValue)) = std::get<1>(params);
    TUnversionedRow row(reinterpret_cast<TUnversionedRowHeader*>(buf));

    auto expected = std::get<4>(params);

    static_assert(std::is_same_v<ui64, decltype(TDefaultUnversionedRowHash()(row))>);
    EXPECT_EQ(expected, TDefaultUnversionedRowHash()(row));

    static_assert(std::is_same_v<ui64, decltype(GetFarmFingerprint(row))>);
    EXPECT_EQ(expected, GetFarmFingerprint(row));
}

INSTANTIATE_TEST_SUITE_P(
    TFarmHashTest,
    TFarmHashTest,
    ::testing::Values(
        std::tuple(
            MakeUnversionedInt64Value(12345678, /*id*/ 0, EValueFlags::None),
            MakeUnversionedUint64Value(42, /*id*/ 1, EValueFlags::Aggregate),
            18329046069279503950ULL,
            17355217915646310598ULL,
            16453323425893019626ULL),
        std::tuple(
            MakeUnversionedUint64Value(12345678, /*id*/ 1, EValueFlags::Aggregate),
            MakeUnversionedBooleanValue(true, /*id*/ 2, EValueFlags::Aggregate),
            18329046069279503950ULL,
            10105606910506535461ULL,
            10502610411105654667ULL),
        std::tuple(
            MakeUnversionedDoubleValue(42.0, /*id*/ 2, EValueFlags::Aggregate),
            MakeUnversionedStringValue("0", /*id*/ 3, EValueFlags::None),
            6259286942292166412ULL,
            15198969275252572735ULL,
            12125805494429148155ULL),
        std::tuple(
            MakeUnversionedBooleanValue(false, /*id*/ 3, EValueFlags::Aggregate),
            MakeUnversionedStringValue("", /*id*/ 4, EValueFlags::None),
            0ULL,
            11160318154034397263ULL,
            10248854568006048452ULL),
        std::tuple(
            MakeUnversionedStringValue("abc", /*id*/ 4, EValueFlags::None),
            MakeUnversionedInt64Value(-1000000, /*id*/ 5, EValueFlags::None),
            2640714258260161385ULL,
            13952380479379003069ULL,
            9998489714118868374ULL)));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NTableClient
