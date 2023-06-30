#include <yt/yt/client/chaos_client/replication_card.h>
#include <yt/yt/client/chaos_client/replication_card_serialization.h>

#include <yt/yt/core/yson/string.h>

#include <yt/yt/core/ytree/convert.h>

#include <yt/yt/core/test_framework/framework.h>

#include <library/cpp/iterator/zip.h>

namespace NYT::NChaosClient {
namespace {

using namespace NYson;
using namespace NYTree;
using namespace NTableClient;

////////////////////////////////////////////////////////////////////////////////

bool Equal(const TReplicationProgress& lhs, const TReplicationProgress& rhs)
{
    if (lhs.UpperKey != rhs.UpperKey || lhs.Segments.size() != rhs.Segments.size()) {
        return false;
    }
    for (const auto& [lhs, rhs] : Zip(lhs.Segments, rhs.Segments)) {
        if (lhs.Timestamp != rhs.Timestamp || lhs.LowerKey != rhs.LowerKey) {
            return false;
        }
    }
    return true;
}

////////////////////////////////////////////////////////////////////////////////

class TUpdateReplicationProgressTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        const char*>>
{ };

TEST_P(TUpdateReplicationProgressTest, Simple)
{
    const auto& params = GetParam();
    auto progress = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<0>(params)));
    const auto& update = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<1>(params)));
    const auto& expected = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<2>(params)));

    UpdateReplicationProgress(&progress, update);

    EXPECT_TRUE(Equal(progress, expected))
        << "progress: " << std::get<0>(params) << std::endl
        << "update: " << std::get<1>(params) << std::endl
        << "expected: " << std::get<2>(params) << std::endl
        << "actual: " << ConvertToYsonString(progress, EYsonFormat::Text).AsStringBuf() << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TUpdateReplicationProgressTest,
    TUpdateReplicationProgressTest,
    ::testing::Values(
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1u}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1u}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=1}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1u}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=1}];upper_key=[2]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[2]}",
            "{segments=[{lower_key=[1];timestamp=1}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1}];upper_key=[2]}"),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=2}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=1};{lower_key=[3];timestamp=3}];upper_key=[4]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=2};{lower_key=[3];timestamp=3};{lower_key=[4];timestamp=2}];upper_key=[<type=max>#]}")
));

////////////////////////////////////////////////////////////////////////////////

class TCompareReplicationProgressTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        bool>>
{ };

TEST_P(TCompareReplicationProgressTest, Simple)
{
    const auto& params = GetParam();
    const auto& progress = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<0>(params)));
    const auto& other = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<1>(params)));
    bool expected = std::get<2>(params);

    bool result = IsReplicationProgressGreaterOrEqual(progress, other);

    EXPECT_EQ(result, expected)
        << "progress: " << std::get<0>(params) << std::endl
        << "other: " << std::get<1>(params) << std::endl
        << "expected: " << expected << std::endl
        << "actual: " << result << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TCompareReplicationProgressTest,
    TCompareReplicationProgressTest,
    ::testing::Values(
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[<type=max>#]}",
            false),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[1];timestamp=0};];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[<type=max>#]}",
            false),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[1]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=2}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=1};{lower_key=[4];timestamp=2}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[1];timestamp=0};{lower_key=[3];timestamp=1}];upper_key=[4]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[1]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            true),
        std::make_tuple(
            "{segments=[{lower_key=[2];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[3];timestamp=0}];upper_key=[<type=max>#]}",
            false),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}",
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1}];upper_key=[<type=max>#]}",
            false)
));

////////////////////////////////////////////////////////////////////////////////

class TGatherReplicationProgressTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        std::vector<const char*>,
        const char*>>
{ };

TEST_P(TGatherReplicationProgressTest, Simple)
{
    const auto& params = GetParam();
    const auto& serializedProgresses = std::get<0>(params);
    const auto& expected = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<1>(params)));

    std::vector<TReplicationProgress> progresses;
    std::vector<TUnversionedRow> pivotKeys;
    for (const auto& serialized : serializedProgresses) {
        progresses.push_back(ConvertTo<TReplicationProgress>(TYsonStringBuf(serialized)));
        pivotKeys.push_back(progresses.back().Segments.front().LowerKey);
    }

    auto result = GatherReplicationProgress(progresses, pivotKeys, progresses.back().UpperKey.Get());

    EXPECT_TRUE(IsReplicationProgressEqual(result, expected))
        << "progresses: " << Format("%v", std::get<0>(params)) << std::endl
        << "expected: " << std::get<1>(params) << std::endl
        << "actual: " << ConvertToYsonString(result, EYsonFormat::Text).AsStringBuf() << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TGatherReplicationProgressTest,
    TGatherReplicationProgressTest,
    ::testing::Values(
        std::make_tuple(
            std::vector<const char*>{
                "{segments=[{lower_key=[];timestamp=0}];upper_key=[1]}",
                "{segments=[{lower_key=[1];timestamp=1}];upper_key=[2]}"
            },
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1}];upper_key=[2]}"),
        std::make_tuple(
            std::vector<const char*>{
                "{segments=[{lower_key=[];timestamp=0}];upper_key=[1]}",
                "{segments=[{lower_key=[0];timestamp=0}];upper_key=[2]}"
            },
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[2]}"),
        std::make_tuple(
            std::vector<const char*>{
                "{segments=[{lower_key=[];timestamp=0}];upper_key=[1]}",
                "{segments=[{lower_key=[1];timestamp=1}];upper_key=[2]}",
                "{segments=[{lower_key=[2];timestamp=1}];upper_key=[3]}"
            },
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1}];upper_key=[3]}"),
        std::make_tuple(
            std::vector<const char*>{
                "{segments=[{lower_key=[];timestamp=0}];upper_key=[1]}",
                "{segments=[{lower_key=[1];timestamp=0}];upper_key=[2]}",
                "{segments=[{lower_key=[2];timestamp=1}];upper_key=[3]}"
            },
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=1}];upper_key=[3]}")
));

////////////////////////////////////////////////////////////////////////////////

class TScatterReplicationProgressTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        const char*,
        std::vector<const char*>>>
{ };

TEST_P(TScatterReplicationProgressTest, Simple)
{
    const auto& params = GetParam();
    const auto& progress = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<0>(params)));
    const auto& owningPivotKeys = ConvertTo<std::vector<TUnversionedOwningRow>>(TYsonStringBuf(std::get<1>(params)));
    const auto& upperKey = ConvertTo<TUnversionedOwningRow>(TYsonStringBuf(std::get<2>(params)));
    const auto& serializedExpected = std::get<3>(params);

    std::vector<TUnversionedRow> pivotKeys;
    for (const auto& row : owningPivotKeys) {
        pivotKeys.push_back(row.Get());
    }

    std::vector<TReplicationProgress> expected;
    for (const auto& serialized : serializedExpected) {
        expected.push_back(ConvertTo<TReplicationProgress>(TYsonStringBuf(serialized)));
    }

    auto result = ScatterReplicationProgress(progress, pivotKeys, upperKey.Get());
    bool allEqual = true;
    if (expected.size() != result.size()) {
        allEqual = false;
    } else {
        for (int index = 0; index < std::ssize(expected); ++index) {
            if (!IsReplicationProgressEqual(result[index], expected[index])) {
                allEqual = false;
                break;
            }
        }
    }

    EXPECT_TRUE(allEqual)
        << "progresses: " << std::get<0>(params) << std::endl
        << "pivot keys: " << std::get<1>(params) << std::endl
        << "upper key: " << std::get<2>(params) << std::endl
        << "expected: " << Format("%v", expected) << std::endl
        << "actual: " << Format("%v", result) << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TScatterReplicationProgressTest,
    TScatterReplicationProgressTest,
    ::testing::Values(
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "[[]; [1]]",
            "[<type=max>#]",
            std::vector<const char*>{
                "{segments=[{lower_key=[];timestamp=0}];upper_key=[1]}",
                "{segments=[{lower_key=[1];timestamp=0}];upper_key=[<type=max>#]}"
            }),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0}];upper_key=[<type=max>#]}",
            "[[]; [1]]",
            "[2]",
            std::vector<const char*>{
                "{segments=[{lower_key=[];timestamp=0}];upper_key=[1]}",
                "{segments=[{lower_key=[1];timestamp=0}];upper_key=[2]}"
            }),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1}];upper_key=[2]}",
            "[[]; [1]]",
            "[2]",
            std::vector<const char*>{
                "{segments=[{lower_key=[];timestamp=0}];upper_key=[1]}",
                "{segments=[{lower_key=[1];timestamp=1}];upper_key=[2]}"
            }),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=1};{lower_key=[4];timestamp=0}];upper_key=[6]}",
            "[[1]; [3]]",
            "[5]",
            std::vector<const char*>{
                "{segments=[{lower_key=[1];timestamp=0};{lower_key=[2];timestamp=1}];upper_key=[3]}",
                "{segments=[{lower_key=[3];timestamp=1};{lower_key=[4];timestamp=0}];upper_key=[5]}",
            }),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=1};{lower_key=[4];timestamp=0}];upper_key=[6]}",
            "[[1]; [4]]",
            "[6]",
            std::vector<const char*>{
                "{segments=[{lower_key=[1];timestamp=0};{lower_key=[2];timestamp=1}];upper_key=[4]}",
                "{segments=[{lower_key=[4];timestamp=0}];upper_key=[6]}"
            }),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=1};{lower_key=[4];timestamp=0}];upper_key=[6]}",
            "[[3]; [4]]",
            "[5]",
            std::vector<const char*>{
                "{segments=[{lower_key=[3];timestamp=1}];upper_key=[4]}",
                "{segments=[{lower_key=[4];timestamp=0}];upper_key=[5]}"
            }),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[2];timestamp=1};{lower_key=[4];timestamp=0}];upper_key=[6]}",
            "[[1]; [2]; [3]; [4]; [5]]",
            "[6]",
            std::vector<const char*>{
                "{segments=[{lower_key=[1];timestamp=0}];upper_key=[2]}",
                "{segments=[{lower_key=[2];timestamp=1}];upper_key=[3]}",
                "{segments=[{lower_key=[3];timestamp=1}];upper_key=[4]}",
                "{segments=[{lower_key=[4];timestamp=0}];upper_key=[5]}",
                "{segments=[{lower_key=[5];timestamp=0}];upper_key=[6]}"
            })
));

////////////////////////////////////////////////////////////////////////////////

class TReplicationProgressTimestampForKeyTest
    : public ::testing::Test
    , public ::testing::WithParamInterface<std::tuple<
        const char*,
        const char*,
        std::optional<TTimestamp>>>
{ };

TEST_P(TReplicationProgressTimestampForKeyTest, Simple)
{
    const auto& params = GetParam();
    const auto& progress = ConvertTo<TReplicationProgress>(TYsonStringBuf(std::get<0>(params)));
    const auto& key = ConvertTo<TUnversionedOwningRow>(TYsonStringBuf(std::get<1>(params)));
    const auto& expected = std::get<2>(params);

    auto result = FindReplicationProgressTimestampForKey(progress, key.Elements());

    EXPECT_EQ(result, expected)
        << "progresses: " << std::get<0>(params) << std::endl
        << "key: " << std::get<1>(params) << std::endl
        << "expected: " << Format("%v", expected) << std::endl
        << "actual: " << Format("%v", result) << std::endl;
}

INSTANTIATE_TEST_SUITE_P(
    TReplicationProgressTimestampForKeyTest,
    TReplicationProgressTimestampForKeyTest,
    ::testing::Values(
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[<type=max>#]}",
            "[1]",
            1),
        std::make_tuple(
            "{segments=[{lower_key=[1];timestamp=1}];upper_key=[<type=max>#]}",
            "[]",
            std::nullopt),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[1]}",
            "[1]",
            std::nullopt),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1}];upper_key=[1]}",
            "[2]",
            std::nullopt),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1}];upper_key=[<type=max>#]}",
            "[1]",
            1),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=0};{lower_key=[1];timestamp=1};{lower_key=[2];timestamp=2};];upper_key=[<type=max>#]}",
            "[1]",
            1),
        std::make_tuple(
            "{segments=[{lower_key=[];timestamp=1};{lower_key=[2];timestamp=2};];upper_key=[<type=max>#]}",
            "[1]",
            1)
));

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NChaosClient
