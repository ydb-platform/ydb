#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/yson/format.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;
using namespace NStatisticPath;

template <>
struct TYsonFormatTraits<TSummary>
    : public TYsonTextFormatTraits
{ };

std::ostream& operator<<(std::ostream& out, const TSummary& summary)
{
    return out << ToStringViaBuilder(summary);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatistics, Summary)
{
    TSummary summary;

    summary.AddSample(10);
    summary.AddSample(20);

    EXPECT_EQ(10, summary.GetMin());
    EXPECT_EQ(20, summary.GetMax());
    EXPECT_EQ(2, summary.GetCount());
    EXPECT_EQ(30, summary.GetSum());
    EXPECT_EQ(20, summary.GetLast());

    summary.AddSample(15);
    EXPECT_EQ(10, summary.GetMin());
    EXPECT_EQ(20, summary.GetMax());
    EXPECT_EQ(3, summary.GetCount());
    EXPECT_EQ(45, summary.GetSum());
    EXPECT_EQ(15, summary.GetLast());

    summary.Merge(summary);
    EXPECT_EQ(10, summary.GetMin());
    EXPECT_EQ(20, summary.GetMax());
    EXPECT_EQ(6, summary.GetCount());
    EXPECT_EQ(90, summary.GetSum());
    EXPECT_EQ(std::nullopt, summary.GetLast());

    summary.Reset();
    EXPECT_EQ(0, summary.GetCount());
    EXPECT_EQ(0, summary.GetSum());
    EXPECT_EQ(std::nullopt, summary.GetLast());
}

////////////////////////////////////////////////////////////////////////////////

TStatistics CreateStatistics(std::initializer_list<std::pair<TStatisticPath, i64>> data)
{
    TStatistics result;
    for (const auto& item : data) {
        result.AddSample(item.first, item.second);
    }
    return result;
}

TEST(TStatistics, AddSample)
{
    std::map<TString, int> origin = {{"x", 5}, {"y", 7}};

    TStatistics statistics;
    statistics.AddSample(
        "key"_L / "subkey"_L,
        origin);

    EXPECT_EQ(5, GetNumericValue(statistics, "key"_L / "subkey"_L / "x"_L));
    EXPECT_EQ(7, GetNumericValue(statistics, "key"_L / "subkey"_L / "y"_L));

    statistics.AddSample("key"_L / "sub"_L, 42);
    EXPECT_EQ(42, GetNumericValue(statistics, "key"_L / "sub"_L));

    // Cannot add sample to the map node.
    EXPECT_THROW(statistics.AddSample("key"_L / "subkey"_L, 24), std::exception);

    statistics.Merge(CreateStatistics({
        {"key"_L / "subkey"_L / "x"_L, 5},
        {"key"_L / "subkey"_L / "z"_L, 9}}));

    EXPECT_EQ(10, GetNumericValue(statistics, "key"_L / "subkey"_L / "x"_L));
    EXPECT_EQ(7, GetNumericValue(statistics, "key"_L / "subkey"_L / "y"_L));
    EXPECT_EQ(9, GetNumericValue(statistics, "key"_L / "subkey"_L / "z"_L));

    EXPECT_THROW(
        statistics.Merge(CreateStatistics({{"key"_L, 5}})),
        std::exception);

    statistics.AddSample("key"_L / "subkey"_L / "x"_L, 10);
    EXPECT_EQ(20, GetNumericValue(statistics, "key"_L / "subkey"_L / "x"_L));

    auto ysonStatistics = ConvertToYsonString(statistics);
    auto deserializedStatistics = ConvertTo<TStatistics>(ysonStatistics);

    EXPECT_EQ(statistics.Data(), deserializedStatistics.Data());

    EXPECT_EQ(20, GetNumericValue(deserializedStatistics, "key"_L / "subkey"_L / "x"_L));
    EXPECT_EQ(42, GetNumericValue(deserializedStatistics, "key"_L / "sub"_L));
}

////////////////////////////////////////////////////////////////////////////////

class TStatisticsUpdater
{
public:
    void AddSample(const INodePtr& node)
    {
        Statistics_.AddSample("custom"_L, node);
    }

    const TStatistics& GetStatistics()
    {
        return Statistics_;
    }

private:
    TStatistics Statistics_;
};

TEST(TStatistics, Consumer)
{
    TStatisticsUpdater statisticsUpdater;
    TStatisticsConsumer consumer(BIND(&TStatisticsUpdater::AddSample, &statisticsUpdater));
    BuildYsonListFragmentFluently(&consumer)
        .Item()
            .BeginMap()
                .Item("k1").Value(4)
            .EndMap()
        .Item()
            .BeginMap()
                .Item("k2").Value(-7)
            .EndMap()
        .Item()
            .BeginMap()
                .Item("key")
                .BeginMap()
                    .Item("subkey")
                    .Value(42)
                .EndMap()
            .EndMap();

    const auto& statistics = statisticsUpdater.GetStatistics();
    EXPECT_EQ(4, GetNumericValue(statistics, "custom"_L / "k1"_L));
    EXPECT_EQ(-7, GetNumericValue(statistics, "custom"_L / "k2"_L));
    EXPECT_EQ(42, GetNumericValue(statistics, "custom"_L / "key"_L / "subkey"_L));
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatistics, BuildingConsumer)
{
    TYsonString statisticsYson(TStringBuf(
        "{"
            "abc="
            "{"
                "def="
                "{"
                    "sum=42; count=3; min=5; max=21; last=10;"
                "};"
                "degh="
                "{"
                    "sum=27; count=1; min=27; max=27; last=27;"
                "};"
            "};"
            "xyz="
            "{"
                "sum=50; count=5; min=8; max=12;"
            "};"
        "}"));
    auto statistics = ConvertTo<TStatistics>(statisticsYson);
    auto data = statistics.Data();

    std::map<TStatisticPath, TSummary> expectedData {
        { "abc"_L / "def"_L, TSummary(42, 3, 5, 21, 10) },
        { "abc"_L / "degh"_L, TSummary(27, 1, 27, 27, 27) },
        { "xyz"_L, TSummary(50, 5, 8, 12, std::nullopt) },
    };

    EXPECT_EQ(expectedData, data);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TStatistics, GetRangeByPrefix) {
    TStatistics statistics = CreateStatistics({
        {"a"_L, 1},
        {"b"_L / "a"_L, 1},
        {"b"_L / "aa"_L / "a"_L, 1},
        {"b"_L / TStatisticPathLiteral(TString(std::numeric_limits<char>::max())), 1},
        {"c"_L, 1},
    });

    {
        // We should not return the node that exactly matches the prefix.
        EXPECT_TRUE(statistics.GetRangeByPrefix("a"_L).empty());
    }

    {
        auto actualRange = statistics.GetRangeByPrefix("b"_L);
        TStatistics expectedRange = CreateStatistics({
            {"b"_L / "a"_L, 1},
            {"b"_L / "aa"_L / "a"_L, 1},
            {"b"_L / TStatisticPathLiteral(TString(std::numeric_limits<char>::max())), 1},
        });

        EXPECT_EQ(std::map(actualRange.begin(), actualRange.end()), expectedRange.Data());
    }
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTaggedStatistics, AppendStatistics)
{
    TTaggedStatistics<int> taggedStatistics;
    {
        TStatistics statistics;
        statistics.AddSample("abc"_L / "def"_L, 1);
        statistics.AddSample("abc"_L / "defg"_L, 2);
        statistics.AddSample("xyz"_L, 3);
        taggedStatistics.AppendStatistics(statistics, 1);
    }

    {
        TStatistics statistics;
        statistics.AddSample("abc"_L / "def"_L, 1);
        statistics.AddSample("ijk"_L, 2);
        taggedStatistics.AppendStatistics(statistics, 2);
    }

    {
        TStatistics statistics;
        statistics.AddSample("abc"_L / "def"_L, 2);
        taggedStatistics.AppendStatistics(statistics, 1);
    }

    {
        auto actualData = taggedStatistics.GetData();

        std::map<TStatisticPath, THashMap<int, TSummary>> expectedData {
            // std::nullopt because Last is always dropped during merge, see TSummary::Merge.
            {"abc"_L / "def"_L, {
                {1, TSummary(3, 2, 1, 2, std::nullopt)},
                {2, TSummary(1, 1, 1, 1, 1)}}},
            {"abc"_L / "defg"_L, {
                {1, TSummary(2, 1, 2, 2, 2)}}},
            {"xyz"_L, {
                {1, TSummary(3, 1, 3, 3, 3)}}},
            {"ijk"_L, {
                {2, TSummary(2, 1, 2, 2, 2)}}}
        };

        EXPECT_EQ(expectedData, actualData);
    }

    {
        TStatistics statistics;
        statistics.AddSample("xyz"_L / "suffix"_L, 1);
        EXPECT_THROW(taggedStatistics.AppendStatistics(statistics, 3), std::exception);
    }

    {
        TStatistics statistics;
        statistics.AddSample("abc"_L, 1); // prefix
        EXPECT_THROW(taggedStatistics.AppendStatistics(statistics, 3), std::exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

