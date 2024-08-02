#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/statistics.h>
#include <yt/yt/core/misc/protobuf_helpers.h>

#include <yt/yt/core/yson/format.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NYTree;
using namespace NYson;

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

TStatistics CreateStatistics(std::initializer_list<std::pair<NYPath::TYPath, i64>> data)
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
        "/key/subkey",
        origin);

    EXPECT_EQ(5, GetNumericValue(statistics, "/key/subkey/x"));
    EXPECT_EQ(7, GetNumericValue(statistics, "/key/subkey/y"));

    statistics.AddSample("/key/sub", 42);
    EXPECT_EQ(42, GetNumericValue(statistics, "/key/sub"));

    // Cannot add sample to the map node.
    EXPECT_THROW(statistics.AddSample("/key/subkey", 24), std::exception);

    statistics.Merge(CreateStatistics({
        {"/key/subkey/x", 5},
        {"/key/subkey/z", 9}}));

    EXPECT_EQ(10, GetNumericValue(statistics, "/key/subkey/x"));
    EXPECT_EQ(7, GetNumericValue(statistics, "/key/subkey/y"));
    EXPECT_EQ(9, GetNumericValue(statistics, "/key/subkey/z"));

    EXPECT_THROW(
        statistics.Merge(CreateStatistics({{"/key", 5}})),
        std::exception);

    statistics.AddSample("/key/subkey/x", 10);
    EXPECT_EQ(20, GetNumericValue(statistics, "/key/subkey/x"));

    auto ysonStatistics = ConvertToYsonString(statistics);
    auto deserializedStatistics = ConvertTo<TStatistics>(ysonStatistics);

    EXPECT_EQ(20, GetNumericValue(deserializedStatistics, "/key/subkey/x"));
    EXPECT_EQ(42, GetNumericValue(deserializedStatistics, "/key/sub"));
}

////////////////////////////////////////////////////////////////////////////////

class TStatisticsUpdater
{
public:
    void AddSample(const INodePtr& node)
    {
        Statistics_.AddSample("/custom", node);
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
    EXPECT_EQ(4, GetNumericValue(statistics, "/custom/k1"));
    EXPECT_EQ(-7, GetNumericValue(statistics, "/custom/k2"));
    EXPECT_EQ(42, GetNumericValue(statistics, "/custom/key/subkey"));
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

    std::map<TString, TSummary> expectedData {
        { "/abc/def", TSummary(42, 3, 5, 21, 10) },
        { "/abc/degh", TSummary(27, 1, 27, 27, 27) },
        { "/xyz", TSummary(50, 5, 8, 12, std::nullopt) },
    };

    EXPECT_EQ(expectedData, data);
}

////////////////////////////////////////////////////////////////////////////////

TEST(TTaggedStatistics, AppendStatistics)
{
    TTaggedStatistics<int> taggedStatistics;
    {
        TStatistics statistics;
        statistics.AddSample("/abc/def", 1);
        statistics.AddSample("/abc/defg", 2);
        statistics.AddSample("/xyz", 3);
        taggedStatistics.AppendStatistics(statistics, 1);
    }

    {
        TStatistics statistics;
        statistics.AddSample("/abc/def", 1);
        statistics.AddSample("/ijk", 2);
        taggedStatistics.AppendStatistics(statistics, 2);
    }

    {
        TStatistics statistics;
        statistics.AddSample("/abc/def", 2);
        taggedStatistics.AppendStatistics(statistics, 1);
    }

    {
        auto actualData = taggedStatistics.GetData();

        std::map<TString, THashMap<int, TSummary>> expectedData {
            // std::nullopt because Last is always dropped during merge, see TSummary::Merge.
            {"/abc/def", {
                    {1, TSummary(3, 2, 1, 2, std::nullopt)},
                    {2, TSummary(1, 1, 1, 1, 1)}}},
            {"/abc/defg", {{1, TSummary(2, 1, 2, 2, 2)}}},
            {"/xyz", {{1, TSummary(3, 1, 3, 3, 3)}}},
            {"/ijk", {{2, TSummary(2, 1, 2, 2, 2)}}}
        };

        EXPECT_EQ(expectedData, actualData);
    }

    {
        TStatistics statistics;
        statistics.AddSample("/xyz/suffix", 1);
        EXPECT_THROW(taggedStatistics.AppendStatistics(statistics, 3), std::exception);
    }

    {
        TStatistics statistics;
        statistics.AddSample("/abc", 1); // prefix
        EXPECT_THROW(taggedStatistics.AppendStatistics(statistics, 3), std::exception);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

