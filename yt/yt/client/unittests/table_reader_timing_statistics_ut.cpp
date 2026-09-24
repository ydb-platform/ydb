#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/core/ytree/convert.h>
#include <yt/yt/core/ytree/ypath_client.h>

namespace NYT::NApi {
namespace {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

TTableReaderTimingStatistics MakeFullStatistics()
{
    return TTableReaderTimingStatistics{
        .MasterFetchTime = TDuration::MilliSeconds(10),
        .DataReadTiming = NChunkClient::TTimingStatistics{
            .WaitTime = TDuration::MilliSeconds(20),
            .ReadTime = TDuration::MilliSeconds(30),
            .IdleTime = TDuration::MilliSeconds(40),
        },
        .TotalTime = TDuration::MilliSeconds(100),
    };
}

TEST(TTableReaderTimingStatisticsTest, SerializeFull)
{
    auto expected = ConvertToNode(TYsonStringBuf(
        "{master_fetch_time=10;data_read_timing={wait_time=20;read_time=30;idle_time=40};total_time=100}"));

    EXPECT_TRUE(AreNodesEqual(ConvertToNode(MakeFullStatistics()), expected));
}

TEST(TTableReaderTimingStatisticsTest, SerializeAbsentFields)
{
    auto statistics = TTableReaderTimingStatistics{
        .TotalTime = TDuration::MilliSeconds(100),
    };
    auto expected = ConvertToNode(TYsonStringBuf("{total_time=100}"));

    EXPECT_TRUE(AreNodesEqual(ConvertToNode(statistics), expected));
}

TEST(TTableReaderTimingStatisticsTest, FormatFull)
{
    EXPECT_EQ(
        ToString(MakeFullStatistics()),
        "{MasterFetch: 10000us, DataRead: {Wait: 20000us, Read: 30000us, Idle: 40000us}, Total: 100000us}");
}

TEST(TTableReaderTimingStatisticsTest, FormatAbsentFields)
{
    auto statistics = TTableReaderTimingStatistics{
        .TotalTime = TDuration::MilliSeconds(100),
    };

    EXPECT_EQ(ToString(statistics), "{MasterFetch: <null>, DataRead: <null>, Total: 100000us}");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi
