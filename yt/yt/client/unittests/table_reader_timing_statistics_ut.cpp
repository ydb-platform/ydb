#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/api/table_reader.h>

#include <yt/yt/client/api/rpc_proxy/helpers.h>

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

TRemoteTableReaderTimingStatistics MakeRemoteStatistics()
{
    return TRemoteTableReaderTimingStatistics{
        .MasterFetchTime = TDuration::MilliSeconds(1),
        .DataReadTiming = NChunkClient::TTimingStatistics{
            .WaitTime = TDuration::MilliSeconds(2),
            .ReadTime = TDuration::MilliSeconds(3),
            .IdleTime = TDuration::MilliSeconds(4),
        },
        .TotalTime = TDuration::MilliSeconds(50),
        .EncodeTime = TDuration::MilliSeconds(6),
        .WriteStallTime = TDuration::MilliSeconds(7),
        .WindowDrainedTime = TDuration::MilliSeconds(8),
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

TEST(TTableReaderTimingStatisticsTest, SerializeRemote)
{
    auto statistics = TTableReaderTimingStatistics{
        .TotalTime = TDuration::MilliSeconds(100),
        .DecodeTime = TDuration::MilliSeconds(5),
        .Remote = MakeRemoteStatistics(),
    };
    auto expected = ConvertToNode(TYsonStringBuf(
        "{total_time=100;decode_time=5;remote={"
        "master_fetch_time=1;data_read_timing={wait_time=2;read_time=3;idle_time=4};total_time=50;"
        "encode_time=6;write_stall_time=7;window_drained_time=8}}"));

    EXPECT_TRUE(AreNodesEqual(ConvertToNode(statistics), expected));
}

TEST(TTableReaderTimingStatisticsTest, FormatFull)
{
    EXPECT_EQ(
        ToString(MakeFullStatistics()),
        "{MasterFetch: 10000us, DataRead: {Wait: 20000us, Read: 30000us, Idle: 40000us}, Total: 100000us, "
        "Decode: <null>, Remote: <null>}");
}

TEST(TTableReaderTimingStatisticsTest, FormatAbsentFields)
{
    auto statistics = TTableReaderTimingStatistics{
        .TotalTime = TDuration::MilliSeconds(100),
    };

    EXPECT_EQ(ToString(statistics), "{MasterFetch: <null>, DataRead: <null>, Total: 100000us, Decode: <null>, Remote: <null>}");
}

TEST(TTableReaderTimingStatisticsTest, FormatRemote)
{
    EXPECT_EQ(
        ToString(MakeRemoteStatistics()),
        "{MasterFetch: 1000us, DataRead: {Wait: 2000us, Read: 3000us, Idle: 4000us}, Total: 50000us, "
        "Encode: 6000us, WriteStall: 7000us, WindowDrained: 8000us}");
}

TEST(TTableReaderTimingStatisticsTest, RemoteProtoRoundTrip)
{
    auto statistics = MakeRemoteStatistics();

    NRpcProxy::NProto::TRemoteTableReaderTimingStatistics proto;
    ToProto(&proto, statistics);
    auto restored = FromProto<TRemoteTableReaderTimingStatistics>(proto);

    EXPECT_EQ(ToString(restored), ToString(statistics));
}

TEST(TTableReaderTimingStatisticsTest, RemoteProtoRoundTripAbsentFields)
{
    auto statistics = TRemoteTableReaderTimingStatistics{
        .TotalTime = TDuration::MilliSeconds(50),
    };

    NRpcProxy::NProto::TRemoteTableReaderTimingStatistics proto;
    ToProto(&proto, statistics);
    auto restored = FromProto<TRemoteTableReaderTimingStatistics>(proto);

    EXPECT_FALSE(proto.has_master_fetch_time());
    EXPECT_FALSE(proto.has_data_read_timing());
    EXPECT_EQ(ToString(restored), ToString(statistics));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NApi
