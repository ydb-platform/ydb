#include "table_reader.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TRemoteTableReaderTimingStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("master_fetch_time", statistics.MasterFetchTime)
            .OptionalItem("data_read_timing", statistics.DataReadTiming)
            .Item("total_time").Value(statistics.TotalTime)
            .Item("encode_time").Value(statistics.EncodeTime)
            .Item("write_stall_time").Value(statistics.WriteStallTime)
            .Item("window_drained_time").Value(statistics.WindowDrainedTime)
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TRemoteTableReaderTimingStatistics& statistics, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{MasterFetch: %v, DataRead: %v, Total: %v, Encode: %v, WriteStall: %v, WindowDrained: %v}",
        statistics.MasterFetchTime,
        statistics.DataReadTiming,
        statistics.TotalTime,
        statistics.EncodeTime,
        statistics.WriteStallTime,
        statistics.WindowDrainedTime);
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTableReaderTimingStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("master_fetch_time", statistics.MasterFetchTime)
            .OptionalItem("data_read_timing", statistics.DataReadTiming)
            .Item("total_time").Value(statistics.TotalTime)
            .OptionalItem("decode_time", statistics.DecodeTime)
            .OptionalItem("remote", statistics.Remote)
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TTableReaderTimingStatistics& statistics, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{MasterFetch: %v, DataRead: %v, Total: %v, Decode: %v, Remote: %v}",
        statistics.MasterFetchTime,
        statistics.DataReadTiming,
        statistics.TotalTime,
        statistics.DecodeTime,
        statistics.Remote);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
