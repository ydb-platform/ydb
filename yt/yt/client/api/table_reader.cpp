#include "table_reader.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NYTree;
using namespace NYson;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TTableReaderTimingStatistics& statistics, IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .OptionalItem("master_fetch_time", statistics.MasterFetchTime)
            .OptionalItem("data_read_timing", statistics.DataReadTiming)
            .Item("total_time").Value(statistics.TotalTime)
        .EndMap();
}

void FormatValue(TStringBuilderBase* builder, const TTableReaderTimingStatistics& statistics, TStringBuf /*spec*/)
{
    Format(
        builder,
        "{MasterFetch: %v, DataRead: %v, Total: %v}",
        statistics.MasterFetchTime,
        statistics.DataReadTiming,
        statistics.TotalTime);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
