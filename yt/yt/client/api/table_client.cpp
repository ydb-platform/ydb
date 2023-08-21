#include "table_client.h"

#include <yt/yt/client/ypath/rich.h>

#include <yt/yt/core/ytree/fluent.h>

namespace NYT::NApi {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMultiTablePartition& partition, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("table_ranges").Value(partition.TableRanges)
            .Item("aggregate_statistics").Value(partition.AggregateStatistics)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

void Serialize(const TMultiTablePartitions& partitions, NYson::IYsonConsumer* consumer)
{
    BuildYsonFluently(consumer)
        .BeginMap()
            .Item("partitions").Value(partitions.Partitions)
        .EndMap();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

