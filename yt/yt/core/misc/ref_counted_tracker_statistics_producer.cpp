#include "ref_counted_tracker_statistics_producer.h"

#include <yt/yt/core/ytree/fluent.h>

namespace NYT {

using namespace NYson;
using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

TYsonProducer CreateRefCountedTrackerStatisticsProducer()
{
    return BIND([] (IYsonConsumer* consumer) {
        auto statistics = TRefCountedTracker::Get()->GetStatistics();

        BuildYsonFluently(consumer)
            .BeginMap()
                .Item("statistics")
                    .DoListFor(statistics.NamedStatistics, [] (TFluentList fluent, const auto& namedSlotStatistics) {
                        fluent
                            .Item().BeginMap()
                                .Item("name").Value(namedSlotStatistics.FullName)
                                .Item("objects_alive").Value(namedSlotStatistics.ObjectsAlive)
                                .Item("objects_allocated").Value(namedSlotStatistics.ObjectsAllocated)
                                .Item("bytes_alive").Value(namedSlotStatistics.BytesAlive)
                                .Item("bytes_allocated").Value(namedSlotStatistics.BytesAllocated)
                            .EndMap();
                    })
                .Item("total").BeginMap()
                    .Item("objects_alive").Value(statistics.TotalStatistics.ObjectsAlive)
                    .Item("objects_allocated").Value(statistics.TotalStatistics.ObjectsAllocated)
                    .Item("bytes_alive").Value(statistics.TotalStatistics.BytesAlive)
                    .Item("bytes_allocated").Value(statistics.TotalStatistics.BytesAllocated)
                .EndMap()
            .EndMap();
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
