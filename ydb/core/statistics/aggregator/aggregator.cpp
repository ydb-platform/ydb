#include "aggregator.h"

#include "aggregator_impl.h"

namespace NKikimr::NStat {

IActor* CreateStatisticsAggregator(const NActors::TActorId& tablet, TTabletStorageInfo* info) {
    return new TStatisticsAggregator(tablet, info, false);
}

IActor* CreateStatisticsAggregatorForTests(const NActors::TActorId& tablet, TTabletStorageInfo* info) {
    return new TStatisticsAggregator(tablet, info, true);
}

} // NKikimr::NStat
