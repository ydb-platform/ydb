#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr::NStat {

IActor* CreateStatisticsAggregator(const NActors::TActorId& tablet, TTabletStorageInfo* info);

IActor* CreateStatisticsAggregatorForTests(const NActors::TActorId& tablet, TTabletStorageInfo* info);

} // NKikimr::NStat
