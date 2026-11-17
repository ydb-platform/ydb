#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/library/actors/core/actor.h>

namespace NKikimr::NCdcAggregator {

IActor* CreateCdcAggregator(const NActors::TActorId& tablet, TTabletStorageInfo* info);

} // namespace NKikimr::NCdcAggregator
