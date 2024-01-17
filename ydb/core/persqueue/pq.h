#pragma once

#include <ydb/core/base/blobstorage.h>

#include <ydb/library/actors/core/actorid.h>

namespace NKikimr {

IActor* CreatePersQueue(const TActorId& tablet, TTabletStorageInfo *info);
IActor* CreatePersQueueReadBalancer(const TActorId& tablet, TTabletStorageInfo *info);

} //NKikimr
