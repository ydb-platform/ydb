#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NGraph {

using namespace NActors;

IActor* CreateGraphShard(const TActorId& tablet, TTabletStorageInfo* info);

} // NGraph
} // NKikimr
