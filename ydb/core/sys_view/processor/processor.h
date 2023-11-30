#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NSysView {

IActor* CreateSysViewProcessor(const NActors::TActorId& tablet, TTabletStorageInfo* info);
IActor* CreateSysViewProcessorForTests(const NActors::TActorId& tablet, TTabletStorageInfo* info);

} // NSysView
} // NKikimr
