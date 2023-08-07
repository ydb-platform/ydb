#pragma once

#include <library/cpp/actors/core/actor.h>
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NSysView {

IActor* CreateSysViewProcessor(const NActors::TActorId& tablet, TTabletStorageInfo* info);
IActor* CreateSysViewProcessorForTests(const NActors::TActorId& tablet, TTabletStorageInfo* info);

} // NSysView
} // NKikimr
