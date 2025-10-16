#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

IActor* CreateRestoreShard(const TActorId &tablet, TTabletStorageInfo *info);

} // namespace NKikimr
