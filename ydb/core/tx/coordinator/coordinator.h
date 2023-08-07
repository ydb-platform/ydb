#pragma once
#include "defs.h"
#include <ydb/core/tx/tx.h>
#include <util/generic/bitmap.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/hash_set.h>

namespace NKikimr {

IActor* CreateFlatTxCoordinator(const TActorId &tablet, TTabletStorageInfo *info);

} // namespace NKikimr
