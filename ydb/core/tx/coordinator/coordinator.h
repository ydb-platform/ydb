#pragma once
#include "defs.h"
#include <ydb/core/tx/tx.h>
#include <util/generic/bitmap.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/map.h>
#include <util/generic/hash_set.h>

namespace NKikimr {

// Only used below as a pointer in a factory-function declaration; a
// forward declaration avoids pulling the heavy ydb/core/base/blobstorage.h
// into this widely-included header.
class TTabletStorageInfo;

IActor* CreateFlatTxCoordinator(const TActorId &tablet, TTabletStorageInfo *info);

} // namespace NKikimr
