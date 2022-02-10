#pragma once

#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/defs.h>

namespace NKikimr {
namespace NReplication {

IActor* CreateController(const TActorId& tablet, TTabletStorageInfo* info);

} // NReplication
} // NKikimr
