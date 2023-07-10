#pragma once

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

IActor* CreateFlatBsController(const TActorId &tablet, TTabletStorageInfo *info);

} //NKikimr
