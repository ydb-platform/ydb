#pragma once
#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {

IActor* CreateTxMediator(const TActorId &tablet, TTabletStorageInfo *info);

}
