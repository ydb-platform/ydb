#pragma once

#include "defs.h"

#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NKesus {

IActor* CreateKesusTablet(const TActorId& tablet, TTabletStorageInfo* info);

void AddKesusProbesList();

}
}
