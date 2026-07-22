#pragma once

#include "defs.h"

namespace NKikimr {
namespace NKeyValue {

IActor* CreateKeyValueCopyBlobActor(
    const TActorId& keyValueActorId,
    TTabletStorageInfo* tabletInfo,
    const TLogoBlobID& blobId,
    const TLogoBlobID& newBlobId);

} // NKeyValue
} // NKikimr
