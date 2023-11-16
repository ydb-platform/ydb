#pragma once
#include "defs.h"
#include "keyvalue_collect_operation.h"

namespace NKikimr {
namespace NKeyValue {

IActor* CreateKeyValueCollector(const TActorId &keyValueActorId, TIntrusivePtr<TCollectOperation> &collectOperation,
        const TTabletStorageInfo *tabletInfo, ui32 recordGeneration, ui32 perGenerationCounter);

} // NKeyValue
} // NKikimr
