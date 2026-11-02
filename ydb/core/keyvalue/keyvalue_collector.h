#pragma once
#include "defs.h"
#include "keyvalue_collect_operation.h"
#include <ydb/core/base/blobstorage.h>

namespace NKikimr {
namespace NKeyValue {

// DSProxy splits anything larger into messages of this size again
constexpr ui64 CollectorMaxFlagsPerMessage = MaxCollectGarbageFlagsPerMessage;

IActor* CreateKeyValueCollector(const TActorId &keyValueActorId, TIntrusivePtr<TCollectOperation> &collectOperation,
        const TTabletStorageInfo *tabletInfo, ui32 recordGeneration, ui32 perGenerationCounter);

} // NKeyValue
} // NKikimr
