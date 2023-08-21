#pragma once

#include "public.h"

#include <yt/yt/core/ytree/attributes.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM(ERawIOTag,
    ((ReadSessionId)  (0))
    ((WriteSessionId) (1))
    ((JobId)          (2))
    ((ObjectPath)     (3))
    ((ObjectId)       (4))
    ((OperationId)    (5))
    ((ChunkId)        (6))
    ((LocationId)     (7))
    ((SlotIndex)      (8))
);

DEFINE_ENUM(EAggregateIOTag,
    ((Direction)         (0))
    ((Medium)            (1))
    ((DiskFamily)        (2))
    ((User)              (3))
    ((LocationType)      (4))
    ((DataNodeMethod)    (5))
    ((JobType)           (6))
    ((Account)           (7))
    ((ApiMethod )        (8))
    ((ProxyKind )        (9))
    ((Pool)             (10))
    ((PoolTree)         (11))
    ((OperationType)    (12))
    ((TaskName)         (13))
    ((JobIoKind)        (14))
    ((PoolPath)         (15))
    ((ErasureCodec)     (16))
    ((CompressionCodec) (17))
);

TString FormatIOTag(ERawIOTag tag);
TString FormatIOTag(EAggregateIOTag tag);

////////////////////////////////////////////////////////////////////////////////

template <class T>
void AddTagToBaggage(const NYTree::IAttributeDictionaryPtr& baggage, T tag, const TStringBuf& value);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
