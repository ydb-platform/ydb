#pragma once

#include "blob.h"
#include "key.h"
#include "events/internal.h"

namespace NKikimr::NPQ {

class TKeyLevel;

struct TPartitionWorkZone {
    explicit TPartitionWorkZone(const TPartitionId& partition);

    THead Head;
    THead NewHead;
    TPartitionedBlob PartitionedBlob;
    std::deque<std::pair<TKey, ui32>> CompactedKeys; //key and blob size
    TDataKey NewHeadKey;

    ui64 BodySize;
    ui32 MaxWriteResponsesSize;

    std::deque<TDataKey> DataKeysBody;
    TVector<TKeyLevel> DataKeysHead;
    std::deque<TDataKey> HeadKeys;
};

}
