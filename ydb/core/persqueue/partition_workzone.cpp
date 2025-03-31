#include "partition_workzone.h"
#include "partition_util.h"

namespace NKikimr::NPQ {

TPartitionWorkZone::TPartitionWorkZone(const TPartitionId& partition)
    : PartitionedBlob(partition, 0, "", 0, 0, 0, Head, NewHead, true, false, 8_MB)
    , NewHeadKey{TKey{}, 0, TInstant::Zero(), 0}
    , BodySize(0)
    , MaxWriteResponsesSize(0)
{
}

bool TPartitionWorkZone::PositionInBody(ui64 offset, ui32 partNo) const
{
    return offset < Head.Offset || ((Head.Offset == offset) && (partNo < Head.PartNo));
}

bool TPartitionWorkZone::PositionInHead(ui64 offset, ui32 partNo) const
{
    return Head.Offset < offset || ((Head.Offset == offset) && (Head.PartNo < partNo));
}

void TPartitionWorkZone::NewPartitionedBlob(const TPartitionId& partitionId,
                                            const ui64 offset,
                                            const TString& sourceId,
                                            const ui64 seqNo,
                                            const ui16 totalParts,
                                            const ui32 totalSize,
                                            bool headCleared,
                                            bool needCompactHead,
                                            const ui32 maxBlobSize,
                                            ui16 nextPartNo)
{
    PartitionedBlob = TPartitionedBlob(partitionId,
                                       offset,
                                       sourceId,
                                       seqNo,
                                       totalParts,
                                       totalSize,
                                       Head,
                                       NewHead,
                                       headCleared,
                                       needCompactHead,
                                       maxBlobSize,
                                       nextPartNo);
}

void TPartitionWorkZone::ClearPartitionedBlob(const TPartitionId& partitionId, ui32 maxBlobSize)
{
    PartitionedBlob = TPartitionedBlob(partitionId,
                                       0,
                                       "",
                                       0,
                                       0,
                                       0,
                                       Head,
                                       NewHead,
                                       true,
                                       false,
                                       maxBlobSize);
}

}
