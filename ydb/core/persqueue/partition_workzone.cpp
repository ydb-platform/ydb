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

}
