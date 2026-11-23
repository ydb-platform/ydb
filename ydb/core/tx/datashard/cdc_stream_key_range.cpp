#include "cdc_stream_key_range.h"

namespace NKikimr::NDataShard {

bool ShouldUseKeyRangeHeartbeats(NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode, TConstArrayRef<NScheme::TTypeInfo> topicKeySchema) {
    return heartbeatMode == NKikimrSchemeOp::ECdcStreamHeartbeatModeKeyRange
        && !topicKeySchema.empty();
}

NPQ::TPartitionKeyRange MakeKeyRange(const TSerializedCellVec& from, const TSerializedCellVec& to) {
    NPQ::TPartitionKeyRange range;
    if (!from.GetCells().empty()) {
        range.FromBound = from;
    }
    if (!to.GetCells().empty()) {
        range.ToBound = to;
    }
    return range;
}

} // namespace NKikimr::NDataShard
