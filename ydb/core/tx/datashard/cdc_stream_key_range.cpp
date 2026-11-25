#include "cdc_stream_key_range.h"

#include <ydb/core/scheme/scheme_borders.h>

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

namespace {

bool IsEntirelyBefore(const NPQ::TPartitionKeyRange& range, const NPQ::TPartitionKeyRange& other, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    if (!range.ToBound || !other.FromBound) {
        return false;
    }
    return ComparePrefixBorders(keyTypes,
        other.FromBound->GetCells(), PrefixModeLeftBorderInclusive,
        range.ToBound->GetCells(), PrefixModeRightBorderNonInclusive) >= 0;
}

} // anonymous

bool KeyRangesIntersect(const NPQ::TPartitionKeyRange& lhs, const NPQ::TPartitionKeyRange& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return !IsEntirelyBefore(lhs, rhs, keyTypes) && !IsEntirelyBefore(rhs, lhs, keyTypes);
}

} // namespace NKikimr::NDataShard
