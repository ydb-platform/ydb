#include "cdc_stream_key_range.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_borders.h>

#include <util/generic/algorithm.h>

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

NPQ::TPartitionKeyRange MakeShardKeyRange(const NKikimrTxDataShard::TKeyRangeToTablet& shard) {
    return MakeKeyRange(TSerializedCellVec(shard.GetKeyRangeBegin()), TSerializedCellVec(shard.GetKeyRangeEnd()));
}

} // anonymous

bool KeyRangesIntersect(const NPQ::TPartitionKeyRange& lhs, const NPQ::TPartitionKeyRange& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes) {
    return !IsEntirelyBefore(lhs, rhs, keyTypes) && !IsEntirelyBefore(rhs, lhs, keyTypes);
}

TKeyRangeSplit::TKeyRangeSplit(ui64 parentTabletId, const NKikimrTxDataShard::TSplitMergeDescription& splitDescription) {
    const auto& parents = splitDescription.GetSourceRanges();
    const auto parent = FindIf(parents, [&](const auto& range) {
        return range.GetTabletID() == parentTabletId;
    });
    Y_ENSURE(parent != parents.end());
    ParentKeyRange = MakeShardKeyRange(*parent);

    for (const auto& child : splitDescription.GetDestinationRanges()) {
        ChildKeyRanges.emplace(child.GetTabletID(), MakeShardKeyRange(child));
    }
}

TVector<ui64> TKeyRangeSplit::GetChildrenIntersecting(const NPQ::TPartitionKeyRange& partition, TConstArrayRef<NScheme::TTypeInfo> keyTypes) const {
    TVector<ui64> children;
    if (KeyRangesIntersect(partition, ParentKeyRange, keyTypes)) {
        for (const auto& [childTabletId, childKeyRange] : ChildKeyRanges) {
            if (KeyRangesIntersect(partition, childKeyRange, keyTypes)) {
                children.push_back(childTabletId);
            }
        }
    }
    return children;
}

const NPQ::TPartitionKeyRange& TKeyRangeSplit::GetChildKeyRange(ui64 childTabletId) const {
    return ChildKeyRanges.at(childTabletId);
}

} // namespace NKikimr::NDataShard
