#pragma once

#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <util/generic/array_ref.h>
#include <util/generic/map.h>
#include <util/generic/vector.h>

namespace NKikimr::NDataShard {

bool ShouldUseKeyRangeHeartbeats(NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode, TConstArrayRef<NScheme::TTypeInfo> topicKeySchema);

NPQ::TPartitionKeyRange MakeKeyRange(const TSerializedCellVec& from, const TSerializedCellVec& to);

bool KeyRangesIntersect(const NPQ::TPartitionKeyRange& lhs, const NPQ::TPartitionKeyRange& rhs, TConstArrayRef<NScheme::TTypeInfo> keyTypes);

class TKeyRangeSplit {
public:
    TKeyRangeSplit(ui64 parentTabletId, const NKikimrTxDataShard::TSplitMergeDescription& splitDescription);

    TVector<ui64> GetChildrenIntersecting(const NPQ::TPartitionKeyRange& partition, TConstArrayRef<NScheme::TTypeInfo> keyTypes) const;
    const NPQ::TPartitionKeyRange& GetChildKeyRange(ui64 childTabletId) const;

private:
    NPQ::TPartitionKeyRange ParentKeyRange;
    TMap<ui64, NPQ::TPartitionKeyRange> ChildKeyRanges;
};

} // namespace NKikimr::NDataShard
