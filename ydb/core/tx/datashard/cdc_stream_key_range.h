#pragma once

#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme_types/scheme_type_info.h>

#include <util/generic/array_ref.h>

namespace NKikimr::NDataShard {

bool ShouldUseKeyRangeHeartbeats(NKikimrSchemeOp::ECdcStreamHeartbeatMode heartbeatMode, TConstArrayRef<NScheme::TTypeInfo> topicKeySchema);

NPQ::TPartitionKeyRange MakeKeyRange(const TSerializedCellVec& from, const TSerializedCellVec& to);

} // namespace NKikimr::NDataShard
