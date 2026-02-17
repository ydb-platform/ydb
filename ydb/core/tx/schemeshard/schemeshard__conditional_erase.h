#pragma once

#include <functional>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/datetime/base.h>

#include <ydb/core/tx/schemeshard/schemeshard_identificators.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>

namespace NKikimr::NSchemeShard {

struct TCondEraseAffectedShard {
    TShardIdx ShardIdx;
    ui64 PartitionIdx;
    TTabletId TabletId;
    TDuration Next;
};
struct TCondEraseAffectedTable {
    TTableInfo::TPtr TableInfo;
    TVector<TCondEraseAffectedShard> AffectedShards;
};

extern std::function<void (const TInstant, const THashMap<TPathId, TCondEraseAffectedTable>&)> CondEraseTestObserver;

}  // namespace NKikimr::NSchemeShard
