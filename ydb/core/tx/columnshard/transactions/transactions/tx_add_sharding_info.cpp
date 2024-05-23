#include "tx_add_sharding_info.h"
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>

namespace NKikimr::NColumnShard {

bool TTxAddShardingInfo::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    AFL_VERIFY(!!SnapshotVersion);
    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::ShardingInfo>()
        .Key(PathId, ShardingVersion)
        .Update(NIceDb::TUpdate<Schema::ShardingInfo::Snapshot>(SnapshotVersion->SerializeToString()), 
            NIceDb::TUpdate<Schema::ShardingInfo::Logic>(GranuleShardingLogic.SerializeToString()));
    return true;
}

void TTxAddShardingInfo::Complete(const TActorContext& /*ctx*/) {
    AFL_VERIFY(!!SnapshotVersion);
    NOlap::TGranuleShardingInfo info(GranuleShardingLogic, *SnapshotVersion, ShardingVersion, PathId);
    Self->MutableIndexAs<NOlap::TColumnEngineForLogs>().AddShardingInfo(info);
}

}
