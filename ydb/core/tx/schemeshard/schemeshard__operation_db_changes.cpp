#include "schemeshard__operation_db_changes.h"
#include "schemeshard_impl.h"

#include <ydb/core/tx/tx_processing.h>

namespace NKikimr::NSchemeShard {

void TStorageChanges::Apply(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext& txc, const TActorContext&) {
    NIceDb::TNiceDb db(txc.DB);

    //TODO: paths/other changes could be repeated many times, could it be a problem?

    for (const auto& pId : Paths) {
        ss->PersistPath(db, pId);
    }

    for (const auto& pId : AlterUserAttrs) {
        ss->PersistAlterUserAttributes(db, pId);
    }

    for (const auto& pId : ApplyUserAttrs) {
        ss->ApplyAndPersistUserAttrs(db, pId);
    }

    for (const auto& pId : AlterIndexes) {
        ss->PersistTableIndexAlterData(db, pId);
    }

    for (const auto& pId : ApplyIndexes) {
        ss->PersistTableIndex(db, pId);
    }

    for (const auto& pId : AlterCdcStreams) {
        ss->PersistCdcStreamAlterData(db, pId);
    }

    for (const auto& pId : ApplyCdcStreams) {
        ss->PersistCdcStream(db, pId);
    }

    for (const auto& pId : Tables) {
        ss->PersistTable(db, pId);
    }

    for (const auto& [pId, snapshotTxId] : TableSnapshots) {
        ss->PersistSnapshotTable(db, snapshotTxId, pId);
    }

    for (const auto& [pId, lockTxId] : LongLocks) {
        ss->PersistLongLock(db, lockTxId, pId);
    }

    for (const auto& pId : Unlocks) {
        ss->PersistUnLock(db, pId);
    }

    for (const auto& shardIdx : Shards) {
        const TShardInfo& shardInfo = ss->ShardInfos.at(shardIdx);
        const TPathId& pId = shardInfo.PathId;

        ss->PersistShardMapping(db, shardIdx, shardInfo.TabletID, pId, shardInfo.CurrentTxId, shardInfo.TabletType);
        ss->PersistChannelsBinding(db, shardIdx, shardInfo.BindedChannels);

        if (ss->Tables.contains(pId)) {
            auto tableInfo = ss->Tables.at(pId);
            if (tableInfo->PerShardPartitionConfig.contains(shardIdx)) {
                ss->PersistAddTableShardPartitionConfig(db, shardIdx, tableInfo->PerShardPartitionConfig.at(shardIdx));
            }
        }
    }

    for (const auto& opId : TxStates) {
        ss->PersistTxState(db, opId);
    }


    for (const auto& pId : AlterSubDomains) {
        auto subdomainInfo = ss->SubDomains.at(pId);
        ss->PersistSubDomainAlter(db, pId, *subdomainInfo->GetAlter());
    }

    for (const auto& pId : Views) {
        ss->PersistView(db, pId);
    }

    ss->PersistUpdateNextPathId(db);
    ss->PersistUpdateNextShardIdx(db);

    for (const auto& [pathId, shardIdx, pqInfo] : PersQueue) {
        ss->PersistPersQueue(db, pathId, shardIdx, pqInfo);
    }

    for (const auto& [pathId, pqGroup] : PersQueueGroup) {
        ss->PersistPersQueueGroup(db, pathId, pqGroup);
    }

    for (const auto& [pathId, pqGroup] : AddPersQueueGroupAlter) {
        ss->PersistAddPersQueueGroupAlter(db, pathId, pqGroup);
    }
}

}
