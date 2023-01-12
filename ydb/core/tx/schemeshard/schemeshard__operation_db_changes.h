#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_path_element.h"
#include "schemeshard_info_types.h"

#include <ydb/core/tablet_flat/tablet_flat_executor.h>

#include <util/generic/ptr.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TStorageChanges: public TSimpleRefCount<TStorageChanges> {
    TDeque<TPathId> Paths;

    TDeque<TPathId> Tables;
    TDeque<std::pair<TPathId, TTxId>> TableSnapshots;
    TDeque<std::pair<TPathId, TTxId>> LongLocks;
    TDeque<TPathId> Unlocks;

    TDeque<TShardIdx> Shards;

    TDeque<TPathId> AlterUserAttrs;
    TDeque<TPathId> ApplyUserAttrs;

    TDeque<TPathId> AlterIndexes;
    TDeque<TPathId> ApplyIndexes;

    TDeque<TPathId> AlterCdcStreams;
    TDeque<TPathId> ApplyCdcStreams;

    TDeque<TOperationId> TxStates;

    TDeque<TPathId> AlterSubDomains;

public:
    ~TStorageChanges() = default;

    void PersistPath(const TPathId& pathId) {
        Paths.push_back(pathId);
    }

    void PersistTable(const TPathId& pathId) {
        Tables.push_back(pathId);
    }

    void PersistTableSnapshot(const TPathId& pathId, TTxId snapshotTxId) {
        TableSnapshots.emplace_back(pathId, snapshotTxId);
    }

    void PersistLongLock(const TPathId& pathId, TTxId lockTxId) {
        LongLocks.emplace_back(pathId, lockTxId);
    }

    void PersistUnLock(const TPathId& pathId) {
        Unlocks.emplace_back(pathId);
    }

    void PersistAlterUserAttrs(const TPathId& pathId) {
        AlterUserAttrs.push_back(pathId);
    }

    void PersistApplyUserAttrs(const TPathId& pathId) {
        ApplyUserAttrs.push_back(pathId);
    }

    void PersistAlterIndex(const TPathId& pathId) {
        AlterIndexes.push_back(pathId);
    }

    void PersistApplyIndex(const TPathId& pathId) {
        ApplyIndexes.push_back(pathId);
    }

    void PersistAlterCdcStream(const TPathId& pathId) {
        AlterCdcStreams.push_back(pathId);
    }

    void PersistApplyCdcStream(const TPathId& pathId) {
        ApplyCdcStreams.push_back(pathId);
    }

    void PersistTxState(const TOperationId& opId) {
        TxStates.push_back(opId);
    }

    void PersistShard(const TShardIdx& shardIdx) {
        Shards.push_back(shardIdx);
    }

    void PersistSubDomainAlter(const TPathId& pathId) {
        AlterSubDomains.push_back(pathId);
    }

    void Apply(TSchemeShard* ss, NTabletFlatExecutor::TTransactionContext &txc, const TActorContext &ctx);
};

}
