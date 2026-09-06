#pragma once

#include "schemeshard_identificators.h"

#include <util/generic/ptr.h>

#include <memory>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TMemoryChanges: public TSimpleRefCount<TMemoryChanges> {
    struct TImpl;
    std::unique_ptr<TImpl> Impl;

public:
    TMemoryChanges();
    ~TMemoryChanges();

    void GrabNewTxState(TSchemeShard* ss, const TOperationId& op);

    void GrabNewPath(TSchemeShard* ss, const TPathId& pathId);
    void GrabPath(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewTable(TSchemeShard* ss, const TPathId& pathId);
    void GrabTable(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewColumnTable(TSchemeShard* ss, const TPathId& pathId);
    void GrabColumnTable(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewShard(TSchemeShard* ss, const TShardIdx& shardId);
    void GrabShard(TSchemeShard* ss, const TShardIdx& shardId);

    void GrabDomain(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewIndex(TSchemeShard* ss, const TPathId& pathId);
    void GrabIndex(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewSequence(TSchemeShard* ss, const TPathId& pathId);
    void GrabSequence(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId);
    void GrabCdcStream(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId);

    void GrabNewLongLock(TSchemeShard* ss, const TPathId& pathId);
    void GrabLongLock(TSchemeShard* ss, const TPathId& pathId, TTxId lockTxId);

    void GrabNewExternalTable(TSchemeShard* ss, const TPathId& pathId);
    void GrabExternalTable(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewExternalDataSource(TSchemeShard* ss, const TPathId& pathId);
    void GrabExternalDataSource(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewView(TSchemeShard* ss, const TPathId& pathId);
    void GrabView(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewResourcePool(TSchemeShard* ss, const TPathId& pathId);
    void GrabResourcePool(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewBackupCollection(TSchemeShard* ss, const TPathId& pathId);
    void GrabBackupCollection(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewSysView(TSchemeShard* ss, const TPathId& pathId);
    void GrabSysView(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId);
    void GrabLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId);

    void GrabNewLongIncrementalBackupOp(TSchemeShard* ss, ui64 id);

    void GrabNewFullBackupOp(TSchemeShard* ss, ui64 id);
    void GrabNewBCPathToFullBackup(TSchemeShard* ss, const TPathId& bcPathId);

    void GrabNewSecret(TSchemeShard* ss, const TPathId& pathId);
    void GrabSecret(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewStreamingQuery(TSchemeShard* ss, const TPathId& pathId);
    void GrabStreamingQuery(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewSharedShard(TSchemeShard* ss, const TShardIdx& shardIdx, const TPathId& pathId);
    void GrabSharedShard(TSchemeShard* ss, const TShardIdx& shardIdx, const TPathId& pathId);

    void GrabNewTestShardSet(TSchemeShard* ss, const TPathId& pathId);
    void GrabTestShardSet(TSchemeShard* ss, const TPathId& pathId);

    void UnDo(TSchemeShard* ss);
};

}
