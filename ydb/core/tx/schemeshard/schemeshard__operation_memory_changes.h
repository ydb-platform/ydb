#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_path_element.h"

#include "schemeshard_info_types_fwd.h"

#include <util/generic/ptr.h>
#include <util/generic/stack.h>

namespace NKikimr::NSchemeShard {

struct TSchemeshardState;

class TMemoryChanges: public TSimpleRefCount<TMemoryChanges> {
    using TPathState = std::pair<TPathId, TPathElementPtr>;
    TStack<TPathState> Paths;

    using TIndexState = std::pair<TPathId, TTableIndexInfoPtr>;
    TStack<TIndexState> Indexes;

    using TCdcStreamState = std::pair<TPathId, TCdcStreamInfoPtr>;
    TStack<TCdcStreamState> CdcStreams;

    using TTableSnapshotState = std::pair<TPathId, TTxId>;
    TStack<TTableSnapshotState> TablesWithSnapshots;

    using TLockState = std::pair<TPathId, TTxId>;
    TStack<TLockState> LockedPaths;

    using TTableState = std::pair<TPathId, TTableInfoPtr>;
    TStack<TTableState> Tables;

    using TSequenceState = std::pair<TPathId, TSequenceInfoPtr>;
    TStack<TSequenceState> Sequences;

    using TShardState = std::pair<TShardIdx, THolder<TShardInfo>>;
    TStack<TShardState> Shards;

    // Actually, any single subdomain should not be grabbed at more than one version
    // per transaction/operation.
    // And transaction/operation could not work on more than one subdomain.
    // But just to be on the safe side (migrated paths, anyone?) we allow several
    // subdomains to be grabbed.
    THashMap<TPathId, TSubDomainInfoPtr> SubDomains;

    using TTxState = std::pair<TOperationId, THolder<TTxState>>;
    TStack<TTxState> TxStates;

    using TExternalTableState = std::pair<TPathId, TExternalTableInfoPtr>;
    TStack<TExternalTableState> ExternalTables;

    using TExternalDataSourceState = std::pair<TPathId, TExternalDataSourceInfoPtr>;
    TStack<TExternalDataSourceState> ExternalDataSources;

    using TViewState = std::pair<TPathId, TViewInfoPtr>;
    TStack<TViewState> Views;

    using TResourcePoolState = std::pair<TPathId, TResourcePoolInfoPtr>;
    TStack<TResourcePoolState> ResourcePools;

    using TBackupCollectionState = std::pair<TPathId, TBackupCollectionInfoPtr>;
    TStack<TBackupCollectionState> BackupCollections;

public:
    ~TMemoryChanges() = default;

    void GrabNewTxState(TSchemeshardState* ss, const TOperationId& op);

    void GrabNewPath(TSchemeshardState* ss, const TPathId& pathId);
    void GrabPath(TSchemeshardState* ss, const TPathId& pathId);

    void GrabNewTable(TSchemeshardState* ss, const TPathId& pathId);
    void GrabTable(TSchemeshardState* ss, const TPathId& pathId);

    void GrabNewShard(TSchemeshardState* ss, const TShardIdx& shardId);
    void GrabShard(TSchemeshardState* ss, const TShardIdx& shardId);

    void GrabDomain(TSchemeshardState* ss, const TPathId& pathId);

    void GrabNewIndex(TSchemeshardState* ss, const TPathId& pathId);
    void GrabIndex(TSchemeshardState* ss, const TPathId& pathId);

    void GrabNewSequence(TSchemeshardState* ss, const TPathId& pathId);
    void GrabSequence(TSchemeshardState* ss, const TPathId& pathId);

    void GrabNewCdcStream(TSchemeshardState* ss, const TPathId& pathId);
    void GrabCdcStream(TSchemeshardState* ss, const TPathId& pathId);

    void GrabNewTableSnapshot(TSchemeshardState* ss, const TPathId& pathId, TTxId snapshotTxId);

    void GrabNewLongLock(TSchemeshardState* ss, const TPathId& pathId);
    void GrabLongLock(TSchemeshardState* ss, const TPathId& pathId, TTxId lockTxId);

    void GrabExternalTable(TSchemeshardState* ss, const TPathId& pathId);

    void GrabExternalDataSource(TSchemeshardState* ss, const TPathId& pathId);

    void GrabNewView(TSchemeshardState* ss, const TPathId& pathId);
    void GrabView(TSchemeshardState* ss, const TPathId& pathId);

    void GrabResourcePool(TSchemeshardState* ss, const TPathId& pathId);

    void GrabBackupCollection(TSchemeshardState* ss, const TPathId& pathId);

    void UnDo(TSchemeshardState* ss);
};

}
