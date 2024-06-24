#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_path_element.h"
#include "schemeshard_info_types.h"

#include <util/generic/ptr.h>
#include <util/generic/stack.h>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TMemoryChanges: public TSimpleRefCount<TMemoryChanges> {
    using TPathState = std::pair<TPathId, TPathElement::TPtr>;
    TStack<TPathState> Paths;

    using TIndexState = std::pair<TPathId, TTableIndexInfo::TPtr>;
    TStack<TIndexState> Indexes;

    using TCdcStreamState = std::pair<TPathId, TCdcStreamInfo::TPtr>;
    TStack<TCdcStreamState> CdcStreams;

    using TTableSnapshotState = std::pair<TPathId, TTxId>;
    TStack<TTableSnapshotState> TablesWithSnapshots;

    using TLockState = std::pair<TPathId, TTxId>;
    TStack<TLockState> LockedPaths;

    using TTableState = std::pair<TPathId, TTableInfo::TPtr>;
    TStack<TTableState> Tables;

    using TShardState = std::pair<TShardIdx, THolder<TShardInfo>>;
    TStack<TShardState> Shards;

    // Actually, any single subdomain should not be grabbed at more than one version
    // per transaction/operation.
    // And transaction/operation could not work on more than one subdomain.
    // But just to be on the safe side (migrated paths, anyone?) we allow several
    // subdomains to be grabbed.
    THashMap<TPathId, TSubDomainInfo::TPtr> SubDomains;

    using TTxState = std::pair<TOperationId, THolder<TTxState>>;
    TStack<TTxState> TxStates;

    using TExternalTableState = std::pair<TPathId, TExternalTableInfo::TPtr>;
    TStack<TExternalTableState> ExternalTables;

    using TExternalDataSourceState = std::pair<TPathId, TExternalDataSourceInfo::TPtr>;
    TStack<TExternalDataSourceState> ExternalDataSources;

    using TViewState = std::pair<TPathId, TViewInfo::TPtr>;
    TStack<TViewState> Views;

    using TResourcePoolState = std::pair<TPathId, TResourcePoolInfo::TPtr>;
    TStack<TResourcePoolState> ResourcePools;

public:
    ~TMemoryChanges() = default;

    void GrabNewTxState(TSchemeShard* ss, const TOperationId& op);

    void GrabNewPath(TSchemeShard* ss, const TPathId& pathId);
    void GrabPath(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewTable(TSchemeShard* ss, const TPathId& pathId);
    void GrabTable(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewShard(TSchemeShard* ss, const TShardIdx& shardId);
    void GrabShard(TSchemeShard* ss, const TShardIdx& shardId);

    void GrabDomain(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewIndex(TSchemeShard* ss, const TPathId& pathId);
    void GrabIndex(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId);
    void GrabCdcStream(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId);

    void GrabNewLongLock(TSchemeShard* ss, const TPathId& pathId);
    void GrabLongLock(TSchemeShard* ss, const TPathId& pathId, TTxId lockTxId);

    void GrabExternalTable(TSchemeShard* ss, const TPathId& pathId);

    void GrabExternalDataSource(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewView(TSchemeShard* ss, const TPathId& pathId);
    void GrabView(TSchemeShard* ss, const TPathId& pathId);

    void GrabResourcePool(TSchemeShard* ss, const TPathId& pathId);

    void UnDo(TSchemeShard* ss);
};

}
