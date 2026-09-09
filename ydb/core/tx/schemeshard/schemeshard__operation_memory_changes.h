#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_info_types.h"
#include "schemeshard_path_element.h"

#include <ydb/core/tx/schemeshard/olap/table/table.h>

#include <util/generic/ptr.h>
#include <util/generic/stack.h>

#include <memory>
#include <optional>
#include <tuple>

namespace NKikimr::NSchemeShard {

class TSchemeShard;
struct TOlapStoreInfo;

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

    // Preserve the original table object as well as its independent snapshot.
    using TTableState = std::tuple<TPathId, TTableInfo::TPtr, TTableInfo::TPtr>;
    TStack<TTableState> Tables;

    using TColumnTableState = std::pair<TPathId, TColumnTableInfo::TPtr>;
    TStack<TColumnTableState> ColumnTables;

    using TSequenceState = std::pair<TPathId, TSequenceInfo::TPtr>;
    TStack<TSequenceState> Sequences;

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

    using TBackupCollectionState = std::pair<TPathId, TBackupCollectionInfo::TPtr>;
    TStack<TBackupCollectionState> BackupCollections;

    using TSysViewState = std::pair<TPathId, TSysViewInfo::TPtr>;
    TStack<TSysViewState> SysViews;

    using TLongIncrementalRestoreOpState = std::pair<TOperationId, std::optional<NKikimrSchemeOp::TLongIncrementalRestoreOp>>;
    TStack<TLongIncrementalRestoreOpState> LongIncrementalRestoreOps;

    using TIncrementalBackupState = std::pair<ui64, TIncrementalBackupInfo::TPtr>;
    TStack<TIncrementalBackupState> IncrementalBackups;

    // Mirrors IncrementalBackups: UnDo erases the id from Self->FullBackups.
    using TFullBackupState = std::pair<ui64, TFullBackupInfo::TPtr>;
    TStack<TFullBackupState> FullBackups;

    // UnDo erases the (bcPathId -> id) entry, keeping BCPathToFullBackup atomic with FullBackups.
    using TBCPathToFullBackupState = std::pair<TPathId, std::optional<ui64>>;
    TStack<TBCPathToFullBackupState> BCPathToFullBackup;

    using TSecretState = std::pair<TPathId, TSecretInfo::TPtr>;
    TStack<TSecretState> Secrets;

    using TStreamingQueryState = std::pair<TPathId, TStreamingQueryInfo::TPtr>;
    TStack<TStreamingQueryState> StreamingQueries;

    using TSharedShardEntry = std::tuple<TShardIdx, TPathId, std::optional<TTxId>>;
    TStack<TSharedShardEntry> SharedShardEntries;

    using TTestShardSetState = std::pair<TPathId, TTestShardSetInfo::TPtr>;
    TStack<TTestShardSetState> TestShardSets;

    using TTopicState = std::pair<TPathId, TTopicInfo::TPtr>;
    TStack<TTopicState> Topics;

    using TBlockStoreVolumeState = std::pair<TPathId, TBlockStoreVolumeInfo::TPtr>;
    TStack<TBlockStoreVolumeState> BlockStoreVolumes;

    using TFileStoreInfoState = std::pair<TPathId, TFileStoreInfo::TPtr>;
    TStack<TFileStoreInfoState> FileStoreInfos;

    using TKesusInfoState = std::pair<TPathId, TKesusInfo::TPtr>;
    TStack<TKesusInfoState> KesusInfos;

    using TReplicationState = std::pair<TPathId, TReplicationInfo::TPtr>;
    TStack<TReplicationState> Replications;

    using TSolomonVolumeState = std::pair<TPathId, TSolomonVolumeInfo::TPtr>;
    TStack<TSolomonVolumeState> SolomonVolumes;

    using TBlobDepotState = std::pair<TPathId, TBlobDepotInfo::TPtr>;
    TStack<TBlobDepotState> BlobDepots;

    using TRtmrVolumeState = std::pair<TPathId, TRtmrVolumeInfo::TPtr>;
    TStack<TRtmrVolumeState> RtmrVolumes;

    using TOlapStoreState = std::pair<TPathId, std::shared_ptr<TOlapStoreInfo>>;
    TStack<TOlapStoreState> OlapStores;

public:
    ~TMemoryChanges() = default;

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

    void GrabNewTopic(TSchemeShard* ss, const TPathId& pathId);
    void GrabTopic(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewBlockStoreVolume(TSchemeShard* ss, const TPathId& pathId);
    void GrabBlockStoreVolume(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewFileStoreInfo(TSchemeShard* ss, const TPathId& pathId);
    void GrabFileStoreInfo(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewKesusInfo(TSchemeShard* ss, const TPathId& pathId);
    void GrabKesusInfo(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewReplication(TSchemeShard* ss, const TPathId& pathId);
    void GrabReplication(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewSolomonVolume(TSchemeShard* ss, const TPathId& pathId);
    void GrabSolomonVolume(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewBlobDepot(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewRtmrVolume(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewOlapStore(TSchemeShard* ss, const TPathId& pathId);

    void UnDo(TSchemeShard* ss);
};

}
