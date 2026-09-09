#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_info_types.h"
#include "schemeshard_path_element.h"

#include <ydb/core/tx/schemeshard/olap/table/table.h>

#include <util/generic/hash.h>
#include <util/generic/ptr.h>
#include <util/generic/stack.h>

#include <functional>
#include <optional>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TMemoryChanges: public TSimpleRefCount<TMemoryChanges> {
    using TPathState = std::pair<TPathId, TPathElement::TPtr>;
    // Holds both GrabPath snapshots (non-null elem) and GrabNewPath markers (null elem).
    // Subclassed to check the snapshot required by a new reference-owning entry.
    struct TPathStack : TStack<TPathState> {
        bool Contains(const TPathId& id) const {
            for (const auto& [pid, elem] : this->c) {
                if (pid == id) {
                    return true;
                }
            }
            return false;
        }
    };
    TPathStack Paths;

    using TTableSnapshotState = std::pair<TPathId, TTxId>;
    TStack<TTableSnapshotState> TablesWithSnapshots;

    using TLockState = std::pair<TPathId, TTxId>;
    TStack<TLockState> LockedPaths;

    using TColumnTableState = std::pair<TPathId, TColumnTableInfo::TPtr>;
    TStack<TColumnTableState> ColumnTables;

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

    using TSharedShardEntry = std::tuple<TShardIdx, TPathId, std::optional<TTxId>>;
    TStack<TSharedShardEntry> SharedShardEntries;

    // Common LIFO stack for typed snapshots, new-entry markers and field undo.
    TStack<std::function<void()>> UndoActions;

    // Only the propose tx can roll back (UnDo runs only from AbortOperationPropose),
    // so only it records undos; other txs would just accumulate dead weight.
    bool Armed = false;

public:
    // The proposal coordinator scopes registration; containers do not know it.
    void Arm(TSchemeShard* ss);
    void Disarm();

    // Snapshot/undo registration is legal only inside a proposal.
    bool IsArmed() const { return Armed; }

    // New membership requires a Paths snapshot to restore its reference count.
    bool IsPathTracked(const TPathId& id) const { return Paths.Contains(id); }

    void GrabNewTable(TSchemeShard* ss, const TPathId& pathId);
    void GrabTable(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewIndex(TSchemeShard* ss, const TPathId& pathId);
    void GrabIndex(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewSequence(TSchemeShard* ss, const TPathId& pathId);
    void GrabSequence(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewCdcStream(TSchemeShard* ss, const TPathId& pathId);
    void GrabCdcStream(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewReplication(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewBlobDepot(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewTopic(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewRtmrVolume(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewSolomonVolume(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewBlockStoreVolume(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewFileStoreInfo(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewKesusInfo(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewOlapStore(TSchemeShard* ss, const TPathId& pathId);
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
    void GrabNewSecret(TSchemeShard* ss, const TPathId& pathId);
    void GrabSecret(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewStreamingQuery(TSchemeShard* ss, const TPathId& pathId);
    void GrabStreamingQuery(TSchemeShard* ss, const TPathId& pathId);
    void GrabNewTestShardSet(TSchemeShard* ss, const TPathId& pathId);
    void GrabTestShardSet(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewTxState(TSchemeShard* ss, const TOperationId& op);

    void GrabNewPath(TSchemeShard* ss, const TPathId& pathId);
    void GrabPath(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewColumnTable(TSchemeShard* ss, const TPathId& pathId);
    void GrabColumnTable(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewShard(TSchemeShard* ss, const TShardIdx& shardId);
    void GrabShard(TSchemeShard* ss, const TShardIdx& shardId);

    void GrabDomain(TSchemeShard* ss, const TPathId& pathId);

    void GrabNewTableSnapshot(TSchemeShard* ss, const TPathId& pathId, TTxId snapshotTxId);

    void GrabNewLongLock(TSchemeShard* ss, const TPathId& pathId);
    void GrabLongLock(TSchemeShard* ss, const TPathId& pathId, TTxId lockTxId);

    void GrabNewLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId);
    void GrabLongIncrementalRestoreOp(TSchemeShard* ss, const TOperationId& opId);

    void GrabNewLongIncrementalBackupOp(TSchemeShard* ss, ui64 id);

    void GrabNewFullBackupOp(TSchemeShard* ss, ui64 id);
    void GrabNewBCPathToFullBackup(TSchemeShard* ss, const TPathId& bcPathId);

    void GrabNewSharedShard(TSchemeShard* ss, const TShardIdx& shardIdx, const TPathId& pathId);
    void GrabSharedShard(TSchemeShard* ss, const TShardIdx& shardIdx, const TPathId& pathId);

    // Record map membership changes and explicit field undo on one LIFO stack,
    // so mutations are undone before an earlier insertion/replacement is undone.
    void RecordUndo(std::function<void()> undo) {
        Y_ABORT_UNLESS(Armed, "undo registration outside proposal");
        UndoActions.push(std::move(undo));
    }

    void UnDo(TSchemeShard* ss);
};

}
