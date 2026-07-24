#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_info_types.h"
#include "schemeshard_path_element.h"

#include <ydb/core/tx/schemeshard/olap/table/table.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/ptr.h>
#include <util/generic/stack.h>

#include <functional>

#include <optional>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TMemoryChanges: public TSimpleRefCount<TMemoryChanges> {
    using TPathState = std::pair<TPathId, TPathElement::TPtr>;
    // Holds both GrabPath snapshots (non-null elem) and GrabNewPath markers (null elem).
    // Subclassed to expose the underlying container for the debug-only scan below.
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


    TStack<std::function<void()>> DbRefUndos;

    // Dedup: at most one value snapshot per (self-ref map, path) per tx, so
    // repeated Update() on the same object doesn't re-copy it.
    THashMap<const void*, THashSet<TPathId>> UpdateSnapshotted;

    // Only the propose tx can roll back (UnDo runs only from AbortOperationPropose),
    // so only it records undos; other txs would just accumulate dead weight.
    bool Armed = false;

public:
    ~TMemoryChanges() = default;

    // Called from IgniteOperation.
    void Arm() { Armed = true; }

    // True only inside an armed propose; tracked Set/Update are legal only then.
    bool IsArmed() const { return Armed; }

    // Debug: was this path grabbed (GrabPath/GrabNewPath) in this tx? A tracked Set()
    // that acquires a ref on an ungrabbed path can't fully roll back.
    bool IsPathTracked(const TPathId& id) const { return Paths.Contains(id); }

    // True the first time this (map, path) needs a snapshot; false when disarmed.
    bool NeedsUpdateSnapshot(const void* map, const TPathId& id) {
        return Armed && UpdateSnapshotted[map].insert(id).second;
    }

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


    // TDbRefMap::Set records its own rollback closure here (undone LIFO),
    // replacing the per-map GrabNew*/Grab* + UnDo branches.
    void RecordDbRefUndo(std::function<void()> undo) {
        if (Armed) {
            DbRefUndos.push(std::move(undo));
        }
    }

    void UnDo(TSchemeShard* ss);
};

}
