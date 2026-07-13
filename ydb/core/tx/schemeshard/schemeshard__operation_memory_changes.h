#pragma once

#include "schemeshard_identificators.h"
#include "schemeshard_info_types.h"
#include "schemeshard_path_element.h"
#include "schemeshard_self_ref_map.h"

#include <ydb/core/tx/schemeshard/olap/table/table.h>

#include <util/generic/ptr.h>
#include <util/generic/stack.h>

#include <functional>

#include <optional>

namespace NKikimr::NSchemeShard {

class TSchemeShard;

class TMemoryChanges: public TSimpleRefCount<TMemoryChanges> {
    using TPathState = std::pair<TPathId, TPathElement::TPtr>;
    TStack<TPathState> Paths;



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


    TStack<std::function<void()>> SelfRefUndos;

public:
    ~TMemoryChanges() = default;

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


    // Self-ref maps record their own rollback here at mutation time (see
    // TSelfRefMap::Set), replacing the per-map GrabNew*/Grab* + UnDo branches:
    // one closure per forward mutation, undone LIFO. A forward Set cannot be
    // applied without recording its undo.
    template <class V>
    void RecordSelfRef(TSelfRefMap<V>& map, const TPathId& id, V old) {
        SelfRefUndos.push([&map, id, old = std::move(old)]() {
            if (old) {
                map.UndoRestore(id, old);
            } else {
                map.UndoErase(id);
            }
        });
    }

    void UnDo(TSchemeShard* ss);
};

}
