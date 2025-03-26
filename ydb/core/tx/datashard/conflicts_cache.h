#pragma once
#include "datashard_active_transaction.h"

#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <variant>
#include <vector>

namespace NKikimr::NDataShard {

class TDataShard;

/**
 * Caches conflicts for distributed operations
 *
 * Basically holds a hash table which maps known operation write keys to a set
 * of uncommitted writes at that key, as well as pending operations that may
 * overwrite that key. When new uncommitted writes are performed they lookup
 * pending operations on their key and mark that their uncommitted write
 * conflicts with those operations. This means later distributed operations
 * don't need to search for conflicts on disk and may either gather a set of
 * conflicts from memory, or trust that their conflicts are already accounted
 * for.
 */
class TTableConflictsCache {
    struct TWriteKey {
        TOwnedCellVec Key;
        absl::flat_hash_set<ui64> UncommittedWrites;
        size_t DistributedWrites = 0;
    };

    struct TUncommittedWrite {
        absl::flat_hash_set<TWriteKey*> WriteKeys;
    };

    struct TDistributedWrite {
        absl::flat_hash_set<TWriteKey*> WriteKeys;
    };

    using TWriteKeys = absl::flat_hash_map<TOwnedCellVec, std::unique_ptr<TWriteKey>,
        NKikimr::TCellVectorsHash, NKikimr::TCellVectorsEquals>;
    using TUncommittedWrites = absl::flat_hash_map<ui64, std::unique_ptr<TUncommittedWrite>>;
    using TDistributedWrites = absl::flat_hash_map<ui64, std::unique_ptr<TDistributedWrite>>;

    class TTxObserver;

public:
    explicit TTableConflictsCache(ui64 localTid);

    void AddUncommittedWrite(TConstArrayRef<TCell> key, ui64 txId, NTable::TDatabase& db);
    void RemoveUncommittedWrites(TConstArrayRef<TCell> key, NTable::TDatabase& db);
    void RemoveUncommittedWrites(ui64 txId, NTable::TDatabase& db);

    bool RegisterDistributedWrite(ui64 txId, const TOwnedCellVec& key, NTable::TDatabase& db);
    void UnregisterDistributedWrites(ui64 txId);

    const absl::flat_hash_set<ui64>* FindUncommittedWrites(TConstArrayRef<TCell> key);

private:
    void DropWriteKey(TWriteKey* k);

private:
    struct TRollbackOpAddUncommittedWrite {
        ui64 TxId;
        TWriteKey* WriteKey;
    };

    struct TRollbackOpRemoveUncommittedWrite {
        ui64 TxId;
        TWriteKey* WriteKey;
    };

    struct TRollbackOpRestoreUncommittedWrite {
        ui64 TxId;
        std::unique_ptr<TUncommittedWrite> Data;
    };

    using TRollbackOp = std::variant<
        TRollbackOpAddUncommittedWrite,
        TRollbackOpRemoveUncommittedWrite,
        TRollbackOpRestoreUncommittedWrite>;

    void AddRollbackOp(TRollbackOp&& op, NTable::TDatabase& db);
    void OnRollbackChanges();
    void OnCommitChanges();

private:
    const ui32 LocalTid;
    TWriteKeys WriteKeys;
    TUncommittedWrites UncommittedWrites;
    TDistributedWrites DistributedWrites;
    NTable::ITransactionObserverPtr TxObserver;
    std::vector<TRollbackOp> RollbackOps;
    bool RollbackAllowed = true;
};

class TConflictsCache {
public:
    struct TPendingWrite {
        ui32 LocalTid;
        TOwnedCellVec WriteKey;

        TPendingWrite(ui32 localTid, TOwnedCellVec writeKey)
            : LocalTid(localTid)
            , WriteKey(std::move(writeKey))
        { }
    };

    using TPendingWrites = std::vector<TPendingWrite>;

    class TTxFindWriteConflicts;

public:
    TConflictsCache(TDataShard* self)
        : Self(self)
    { }

    TTableConflictsCache& GetTableCache(ui32 localTid) {
        auto it = Tables.find(localTid);
        if (it != Tables.end()) {
            return it->second;
        }
        auto res = Tables.emplace(
            std::piecewise_construct,
            std::forward_as_tuple(localTid),
            std::forward_as_tuple(localTid));
        Y_ENSURE(res.second);
        return res.first->second;
    }

    void DropTableCaches(ui32 localTid) {
        Tables.erase(localTid);
    }

    void RegisterDistributedWrites(ui64 txId, TPendingWrites&& writes, NTable::TDatabase& db);
    void UnregisterDistributedWrites(ui64 txId);

private:
    TDataShard* const Self;
    THashMap<ui32, TTableConflictsCache> Tables;
    THashMap<ui64, TPendingWrites> PendingWrites; // TxId -> Writes
};

} // namespace NKikimr::NDataShard
