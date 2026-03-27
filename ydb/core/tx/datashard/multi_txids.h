#pragma once
#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <util/generic/intrlist.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <optional>

namespace NKikimr::NTabletFlatExecutor {

    class TTransactionContext;

} // namespace NKikimr::NTabletFlatExecutor

namespace NKikimr::NIceDb {

    class TNiceDb;

} // namespace NKikimr::NIceDb

namespace NKikimr::NDataShard {

    class TDataShard;

    enum class EMultiTxIdFlag : ui64 {
        Broken = 1,
    };

    class TMultiTxIdManager;

    struct TMultiTxIdCleanupQueueTag {};
    struct TMultiTxIdReferenceTag {};

    /**
     * This class is used to represent MultiTxId in the graph, with multiple
     * locks from different transactions or lock modes. Its multi identifier is
     * used in place of a contained set of transactions. MultiTxId forms a tree,
     * which is a union of other MultiTxIds or transaction locks. In the common
     * case there are at most two edges. Since there cannot be more than two
     * non-conflicting lock modes, it will either have two edges with the same
     * lock mode (one of which may be a previously seen MultiTxId), or two
     * edges with different lock modes.
     *
     * In complex cases where the same transaction acquires different lock modes
     * at different savepoints, it may have more than two edges, one per
     * corresponding lock mode.
     */
    struct TMultiTxId
        : public TIntrusiveListItem<TMultiTxId, TMultiTxIdCleanupQueueTag>
    {
        struct TEdge
            : public TIntrusiveListItem<TEdge>
            , public TIntrusiveListItem<TEdge, TMultiTxIdReferenceTag>
        {
            ui64 EdgeId;
            ui64 MultiTxId;
            ui64 NestedTxId;
            NTable::ELockMode LockMode;

            TEdge(ui64 edgeId, ui64 multiTxId, ui64 nestedTxId, NTable::ELockMode lockMode)
                : EdgeId(edgeId)
                , MultiTxId(multiTxId)
                , NestedTxId(nestedTxId)
                , LockMode(lockMode)
            {}

            const TEdge* PrevEdge() const {
                const TIntrusiveListItem<TEdge>* self = this;
                return self->Prev()->Node();
            }

            const TEdge* NextEdge() const {
                const TIntrusiveListItem<TEdge>* self = this;
                return self->Next()->Node();
            }

            TIntrusiveListItem<TEdge>* ToEdgesItem() {
                return this;
            }

            TIntrusiveListItem<TEdge, TMultiTxIdReferenceTag>* ToReferencesItem() {
                return this;
            }
        };

        const ui64 MultiTxId;
        ui64 LockedRowsCount = 0;
        ui64 Flags = 0;
        TIntrusiveListWithAutoDelete<TEdge, TDelete> Edges;
        TIntrusiveList<TEdge, TMultiTxIdReferenceTag> References;
        size_t ReferencesCount = 0;

        explicit TMultiTxId(ui64 multiTxId)
            : MultiTxId(multiTxId)
        {}

        void SetFlag(EMultiTxIdFlag flag) {
            Flags |= ui64(flag);
        }

        void ResetFlag(EMultiTxIdFlag flag) {
            Flags &= ~ui64(flag);
        }

        bool HasFlag(EMultiTxIdFlag flag) const {
            return (Flags & ui64(flag)) == ui64(flag);
        }

        std::optional<NTable::ELockMode> GetLockMode() const {
            auto it = Edges.begin();
            auto end = Edges.end();
            if (it == end) [[unlikely]] {
                return std::nullopt;
            }
            NTable::ELockMode lockMode = it->LockMode;
            while (++it != end) {
                if (it->LockMode != lockMode) {
                    // Multiple lock modes
                    return std::nullopt;
                }
            }
            return lockMode;
        }

        TIntrusiveListItem<TMultiTxId, TMultiTxIdCleanupQueueTag>* ToCleanupQueueItem() {
            return this;
        }
    };

    /**
     * This class is used to represent leaf nodes (transactions) referenced
     * by MultiTxIds. This decouples MultiTxIds from the lock table, and allows
     * tracking and cleaning up unnecessary transactions and MultiTxId nodes
     * when transactions are committed or rolled back. Additionally we use these
     * shadow nodes to cache pathways for reusing MultiTxIds with the same
     * construction path.
     */
    struct TMultiTxIdLockShadow
        : public TIntrusiveListItem<TMultiTxIdLockShadow, TMultiTxIdCleanupQueueTag>
    {
        const ui64 LockId;
        TIntrusiveList<TMultiTxId::TEdge, TMultiTxIdReferenceTag> References;
        size_t ReferencesCount = 0;
        bool Broken = false;

        explicit TMultiTxIdLockShadow(ui64 lockId)
            : LockId(lockId)
        {}
    };

    /**
     * Allows enumerating (LockId, LockMode) pairs contained in the given
     * MultiTxId, in the reverse order (from newer to older). When filter is
     * not ELockMode::None, attempts to efficiently enumerate only locks which
     * conflict the given LockMode.
     */
    class TMultiTxIdEnumerator {
    public:
        TMultiTxIdEnumerator(TMultiTxIdManager& self);
        TMultiTxIdEnumerator(TMultiTxIdManager& self, const TMultiTxId* entry, NTable::ELockMode filter = NTable::ELockMode::None);

        struct TResult {
            ui64 LockId = 0;
            NTable::ELockMode LockMode = NTable::ELockMode::None;

            explicit operator bool() const {
                return LockId != 0;
            }
        };

        /**
         * Restarts enumeration with the given MultiTxId and filter.
         */
        void Reset(const TMultiTxId* entry, NTable::ELockMode filter = NTable::ELockMode::None);

        /**
         * Returns the next (LockId, LockMode) pair contained in the enumerated MultiTxId.
         */
        TResult Next();

    private:
        void Add(const TMultiTxId* entry, const TMultiTxId::TEdge* last = nullptr);

    private:
        struct TNextEdge {
            const TMultiTxId* Entry;
            const TMultiTxId::TEdge* Edge;
        };
        TMultiTxIdManager& Self;
        TStackVec<TNextEdge> Stack;
        NTable::ELockMode Filter;
    };

    class TMultiTxIdManager {
    public:
        TMultiTxIdManager(TDataShard& self);
        ~TMultiTxIdManager();

        void Clear();
        bool Load(NIceDb::TNiceDb& db);
        void Start();

        TMultiTxId* FindMultiTxId(ui64 multiTxId) { return MultiTxIds.FindPtr(multiTxId); }
        const TMultiTxId* FindMultiTxId(ui64 multiTxId) const { return MultiTxIds.FindPtr(multiTxId); }

        void DecrementLockedRowsCount(NIceDb::TNiceDb& db, ui64 multiTxId);
        ui64 CombineRowLocks(NIceDb::TNiceDb& db, ui64 currentTxId, NTable::ELockMode currentLockMode, ui64 lockTxId, NTable::ELockMode lockMode, ui64& globalTxId);

        TMultiTxIdEnumerator EnumerateLocks(const TMultiTxId* entry, NTable::ELockMode filter = NTable::ELockMode::None) {
            return TMultiTxIdEnumerator(*this, entry, filter);
        }

        void BreakMultiTxId(const TMultiTxId* entry);
        void AddWriteConflict(const TMultiTxId* entry);

        void OnLockBroken(NIceDb::TNiceDb& db, ui64 lockId);
        void OnLockRemoved(NIceDb::TNiceDb& db, ui64 lockId);
        void RunCleanup(NIceDb::TNiceDb& db);

    private:
        bool LoadMultiTxIds(NIceDb::TNiceDb& db);
        bool LoadMultiTxIdGraph(NIceDb::TNiceDb& db);

    private:
        void InitializeReference(TMultiTxId::TEdge& edge);
        TMultiTxIdLockShadow& GetLockShadow(ui64 lockId);
        void BreakShadow(NIceDb::TNiceDb& db, TMultiTxIdLockShadow* shadow);
        void EnqueueCleanupTx();
        bool NeedCleanup(const TMultiTxId* entry) const;
        void MaybeEnqueueCleanup(TMultiTxId* entry);
        void RunLockShadowCleanup(NIceDb::TNiceDb& db, TMultiTxIdLockShadow* shadow);
        void RunMultiTxIdCleanup(NIceDb::TNiceDb& db, TMultiTxId* entry);

    private:
        void DeleteEdge(NIceDb::TNiceDb& db, TMultiTxId::TEdge* edge);
        void DeleteMultiTxId(NIceDb::TNiceDb& db, TMultiTxId* entry);

    private:
        TDataShard& Self;
        ui64 NextEdgeId = 1;
        THashMap<ui64, TMultiTxId> MultiTxIds;
        THashMap<ui64, TMultiTxIdLockShadow> LockShadows;
        TIntrusiveList<TMultiTxId, TMultiTxIdCleanupQueueTag> MultiTxIdCleanupQueue;
        TIntrusiveList<TMultiTxIdLockShadow, TMultiTxIdCleanupQueueTag> LockShadowCleanupQueue;
        bool CleanupTxInFlight = false;
    };

    /**
     * Returns the strongest combined lock mode when the same transaction tries to
     * lock the same row in multiple lock modes.
     */
    inline NTable::ELockMode CombinedRowLockMode(NTable::ELockMode currentMode, NTable::ELockMode lockMode) {
        // Lock modes are ordered: KeyShared, Shared, NoKeyExclusive, Exclusive
        // We can use the maximum even when lock modes don't overlap. For example,
        // rows may be locked in KeyShared mode and the same transaction tries to
        // lock these rows in NoKeyExclusive mode. There are effectively two
        // disjoint locks, but we will choose one (NoKeyExclusive). This is fine as
        // long as these locks are for the same LockId. Since individual locks
        // cannot be removed without committing or rolling back the corresponding
        // LockId, it will continue to block new Shared and Exclusive locks, but
        // will allow other transactions to lock in KeyShared mode, which doesn't
        // change whether KeyShared lock by the current LockId exists or not.
        return Max(currentMode, lockMode);
    }

    inline bool IsCompatibleRowLockMode(NTable::ELockMode currentMode, NTable::ELockMode lockMode) {
        // Note: for simplicity Multi lock mode conflicts with all lock modes
        switch (currentMode) {
            case NTable::ELockMode::None:
                return lockMode < NTable::ELockMode::Multi;
            case NTable::ELockMode::KeyShared:
                return lockMode < NTable::ELockMode::Exclusive;
            case NTable::ELockMode::Shared:
                return lockMode < NTable::ELockMode::NoKeyExclusive;
            case NTable::ELockMode::NoKeyExclusive:
                return lockMode < NTable::ELockMode::Shared;
            default:
                return false;
        }
    }

} // namespace NKikimr::NDataShard
