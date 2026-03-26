#pragma once
#include <ydb/core/tablet_flat/flat_row_eggs.h>
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

    struct TMultiTxIdReferenceTag {};

    struct TMultiTxId {
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
    };

    /**
     * Allows enumerating (LockId, LockMode) pairs contained in the given
     * MultiTxId, in the reverse order (from newer to older). When filter is
     * not ELockMode::None, attempts to efficiently enumerate only locks which
     * conflict the given LockMode.
     */
    class TMultiTxIdEnumerator {
    public:
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
        TVector<TNextEdge> Stack;
        NTable::ELockMode Filter;
    };

    class TMultiTxIdManager {
    public:
        TMultiTxIdManager(TDataShard& self);
        ~TMultiTxIdManager();

        void Clear();
        bool Load(NIceDb::TNiceDb& db);
        void Start();

        const TMultiTxId* FindMultiTxId(ui64 multiTxId) const {
            return MultiTxIds.FindPtr(multiTxId);
        }

        void DecrementLockedRowsCount(NIceDb::TNiceDb& db, ui64 multiTxId);
        ui64 CombineRowLocks(NIceDb::TNiceDb& db, ui64 currentTxId, NTable::ELockMode currentLockMode, ui64 lockTxId, NTable::ELockMode lockMode, ui64& globalTxId);

    private:
        bool LoadMultiTxIds(NIceDb::TNiceDb& db);
        bool LoadMultiTxIdGraph(NIceDb::TNiceDb& db);

    private:
        TDataShard& Self;
        THashMap<ui64, TMultiTxId> MultiTxIds;
        ui64 NextEdgeId = 1;
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
