#pragma once
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tablet_flat/flat_row_eggs.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/async/cancellation.h>
#include <ydb/library/actors/async/event.h>
#include <util/generic/intrlist.h>
#include <util/digest/multi.h>

namespace NKikimr::NTabletFlatExecutor {

    class TTransactionContext;

} // namespace NKikimr::NTabletFlatExecutor

namespace NKikimr::NIceDb {

    class TNiceDb;

} // namespace NKikimr::NIceDb

namespace NKikimr::NDataShard {

    class TDataShard;

    struct TLockRowsRequestId {
        TActorId Sender;
        ui64 RequestId = 0;

        TLockRowsRequestId() = default;

        TLockRowsRequestId(const TActorId& sender, ui64 requestId)
            : Sender(sender)
            , RequestId(requestId)
        {}

        size_t Hash() const {
            return MultiHash(Sender, RequestId);
        }

        bool operator<=>(const TLockRowsRequestId& rhs) const = default;
    };

    struct TLockRowsRequestPipeServerListTag {};

    struct TLockRowsRequestState
        : public TIntrusiveListItem<TLockRowsRequestState, TLockRowsRequestPipeServerListTag>
    {
        TLockRowsRequestId RequestId;
        std::unique_ptr<NEvents::TDataEvents::TEvLockRowsResult> Result;
        NActors::TAsyncCancellationScope Scope;
        NActors::TAsyncEvent Finished;
        bool Waiting = false;

        TLockRowsRequestState(const TLockRowsRequestId& requestId)
            : RequestId(requestId)
        {}
    };

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
        void Start(const TActorContext& ctx);

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

} // namespace NKikimr::NDataShard

template<>
struct std::hash<::NKikimr::NDataShard::TLockRowsRequestId> {
    size_t operator()(const ::NKikimr::NDataShard::TLockRowsRequestId& value) const {
        return value.Hash();
    }
};

template<>
struct THash<NKikimr::NDataShard::TLockRowsRequestId> {
    size_t operator()(const NKikimr::NDataShard::TLockRowsRequestId& value) const {
        return value.Hash();
    }
};

