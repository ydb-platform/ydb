#pragma once
#include <ydb/core/tablet_flat/flat_table_committed.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_map.h>
#include <library/cpp/containers/absl_flat_hash/flat_hash_set.h>
#include <util/generic/hash.h>

namespace NKikimr::NTabletFlatExecutor {

    class TTransactionContext;

} // namespace NKikimr::NTabletFlatExecutor

namespace NKikimr::NIceDb {

    class TNiceDb;

} // namespace NKikimr::NIceDb

namespace NKikimr::NDataShard {

    class TDataShard;

    enum class EVolatileTxState : ui32 {
        // Volatile transaction is waiting for decisions from other participants
        Waiting = 0,
        // Volatile transaction is logically committed, but not yet committed in storage
        Committed = 1,
        // Volatile transaction is aborted, waiting to be garbage collected
        Aborted = 2,
    };

    struct TVolatileTxInfo {
        ui64 TxId;
        EVolatileTxState State = EVolatileTxState::Waiting;
        TRowVersion Version;
        absl::flat_hash_set<ui64> CommitTxIds;
        absl::flat_hash_set<ui64> Dependencies;
        absl::flat_hash_set<ui64> Dependents;
        absl::flat_hash_set<ui64> Participants;
        bool AddCommitted = false;
    };

    class TVolatileTxManager {
        using TTransactionContext = NKikimr::NTabletFlatExecutor::TTransactionContext;

    private:
        /**
         * A custom implementation of tx map without any copy-on-write support,
         * instead the map is changed directly whenever the set of transaction
         * changes, possibly affecting existing iterators. Since this tx map
         * is never used for scans it should be safe to do.
         */
        class TTxMap final : public NTable::ITransactionMap {
        public:
            void Add(ui64 txId, TRowVersion version);
            void Remove(ui64 txId);

            bool Empty() const {
                return Map.empty();
            }

            const TRowVersion* Find(ui64 txId) const override;

        private:
            absl::flat_hash_map<ui64, TRowVersion> Map;
        };

        /**
         * A helper access class that automatically converts to the optimal
         * executor argument type.
         */
        class TTxMapAccess {
        public:
            TTxMapAccess(const TIntrusivePtr<TTxMap>& txMap)
                : TxMap(txMap)
            { }

            explicit operator bool() const {
                return TxMap && !TxMap->Empty();
            }

            operator NTable::ITransactionMapPtr() const {
                if (TxMap && !TxMap->Empty()) {
                    return TxMap;
                } else {
                    return nullptr;
                }
            }

            operator NTable::ITransactionMapSimplePtr() const {
                if (TxMap && !TxMap->Empty()) {
                    return static_cast<NTable::ITransactionMap*>(TxMap.Get());
                } else {
                    return nullptr;
                }
            }

        private:
            const TIntrusivePtr<TTxMap>& TxMap;
        };

    public:
        TVolatileTxManager(TDataShard* self)
            : Self(self)
        { }

        void Clear();
        bool Load(NIceDb::TNiceDb& db);

        TVolatileTxInfo* FindByTxId(ui64 txId) const;
        TVolatileTxInfo* FindByCommitTxId(ui64 txId) const;

        void PersistAddVolatileTx(
            ui64 txId, const TRowVersion& version,
            TConstArrayRef<ui64> commitTxIds,
            TConstArrayRef<ui64> dependencies,
            TConstArrayRef<ui64> participants,
            TTransactionContext& txc);

        TTxMapAccess GetTxMap() const {
            return TTxMapAccess(TxMap);
        }

    private:
        bool LoadTxDetails(NIceDb::TNiceDb& db);
        bool LoadTxParticipants(NIceDb::TNiceDb& db);

    private:
        TDataShard* const Self;
        absl::flat_hash_map<ui64, std::unique_ptr<TVolatileTxInfo>> VolatileTxs; // TxId -> Info
        absl::flat_hash_map<ui64, TVolatileTxInfo*> VolatileTxByCommitTxId; // CommitTxId -> Info
        TIntrusivePtr<TTxMap> TxMap;
    };

} // namespace NKikimr::NDataShard
