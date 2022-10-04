#pragma once

#include "datashard.h"

namespace NKikimr::NTabletFlatExecutor {
    class TTransactionContext;
}

namespace NKikimr::NDataShard {

    class IReplicationSourceOffsetsDb {
    public:
        virtual void PersistSource(const TPathId& pathId, ui64 sourceId, const TString& sourceName) = 0;
        virtual void PersistSplitKey(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId, const TString& splitKey, i64 offset) = 0;
        virtual void PersistSplitKeyOffset(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId, i64 offset) = 0;
        virtual void PersistSplitKeyDelete(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId) = 0;
    };

    class TReplicationSourceOffsetsDb : public IReplicationSourceOffsetsDb {
    public:
        TReplicationSourceOffsetsDb(NTabletFlatExecutor::TTransactionContext& txc)
            : Txc(txc)
        { }

        void PersistSource(const TPathId& pathId, ui64 sourceId, const TString& sourceName) override;
        void PersistSplitKey(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId, const TString& splitKey, i64 offset) override;
        void PersistSplitKeyOffset(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId, i64 offset) override;
        void PersistSplitKeyDelete(const TPathId& pathId, ui64 sourceId, ui64 splitKeyId) override;

    private:
        NTabletFlatExecutor::TTransactionContext& Txc;
    };

    class TReplicationSourceState {
    public:
        struct TOffsetState {
            ui64 SplitKeyId = 0;
            TSerializedCellVec SplitKey;
            i64 MaxOffset = -1;
        };

        struct TLessByKeyPrefix {
            // Points to vector in TReplicatedTableState
            const TVector<NScheme::TTypeInfo>* KeyColumnTypes;

            using is_transparent = void;

            bool operator()(TConstArrayRef<TCell> a, TConstArrayRef<TCell> b) const;

            bool operator()(const TOffsetState* a, const TOffsetState* b) const {
                return (*this)(a->SplitKey.GetCells(), b->SplitKey.GetCells());
            }

            bool operator()(const TOffsetState* a, TConstArrayRef<TCell> b) const {
                return (*this)(a->SplitKey.GetCells(), b);
            }

            bool operator()(TConstArrayRef<TCell> a, const TOffsetState* b) const {
                return (*this)(a, b->SplitKey.GetCells());
            }
        };

        const TPathId PathId;
        const ui64 Id;
        const TString Name;
        TMap<ui64, TOffsetState> OffsetBySplitKeyId;
        TSet<TOffsetState*, TLessByKeyPrefix> Offsets;
        ui64 NextSplitKeyId = 1;
        ui64 StatBytes = 0;

        TReplicationSourceState(
                const TPathId& pathId,
                const ui64 id,
                const TString& name,
                const TVector<NScheme::TTypeInfo>* keyColumnTypes);

        ui64 CalcStatBytes(const TOffsetState& state) const;
        void AddStatBytes(const TOffsetState& state);
        void RemoveStatBytes(const TOffsetState& state);

        TOffsetState* FindSplitKey(TConstArrayRef<TCell> key);

        void LoadSplitKey(ui64 splitKeyId, TSerializedCellVec splitKey, i64 maxOffset);

        void PersistCreated(IReplicationSourceOffsetsDb& db) const;
        void PersistSplitKeyCreated(IReplicationSourceOffsetsDb& db, const TOffsetState& state) const;
        void PersistSplitKeyOffset(IReplicationSourceOffsetsDb& db, const TOffsetState& state) const;

        TOffsetState* AddSplitKey(IReplicationSourceOffsetsDb& db, TSerializedCellVec key);
        TOffsetState* AddSplitKey(IReplicationSourceOffsetsDb& db, TSerializedCellVec key, i64 offset);
        bool AdvanceMaxOffset(IReplicationSourceOffsetsDb& db, TConstArrayRef<TCell> key, i64 offset);

        bool EnsureSplitKey(IReplicationSourceOffsetsDb& db, const TSerializedCellVec& key);
        bool EnsureSplitKey(IReplicationSourceOffsetsDb& db, const TSerializedCellVec& key, i64 offset);

        void OptimizeSplitKeys(IReplicationSourceOffsetsDb& db);
    };

    class TReplicatedTableState {
    public:
        const TPathId PathId;
        TVector<NScheme::TTypeInfo> KeyColumnTypes;
        TMap<ui64, TReplicationSourceState> SourceById;
        THashMap<TString, TReplicationSourceState*> Sources;
        ui64 NextSourceId = 1;

        TReplicatedTableState(
                const TPathId& pathId,
                const TVector<NScheme::TTypeInfo>& keyColumnTypes);

        TReplicationSourceState* FindSource(ui64 sourceId);
        TReplicationSourceState& LoadSource(ui64 sourceId, const TString& sourceName);

        TReplicationSourceState& EnsureSource(IReplicationSourceOffsetsDb& db, const TString& sourceName);

        void OptimizeSplitKeys(IReplicationSourceOffsetsDb& db);
    };


} // namespace NKikimr::NDataShard
