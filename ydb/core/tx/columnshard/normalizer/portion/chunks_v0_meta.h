#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NColumnShard {
class TTablesManager;
}

namespace NKikimr::NOlap {

class TChunksV0MetaNormalizer: public TNormalizationController::INormalizerComponent {
private:
    using TBase = TNormalizationController::INormalizerComponent;

    class TPortionKey {
        YDB_READONLY(ui64, PathId, 0);
        YDB_READONLY(ui64, PortionId, 0);

    public:
        TPortionKey(const ui64 pathId, const ui64 portionId)
            : PathId(pathId)
            , PortionId(portionId) {
        }

        auto operator<=>(const TPortionKey& other) const {
            return std::tie(PathId, PortionId) <=> std::tie(other.PathId, other.PortionId);
        }
        operator size_t() const {
            return CombineHashes(PathId, PortionId);
        }
    };

    TConclusion<NKikimrTxColumnShard::TIndexPortionMeta> GetPortionMeta(const TPortionKey& key, NIceDb::TNiceDb& db) {
        if (auto findMeta = PortionMetaCache.FindPtr(key)) {
            return *findMeta;
        }

        auto rowset = db.Table<NColumnShard::Schema::IndexPortions>().Key(key.GetPathId(), key.GetPortionId()).Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }
        AFL_VERIFY(!rowset.EndOfSet());
        NKikimrTxColumnShard::TIndexColumnMeta metaProto;
        AFL_VERIFY(metaProto.ParseFromString(rowset.GetValue<NColumnShard::Schema::IndexPortions::Metadata>()));
        auto emplaced = PortionMetaCache.emplace(key, metaProto.GetPortionMeta());
        AFL_VERIFY(emplaced.second);
        return emplaced.first->second;
    }

public:
    static TString GetClassNameStatic() {
        return ::ToString(ENormalizerSequentialId::RestoreV0ChunksMeta);
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::RestoreV0ChunksMeta;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    class TNormalizerResult;

    class TColumnKey {
        YDB_READONLY(ui64, Index, 0);
        YDB_READONLY(ui64, Granule, 0);
        YDB_READONLY(ui64, ColumnId, 0);
        YDB_READONLY(ui64, PlanStep, 0);
        YDB_READONLY(ui64, TxId, 0);
        YDB_READONLY(ui64, Portion, 0);
        YDB_READONLY(ui64, Chunk, 0);

    public:
        template <class TRowset>
        void Load(TRowset& rowset) {
            using namespace NColumnShard;
            Index = rowset.template GetValue<Schema::IndexColumns::Index>();
            Granule = rowset.template GetValue<Schema::IndexColumns::Granule>();
            ColumnId = rowset.template GetValue<Schema::IndexColumns::ColumnIdx>();
            PlanStep = rowset.template GetValue<Schema::IndexColumns::PlanStep>();
            TxId = rowset.template GetValue<Schema::IndexColumns::TxId>();
            Portion = rowset.template GetValue<Schema::IndexColumns::Portion>();
            Chunk = rowset.template GetValue<Schema::IndexColumns::Chunk>();
        }

        bool operator<(const TColumnKey& other) const {
            return std::make_tuple(Portion, Chunk, ColumnId) < std::make_tuple(other.Portion, other.Chunk, other.ColumnId);
        }
    };

    class TUpdate {
    private:
        YDB_READONLY_DEF(NKikimrTxColumnShard::TIndexPortionMeta, PortionMeta);

    public:
        TUpdate() = default;
        TUpdate(NKikimrTxColumnShard::TIndexPortionMeta&& portionMeta)
            : PortionMeta(std::move(portionMeta)) {
        }
    };

    class TChunkInfo {
        YDB_READONLY_DEF(TColumnKey, Key);
        YDB_READONLY_DEF(ui64, PathId);
        TColumnChunkLoadContext CLContext;
        ISnapshotSchema::TPtr Schema;

        YDB_ACCESSOR_DEF(TUpdate, Update);

        void InitSchema(const NColumnShard::TTablesManager& tm);

    public:
        template <class TSource>
        TChunkInfo(TColumnKey&& key, const TSource& rowset, const IBlobGroupSelector* dsGroupSelector,
            const NColumnShard::TTablesManager& tablesManager)
            : Key(std::move(key))
            , PathId(rowset.template GetValue<NColumnShard::Schema::IndexColumns::PathId>())
            , CLContext(rowset, dsGroupSelector) {
            InitSchema(tablesManager);
        }

        const NKikimrTxColumnShard::TIndexColumnMeta& GetMetaProto() const {
            return CLContext.GetMetaProto();
        }

        bool NormalizationRequired() const {
            return Key.GetColumnId() == Schema->GetIndexInfo().GetPKFirstColumnId() && Key.GetChunk() == 0 &&
                   !CLContext.GetMetaProto().HasPortionMeta();
        }

        bool operator<(const TChunkInfo& other) const {
            return Key < other.Key;
        }
    };

    static inline INormalizerComponent::TFactory::TRegistrator<TChunksV0MetaNormalizer> Registrator =
        INormalizerComponent::TFactory::TRegistrator<TChunksV0MetaNormalizer>(GetClassNameStatic());

public:
    TChunksV0MetaNormalizer(const TNormalizationController::TInitContext& info)
        : TBase(info)
        , DsGroupSelector(std::make_shared<NColumnShard::TBlobGroupSelector>(info.GetStorageInfo())) {
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;

private:
    std::shared_ptr<NColumnShard::TBlobGroupSelector> DsGroupSelector;
    THashMap<TPortionKey, NKikimrTxColumnShard::TIndexPortionMeta> PortionMetaCache;
};
}   // namespace NKikimr::NOlap
