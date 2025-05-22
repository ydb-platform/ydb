#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/defs.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>

namespace NKikimr::NColumnShard {
class TTablesManager;
}

namespace NKikimr::NOlap {

class TDetectMissingChunks: public TNormalizationController::INormalizerComponent {
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
        return "DetectMissingChunks";
    }

    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return ENormalizerSequentialId::RestoreV0ChunksMeta;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    class TNormalizerResult;

    static inline INormalizerComponent::TFactory::TRegistrator<TDetectMissingChunks> Registrator =
        INormalizerComponent::TFactory::TRegistrator<TDetectMissingChunks>(GetClassNameStatic());

public:
    TDetectMissingChunks(const TNormalizationController::TInitContext& info)
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
