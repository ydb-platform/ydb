#include "normalizer.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

class TRemovedTablesNormalizer::TNormalizerResult : public INormalizerChanges {
    struct TPathInfo {
        ui64 PathId;
        ui64 Step;
        ui64 TxId;
    };

    std::vector<TPathInfo> PathIds;

public:
    TNormalizerResult(std::vector<TPathInfo>&& pathIds)
        : PathIds(std::move(pathIds))
    {}

public:
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        for (auto&& pathInfo: PathIds) {
            db.Table<Schema::TableVersionInfo>().Key(pathInfo.PathId, pathInfo.Step, pathInfo.TxId).Delete();
            db.Table<Schema::TableInfo>().Key(pathInfo.PathId).Delete();
        }
        return true;
    }

    ui64 GetSize() const override {
        return PathIds.size();
    }

    static std::optional<std::vector<INormalizerChanges::TPtr>> Init(NTabletFlatExecutor::TTransactionContext& txc) {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        bool ready = true;
        ready = ready & Schema::Precharge<Schema::TableVersionInfo>(db, txc.DB.GetScheme());
        ready = ready & Schema::Precharge<Schema::TableInfo>(db, txc.DB.GetScheme());
        ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
        if (!ready) {
            return std::nullopt;
        }

        std::set<ui64> notEmptyPaths;
        {
            auto rowset = db.Table<Schema::IndexColumns>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }

            while (!rowset.EndOfSet()) {
                const auto pathId = rowset.GetValue<Schema::IndexColumns::PathId>();
                notEmptyPaths.emplace(pathId);

                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }
        }


        std::set<ui64> droppedTables;
        {
            auto rowset = db.Table<Schema::TableInfo>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }

            while (!rowset.EndOfSet()) {
                const auto pathId = rowset.GetValue<Schema::TableInfo::PathId>();
                const NOlap::TSnapshot dropSnapshot(rowset.GetValue<Schema::TableInfo::DropStep>(), rowset.GetValue<Schema::TableInfo::DropTxId>());

                if (dropSnapshot.Valid() && !notEmptyPaths.contains(pathId)) {
                    droppedTables.emplace(pathId);
                }

                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }
        }

        std::vector<INormalizerChanges::TPtr> changes;
        ui64 fullCount = 0;

        {
            auto rowset = db.Table<Schema::TableVersionInfo>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }

            std::vector<TPathInfo> toRemove;
            while (!rowset.EndOfSet()) {
                TPathInfo pathInfo;
                pathInfo.PathId = rowset.GetValue<Schema::TableVersionInfo::PathId>();
                if (droppedTables.contains(pathInfo.PathId)) {
                    pathInfo.Step = rowset.GetValue<Schema::TableVersionInfo::SinceStep>();
                    pathInfo.TxId = rowset.GetValue<Schema::TableVersionInfo::SinceTxId>();
                    toRemove.emplace_back(pathInfo);
                    ++fullCount;
                }

                if (toRemove.size() == 10000) {
                    changes.emplace_back(std::make_shared<TNormalizerResult>(std::move(toRemove)));
                    toRemove.clear();
                }

                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }

            if (toRemove.size() > 0) {
                changes.emplace_back(std::make_shared<TNormalizerResult>(std::move(toRemove)));
            }
        }

        ACFL_INFO("normalizer", "TGranulesNormalizer")("message", TStringBuilder() << fullCount << " chunks found");
        return changes;
    }

};

TConclusion<std::vector<INormalizerTask::TPtr>> TRemovedTablesNormalizer::DoInit(const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    auto changes = TNormalizerResult::Init(txc);
    if (!changes) {
        return TConclusionStatus::Fail("Not ready");;
    }
    std::vector<INormalizerTask::TPtr> tasks;
    for (auto&& c : *changes) {
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(c));
    }
    return tasks;
}

}
