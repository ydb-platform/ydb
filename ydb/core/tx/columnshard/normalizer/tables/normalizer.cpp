#include "normalizer.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

class TTrivialNormalizerTask: public INormalizerTask {
    INormalizerChanges::TPtr Changes;

public:
    TTrivialNormalizerTask(INormalizerChanges::TPtr changes)
        : Changes(std::move(changes)) {
    }

    void Start(const TNormalizationController& /* controller */, const TNormalizationContext& nCtx) override {
        TActorContext::AsActorContext().Send(
            nCtx.GetColumnshardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(Changes));
    }
};

class TRemovedTablesNormalizer::TNormalizerResult: public INormalizerChanges {
    struct TDeleteKey {
        ui32 Index;
        ui64 Granule;
        ui32 ColumnIdx;
        ui64 ColumnStep;
        ui64 PlanStep;
        ui64 TxId;
        ui64 Portion;
        ui32 Chunk;
    };

    std::vector<TDeleteKey> ToDelete;

public:
    TNormalizerResult(std::vector<TDeleteKey>&& toDelete)
        : ToDelete(std::move(toDelete)) {
    }

public:
    bool Apply(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        std::set<ui64> brokenPortions;
        for (auto&& key : ToDelete) {
            // db.Table<Schema::IndexColumns>().Key(key.Index, key.Granule, key.ColumnIdx, key.PlanStep, key.TxId, key.Portion, key.Chunk).Delete();
            brokenPortions.insert(key.Portion);
        }

        TStringBuilder portionsString;
        for (auto& id : brokenPortions) {
            portionsString << id << ", ";
        }

        ACFL_NOTICE("normalizer", "TRemovedTablesNormalizer")("I want to remove protions: ", portionsString);

        return true;
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

        std::set<ui64> notEmptyTables;
        {
            auto rowset = db.Table<Schema::TableInfo>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }

            while (!rowset.EndOfSet()) {
                const auto pathId = rowset.GetValue<Schema::TableInfo::PathId>();
                notEmptyTables.emplace(pathId);

                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }
        }

        std::vector<INormalizerChanges::TPtr> changes;

        {
            std::vector<TDeleteKey> toRemove;

            auto rowset = db.Table<Schema::IndexColumns>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }

            while (!rowset.EndOfSet()) {
                TDeleteKey key;

                key.Index = rowset.GetValue<Schema::IndexColumns::Index>();
                key.Granule = rowset.GetValue<Schema::IndexColumns::Granule>();
                key.ColumnIdx = rowset.GetValue<Schema::IndexColumns::ColumnIdx>();
                key.PlanStep = rowset.GetValue<Schema::IndexColumns::PlanStep>();
                key.TxId = rowset.GetValue<Schema::IndexColumns::TxId>();
                key.Portion = rowset.GetValue<Schema::IndexColumns::Portion>();
                key.Chunk = rowset.GetValue<Schema::IndexColumns::Chunk>();

                if (!notEmptyTables.count(rowset.GetValue<Schema::IndexColumns::PathId>())) {
                    toRemove.push_back(key);
                }

                if (toRemove.size() == 10000) {
                    changes.emplace_back(std::make_shared<TNormalizerResult>(std::move(toRemove)));
                    toRemove.clear();
                }

                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }
        }

        return changes;
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TRemovedTablesNormalizer::Init(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    ACFL_NOTICE("normalizer", "TRemovedTablesNormalizer")("message", "Initializing TRemovedTablesNormalizer");

    auto changes = TNormalizerResult::Init(txc);
    if (!changes) {
        return TConclusionStatus::Fail("Not ready");
        ;
    }
    std::vector<INormalizerTask::TPtr> tasks;
    for (auto&& c : *changes) {
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(c));
    }
    AtomicSet(ActiveTasksCount, tasks.size());
    return tasks;
}

}   // namespace NKikimr::NOlap
