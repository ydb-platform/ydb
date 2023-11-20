#include "normalizer.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

namespace {

    struct TChunkData {
        ui64 Index = 0;
        ui64 GranuleId = 0;
        ui64 PlanStep = 0;
        ui64 TxId = 0;
        ui64 PortionId = 0;
        ui32 Chunk = 0;
        ui64 ColumnIdx = 0;
    };
}

class TGranulesNormalizer::TNormalizerResult : public INormalizerChanges {
    std::vector<TChunkData> Chunks;
    THashMap<ui64, ui64> Granule2Path;
public:
    bool Apply(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        for (auto&& key : Chunks) {
            auto granuleIt = Granule2Path.find(key.GranuleId);
            Y_ABORT_UNLESS(granuleIt != Granule2Path.end());

            db.Table<Schema::IndexColumns>().Key(key.Index, key.GranuleId, key.ColumnIdx,
            key.PlanStep, key.TxId, key.PortionId, key.Chunk).Update(
                NIceDb::TUpdate<Schema::IndexColumns::PathId>(granuleIt->second)
            );
        }
        return true;
    }

    bool Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        bool ready = true;
        ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
        ready = ready & Schema::Precharge<Schema::IndexGranules>(db, txc.DB.GetScheme());
        if (!ready) {
            return false;
        }

        {
            auto rowset = db.Table<Schema::IndexColumns>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                if (!rowset.HaveValue<Schema::IndexColumns::PathId>() || rowset.GetValue<Schema::IndexColumns::PathId>() == 0) {
                    TChunkData key;
                    key.PlanStep = rowset.GetValue<Schema::IndexColumns::PlanStep>();
                    key.TxId = rowset.GetValue<Schema::IndexColumns::TxId>();
                    key.PortionId = rowset.GetValue<Schema::IndexColumns::Portion>();
                    key.GranuleId = rowset.GetValue<Schema::IndexColumns::Granule>();
                    key.Chunk = rowset.GetValue<Schema::IndexColumns::Chunk>();
                    key.Index = rowset.GetValue<Schema::IndexColumns::Index>();
                    key.ColumnIdx = rowset.GetValue<Schema::IndexColumns::ColumnIdx>();

                    Chunks.emplace_back(std::move(key));
                }

                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        ACFL_INFO("normalizer", "TGranulesNormalizer")("message", TStringBuilder() << Chunks.size() << " chunks found");
        if (Chunks.empty()) {
            return true;
        }
        controller.GetCounters().CountObjects(Chunks.size());
        {
            auto rowset = db.Table<Schema::IndexGranules>().Select();
            if (!rowset.IsReady()) {
                return false;
            }

            while (!rowset.EndOfSet()) {
                ui64 pathId = rowset.GetValue<Schema::IndexGranules::PathId>();
                ui64 granuleId = rowset.GetValue<Schema::IndexGranules::Granule>();
                Y_ABORT_UNLESS(granuleId != 0);
                Granule2Path[granuleId] = pathId;
                if (!rowset.Next()) {
                    return false;
                }
            }
        }
        return true;
    }
};

class TGranulesNormalizerTask : public INormalizerTask {
    INormalizerChanges::TPtr Changes;
public:
    TGranulesNormalizerTask(const INormalizerChanges::TPtr& changes)
        : Changes(changes)
    {}

    void Start(const TNormalizationController& /* controller */, const TNormalizationContext& nCtx) override {
        TActorContext::AsActorContext().Send(nCtx.GetColumnshardActor(), std::make_unique<NColumnShard::TEvPrivate::TEvNormalizerResult>(Changes));
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TGranulesNormalizer::Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    auto changes = std::make_shared<TNormalizerResult>();
    if (!changes->Init(controller, txc)) {
        return TConclusionStatus::Fail("Not ready");;
    }
    std::vector<INormalizerTask::TPtr> tasks = { std::make_shared<TGranulesNormalizerTask>(changes) };
    return tasks;
}

}
