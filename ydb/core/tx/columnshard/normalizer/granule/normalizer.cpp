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

private:
    void AddChunk(TChunkData&& chunk) {
        Chunks.push_back(std::move(chunk));
    }

    TNormalizerResult(const THashMap<ui64, ui64>& g2p)
        : Granule2Path(g2p)
    {}

public:
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        ACFL_INFO("normalizer", "TGranulesNormalizer")("message", TStringBuilder() << "apply " << Chunks.size() << " chunks");

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

    ui64 GetSize() const override {
        return Chunks.size();
    }

    static std::optional<std::vector<INormalizerChanges::TPtr>> Init(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        bool ready = true;
        ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
        ready = ready & Schema::Precharge<Schema::IndexGranules>(db, txc.DB.GetScheme());
        if (!ready) {
            return std::nullopt;
        }

        THashMap<ui64, ui64> granule2Path;
        {
            auto rowset = db.Table<Schema::IndexGranules>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }

            while (!rowset.EndOfSet()) {
                ui64 pathId = rowset.GetValue<Schema::IndexGranules::PathId>();
                ui64 granuleId = rowset.GetValue<Schema::IndexGranules::Granule>();
                Y_ABORT_UNLESS(granuleId != 0);
                granule2Path[granuleId] = pathId;
                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }
        }

        std::vector<INormalizerChanges::TPtr> tasks;
        ui64 fullChunksCount = 0;
        {
            auto rowset = db.Table<Schema::IndexColumns>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }
            std::shared_ptr<TNormalizerResult> changes(new TNormalizerResult(granule2Path));
            ui64 chunksCount = 0;

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

                    changes->AddChunk(std::move(key));
                    ++chunksCount;
                    ++fullChunksCount;

                    if (chunksCount == 10000) {
                        tasks.emplace_back(changes);
                        controller.GetCounters().CountObjects(chunksCount);
                        changes.reset(new TNormalizerResult(granule2Path));
                        chunksCount = 0;
                    }
                }

                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }

            if (chunksCount > 0) {
                tasks.emplace_back(changes);
                controller.GetCounters().CountObjects(chunksCount);
            }
        }
        ACFL_INFO("normalizer", "TGranulesNormalizer")("message", TStringBuilder() << fullChunksCount << " chunks found");
        return tasks;
    }

};

TConclusion<std::vector<INormalizerTask::TPtr>> TGranulesNormalizer::DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    auto changes = TNormalizerResult::Init(controller, txc);
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
