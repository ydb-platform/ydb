#include "clean_granule.h"

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

        ui64 XPlanStep = 0;
        ui64 XTxId = 0;
        TString Blob;
        TString Metadata;
        ui64 Offset;
        ui32 Size;
        ui64 PathId;

        template <class TRowSet>
        TChunkData(const TRowSet& rowset) {
            using Schema = NColumnShard::Schema;
            PlanStep = rowset.template GetValue<Schema::IndexColumns::PlanStep>();
            TxId = rowset.template GetValue<Schema::IndexColumns::TxId>();
            PortionId = rowset.template GetValue<Schema::IndexColumns::Portion>();
            GranuleId = rowset.template GetValue<Schema::IndexColumns::Granule>();
            Chunk = rowset.template GetValue<Schema::IndexColumns::Chunk>();
            Index = rowset.template GetValue<Schema::IndexColumns::Index>();
            ColumnIdx = rowset.template GetValue<Schema::IndexColumns::ColumnIdx>();

            XPlanStep = rowset.template GetValue<Schema::IndexColumns::XPlanStep>();
            XTxId = rowset.template GetValue<Schema::IndexColumns::XTxId>();
            Blob = rowset.template GetValue<Schema::IndexColumns::Blob>();
            Metadata = rowset.template GetValue<Schema::IndexColumns::Metadata>();
            Offset = rowset.template GetValue<Schema::IndexColumns::Offset>();
            Size = rowset.template GetValue<Schema::IndexColumns::Size>();
            PathId = rowset.template GetValue<Schema::IndexColumns::PathId>();
        }
    };
}

class TCleanGranuleIdNormalizer::TNormalizerResult : public INormalizerChanges {
private:
    std::vector<TChunkData> Chunks;

    void AddChunk(TChunkData&& chunk) {
        Chunks.push_back(std::move(chunk));
    }

    TNormalizerResult() = default;

public:
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using Schema = NColumnShard::Schema;
        NIceDb::TNiceDb db(txc.DB);
        ACFL_INFO("normalizer", "TCleanGranuleIdNormalizer")("message", TStringBuilder() << "apply " << Chunks.size() << " chunks");

        for (auto&& key : Chunks) {
            db.Table<Schema::IndexColumns>().Key(key.Index, key.GranuleId, key.ColumnIdx,
                key.PlanStep, key.TxId, key.PortionId, key.Chunk).Delete();

            db.Table<Schema::IndexColumns>().Key(0, 0, key.ColumnIdx,
                key.PlanStep, key.TxId, key.PortionId, key.Chunk).Update(
                    NIceDb::TUpdate<Schema::IndexColumns::PathId>(key.PathId),
                    NIceDb::TUpdate<Schema::IndexColumns::Blob>(key.Blob),
                    NIceDb::TUpdate<Schema::IndexColumns::Metadata>(key.Metadata),
                    NIceDb::TUpdate<Schema::IndexColumns::Offset>(key.Offset),
                    NIceDb::TUpdate<Schema::IndexColumns::Size>(key.Size),
                    NIceDb::TUpdate<Schema::IndexColumns::XPlanStep>(key.XPlanStep),
                    NIceDb::TUpdate<Schema::IndexColumns::XTxId>(key.XTxId)

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
        if (!ready) {
            return std::nullopt;
        }

        std::vector<INormalizerChanges::TPtr> tasks;
        ui64 fullChunksCount = 0;
        {
            auto rowset = db.Table<Schema::IndexColumns>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }
            std::shared_ptr<TNormalizerResult> changes(new TNormalizerResult());
            ui64 chunksCount = 0;

            while (!rowset.EndOfSet()) {
                if (rowset.GetValue<Schema::IndexColumns::Granule>() || rowset.GetValue<Schema::IndexColumns::Index>()) {
                    TChunkData key(rowset);

                    changes->AddChunk(std::move(key));
                    ++chunksCount;
                    ++fullChunksCount;

                    if (chunksCount == 10000) {
                        tasks.emplace_back(changes);
                        changes.reset(new TNormalizerResult());
                        controller.GetCounters().CountObjects(chunksCount);
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
        ACFL_INFO("normalizer", "TCleanGranuleIdNormalizer")("message", TStringBuilder() << fullChunksCount << " chunks found");
        return tasks;
    }

};

TConclusion<std::vector<INormalizerTask::TPtr>> TCleanGranuleIdNormalizer::DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
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
