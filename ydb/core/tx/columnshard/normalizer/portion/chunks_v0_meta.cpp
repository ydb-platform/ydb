#include "chunks_v0_meta.h"
#include "normalizer.h"

#include <ydb/core/formats/arrow/size_calcer.h>
#include <ydb/core/tx/columnshard/counters/portion_index.h>
#include <ydb/core/tx/columnshard/data_accessor/manager.h>
#include <ydb/core/tx/columnshard/engines/column_engine_logs.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap {

class TChunksV0MetaNormalizer::TNormalizerResult: public INormalizerChanges {
    std::vector<TChunksV0MetaNormalizer::TChunkInfo> Chunks;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;

public:
    TNormalizerResult(std::vector<TChunksV0MetaNormalizer::TChunkInfo>&& chunks)
        : Chunks(std::move(chunks)) {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);

        THashMap<TPortionKey, NKikimrTxColumnShard::TIndexPortionMeta> portionMeta;
        for (auto&& chunkInfo : Chunks) {
            NKikimrTxColumnShard::TIndexColumnMeta metaProto = chunkInfo.GetMetaProto();
            auto portionKey = TPortionKey(chunkInfo.GetPathId(), chunkInfo.GetKey().GetPortion());
            auto* findMeta = portionMeta.FindPtr(portionKey);
            if (!findMeta) {
                auto metadata = GetPortionMeta(db, portionKey);
                if (metadata.IsFail()) {
                    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", metadata.GetErrorMessage());
                    return false;
                }
                findMeta = &portionMeta.emplace(portionKey, metadata.DetachResult()).first->second;
            }
            metaProto.MutablePortionMeta()->CopyFrom(*findMeta);

            const auto& key = chunkInfo.GetKey();

            db.Table<Schema::IndexColumns>()
                .Key(key.GetIndex(), key.GetGranule(), key.GetColumnId(), key.GetPlanStep(), key.GetTxId(), key.GetPortion(), key.GetChunk())
                .Update(NIceDb::TUpdate<Schema::IndexColumns::Metadata>(metaProto.SerializeAsString()));
        }
        return true;
    }

    ui64 GetSize() const override {
        return Chunks.size();
    }

    TConclusion<NKikimrTxColumnShard::TIndexPortionMeta> GetPortionMeta(NIceDb::TNiceDb& db, const TPortionKey& key) const {
        auto rowset = db.Table<NColumnShard::Schema::IndexPortions>().Key(key.GetPathId(), key.GetPortionId()).Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }
        AFL_VERIFY(!rowset.EndOfSet());
        NKikimrTxColumnShard::TIndexColumnMeta metaProto;
        AFL_VERIFY(metaProto.ParseFromString(rowset.GetValue<NColumnShard::Schema::IndexPortions::Metadata>()));
        return metaProto.GetPortionMeta();
    }
};

void TChunksV0MetaNormalizer::TChunkInfo::InitSchema(const NColumnShard::TTablesManager& tm) {
    Schema = tm.GetPrimaryIndexSafe().GetVersionedIndex().GetSchemaVerified(NOlap::TSnapshot(Key.GetPlanStep(), Key.GetTxId()));
}

TConclusion<std::vector<INormalizerTask::TPtr>> TChunksV0MetaNormalizer::DoInit(
    const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    if (!AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        return std::vector<INormalizerTask::TPtr>();
    }

    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme());
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    TTablesManager tablesManager(
        controller.GetStoragesManager(), controller.GetDataAccessorsManager(), std::make_shared<TPortionIndexStats>(), 0);
    if (!tablesManager.InitFromDB(db, nullptr)) {
        ACFL_TRACE("normalizer", "TChunksV0MetaNormalizer")("error", "can't initialize tables manager");
        return TConclusionStatus::Fail("Can't load index");
    }

    std::vector<TChunkInfo> chunks;
    {
        auto rowset = db.Table<Schema::IndexColumns>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TColumnKey key;
            key.Load(rowset);

            TChunkInfo chunkInfo(std::move(key), rowset, &*DsGroupSelector, tablesManager);
            if (chunkInfo.NormalizationRequired()) {
                chunks.emplace_back(std::move(chunkInfo));
            }

            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    std::vector<INormalizerTask::TPtr> tasks;
    ACFL_INFO("normalizer", "TChunksV0MetaNormalizer")("message", TStringBuilder() << chunks.size() << " chunks found");
    if (chunks.empty()) {
        return tasks;
    }

    std::vector<TChunkInfo> package;
    package.reserve(100);

    for (auto&& chunk : chunks) {
        package.emplace_back(chunk);
        if (package.size() == 100) {
            std::vector<TChunkInfo> local;
            local.swap(package);
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TNormalizerResult>(std::move(local))));
        }
    }

    if (package.size() > 0) {
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TNormalizerResult>(std::move(package))));
    }
    return tasks;
}

}   // namespace NKikimr::NOlap
