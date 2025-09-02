#include "clean_deprecated_snapshot.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanDeprecatedSnapshot {

std::optional<std::vector<TColumnChunkLoadContext>> GetChunksToRewrite(
    NTabletFlatExecutor::TTransactionContext& txc, NColumnShard::TBlobGroupSelector& dsGroupSelector) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    if (!Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme())) {
        return std::nullopt;
    }

    std::vector<TColumnChunkLoadContext> chunksToRewrite;
    auto rowset = db.Table<Schema::IndexColumns>().Select();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }
    while (!rowset.EndOfSet()) {
        TColumnChunkLoadContext chunk(rowset, &dsGroupSelector);
        if (chunk.GetMinSnapshotDeprecated().GetPlanStep() != 1 || chunk.GetMinSnapshotDeprecated().GetTxId() != 1) {
            chunksToRewrite.emplace_back(chunk);
        }
        if (!rowset.Next()) {
            return std::nullopt;
        }
    }
    AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("tasks_for_rewrite", chunksToRewrite.size());
    return chunksToRewrite;
}

class TChanges: public INormalizerChanges {
private:
    const std::vector<TColumnChunkLoadContext> Chunks;

public:
    TChanges(std::vector<TColumnChunkLoadContext>&& chunks)
        : Chunks(std::move(chunks)) {
    }
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& i : Chunks) {
            db.Table<Schema::IndexColumns>()
                .Key(0, 0, i.GetAddress().GetColumnId(), i.GetMinSnapshotDeprecated().GetPlanStep(), i.GetMinSnapshotDeprecated().GetTxId(),
                    i.GetPortionId(), i.GetAddress().GetChunkIdx())
                .Delete();
            db.Table<Schema::IndexColumns>()
                .Key(0, 0, i.GetAddress().GetColumnId(), 1, 1, i.GetPortionId(), i.GetAddress().GetChunkIdx())
                .Update(NIceDb::TUpdate<Schema::IndexColumns::XPlanStep>(i.GetRemoveSnapshot().GetPlanStep()),
                    NIceDb::TUpdate<Schema::IndexColumns::XTxId>(i.GetRemoveSnapshot().GetTxId()),
                    NIceDb::TUpdate<Schema::IndexColumns::Blob>(i.GetBlobRange().BlobId.SerializeBinary()),
                    NIceDb::TUpdate<Schema::IndexColumns::Metadata>(i.GetMetaProto().SerializeAsString()),
                    NIceDb::TUpdate<Schema::IndexColumns::Offset>(i.GetBlobRange().Offset),
                    NIceDb::TUpdate<Schema::IndexColumns::Size>(i.GetBlobRange().Size),
                    NIceDb::TUpdate<Schema::IndexColumns::PathId>(i.GetPathId().GetRawValue()));
        }
        ACFL_WARN("normalizer", "TCleanDeprecatedSnapshotNormalizer")("message", TStringBuilder() << Chunks.size() << " portions rewrited");
        return true;
    }

    ui64 GetSize() const override {
        return Chunks.size();
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TCleanDeprecatedSnapshotNormalizer::DoInit(
    const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;

    if (!AppDataVerified().ColumnShardConfig.GetColumnChunksV0Usage()) {
        return std::vector<INormalizerTask::TPtr>();
    }
    
    auto batchesToDelete = GetChunksToRewrite(txc, DsGroupSelector);
    if (!batchesToDelete) {
        return TConclusionStatus::Fail("Not ready");
    }

    std::vector<INormalizerTask::TPtr> result;
    std::vector<TColumnChunkLoadContext> chunks;
    for (auto&& b : *batchesToDelete) {
        chunks.emplace_back(b);
        if (chunks.size() == 1000) {
            result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(chunks))));
            chunks.clear();
        }
    }
    if (chunks.size()) {
        result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(chunks))));
    }
    return result;
}

}   // namespace NKikimr::NOlap::NCleanDeprecatedSnapshot
