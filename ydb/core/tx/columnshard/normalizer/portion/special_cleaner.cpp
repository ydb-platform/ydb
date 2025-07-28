#include "special_cleaner.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NNormalizer::NSpecialColumns {

namespace {

class TDeleteChunksV0: public TDeleteTrashImpl::IAction {
private:
    ui32 Index;
    ui64 Granule;
    ui32 ColumnIdx;
    ui64 PlanStep;
    ui64 TxId;
    ui64 Portion;
    ui32 Chunk;

public:
    TDeleteChunksV0(ui32 index, ui64 granule, ui32 columnIdx, ui64 planStep, ui64 txId, ui64 portion, ui32 chunk)
        : Index(index)
        , Granule(granule)
        , ColumnIdx(columnIdx)
        , PlanStep(planStep)
        , TxId(txId)
        , Portion(portion)
        , Chunk(chunk)
    {
    }

    virtual TConclusionStatus ApplyOnExecute(NIceDb::TNiceDb& db) const override {
        db.Table<NColumnShard::Schema::IndexColumns>().Key(Index, Granule, ColumnIdx, PlanStep, TxId, Portion, Chunk).Delete();
        return TConclusionStatus::Success();
    }
};

class TDeleteChunksV1: public TDeleteTrashImpl::IAction {
private:
    ui64 PathId;
    ui64 PortionId;
    ui32 SSColumnId;
    ui32 ChunkIdx;

public:
    TDeleteChunksV1(ui64 pathId, ui64 portionId, ui32 ssColumnId, ui32 chunkIdx)
        : PathId(pathId)
        , PortionId(portionId)
        , SSColumnId(ssColumnId)
        , ChunkIdx(chunkIdx)
    {
    }

    virtual TConclusionStatus ApplyOnExecute(NIceDb::TNiceDb& db) const override {
        db.Table<NColumnShard::Schema::IndexColumnsV1>().Key(PathId, PortionId, SSColumnId, ChunkIdx).Delete();
        return TConclusionStatus::Success();
    }
};

class TDeleteChunksV2: public TDeleteTrashImpl::IAction {
private:
    ui64 PathId;
    ui64 PortionId;
    ui32 ColumnId;

public:
    TDeleteChunksV2(ui64 pathId, ui64 portionId, ui32 columnId)
        : PathId(pathId)
        , PortionId(portionId)
        , ColumnId(columnId)
    {
    }

    virtual TConclusionStatus ApplyOnExecute(NIceDb::TNiceDb& db) const override {
        auto rowset = db.Table<NColumnShard::Schema::IndexColumnsV2>().Key(PathId, PortionId).Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        NKikimrTxColumnShard::TIndexPortionAccessor resultMetadata;
        auto metadataString = rowset.GetValue<NColumnShard::Schema::IndexColumnsV2::Metadata>();
        NKikimrTxColumnShard::TIndexPortionAccessor metaProto;
        AFL_VERIFY(metaProto.ParseFromArray(metadataString.data(), metadataString.size()))("event", "cannot parse metadata as protobuf");
        resultMetadata.CopyFrom(metaProto);
        resultMetadata.ClearChunks();
        for (const auto& chunk : metaProto.GetChunks()) {
            if (chunk.GetSSColumnId() != ColumnId) {
                resultMetadata.AddChunks()->CopyFrom(chunk);
            }
        }

        db.Table<NColumnShard::Schema::IndexColumnsV2>()
            .Key(PathId, PortionId)
            .Update(NIceDb::TUpdate<NColumnShard::Schema::IndexColumnsV2::Metadata>(resultMetadata.SerializeAsString()));
        return TConclusionStatus::Success();
    }
};

class TChanges: public INormalizerChanges {
public:
    TChanges(std::vector<std::shared_ptr<TDeleteTrashImpl::IAction>>&& actions)
        : Actions(std::move(actions))
    {
    }
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /*normController*/) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& action : Actions) {
            auto result = action->ApplyOnExecute(db);
            if (result.IsFail()) {
                return false;
            }
        }
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("normalizer", "TDeleteTrash")("message", TStringBuilder() << GetSize() << " chunks deleted");
        return true;
    }

    ui64 GetSize() const override {
        return Actions.size();
    }

private:
    std::vector<std::shared_ptr<TDeleteTrashImpl::IAction>> Actions;
};

}   //namespace

TConclusion<std::vector<INormalizerTask::TPtr>> TDeleteTrashImpl::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    const size_t MaxBatchSize = 10000;
    auto keysToDelete = KeysToDelete(txc);
    if (!keysToDelete) {
        return TConclusionStatus::Fail("Not ready");
    }
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("normalizer", "TDeleteTrash")(
        "message", TStringBuilder() << "found " << keysToDelete->size() << " rows to delete grouped in " << keysToDelete->size() << " batches");

    std::vector<INormalizerTask::TPtr> result;
    std::vector<std::shared_ptr<IAction>> batch;
    for (auto&& action : *keysToDelete) {
        batch.emplace_back(std::move(action));
        if (batch.size() == MaxBatchSize) {
            result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(batch))));
        }
    }
    if (!batch.empty()) {
        result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(batch))));
    }
    return result;
}

std::optional<std::vector<std::shared_ptr<TDeleteTrashImpl::IAction>>> TDeleteTrashImpl::KeysToDelete(
    NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);
    using namespace NColumnShard;
    if (!Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme())) {
        return std::nullopt;
    }
    if (!Schema::Precharge<Schema::IndexColumnsV1>(db, txc.DB.GetScheme())) {
        return std::nullopt;
    }
    if (!Schema::Precharge<Schema::IndexColumnsV2>(db, txc.DB.GetScheme())) {
        return std::nullopt;
    }

    const std::set<ui64> columnIdsToDelete = GetColumnIdsToDelete();
    std::vector<std::shared_ptr<TDeleteTrashImpl::IAction>> actions;

    {
        auto rowset =
            db.Table<Schema::IndexColumns>().Select<Schema::IndexColumns::Index, Schema::IndexColumns::Granule, Schema::IndexColumns::ColumnIdx,
                Schema::IndexColumns::PlanStep, Schema::IndexColumns::TxId, Schema::IndexColumns::Portion, Schema::IndexColumns::Chunk>();
        if (!rowset.IsReady()) {
            return std::nullopt;
        }
        while (!rowset.EndOfSet()) {
            if (columnIdsToDelete.contains(rowset.GetValue<Schema::IndexColumns::ColumnIdx>())) {
                actions.emplace_back(std::make_shared<TDeleteChunksV0>(rowset.GetValue<Schema::IndexColumns::Index>(),
                    rowset.GetValue<Schema::IndexColumns::Granule>(), rowset.GetValue<Schema::IndexColumns::ColumnIdx>(),
                    rowset.GetValue<Schema::IndexColumns::PlanStep>(), rowset.GetValue<Schema::IndexColumns::TxId>(),
                    rowset.GetValue<Schema::IndexColumns::Portion>(), rowset.GetValue<Schema::IndexColumns::Chunk>()));
            }
            if (!rowset.Next()) {
                return std::nullopt;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::IndexColumnsV1>().Select<Schema::IndexColumnsV1::PathId, Schema::IndexColumnsV1::PortionId,
            Schema::IndexColumnsV1::SSColumnId, Schema::IndexColumnsV1::ChunkIdx>();
        if (!rowset.IsReady()) {
            return std::nullopt;
        }
        while (!rowset.EndOfSet()) {
            if (columnIdsToDelete.contains(rowset.GetValue<Schema::IndexColumnsV1::SSColumnId>())) {
                actions.emplace_back(std::make_shared<TDeleteChunksV1>(rowset.GetValue<Schema::IndexColumnsV1::PathId>(),
                    rowset.GetValue<Schema::IndexColumnsV1::PortionId>(), rowset.GetValue<Schema::IndexColumnsV1::SSColumnId>(),
                    rowset.GetValue<Schema::IndexColumnsV1::ChunkIdx>()));
            }
            if (!rowset.Next()) {
                return std::nullopt;
            }
        }
    }

    {
        auto rowset = db.Table<Schema::IndexColumnsV2>()
                          .Select<Schema::IndexColumnsV2::PathId, Schema::IndexColumnsV2::PortionId, Schema::IndexColumnsV2::Metadata>();
        if (!rowset.IsReady()) {
            return std::nullopt;
        }

        while (!rowset.EndOfSet()) {
            auto metadataString = rowset.GetValue<NColumnShard::Schema::IndexColumnsV2::Metadata>();
            NKikimrTxColumnShard::TIndexPortionAccessor metaProto;
            AFL_VERIFY(metaProto.ParseFromArray(metadataString.data(), metadataString.size()))("event", "cannot parse metadata as protobuf");

            for (const auto& chunk : metaProto.GetChunks()) {
                if (columnIdsToDelete.contains(chunk.GetSSColumnId())) {
                    actions.emplace_back(std::make_shared<TDeleteChunksV2>(rowset.GetValue<Schema::IndexColumnsV2::PathId>(),
                        rowset.GetValue<Schema::IndexColumnsV2::PortionId>(), chunk.GetSSColumnId()));
                }
            }
            if (!rowset.Next()) {
                return std::nullopt;
            }
        }
    }

    return actions;
}

}   // namespace NKikimr::NOlap::NNormalizer::NSpecialColumns
