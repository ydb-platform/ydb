#include "special_cleaner.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NNormalizer::NSpecialColumns {

namespace {

class TChanges: public INormalizerChanges {
public:
    TChanges(TDeleteTrashImpl::TKeyBatch&& keys)
        : Keys(keys) {
    }
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /*normController*/) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (const auto& k : Keys) {
            db.Table<Schema::IndexColumns>().Key(k.Index, k.Granule, k.ColumnIdx, k.PlanStep, k.TxId, k.Portion, k.Chunk).Delete();
        }
        AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("normalizer", "TDeleteTrash")("message", TStringBuilder() << GetSize() << " rows deleted");
        return true;
    }

    ui64 GetSize() const override {
        return Keys.size();
    }

private:
    const TDeleteTrashImpl::TKeyBatch Keys;
};

}   //namespace

TConclusion<std::vector<INormalizerTask::TPtr>> TDeleteTrashImpl::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    const size_t MaxBatchSize = 10000;
    auto keysToDelete = KeysToDelete(txc, MaxBatchSize);
    if (!keysToDelete) {
        return TConclusionStatus::Fail("Not ready");
    }
    ui32 removeCount = 0;
    for (auto&& i : *keysToDelete) {
        removeCount += i.size();
    }
    AFL_NOTICE(NKikimrServices::TX_COLUMNSHARD)("normalizer", "TDeleteTrash")(
        "message", TStringBuilder() << "found " << removeCount << " rows to delete grouped in " << keysToDelete->size() << " batches");

    std::vector<INormalizerTask::TPtr> result;
    for (auto&& batch : *keysToDelete) {
        AFL_VERIFY(!batch.empty());
        result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(batch))));
    }
    return result;
}

std::optional<std::vector<TDeleteTrashImpl::TKeyBatch>> TDeleteTrashImpl::KeysToDelete(
    NTabletFlatExecutor::TTransactionContext& txc, const size_t maxBatchSize) {
    NIceDb::TNiceDb db(txc.DB);
    using namespace NColumnShard;
    if (!Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme())) {
        return std::nullopt;
    }
    const std::set<ui64> columnIdsToDelete = GetColumnIdsToDelete();
    std::vector<TKeyBatch> result;
    TKeyBatch currentBatch;
    auto rowset =
        db.Table<Schema::IndexColumns>()
            .Select<Schema::IndexColumns::Index, Schema::IndexColumns::Granule, Schema::IndexColumns::ColumnIdx, Schema::IndexColumns::PlanStep,
                Schema::IndexColumns::TxId, Schema::IndexColumns::Portion, Schema::IndexColumns::Chunk>();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }
    while (!rowset.EndOfSet()) {
        if (columnIdsToDelete.contains(rowset.GetValue<Schema::IndexColumns::ColumnIdx>())) {
            auto key = TKey{
                .Index = rowset.GetValue<Schema::IndexColumns::Index>(),
                .Granule = rowset.GetValue<Schema::IndexColumns::Granule>(),
                .ColumnIdx = rowset.GetValue<Schema::IndexColumns::ColumnIdx>(),
                .PlanStep = rowset.GetValue<Schema::IndexColumns::PlanStep>(),
                .TxId = rowset.GetValue<Schema::IndexColumns::TxId>(),
                .Portion = rowset.GetValue<Schema::IndexColumns::Portion>(),
                .Chunk = rowset.GetValue<Schema::IndexColumns::Chunk>() };
            currentBatch.emplace_back(std::move(key));
            if (currentBatch.size() == maxBatchSize) {
                result.emplace_back(std::move(currentBatch));
                currentBatch = TKeyBatch{};
            }
        }
        if (!rowset.Next()) {
            return std::nullopt;
        }
    }
    if (!currentBatch.empty()) {
        result.emplace_back(std::move(currentBatch));
    }

    return result;
}

}   // namespace NKikimr::NOlap::NNormalizer::NSpecialColumns
