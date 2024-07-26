#include "normalizer.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

namespace {

constexpr ui32 ColumnIdxToDelete = (ui32)IIndexInfo::ESpecialColumn::DELETE_FLAG;


using namespace NColumnShard;

struct TKey {
    ui32 Index;
    ui64 Granule;
    ui32 ColumnIdx;
    ui64 PlanStep;
    ui64 TxId;
    ui64 Portion;
    ui32 Chunk;
};

using TKeyBatch = std::vector<TKey>;

std::optional<std::vector<TKeyBatch>> KeysToDelete(NTabletFlatExecutor::TTransactionContext& txc, size_t maxBatchSize) {
    NIceDb::TNiceDb db(txc.DB);
    if (!Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme())) {
        return std::nullopt;
    }
    std::vector<TKeyBatch> result;
    TKeyBatch currentBatch;
    auto rowset = db.Table<Schema::IndexColumns>().Select<
        Schema::IndexColumns::Index,
        Schema::IndexColumns::Granule,
        Schema::IndexColumns::ColumnIdx,
        Schema::IndexColumns::PlanStep,
        Schema::IndexColumns::TxId,
        Schema::IndexColumns::Portion,
        Schema::IndexColumns::Chunk
    >();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }
    while (!rowset.EndOfSet()) {
        if (rowset.GetValue<Schema::IndexColumns::ColumnIdx>() == ColumnIdxToDelete) {
            auto key = TKey {
                .Index = rowset.GetValue<Schema::IndexColumns::Index>(),
                .Granule = rowset.GetValue<Schema::IndexColumns::Granule>(),
                .ColumnIdx = rowset.GetValue<Schema::IndexColumns::ColumnIdx>(),
                .PlanStep = rowset.GetValue<Schema::IndexColumns::PlanStep>(),
                .Portion = rowset.GetValue<Schema::IndexColumns::Portion>(),
                .Chunk = rowset.GetValue<Schema::IndexColumns::Chunk>()
            };
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

class TChanges : public INormalizerChanges {
public:
    TChanges(TKeyBatch&& keys)
        : Keys(keys)
    {}
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for(const auto& k: Keys) {
            db.Table<Schema::IndexColumns>().Key(
                k.Index,
                k.Granule,
                k.ColumnIdx,
                k.PlanStep,
                k.TxId,
                k.Portion,
                k.Chunk
            ).Delete();
        }
        ACFL_INFO("normalizer", "TDeleteUnsupportedSpecialColumnsNormalier")("message", TStringBuilder() << GetSize() << " rows deleted");
        return true;
    }

    ui64 GetSize() const override {
        return Keys.size();
    }
private:
    const TKeyBatch Keys;
};

} //namespace

TConclusion<std::vector<INormalizerTask::TPtr>> TDeleteUnsupportedSpecialColumnsNormalier::DoInit(const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    const size_t MaxBatchSize = 10000;
    auto keysToDelete = KeysToDelete(txc, MaxBatchSize);
    if (!keysToDelete) {
        return TConclusionStatus::Fail("Not ready");
    }
    ACFL_INFO("normalizer", "TDeleteUnsupportedSpecialColumnsNormalier")("message", 
        TStringBuilder() 
            << "found " 
            << std::accumulate(cbegin(*keysToDelete), cend(*keysToDelete), 0, [](size_t a, const TKeyBatch& b){return a + b.size();})
            << " rows to delete grouped in "
            << keysToDelete->size()
            << " batches"
    );

    std::vector<INormalizerTask::TPtr> result;
    for (auto&& batch: *keysToDelete) {
        AFL_VERIFY(!batch.empty());
        result.emplace_back(std::make_shared<TTrivialNormalizerTask>(
            std::make_shared<TChanges>(std::move(std::move(batch)))    
        ));
    }
    return result;
}

} //namespace NKikimr::NOlap
