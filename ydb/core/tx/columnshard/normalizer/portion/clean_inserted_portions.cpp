#include "clean_inserted_portions.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/portions/read_with_blobs.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

#include <util/string/vector.h>

namespace NKikimr::NOlap::NNormalizer::NCleanInsertedPortions {

class TCleanInsertedPortionsNormalizer::TNormalizerResult: public INormalizerChanges {
    std::vector<TPortionDataAccessor> InsertedPortions;

public:
    TNormalizerResult(std::vector<TPortionDataAccessor>&& portions)
        : InsertedPortions(std::move(portions)) {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& normController) const override {
        NOlap::TBlobManagerDb blobManagerDb(txc.DB);
        TDbWrapper db(txc.DB, nullptr);
        for (auto&& portionInfo : InsertedPortions) {
            auto copy = portionInfo.GetPortionInfo().MakeCopy();
            copy->SetRemoveSnapshot(TSnapshot(1, 1));
            db.WritePortion(portionInfo.GetBlobIds(), *copy);
        }
        if (InsertedPortions.size()) {
            NIceDb::TNiceDb db(txc.DB);
            normController.AddNormalizerEvent(db, "REMOVE_PORTIONS", DebugString());
        }
        return true;
    }

    void ApplyOnComplete(const TNormalizationController& /* normController */) const override {
    }

    ui64 GetSize() const override {
        return InsertedPortions.size();
    }

    TString DebugString() const override {
        TStringBuilder sb;
        ui64 recordsCount = 0;
        sb << "path_ids=[";
        for (auto&& p : InsertedPortions) {
            sb << p.GetPortionInfo().GetPathId() << ",";
            recordsCount += p.GetPortionInfo().GetRecordsCount();
        }
        sb << "]";
        sb << ";records_count=" << recordsCount;
        sb << ";inserted_portions_count=" << InsertedPortions.size();
        return sb;
    }
};

bool TCleanInsertedPortionsNormalizer::CheckPortion(const NColumnShard::TTablesManager& /*tablesManager*/, const TPortionDataAccessor& /*portionInfo*/) const {
    return false;
}

INormalizerTask::TPtr TCleanInsertedPortionsNormalizer::BuildTask(
    std::vector<TPortionDataAccessor>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>>) const {
    std::vector<TPortionDataAccessor> insertedPortions;
    for (auto&& portion : portions) {
        if (portion.GetPortionInfo().GetProduced() == NPortion::EProduced::INSERTED) {
            insertedPortions.push_back(std::move(portion));
        }
    }
    auto taskResult = std::make_shared<TNormalizerResult>(std::move(insertedPortions));
    ACFL_WARN("normalizer", "TCleanInsertedPortionsNormalizer")("message", taskResult->DebugString());
    ACFL_WARN("normalizer", "TCleanInsertedPortionsNormalizer")("all portions", portions.size());
    return std::make_shared<TTrivialNormalizerTask>(taskResult);
}

TConclusion<bool> TCleanInsertedPortionsNormalizer::DoInitImpl(const TNormalizationController&, NTabletFlatExecutor::TTransactionContext&) {
    return true;
}

}   // namespace NKikimr::NOlap::NNormalizer::NCleanInsertedPortions
