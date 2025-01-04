#include "portion.h"

#include <ydb/core/formats/arrow/arrow_helpers.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/portions/data_accessor.h>
#include <ydb/core/tx/columnshard/engines/scheme/filtered_scheme.h>

namespace NKikimr::NOlap {

class TPortionsNormalizer::TNormalizerResult: public INormalizerChanges {
    std::vector<TPortionDataAccessor> Portions;
    std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> Schemas;

public:
    TNormalizerResult(std::vector<TPortionDataAccessor>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas)
        : Portions(std::move(portions))
        , Schemas(schemas) {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        TDbWrapper db(txc.DB, nullptr);

        for (auto&& portionInfo : Portions) {
            auto schema = Schemas->FindPtr(portionInfo.GetPortionInfo().GetPortionId());
            AFL_VERIFY(!!schema)("portion_id", portionInfo.GetPortionInfo().GetPortionId());
            portionInfo.SaveToDatabase(db, (*schema)->GetIndexInfo().GetPKFirstColumnId(), true);
        }
        return true;
    }

    ui64 GetSize() const override {
        return Portions.size();
    }
};

bool TPortionsNormalizer::CheckPortion(const NColumnShard::TTablesManager&, const TPortionDataAccessor& portionInfo) const {
    return KnownPortions.contains(portionInfo.GetPortionInfo().GetAddress());
}

INormalizerTask::TPtr TPortionsNormalizer::BuildTask(
    std::vector<TPortionDataAccessor>&& portions, std::shared_ptr<THashMap<ui64, ISnapshotSchema::TPtr>> schemas) const {
    return std::make_shared<TTrivialNormalizerTask>(std::make_shared<TNormalizerResult>(std::move(portions), schemas));
}

TConclusion<bool> TPortionsNormalizer::DoInitImpl(const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;

    NIceDb::TNiceDb db(txc.DB);
    bool ready = true;
    ready = ready & Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme());
    if (!ready) {
        return TConclusionStatus::Fail("Not ready");
    }

    {
        auto rowset = db.Table<Schema::IndexPortions>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }

        while (!rowset.EndOfSet()) {
            TPortionAddress portionAddr(rowset.GetValue<Schema::IndexPortions::PathId>(), rowset.GetValue<Schema::IndexPortions::PortionId>());
            KnownPortions.insert(portionAddr);
            if (!rowset.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
    }

    return true;
}

}   // namespace NKikimr::NOlap
