#include "clean_empty.h"
#include <ydb/core/tx/columnshard/columnshard_schema.h>


namespace NKikimr::NOlap {

namespace {
std::optional<THashSet<TPortionAddress>> GetColumnPortionAddresses(NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);
    if (!Schema::Precharge<Schema::IndexColumns>(db, txc.DB.GetScheme())) {
        return std::nullopt;
    }
    THashSet<TPortionAddress> usedPortions;
    auto rowset = db.Table<Schema::IndexColumns>().Select<
        Schema::IndexColumns::PathId,
        Schema::IndexColumns::Portion
    >();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }
    while (!rowset.EndOfSet()) {
        usedPortions.emplace(
            rowset.GetValue<Schema::IndexColumns::PathId>(),
            rowset.GetValue<Schema::IndexColumns::Portion>()
        );
        if (!rowset.Next()) {
            return std::nullopt;
        }
    }
    return usedPortions;
}

using TBatch = std::vector<TPortionAddress>;

std::optional<std::vector<TBatch>> GetPortionsToDelete(NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    const auto usedPortions = GetColumnPortionAddresses(txc);
    if (!usedPortions) {
        return std::nullopt;
    }
    const size_t MaxBatchSize = 10000;
    NIceDb::TNiceDb db(txc.DB);
    if (!Schema::Precharge<Schema::IndexPortions>(db, txc.DB.GetScheme())) {
        return std::nullopt;
    }
    auto rowset = db.Table<Schema::IndexPortions>().Select<
        Schema::IndexPortions::PathId,
        Schema::IndexPortions::PortionId
    >();
    if (!rowset.IsReady()) {
        return std::nullopt;
    }
    std::vector<TBatch> result;
    TBatch portionsToDelete;
    while (!rowset.EndOfSet()) {
        TPortionAddress addr(
            rowset.GetValue<Schema::IndexPortions::PathId>(),
            rowset.GetValue<Schema::IndexPortions::PortionId>()
        );
//        if (!usedPortions->contains(addr)) {
            ACFL_WARN("normalizer", "TCleanEmptyPortionsNormalizer")("message", TStringBuilder() << addr.DebugString() << " marked for deletion");
            portionsToDelete.emplace_back(std::move(addr));
            if (portionsToDelete.size() == MaxBatchSize) {
                result.emplace_back(std::move(portionsToDelete));
                portionsToDelete = TBatch{};
            }
//        }
        if (!rowset.Next()) {
            return std::nullopt;
        }
    }
    if (!portionsToDelete.empty()) {
        result.emplace_back(std::move(portionsToDelete));
    }
    return result;
}

class TChanges : public INormalizerChanges {
public:
    TChanges(TBatch&& addresses)
        : Addresses(addresses)
    {}
    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for(const auto& a: Addresses) {
            db.Table<Schema::IndexPortions>().Key(
                a.GetPathId(),
                a.GetPortionId()
            ).Delete();
        }
        ACFL_WARN("normalizer", "TCleanEmptyPortionsNormalizer")("message", TStringBuilder() << GetSize() << " portions deleted");
        return true;
    }

    ui64 GetSize() const override {
        return Addresses.size();
    }
private:
    const TBatch Addresses;
};

} //namespace

TConclusion<std::vector<INormalizerTask::TPtr>> TCleanEmptyPortionsNormalizer::DoInit(const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) {
    using namespace NColumnShard;
    auto batchesToDelete = GetPortionsToDelete(txc);
    if (!batchesToDelete) {
         return TConclusionStatus::Fail("Not ready");
    }
    
    std::vector<INormalizerTask::TPtr> result;
    for (auto&& b: *batchesToDelete) {
        result.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(std::move(b))));
    }
    return result;
}

} //namespace NKikimr::NOlap
