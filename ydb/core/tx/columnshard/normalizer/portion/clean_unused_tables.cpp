#include "clean_unused_tables.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;
using NIceDb::TNiceDb;

class TChanges final : public INormalizerChanges {
    using TKey = std::tuple<ui32, ui64, ui32, ui64, ui64, ui64, ui32>;
    std::vector<TKey> keys;

public:
    explicit TChanges(std::vector<TKey>&& keys) : keys(std::move(keys)) {}

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc,
                        const TNormalizationController&) const override {
        TNiceDb db(txc.DB);
        for (const auto& k : keys) {
            std::apply([&](auto... parts) {
                db.Table<Schema::IndexColumns>().Key(parts...).Delete();
            }, k);
        }
 
        return true;
    }

    ui64 GetSize() const override { return keys.size(); }
};

TConclusion<std::vector<INormalizerTask::TPtr>>
TCleanUnusedTablesNormalizer::DoInit(const TNormalizationController&,
                                     NTabletFlatExecutor::TTransactionContext& txc) {
    std::vector<TKey> keys;
    TNiceDb db(txc.DB);
    auto rs = db.Table<Schema::IndexColumns>().Select();
    if (!rs.IsReady()) {
        return TConclusionStatus::Fail("IndexColumns not ready");
    }

    while (!rs.EndOfSet()) {
        keys.emplace_back(
            rs.GetValue<Schema::IndexColumns::Index>(),
            rs.GetValue<Schema::IndexColumns::Granule>(),
            rs.GetValue<Schema::IndexColumns::ColumnIdx>(),
            rs.GetValue<Schema::IndexColumns::PlanStep>(),
            rs.GetValue<Schema::IndexColumns::TxId>(),
            rs.GetValue<Schema::IndexColumns::Portion>(),
            rs.GetValue<Schema::IndexColumns::Chunk>()
        );

        if (!rs.Next()) {
            return TConclusionStatus::Fail("IndexColumns iterate");
        }
    }

    std::vector<INormalizerTask::TPtr> tasks;
    for (size_t i = 0; i < keys.size(); i += BATCH) {
        size_t to = std::min(i + BATCH, keys.size());
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(
            std::make_shared<TChanges>(
                std::vector<TKey>(keys.begin() + i, keys.begin() + to))));
    }

    return tasks;
}

} // namespace NKikimr::NOlap::NCleanUnusedTables