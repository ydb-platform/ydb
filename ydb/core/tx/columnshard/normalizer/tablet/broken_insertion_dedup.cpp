#include "broken_insertion_dedup.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TInsertionsDedupNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    using namespace NColumnShard;
    auto rowset = db.Table<NColumnShard::Schema::InsertTable>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("cannot read TxInfo");
    }

    THashMap<TInsertWriteId, TInsertTableRecordLoadContext> aborted;
    THashMap<TInsertWriteId, TInsertTableRecordLoadContext> inserted;
    while (!rowset.EndOfSet()) {
        TInsertTableRecordLoadContext constructor;
        constructor.ParseFromDatabase(rowset);
        AFL_VERIFY(!constructor.GetPlanStep());
        if (constructor.GetRecType() != NColumnShard::Schema::EInsertTableIds::Committed && constructor.GetDedupId()) {
            constructor.Remove(db);
            constructor.SetDedupId("");
            constructor.Upsert(db);
            if (constructor.GetRecType() == NColumnShard::Schema::EInsertTableIds::Aborted) {
                aborted.emplace(constructor.GetInsertWriteId(), constructor);
            } else if (constructor.GetRecType() == NColumnShard::Schema::EInsertTableIds::Inserted) {
                inserted.emplace(constructor.GetInsertWriteId(), constructor);
            } else {
                AFL_VERIFY(false);
            }
        }
        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read TxInfo");
        }
    }

    for (auto&& i : inserted) {
        if (aborted.contains(i.first)) {
            i.second.Remove(db);
        }
    }

    return std::vector<INormalizerTask::TPtr>();
}

}
