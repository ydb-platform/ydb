#include "broken_txs.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TInsertionsDedupNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    using namespace NColumnShard;
    auto rowset = db.Table<InsertTable>().Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("cannot read TxInfo");
    }

    THashMap<TInsertWriteId, TInsertTableRecordLoadContext> aborted;
    THashMap<TInsertWriteId, TInsertTableRecordLoadContext> inserted;
    while (!rowset.EndOfSet()) {
        TInsertTableRecordLoadContext constructor;
        constructor.ParseFromDatabase(rowset);
        AFL_VERIFY(!constructor.GetPlanStep());
        if (!constructor.IsCommitted() && constructor.GetDedupId()) {
            constructor.Erase(db);
            constructor.SetDedupId("");
            constructor.Upsert();
            if (constructor.IsAbortion()) {
                aborted.emplace(constructor.GetInsertWriteId(), constructor);
            } else {
                inserted.emplace(constructor.GetInsertWriteId(), constructor);
            }
        }
        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read TxInfo");
        }
    }

    for (auto&& i : inserted) {
        if (aborted.contains(i.first)) {
            i.second.Erase(db);
        }
    }

    return std::vector<INormalizerTask::TPtr>();
}

}
