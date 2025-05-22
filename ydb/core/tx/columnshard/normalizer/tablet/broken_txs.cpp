#include "broken_txs.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TBrokenTxsNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    using namespace NColumnShard;
    auto rowset = db.Table<Schema::TxInfo>().GreaterOrEqual(0).Select();
    if (!rowset.IsReady()) {
        return TConclusionStatus::Fail("cannot read TxInfo");
    }
    while (!rowset.EndOfSet()) {
        const ui64 txId = rowset.GetValue<Schema::TxInfo::TxId>();
        if (!rowset.HaveValue<Schema::TxInfo::TxKind>()) {
            AFL_WARN(NKikimrServices::TX_COLUMNSHARD)("tx_id", txId)("event", "removed_by_normalizer")("condition", "no_kind");
            Schema::EraseTxInfo(db, txId);
        }

        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read TxInfo");
        }
    }
    return std::vector<INormalizerTask::TPtr>();
}

}
