#include "broken_txs.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap {

TConclusion<std::vector<INormalizerTask::TPtr>> TBrokenTxsNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    auto rowset = db.Table<Schema::TxInfo>().GreaterOrEqual(0).Select();
    if (!rowset.IsReady()) {
        return false;
    }
    using namespace NColumnShard;
    NIceDb::TNiceDb db(txc.DB);

    while (!rowset.EndOfSet()) {
        const ui64 txId = rowset.GetValue<Schema::TxInfo::TxId>();
        if (!rowset.HaveValue<Schema::TxInfo::TxKind>()) {
            Schema::EraseTxInfo(db, txId);
        }

        if (!rowset.Next()) {
            return false;
        }
    }
    return std::vector<INormalizerTask::TPtr>();
}

}
