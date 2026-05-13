#include "broken_txs.h"

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

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
            YDB_LOG_WARN("",
                {"tx_id", txId},
                {"event", "removed_by_normalizer"},
                {"condition", "no_kind"});
            Schema::EraseTxInfo(db, txId);
        }

        if (!rowset.Next()) {
            return TConclusionStatus::Fail("cannot read TxInfo");
        }
    }
    return std::vector<INormalizerTask::TPtr>();
}

}   // namespace NKikimr::NOlap
