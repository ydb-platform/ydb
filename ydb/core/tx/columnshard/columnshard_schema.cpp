#include "columnshard_schema.h"
#include "transactions/tx_controller.h"

namespace NKikimr::NColumnShard {

void Schema::SaveTxInfo(NIceDb::TNiceDb& db, const TFullTxInfo& txInfo, const TString& txBody) {
    AFL_VERIFY(txInfo.TxKind != NKikimrTxColumnShard::TX_KIND_NONE);
    db.Table<TxInfo>().Key(txInfo.TxId).Update(
        NIceDb::TUpdate<TxInfo::TxKind>(txInfo.TxKind),
        NIceDb::TUpdate<TxInfo::TxBody>(txBody),
        NIceDb::TUpdate<TxInfo::MaxStep>(txInfo.MaxStep),
        NIceDb::TUpdate<TxInfo::Source>(txInfo.Source),
        NIceDb::TUpdate<TxInfo::Cookie>(txInfo.Cookie),
        NIceDb::TUpdate<TxInfo::SeqNo>(txInfo.SerializeSeqNoAsString())
        );
}

void Schema::UpdateTxInfoSource(NIceDb::TNiceDb& db, const TFullTxInfo& txInfo) {
    db.Table<TxInfo>()
        .Key(txInfo.GetTxId())
        .Update(NIceDb::TUpdate<TxInfo::Source>(txInfo.Source), NIceDb::TUpdate<TxInfo::Cookie>(txInfo.Cookie),
            NIceDb::TUpdate<TxInfo::SeqNo>(txInfo.SerializeSeqNoAsString()));
}

void Schema::UpdateTxInfoBody(NIceDb::TNiceDb& db, const ui64 txId, const TString& txBody) {
    db.Table<TxInfo>().Key(txId).Update(NIceDb::TUpdate<TxInfo::TxBody>(txBody));
}

}   // namespace NKikimr::NColumnShard
