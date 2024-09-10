#include "columnshard_schema.h"
#include "transactions/tx_controller.h"

namespace NKikimr::NColumnShard {

bool Schema::InsertTable_Load(NIceDb::TNiceDb& db, const IBlobGroupSelector* dsGroupSelector, NOlap::TInsertTableAccessor& insertTable, const TInstant& /*loadTime*/) {
    auto rowset = db.Table<InsertTable>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        TInsertTableRecordLoadContext constructor;
        AFL_VERIFY(constructor.ParseFromDatabase(rowset));

        switch (recType) {
            case EInsertTableIds::Inserted:
                insertTable.AddInserted(constructor.BuildInsertedOrAborted(), true);
                break;
            case EInsertTableIds::Committed:
                insertTable.AddCommitted(constructor.BuildCommitted(), true);
                break;
            case EInsertTableIds::Aborted:
                insertTable.AddAborted(constructor.BuildInsertedOrAborted(), true);
                break;
        }
        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

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
