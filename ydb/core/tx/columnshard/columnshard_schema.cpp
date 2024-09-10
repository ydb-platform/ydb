#include "columnshard_schema.h"
#include "transactions/tx_controller.h"

namespace NKikimr::NColumnShard {

bool Schema::InsertTable_Load(NIceDb::TNiceDb& db, const IBlobGroupSelector* dsGroupSelector, NOlap::TInsertTableAccessor& insertTable, const TInstant& /*loadTime*/) {
    auto rowset = db.Table<InsertTable>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        EInsertTableIds recType = (EInsertTableIds)rowset.GetValue<InsertTable::Committed>();
        const ui64 planStep = rowset.GetValue<InsertTable::PlanStep>();
        const ui64 writeTxId = rowset.GetValueOrDefault<InsertTable::WriteTxId>();
        const ui64 pathId = rowset.GetValue<InsertTable::PathId>();
        const TString dedupId = rowset.GetValue<InsertTable::DedupId>();
        const ui64 schemaVersion = rowset.HaveValue<InsertTable::SchemaVersion>() ? rowset.GetValue<InsertTable::SchemaVersion>() : 0;

        TString error;
        NOlap::TUnifiedBlobId blobId = NOlap::TUnifiedBlobId::ParseFromString(rowset.GetValue<InsertTable::BlobId>(), dsGroupSelector, error);
        Y_ABORT_UNLESS(blobId.IsValid(), "Failied to parse blob id: %s", error.c_str());

        NKikimrTxColumnShard::TLogicalMetadata meta;
        if (auto metaStr = rowset.GetValue<InsertTable::Meta>()) {
            Y_ABORT_UNLESS(meta.ParseFromString(metaStr));
        }

        std::optional<ui64> rangeOffset;
        if (rowset.HaveValue<InsertTable::BlobRangeOffset>()) {
            rangeOffset = rowset.GetValue<InsertTable::BlobRangeOffset>();
        }
        std::optional<ui64> rangeSize;
        if (rowset.HaveValue<InsertTable::BlobRangeSize>()) {
            rangeSize = rowset.GetValue<InsertTable::BlobRangeSize>();
        }
        AFL_VERIFY(!!rangeOffset == !!rangeSize);

        auto userData = std::make_shared<NOlap::TUserData>(pathId,
            NOlap::TBlobRange(blobId, rangeOffset.value_or(0), rangeSize.value_or(blobId.BlobSize())), meta, schemaVersion, std::nullopt);

        switch (recType) {
            case EInsertTableIds::Inserted:
                insertTable.AddInserted(NOlap::TInsertedData((TInsertWriteId)writeTxId, userData), true);
                break;
            case EInsertTableIds::Committed:
                insertTable.AddCommitted(NOlap::TCommittedData(userData, planStep, writeTxId, dedupId), true);
                break;
            case EInsertTableIds::Aborted:
                insertTable.AddAborted(NOlap::TInsertedData((TInsertWriteId)writeTxId, userData), true);
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
