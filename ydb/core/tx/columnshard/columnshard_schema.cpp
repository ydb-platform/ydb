#include "columnshard_schema.h"

namespace NKikimr::NColumnShard {

bool Schema::InsertTable_Load(NIceDb::TNiceDb& db, const IBlobGroupSelector* dsGroupSelector, NOlap::TInsertTableAccessor& insertTable, const TInstant& /*loadTime*/) {
    auto rowset = db.Table<InsertTable>().GreaterOrEqual(0, 0, 0, 0, "").Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        EInsertTableIds recType = (EInsertTableIds)rowset.GetValue<InsertTable::Committed>();
        ui64 planStep = rowset.GetValue<InsertTable::PlanStep>();
        ui64 writeTxId = rowset.GetValueOrDefault<InsertTable::WriteTxId>();
        ui64 pathId = rowset.GetValue<InsertTable::PathId>();
        TString dedupId = rowset.GetValue<InsertTable::DedupId>();
        TString strBlobId = rowset.GetValue<InsertTable::BlobId>();
        TString metaStr = rowset.GetValue<InsertTable::Meta>();
        ui64 schemaVersion = rowset.HaveValue<InsertTable::SchemaVersion>() ? rowset.GetValue<InsertTable::SchemaVersion>() : 0;

        TString error;
        NOlap::TUnifiedBlobId blobId = NOlap::TUnifiedBlobId::ParseFromString(strBlobId, dsGroupSelector, error);
        Y_ABORT_UNLESS(blobId.IsValid(), "Failied to parse blob id: %s", error.c_str());

        NKikimrTxColumnShard::TLogicalMetadata meta;
        if (metaStr) {
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
        TInsertedData data(planStep, writeTxId, pathId, dedupId, NOlap::TBlobRange(blobId, rangeOffset.value_or(0), rangeSize.value_or(blobId.BlobSize())), meta, schemaVersion, {});

        switch (recType) {
            case EInsertTableIds::Inserted:
                insertTable.AddInserted(std::move(data), true);
                break;
            case EInsertTableIds::Committed:
                insertTable.AddCommitted(std::move(data), true);
                break;
            case EInsertTableIds::Aborted:
                insertTable.AddAborted(std::move(data), true);
                break;
        }
        if (!rowset.Next()) {
            return false;
        }
    }
    return true;
}

}
