#include "columnshard_schema.h"

namespace NKikimr::NColumnShard {

bool Schema::IndexColumns_Load(NIceDb::TNiceDb& db, const IBlobGroupSelector* dsGroupSelector, ui32 index, const std::function<void(const NOlap::TPortionInfo&, const NOlap::TColumnChunkLoadContext&)>& callback) {
    auto rowset = db.Table<IndexColumns>().Prefix(index).Select();
    if (!rowset.IsReady())
        return false;

    while (!rowset.EndOfSet()) {
        NOlap::TPortionInfo portion = NOlap::TPortionInfo::BuildEmpty();
        portion.SetGranule(rowset.GetValue<IndexColumns::Granule>());
        portion.SetMinSnapshot(rowset.GetValue<IndexColumns::PlanStep>(), rowset.GetValue<IndexColumns::TxId>());
        portion.SetPortion(rowset.GetValue<IndexColumns::Portion>());

        NOlap::TColumnChunkLoadContext chunkLoadContext(rowset, dsGroupSelector);

        portion.SetRemoveSnapshot(rowset.GetValue<IndexColumns::XPlanStep>(), rowset.GetValue<IndexColumns::XTxId>());

        callback(portion, chunkLoadContext);

        if (!rowset.Next())
            return false;
    }
    return true;
}

bool Schema::InsertTable_Load(NIceDb::TNiceDb& db, const IBlobGroupSelector* dsGroupSelector, NOlap::TInsertTableAccessor& insertTable, const TInstant& /*loadTime*/) {
    auto rowset = db.Table<InsertTable>().GreaterOrEqual(0, 0, 0, 0, "").Select();
    if (!rowset.IsReady())
        return false;

    while (!rowset.EndOfSet()) {
        EInsertTableIds recType = (EInsertTableIds)rowset.GetValue<InsertTable::Committed>();
        ui64 planStep = rowset.GetValue<InsertTable::PlanStep>();
        ui64 writeTxId = rowset.GetValueOrDefault<InsertTable::WriteTxId>();
        ui64 pathId = rowset.GetValue<InsertTable::PathId>();
        TString dedupId = rowset.GetValue<InsertTable::DedupId>();
        TString strBlobId = rowset.GetValue<InsertTable::BlobId>();
        TString metaStr = rowset.GetValue<InsertTable::Meta>();

        Y_VERIFY(rowset.HaveValue<InsertTable::IndexPlanStep>());
        ui64 indexPlanStep = rowset.GetValue<InsertTable::IndexPlanStep>();
        ui64 indexTxId = rowset.GetValue<InsertTable::IndexTxId>();
        const NOlap::TSnapshot indexSnapshot(indexPlanStep, indexTxId);

        TString error;
        NOlap::TUnifiedBlobId blobId = NOlap::TUnifiedBlobId::ParseFromString(strBlobId, dsGroupSelector, error);
        Y_VERIFY(blobId.IsValid(), "Failied to parse blob id: %s", error.c_str());

        NKikimrTxColumnShard::TLogicalMetadata meta;
        if (metaStr) {
            Y_VERIFY(meta.ParseFromString(metaStr));
        }
        TInsertedData data(planStep, writeTxId, pathId, dedupId, NOlap::TBlobRange(blobId, 0, blobId.BlobSize()), meta, indexSnapshot);

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

        if (!rowset.Next())
            return false;
    }
    return true;
}

}
