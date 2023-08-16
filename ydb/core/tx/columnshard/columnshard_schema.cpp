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

}
