#include "tx_start_source_cursor.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxStartSourceCursor::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;

    std::vector<NKikimrTxColumnShard::TSchemaPresetVersionInfo> schemeHistory;

    NIceDb::TNiceDb db(txc.DB);

    auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        TSchemaPreset::TSchemaPresetVersionInfo info;
        Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));

        schemeHistory.push_back(info);

        if (!rowset.Next()) {
            return false;
        }
    }

    std::sort(schemeHistory.begin(), schemeHistory.end(), [](const auto& lhs, const auto& rhs) {
        if (lhs.GetId() != rhs.GetId()) {
            return lhs.GetId() < rhs.GetId();
        }
        if (lhs.GetSinceStep() != rhs.GetSinceStep()) {
            return lhs.GetSinceStep() < rhs.GetSinceStep();
        }
        return lhs.GetSinceTxId() < rhs.GetSinceTxId();
    });

    Session->StartCursor(*Self, std::move(Portions), std::move(schemeHistory));
    return true;
}

void TTxStartSourceCursor::DoComplete(const TActorContext& /*ctx*/) {
}

}   // namespace NKikimr::NOlap::NDataSharing
