#include "tx_start_source_cursor.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/scheme/schema_version.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxStartSourceCursor::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;

    std::vector<NOlap::TSchemaPresetVersionInfo> schemeHistory;

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

    std::sort(schemeHistory.begin(), schemeHistory.end());

    Session->StartCursor(*Self, std::move(Portions), std::move(schemeHistory));
    return true;
}

void TTxStartSourceCursor::DoComplete(const TActorContext& /*ctx*/) {
}

}   // namespace NKikimr::NOlap::NDataSharing
