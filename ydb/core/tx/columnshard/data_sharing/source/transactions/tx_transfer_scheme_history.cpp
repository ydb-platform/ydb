#include "tx_transfer_scheme_history.h"

#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NDataSharing {

bool TTxTransferSchemeHistory::DoExecute(NTabletFlatExecutor::TTransactionContext& txc, const TActorContext& /*ctx*/) {
    using namespace NColumnShard;

    std::vector<NKikimrColumnShardDataSharingProto::TSchemeHistoryEntry> schemeHostory;

    NIceDb::TNiceDb db(txc.DB);

    auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
    if (!rowset.IsReady()) {
        return false;
    }

    while (!rowset.EndOfSet()) {
        NOlap::TSnapshot version(
            rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(), rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>());

        TSchemaPreset::TSchemaPresetVersionInfo info;
        Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));

        NKikimrSchemeOp::TColumnTableSchema schema = info.GetSchema();

        if (!rowset.Next()) {
            return false;
        }

        NKikimrColumnShardDataSharingProto::TSchemeHistoryEntry entry;

        *entry.MutableSnapshot() = version.SerializeToProto();
        *entry.MutableScheme() = schema;

        schemeHostory.push_back(entry);
    }

    auto ev = std::make_unique<NEvents::TEvTransferSchemeHistory>(SessionId, schemeHostory, SourceTabletId);

    NActors::TActivationContext::AsActorContext()
        .Send(MakePipePerNodeCacheID(false),
            new TEvPipeCache::TEvForward(ev.release(), (ui64)DestinationTabletId, true), IEventHandle::FlagTrackDelivery, RuntimeId);
    return true;
}

void TTxTransferSchemeHistory::DoComplete(const TActorContext& /*ctx*/) {
}

}   // namespace NKikimr::NOlap::NDataSharing
