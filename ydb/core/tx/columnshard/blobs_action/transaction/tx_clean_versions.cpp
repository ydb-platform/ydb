#include "tx_clean_versions.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

// For given schema version find nearest previous and next schema versions which are not scheduled for erasing
void TTxSchemaVersionsCleanup::AddPrevNextSchemas(const ui64 schemaVersion, std::vector<std::pair<ui64, ui64>>& prevNextSchemas, THashSet<ui64>& checkedSchemas) const {
    if (checkedSchemas.find(schemaVersion) != checkedSchemas.end()) {
        return;
    }
    const NOlap::TVersionedIndex& versionedIndex = Self->TablesManager.MutablePrimaryIndex().GetVersionedIndex();
    auto begin = versionedIndex.FirstSchema();
    auto end = versionedIndex.End();
    auto iter = versionedIndex.FindSchema(schemaVersion);
    AFL_VERIFY(iter != end);
    auto prevIter = iter;
    ui64 prevVersion = 0;
    while (prevIter != begin) {
        prevIter--;
        if (VersionsToRemove.find(prevIter->first) == VersionsToRemove.end()) {
            prevVersion = prevIter->first;
            break;
        }
        checkedSchemas.insert(prevIter->first);
    }
    auto nextIter = iter;
    nextIter++;
    ui64 nextVersion = 0;
    while (nextIter != end) {
        if (VersionsToRemove.find(nextIter->first) == VersionsToRemove.end()) {
            nextVersion = nextIter->first;
            break;
        }
        checkedSchemas.insert(nextIter->first);
        nextIter++;
    }
    prevNextSchemas.emplace_back(prevVersion, nextVersion);
}

bool TTxSchemaVersionsCleanup::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TTxSchemaVersionsCleanup::Execute")("tablet_id", Self->TabletID());
    NIceDb::TNiceDb db(txc.DB);

    THashSet<ui64> checkedSchemaVersions;
    std::vector<std::pair<ui64, ui64>> prevNextSchemaVersions;

    auto table = db.Table<NKikimr::NColumnShard::Schema::SchemaPresetVersionInfo>();
    for (const ui64 version: VersionsToRemove) {
        AddPrevNextSchemas(version, prevNextSchemaVersions, checkedSchemaVersions);
        auto iter = Self->VersionCounters->GetVersionToKey().find(version);
        AFL_VERIFY(iter != Self->VersionCounters->GetVersionToKey().end());
        for (const NOlap::TVersionCounters::TSchemaKey& key: iter->second) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing schema version from db")("vesion", version)("tablet_id", Self->TabletID());
            table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Delete();
        }
    }
    // Now we need to update schema diffs in the next versions, since base for those diffs are just erased
    for (auto& prevNext: prevNextSchemaVersions) {
        if (prevNext.first == 0) { // No previous versions, just clear diff
            AFL_VERIFY(prevNext.second != 0);
            auto iter = Self->VersionCounters->GetVersionToKey().find(prevNext.second);
            AFL_VERIFY(iter != Self->VersionCounters->GetVersionToKey().end());
            for (const NOlap::TVersionCounters::TSchemaKey& key: iter->second) {
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Clearing schema diif in db")("vesion", prevNext.second)("tablet_id", Self->TabletID());
                auto rowset = table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Select();
                AFL_VERIFY(rowset.IsReady() && !rowset.EndOfSet());
                NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
                Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
                info.ClearDiff();
                TString serialized;
                Y_ABORT_UNLESS(info.SerializeToString(&serialized));
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing diff in version from db")("vesion", info.GetSchema().GetVersion())("tablet_id", Self->TabletID());
                table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Update(NIceDb::TUpdate<Schema::SchemaPresetVersionInfo::InfoProto>(serialized));
            }
        } else {
            if (prevNext.second != 0) {
                // Find previous schema
                auto prevIter = Self->VersionCounters->GetVersionToKey().find(prevNext.first);
                AFL_VERIFY(prevIter != Self->VersionCounters->GetVersionToKey().end());
                const NOlap::TVersionCounters::TSchemaKey& pkey = *prevIter->second.cbegin();
                auto rowset = table.Key(pkey.GetId(), pkey.GetPlanStep(), pkey.GetTxId()).Select();
                AFL_VERIFY(rowset.IsReady() && !rowset.EndOfSet());
                NKikimrTxColumnShard::TSchemaPresetVersionInfo pinfo;
                Y_ABORT_UNLESS(pinfo.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));

                // Find next schema
                auto nextIter = Self->VersionCounters->GetVersionToKey().find(prevNext.second);
                AFL_VERIFY(nextIter != Self->VersionCounters->GetVersionToKey().end());
                const NOlap::TVersionCounters::TSchemaKey& nkey = *nextIter->second.cbegin();
                auto nrowset = table.Key(nkey.GetId(), nkey.GetPlanStep(), nkey.GetTxId()).Select();
                AFL_VERIFY(nrowset.IsReady() && !nrowset.EndOfSet());
                NKikimrTxColumnShard::TSchemaPresetVersionInfo ninfo;
                Y_ABORT_UNLESS(ninfo.ParseFromString(nrowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));

                // Calculate schema diff
                auto schemaDiff = NOlap::TSchemaDiffView::MakeSchemasDiff(pinfo.schema(), ninfo.schema());

                // Update diffs
                for (const NOlap::TVersionCounters::TSchemaKey& key: nextIter->second) {
                    auto rowset = table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Select();
                    AFL_VERIFY(rowset.IsReady() && !rowset.EndOfSet());
                    NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
                    Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
                    *info.MutableDiff() = schemaDiff;
                    TString serialized;
                    Y_ABORT_UNLESS(info.SerializeToString(&serialized));
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Updating diff in version from db")("vesion", info.GetSchema().GetVersion())("tablet_id", Self->TabletID());
                    table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Update(NIceDb::TUpdate<Schema::SchemaPresetVersionInfo::InfoProto>(serialized));
                }
            }
        }
    }

    return true;
}
void TTxSchemaVersionsCleanup::Complete(const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TTxSchemaVersionsCleanup::Complete")("tablet_id", Self->TabletID());

    for (const ui64 version: VersionsToRemove) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing schema version from memory")("vesion", version)("tablet_id", Self->TabletID());
        Self->TablesManager.MutablePrimaryIndex().RemoveSchemaVersion(version);
    }

    Self->BackgroundController.FinishActiveCleanupUnusedSchemaVersions();
}

}
