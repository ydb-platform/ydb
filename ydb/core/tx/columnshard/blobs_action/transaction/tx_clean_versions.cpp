#include "tx_clean_versions.h"

#include <ydb/core/tx/columnshard/columnshard_impl.h>

namespace NKikimr::NColumnShard {

// For each schema version in erase list find nearest previous and next schema versions which are not scheduled for erasing
std::vector<std::pair<ui64, ui64>> TTxSchemaVersionsCleanup::GetPrevNextSchemas() const {
    std::vector<std::pair<ui64, ui64>> prevNextSchemas;
    THashSet<ui64> checkedSchemas;
    const NOlap::TVersionedIndex& versionedIndex = Self->TablesManager.MutablePrimaryIndex().GetVersionedIndex();
    const std::map<ui64, NOlap::ISnapshotSchema::TPtr>& snapshotByVersion = versionedIndex.GetSnapshotByVersion();
    for (const ui64 schemaVersion: VersionsToRemove) {
        if (checkedSchemas.find(schemaVersion) != checkedSchemas.end()) {
            continue;
        }
        auto iter = snapshotByVersion.find(schemaVersion);
        AFL_VERIFY(iter != snapshotByVersion.cend());
        auto prevIter = iter;
        ui64 prevVersion = 0;
        while (prevIter != snapshotByVersion.cbegin()) {
            prevIter--;
            if (!VersionsToRemove.contains(prevIter->first)) {
                prevVersion = prevIter->first;
                break;
            }
            checkedSchemas.insert(prevIter->first);
        }
        auto nextIter = iter;
        nextIter++;
        ui64 nextVersion = 0;
        while (nextIter != snapshotByVersion.cend()) {
            if (VersionsToRemove.find(nextIter->first) == VersionsToRemove.end()) {
                nextVersion = nextIter->first;
                break;
            }
            checkedSchemas.insert(nextIter->first);
            nextIter++;
        }
        prevNextSchemas.emplace_back(prevVersion, nextVersion);
    }
    return prevNextSchemas;
}

bool TTxSchemaVersionsCleanup::Execute(TTransactionContext& txc, const TActorContext& /*ctx*/) {
    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "TTxSchemaVersionsCleanup::Execute")("tablet_id", Self->TabletID());
    NIceDb::TNiceDb db(txc.DB);
    auto table = db.Table<NKikimr::NColumnShard::Schema::SchemaPresetVersionInfo>();

    auto getSchemaPresetInfo = [&](const NOlap::TVersionCounters::TSchemaKey& pkey, NKikimrTxColumnShard::TSchemaPresetVersionInfo& info) {
        auto rowset = table.Key(pkey.GetId(), pkey.GetPlanStep(), pkey.GetTxId()).Select();
        AFL_VERIFY(rowset.IsReady() && !rowset.EndOfSet());
        Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
    };

    auto updateDiff = [&](const auto& key, auto&& modifier) {
        NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
        getSchemaPresetInfo(key, info);
        modifier(info);
        TString serialized;
        Y_ABORT_UNLESS(info.SerializeToString(&serialized));
        table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Update(NIceDb::TUpdate<Schema::SchemaPresetVersionInfo::InfoProto>(serialized));
    };

    auto updateDiffs = [&](const ui64 schemaVersion, auto&& modifier) {
        auto iter = Self->VersionCounters->GetVersionToKey().find(schemaVersion);
        AFL_VERIFY(iter != Self->VersionCounters->GetVersionToKey().end());
        for (const NOlap::TVersionCounters::TSchemaKey& key: iter->second) {
            updateDiff(key, modifier);
        }
    };

    auto tryUpdateFromSchemas = [&](const std::pair<ui64, ui64>& prevNext) {
        NKikimrTxColumnShard::TSchemaPresetVersionInfo pinfo;
        auto prevIter = Self->VersionCounters->GetVersionToKey().find(prevNext.first);
        AFL_VERIFY(prevIter != Self->VersionCounters->GetVersionToKey().end());
        getSchemaPresetInfo(*prevIter->second.cbegin(), pinfo);
        if (!pinfo.has_schema()) {
            return false;
        }

        NKikimrTxColumnShard::TSchemaPresetVersionInfo ninfo;
        auto nextIter = Self->VersionCounters->GetVersionToKey().find(prevNext.second);
        AFL_VERIFY(nextIter != Self->VersionCounters->GetVersionToKey().end());
        getSchemaPresetInfo(*nextIter->second.cbegin(), ninfo);
        if (!ninfo.has_schema()) {
            return false;
        }

        auto schemaDiff = NOlap::TSchemaDiffView::MakeSchemasDiff(pinfo.schema(), ninfo.schema());

        for (const NOlap::TVersionCounters::TSchemaKey& key: nextIter->second) {
            updateDiff(key, [&](NKikimrTxColumnShard::TSchemaPresetVersionInfo& info){
                *info.MutableDiff() = schemaDiff;
            });
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Updating diff in version from db")("vesion", prevNext.second)("base version", prevNext.first)("tablet_id", Self->TabletID());
        }
        return true;
    };

    std::vector<std::pair<ui64, ui64>> prevNextSchemaVersions = GetPrevNextSchemas();
    for (const auto& prevNext: prevNextSchemaVersions) {
        if (prevNext.first == 0) {
            AFL_VERIFY(prevNext.second != 0);

            updateDiffs(prevNext.second, [&](NKikimrTxColumnShard::TSchemaPresetVersionInfo& info){
                AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Clearing schema diff in db")("vesion", prevNext.second)("tablet_id", Self->TabletID());
                info.ClearDiff();
            });
        } else {
            if (!tryUpdateFromSchemas(prevNext)) {
                NOlap::TSchemaDiffView newDiff;
                auto iter = Self->VersionCounters->GetVersionToKey().find(prevNext.first);
                auto& key = *iter->second.cbegin();
                AFL_VERIFY(iter != Self->VersionCounters->GetVersionToKey().end());
                auto rowset = table.GreaterOrEqual(key.GetId(), key.GetPlanStep(), key.GetTxId()).Select();
                AFL_VERIFY(rowset.IsReady() && !rowset.EndOfSet());
                std::vector<NKikimrSchemeOp::TColumnTableSchemaDiff> diffProtos;
                while (!rowset.EndOfSet()) {
                    TSchemaPreset::TSchemaPresetVersionInfo info;
                    Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
                    ui64 schemaVersion = info.GetSchema().GetVersion();
                    if (schemaVersion > prevNext.second) {
                        break;
                    }
                    if (schemaVersion > prevNext.first) {
                        diffProtos.push_back(info.GetDiff());
                    }
                    if (!rowset.Next()) {
                        break;
                    }
                }
                AFL_VERIFY(diffProtos.size() > 0);
                for (auto& diffProto: diffProtos) {
                    NOlap::TSchemaDiffView diff;
                    diff.DeserializeFromProto(diffProto);
                    newDiff.AddNext(diff);
                }
                NKikimrSchemeOp::TColumnTableSchemaDiff newDiffProto;
                newDiff.SerializeToProto(newDiffProto);
                updateDiffs(prevNext.second, [&](NKikimrTxColumnShard::TSchemaPresetVersionInfo& info) {
                    AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Updating diff in version from db")("vesion", prevNext.second)("base version", prevNext.first)("tablet_id", Self->TabletID());
                    *info.MutableDiff() = newDiffProto;
                });
            }
        }
    }

    for (const ui64 version: VersionsToRemove) {
        auto iter = Self->VersionCounters->GetVersionToKey().find(version);
        AFL_VERIFY(iter != Self->VersionCounters->GetVersionToKey().end());
        for (const NOlap::TVersionCounters::TSchemaKey& key: iter->second) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing schema version from db")("vesion", version)("tablet_id", Self->TabletID());
            table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Delete();
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
