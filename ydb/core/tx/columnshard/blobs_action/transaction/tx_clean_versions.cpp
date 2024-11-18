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

    auto getSchemaPresetInfo = [&](const NOlap::TVersionCounters::TSchemaKey& pkey, NKikimrTxColumnShard::TSchemaPresetVersionInfo& info)->bool {
        auto rowset = table.Key(pkey.GetId(), pkey.GetPlanStep(), pkey.GetTxId()).Select();
        if (!rowset.IsReady()) {
            return false;
        }
        AFL_VERIFY(!rowset.EndOfSet());
        Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
        return true;
    };

    auto clearDiffAndWrite = [&](const NOlap::TVersionCounters::TSchemaKey& key, NKikimrTxColumnShard::TSchemaPresetVersionInfo& info) {
        info.ClearDiff();
        TString serialized;
        Y_ABORT_UNLESS(info.SerializeToString(&serialized));
        table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Update(NIceDb::TUpdate<Schema::SchemaPresetVersionInfo::InfoProto>(serialized));
    };

    auto updateDiff = [&](const NOlap::TVersionCounters::TSchemaKey& key, auto&& modifier)->bool {
        NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
        if (!getSchemaPresetInfo(key, info)) {
            return false;
        }
        modifier(info);
        TString serialized;
        Y_ABORT_UNLESS(info.SerializeToString(&serialized));
        table.Key(key.GetId(), key.GetPlanStep(), key.GetTxId()).Update(NIceDb::TUpdate<Schema::SchemaPresetVersionInfo::InfoProto>(serialized));
        return true;
    };

    auto getLastSchema = [&](const ui64 schemaVersion, NKikimrTxColumnShard::TSchemaPresetVersionInfo& info)->bool {
        auto foundKeys = Self->VersionCounters->GetVersionToKey().find(schemaVersion);
        AFL_VERIFY(foundKeys != Self->VersionCounters->GetVersionToKey().end());
        return getSchemaPresetInfo(foundKeys->second.back(), info);
    };

    auto updateDiffsBySchemasDiff = [&](const NOlap::TVersionCounters::TSchemaKey& key, const NKikimrTxColumnShard::TSchemaPresetVersionInfo& pinfo, const NKikimrTxColumnShard::TSchemaPresetVersionInfo& ninfo) {
        auto schemaDiff = NOlap::TSchemaDiffView::MakeSchemasDiff(pinfo.schema(), ninfo.schema());

        updateDiff(key, [&](NKikimrTxColumnShard::TSchemaPresetVersionInfo& info) {
            *info.MutableDiff() = schemaDiff;
        });
    };

    auto recalcDiffByRowset = [&](auto& rowset, const ui64 prevSchemaVersion, const ui64 nextSchemaVersion)->bool {
        AFL_VERIFY(rowset.IsReady() && !rowset.EndOfSet());
        std::vector<NKikimrSchemeOp::TColumnTableSchemaDiff> diffProtos;
        std::optional<NKikimrSchemeOp::TColumnTableSchema> firstSchema;
        std::optional<NKikimrSchemeOp::TColumnTableSchema> lastSchema;
        if (!rowset.EndOfSet()) {
            TSchemaPreset::TSchemaPresetVersionInfo info;
            Y_ABORT_UNLESS(info.ParseFromString(rowset.template GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
            ui64 schemaVersion = info.GetSchema().GetVersion();
            if ((schemaVersion == prevSchemaVersion) && info.HasSchema()) {
                firstSchema = info.GetSchema();
            }
        }
        while (!rowset.EndOfSet()) {
            TSchemaPreset::TSchemaPresetVersionInfo info;
            Y_ABORT_UNLESS(info.ParseFromString(rowset.template GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
            ui64 schemaVersion = info.GetSchema().GetVersion();
            if (schemaVersion > prevSchemaVersion) {
                if (info.HasSchema() && firstSchema.has_value()) {
                    lastSchema = info.GetSchema();
                    diffProtos.clear();
                } else {
                    AFL_VERIFY(info.HasDiff());
                    diffProtos.push_back(info.GetDiff());
                }
            }
            if (schemaVersion >= nextSchemaVersion) {
                break;
            }
            if (!rowset.Next()) {
                return false;
            }
        }
        AFL_VERIFY((firstSchema.has_value() && lastSchema.has_value()) || (diffProtos.size() > 0));
        NOlap::TSchemaDiffView newDiff;
        if (firstSchema.has_value() && lastSchema.has_value()) {
            AFL_VERIFY(newDiff.DeserializeFromProto(NOlap::TSchemaDiffView::MakeSchemasDiff(*firstSchema, *lastSchema)).IsSuccess());
        }
        for (const auto& diffProto: diffProtos) {
            NOlap::TSchemaDiffView diff;
            AFL_VERIFY(diff.DeserializeFromProto(diffProto).IsSuccess());
            newDiff.AddNext(diff);
        }
        NKikimrSchemeOp::TColumnTableSchemaDiff newDiffProto;
        newDiff.SerializeToProto(newDiffProto);
        auto foundNextKeys = Self->VersionCounters->GetVersionToKey().find(nextSchemaVersion);
        AFL_VERIFY(foundNextKeys != Self->VersionCounters->GetVersionToKey().end());
        updateDiff(*foundNextKeys->second.cbegin(), [&](NKikimrTxColumnShard::TSchemaPresetVersionInfo& info) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Updating diff in version from db")("vesion", nextSchemaVersion)("base version", prevSchemaVersion)("tablet_id", Self->TabletID());
            *info.MutableDiff() = newDiffProto;
        });
        return true;
    };

    auto recalcDiff = [&](const ui64 prevSchemaVersion, const ui64 nextSchemaVersion)->bool {
        auto foundPrevKeys = Self->VersionCounters->GetVersionToKey().find(prevSchemaVersion);
        AFL_VERIFY(foundPrevKeys != Self->VersionCounters->GetVersionToKey().end());
        auto& key = foundPrevKeys->second.back();
        auto rowset = table.GreaterOrEqual(key.GetId(), key.GetPlanStep(), key.GetTxId()).Select();
        if (!rowset.IsReady()) {
            return false;
        }
        return recalcDiffByRowset(rowset, prevSchemaVersion, nextSchemaVersion);
    };

    auto recalcDiffNoPrev = [&](const ui64 nextSchemaVersion)->bool {
        auto rowset = table.Select();
        if (!rowset.IsReady()) {
            return false;
        }
        return recalcDiffByRowset(rowset, 0, nextSchemaVersion);
    };

    std::vector<std::pair<ui64, ui64>> prevNextSchemaVersions = GetPrevNextSchemas();
    for (const auto& prevNext: prevNextSchemaVersions) {
        AFL_VERIFY(prevNext.second != 0);
        if (prevNext.first == 0) {
            auto foundNextKeys = Self->VersionCounters->GetVersionToKey().find(prevNext.second);
            AFL_VERIFY(foundNextKeys != Self->VersionCounters->GetVersionToKey().end());
            NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
            if (!getSchemaPresetInfo(*foundNextKeys->second.cbegin(), info)) {
                return false;
            }
            if (info.has_schema()) {
                clearDiffAndWrite(*foundNextKeys->second.cbegin(), info);
            } else {
                if (!recalcDiffNoPrev(prevNext.second)) {
                    return false;
                }
            }
        } else {
            NKikimrTxColumnShard::TSchemaPresetVersionInfo pinfo;
            NKikimrTxColumnShard::TSchemaPresetVersionInfo ninfo;

            if (!getLastSchema(prevNext.first, pinfo)) {
                return false;
            }

            auto foundNextKeys = Self->VersionCounters->GetVersionToKey().find(prevNext.second);
            AFL_VERIFY(foundNextKeys != Self->VersionCounters->GetVersionToKey().end());
            if (!getSchemaPresetInfo(*foundNextKeys->second.cbegin(), ninfo)) {
                return false;
            }

            if (pinfo.has_schema() && ninfo.has_schema()) {
                updateDiffsBySchemasDiff(*foundNextKeys->second.cbegin(), pinfo, ninfo);
            } else {
                if (!recalcDiff(prevNext.first, prevNext.second)) {
                    return false;
                }
            }
        }
    }

    for (const ui64 version: VersionsToRemove) {
        auto foundKeysToRemove = Self->VersionCounters->GetVersionToKey().find(version);
        AFL_VERIFY(foundKeysToRemove != Self->VersionCounters->GetVersionToKey().end());
        for (const NOlap::TVersionCounters::TSchemaKey& key: foundKeysToRemove->second) {
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
