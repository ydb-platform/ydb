#include "version.h"

namespace NKikimr::NOlap {

class TSchemaVersionNormalizer::TNormalizerResult : public INormalizerChanges {
private:
    class TKey {
    public:
        ui64 Step;
        ui64 TxId;
        ui64 Version;
        ui32 Id;

    public:
        TKey() = default;

        TKey(ui32 id, ui64 step, ui64 txId, ui64 version)
            : Step(step)
            , TxId(txId)
            , Version(version)
            , Id(id)
        {
        }
    };

    TVector<TKey> VersionsToRemove;
    std::optional<ui64> LastVersion;

public:
    TNormalizerResult(TVector<TKey>&& versions, std::optional<ui64>& lastVersion)
        : VersionsToRemove(versions)
        , LastVersion(lastVersion)
    {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (auto& key: VersionsToRemove) {
            if ((!LastVersion.has_value()) || (key.Version != *LastVersion)) {
                db.Table<Schema::SchemaPresetVersionInfo>().Key(key.Id, key.Step, key.TxId).Delete();
            }
        }
        return true;
    }

    ui64 GetSize() const override {
        return VersionsToRemove.size();
    }

    static std::optional<std::vector<INormalizerChanges::TPtr>> Init(NTabletFlatExecutor::TTransactionContext& txc) {
        using namespace NColumnShard;
        THashSet<ui64> usedSchemaVersions;
        NIceDb::TNiceDb db(txc.DB);
        {
            auto rowset = db.Table<Schema::IndexPortions>().Select();
            if (rowset.IsReady()) {
                while (!rowset.EndOfSet()) {
                    usedSchemaVersions.insert(rowset.GetValue<Schema::IndexPortions::SchemaVersion>());
                    if (!rowset.Next()) {
                        break;
                    }
                }
            }
        }
        {
            auto rowset = db.Table<Schema::InsertTable>().Select();
            if (rowset.IsReady()) {
                while (!rowset.EndOfSet()) {
                    if (rowset.HaveValue<Schema::InsertTable::SchemaVersion>()) {
                        usedSchemaVersions.insert(rowset.GetValue<Schema::InsertTable::SchemaVersion>());
                        if (!rowset.Next()) {
                            break;
                        }
                    }
                }
            }
        }

        TVector<TKey> unusedSchemaIds;
        std::optional<ui64> maxVersion = 0;

        {
            bool emptyUsed = usedSchemaVersions.size() == 0;
            auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
            if (rowset.IsReady()) {
                while (!rowset.EndOfSet()) {
                    const ui32 id = rowset.GetValue<Schema::SchemaPresetVersionInfo::Id>();
                    NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
                    Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
                    if (info.HasSchema()) {
                        ui64 version = info.GetSchema().GetVersion();
                        if (emptyUsed && ((!maxVersion.has_value()) || (version > maxVersion))) {
                            maxVersion = version;
                        }
                        if (!usedSchemaVersions.contains(version)) {
                            unusedSchemaIds.emplace_back(id, rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(), rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>(), version);
                        }
                    }

                    if (!rowset.Next()) {
                        break;
                    }
                }
            }
        }

        if (unusedSchemaIds.size() > 0) {
            std::vector<INormalizerChanges::TPtr> changes;
            changes.emplace_back(std::make_shared<TNormalizerResult>(std::move(unusedSchemaIds), maxVersion));
            return changes;
        } else {
            return std::nullopt;
        }
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TSchemaVersionNormalizer::DoInit(const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) {
    auto changes = TNormalizerResult::Init(txc);
    if (!changes) {
        return TConclusionStatus::Fail("Not ready");;
    }
    std::vector<INormalizerTask::TPtr> tasks;
    for (auto&& c : *changes) {
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(c));
    }
    return tasks;
}

}
