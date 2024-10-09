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

    class TTableKey {
    public:
        ui64 PathId;
        ui64 Step;
        ui64 TxId;
        ui64 Version;

    public:
        TTableKey(ui64 pathId, ui64 step, ui64 txId, ui64 version)
            : PathId(pathId)
            , Step(step)
            , TxId(txId)
            , Version(version)
        {
        }
    };

    std::vector<TKey> VersionsToRemove;
    std::vector<TTableKey> TableVersionsToRemove;

public:
    TNormalizerResult(std::vector<TKey>&& versions, std::vector<TTableKey>&& tableVersions)
        : VersionsToRemove(versions)
        , TableVersionsToRemove(tableVersions)
    {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (auto& key: VersionsToRemove) {
            LOG_S_DEBUG("Removing schema version in TSchemaVersionNormalizer " << key.Version);
            db.Table<Schema::SchemaPresetVersionInfo>().Key(key.Id, key.Step, key.TxId).Delete();
        }
        for (auto& key: TableVersionsToRemove) {
            LOG_S_DEBUG("Removing table version in TSchemaVersionNormalizer " << key.Version << " pathId " << key.PathId);
            db.Table<Schema::TableVersionInfo>().Key(key.PathId, key.Step, key.TxId).Delete();
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
                        return std::nullopt;
                    }
                }
            } else {
                return std::nullopt;
            }
        }
        {
            auto rowset = db.Table<Schema::InsertTable>().Select();
            if (rowset.IsReady()) {
                while (!rowset.EndOfSet()) {
                    if (rowset.HaveValue<Schema::InsertTable::SchemaVersion>()) {
                        usedSchemaVersions.insert(rowset.GetValue<Schema::InsertTable::SchemaVersion>());
                        if (!rowset.Next()) {
                            return std::nullopt;
                        }
                    }
                }
            } else {
                return std::nullopt;
            }
        }

        std::vector<TKey> unusedSchemaIds;
        std::vector<TTableKey> unusedTableSchemaIds;
        std::optional<ui64> maxVersion;
        std::vector<INormalizerChanges::TPtr> changes;

        {
            auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
            if (rowset.IsReady()) {
                while (!rowset.EndOfSet()) {
                    const ui32 id = rowset.GetValue<Schema::SchemaPresetVersionInfo::Id>();
                    NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
                    Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
                    if (info.HasSchema()) {
                        ui64 version = info.GetSchema().GetVersion();
                        if (!maxVersion.has_value() || (version > *maxVersion)) {
                            maxVersion = version;
                        }
                        if (!usedSchemaVersions.contains(version)) {
                            unusedSchemaIds.emplace_back(id, rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(), rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>(), version);
                        }
                    }

                    if (!rowset.Next()) {
                        return std::nullopt;
                    }
                }
            } else {
                return std::nullopt;
            }
        }

        {
            auto rowset = db.Table<Schema::TableVersionInfo>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }

            while (!rowset.EndOfSet()) {
                const ui64 pathId = rowset.GetValue<Schema::TableVersionInfo::PathId>();

                NKikimrTxColumnShard::TTableVersionInfo versionInfo;
                Y_ABORT_UNLESS(versionInfo.ParseFromString(rowset.GetValue<Schema::TableVersionInfo::InfoProto>()));
                if (versionInfo.HasSchema()) {
                    ui64 version = versionInfo.GetSchema().GetVersion();
                    if (!usedSchemaVersions.contains(version)) {
                        unusedTableSchemaIds.emplace_back(pathId, rowset.GetValue<Schema::TableVersionInfo::SinceStep>(), rowset.GetValue<Schema::TableVersionInfo::SinceTxId>(), version);
                    }
                }

                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }
        }

        std::vector<TTableKey> tablePortion;
        std::vector<TKey> portion;
        tablePortion.reserve(10000);
        portion.reserve(10000);
        auto addPortion = [&]() {
            if (portion.size() + tablePortion.size() >= 10000) {
                changes.emplace_back(std::make_shared<TNormalizerResult>(std::move(portion), std::move(tablePortion)));
                portion = std::vector<TKey>();
                tablePortion = std::vector<TTableKey>();
            }
        };
        for (const auto& id: unusedSchemaIds) {
            if (!maxVersion.has_value() || (id.Version != *maxVersion)) {
                portion.push_back(id);
                addPortion();
            }
        }

        for (const auto& id: unusedTableSchemaIds) {
            if (!maxVersion.has_value() || (id.Version != *maxVersion)) {
                tablePortion.push_back(id);
                addPortion();
            }
        }

        if (portion.size() + tablePortion.size() > 0) {
            changes.emplace_back(std::make_shared<TNormalizerResult>(std::move(portion), std::move(tablePortion)));
        }
        return changes;
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
