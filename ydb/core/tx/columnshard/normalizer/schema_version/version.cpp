#include "version.h"

namespace NKikimr::NOlap {

class TSchemaVersionNormalizer::TNormalizerResult: public INormalizerChanges {
private:
    class TKey {
    private:
        std::optional<ui64> Version;

    public:
        ui64 Step;
        ui64 TxId;
        ui32 Id;

    public:
        TKey() = default;

        ui64 GetVersion() const {
            AFL_VERIFY(Version);
            return *Version;
        }

        TKey(ui32 id, ui64 step, ui64 txId, const std::optional<ui64> version)
            : Version(version)
            , Step(step)
            , TxId(txId)
            , Id(id) {
        }

        bool operator<(const TKey& item) const {
            if (Id == item.Id) {
                const bool result = std::tie(Step, TxId) < std::tie(item.Step, item.TxId);
                if (Version && item.Version) {
                    const bool resultVersions = Version < item.Version;
                    AFL_VERIFY(result == resultVersions);
                }
                return result;
            } else {
                return Id < item.Id;
            }
        }

        bool operator==(const TKey& item) const {
            return std::tie(Id, Step, TxId, Version) == std::tie(item.Id, item.Step, item.TxId, item.Version);
        }
    };

    class TTableKey {
    public:
        TInternalPathId PathId;
        ui64 Step;
        ui64 TxId;

    public:
        TTableKey(TInternalPathId pathId, ui64 step, ui64 txId)
            : PathId(pathId)
            , Step(step)
            , TxId(txId) {
        }
    };

    std::vector<TKey> VersionsToRemove;
    std::vector<TTableKey> TableVersionsToRemove;

public:
    TNormalizerResult(std::vector<TKey>&& versions, std::vector<TTableKey>&& tableVersions)
        : VersionsToRemove(versions)
        , TableVersionsToRemove(tableVersions) {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& /* normController */) const override {
        using namespace NColumnShard;
        NIceDb::TNiceDb db(txc.DB);
        for (auto& key : VersionsToRemove) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing schema version in TSchemaVersionNormalizer")("version", key.GetVersion());
            db.Table<Schema::SchemaPresetVersionInfo>().Key(key.Id, key.Step, key.TxId).Delete();
        }
        for (auto& key : TableVersionsToRemove) {
            AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "Removing table version in TSchemaVersionNormalizer")("pathId", key.PathId)(
                "plan_step", key.Step)("tx_id", key.TxId);
            db.Table<Schema::TableVersionInfo>().Key(key.PathId.GetRawValue(), key.Step, key.TxId).Delete();
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

        std::map<TKey, bool> schemaIdUsability;
        std::vector<TTableKey> unusedTableSchemaIds;
        std::vector<INormalizerChanges::TPtr> changes;

        {
            THashMap<ui32, TKey> maxByPresetId;
            auto rowset = db.Table<Schema::SchemaPresetVersionInfo>().Select();
            if (!rowset.IsReady()) {
                return std::nullopt;
            }
            while (!rowset.EndOfSet()) {
                const ui32 id = rowset.GetValue<Schema::SchemaPresetVersionInfo::Id>();
                NKikimrTxColumnShard::TSchemaPresetVersionInfo info;
                Y_ABORT_UNLESS(info.ParseFromString(rowset.GetValue<Schema::SchemaPresetVersionInfo::InfoProto>()));
                AFL_VERIFY(info.HasSchema());
                ui64 version = info.GetSchema().GetVersion();
                TKey presetVersionKey(id, rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceStep>(),
                    rowset.GetValue<Schema::SchemaPresetVersionInfo::SinceTxId>(), version);
                auto it = maxByPresetId.find(id);
                if (it == maxByPresetId.end()) {
                    it = maxByPresetId.emplace(id, presetVersionKey).first;
                } else if (it->second < presetVersionKey) {
                    it->second = presetVersionKey;
                }
                AFL_VERIFY(schemaIdUsability.emplace(presetVersionKey, usedSchemaVersions.contains(version)).second);

                if (!rowset.Next()) {
                    return std::nullopt;
                }
            }
            for (auto&& i : maxByPresetId) {
                auto it = schemaIdUsability.find(i.second);
                AFL_VERIFY(it != schemaIdUsability.end());
                AFL_VERIFY(it->first == i.second);
                it->second = true;
            }
            {
                auto rowset = db.Table<Schema::TableVersionInfo>().Select();
                if (!rowset.IsReady()) {
                    return std::nullopt;
                }

                while (!rowset.EndOfSet()) {
                    const auto pathId = TInternalPathId::FromRawValue(rowset.GetValue<Schema::TableVersionInfo::PathId>());

                    NKikimrTxColumnShard::TTableVersionInfo versionInfo;
                    Y_ABORT_UNLESS(versionInfo.ParseFromString(rowset.GetValue<Schema::TableVersionInfo::InfoProto>()));
                    auto it = schemaIdUsability.find(TKey(versionInfo.GetSchemaPresetId(),
                        rowset.GetValue<Schema::TableVersionInfo::SinceStep>(), rowset.GetValue<Schema::TableVersionInfo::SinceTxId>(), {}));
                    AFL_VERIFY(it != schemaIdUsability.end());
                    if (!it->second) {
                        unusedTableSchemaIds.emplace_back(pathId, rowset.GetValue<Schema::TableVersionInfo::SinceStep>(),
                            rowset.GetValue<Schema::TableVersionInfo::SinceTxId>());
                    }

                    if (!rowset.Next()) {
                        return std::nullopt;
                    }
                }
            }

            std::vector<TTableKey> tableVersionToRemove;
            std::vector<TKey> presetVersionsToRemove;
            auto addNormalizationTask = [&](const ui32 limit) {
                if (presetVersionsToRemove.size() + tableVersionToRemove.size() > limit) {
                    changes.emplace_back(
                        std::make_shared<TNormalizerResult>(std::move(presetVersionsToRemove), std::move(tableVersionToRemove)));
                    presetVersionsToRemove = std::vector<TKey>();
                    tableVersionToRemove = std::vector<TTableKey>();
                }
            };
            for (const auto& id : schemaIdUsability) {
                if (!id.second) {
                    presetVersionsToRemove.push_back(id.first);
                    addNormalizationTask(10000);
                }
            }

            for (const auto& id : unusedTableSchemaIds) {
                tableVersionToRemove.push_back(id);
                addNormalizationTask(10000);
            }

            addNormalizationTask(0);
            return changes;
        }
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TSchemaVersionNormalizer::DoInit(
    const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) {
    auto changes = TNormalizerResult::Init(txc);
    if (!changes) {
        return TConclusionStatus::Fail("Not ready");
    }
    std::vector<INormalizerTask::TPtr> tasks;
    for (auto&& c : *changes) {
        tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(c));
    }
    return tasks;
}

}   // namespace NKikimr::NOlap
