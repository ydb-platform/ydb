#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/tables_manager.h>

namespace NKikimr::NOlap {
namespace {

class TDecimalPrimaryKeyChanges: public INormalizerChanges {
    THashMap<TPortionAddress, NKikimrTxColumnShard::TIndexPortionMeta> Portions;

public:
    explicit TDecimalPrimaryKeyChanges(THashMap<TPortionAddress, NKikimrTxColumnShard::TIndexPortionMeta>&& portions)
        : Portions(std::move(portions))
    {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
        NIceDb::TNiceDb db(txc.DB);
        using TTable = NColumnShard::Schema::IndexPortions;
        for (const auto& [address, meta] : Portions) {
            db.Table<TTable>()
                .Key(address.GetPathId().GetRawValue(), address.GetPortionId())
                .Update(NIceDb::TUpdate<TTable::Metadata>(meta.SerializeAsString()));
        }
        return true;
    }

    ui64 GetSize() const override {
        return Portions.size();
    }
};

class TDecimalPrimaryKeyNormalizer: public TNormalizationController::INormalizerComponent {
    using TBase = TNormalizationController::INormalizerComponent;
    static inline TFactory::TRegistrator<TDecimalPrimaryKeyNormalizer> Registrator{ "RebuildDecimalPrimaryKey" };

public:
    explicit TDecimalPrimaryKeyNormalizer(const TNormalizationController::TInitContext& context)
        : TBase(context)
    {
    }

    TString GetClassName() const override {
        return "RebuildDecimalPrimaryKey";
    }

    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        // This is a manually scheduled repair, not an automatic startup migration.
        return std::nullopt;
    }

    TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override {
        if (!AppDataVerified().ColumnShardConfig.GetEnableDecimalKeyComparison()) {
            return TConclusionStatus::Fail("RebuildDecimalPrimaryKey requires EnableDecimalKeyComparison");
        }
        NIceDb::TNiceDb db(txc.DB);
        NColumnShard::TTablesManager tables(
            controller.GetStoragesManager(), controller.GetDataAccessorsManager(), std::make_shared<NColumnShard::TPortionIndexStats>(), 0);
        if (!tables.InitFromDB(db, nullptr)) {
            return TConclusionStatus::Fail("Cannot load table schemas");
        }
        std::vector<INormalizerTask::TPtr> tasks;
        if (!tables.HasPrimaryIndex()) {
            return tasks;
        }
        using TTable = NColumnShard::Schema::IndexPortions;
        auto rows = db.Table<TTable>().Select();
        if (!rows.IsReady()) {
            return TConclusionStatus::Fail("Not ready");
        }
        THashMap<TPortionAddress, NKikimrTxColumnShard::TIndexPortionMeta> portions;
        while (!rows.EndOfSet()) {
            const auto schema = tables.GetPrimaryIndexSafe().GetVersionedIndex().GetSchemaVerified(rows.GetValue<TTable::SchemaVersion>());
            bool hasDecimalKey = false;
            for (const auto& [name, type] : schema->GetIndexInfo().GetPrimaryKeyColumns()) {
                Y_UNUSED(name);
                hasDecimalKey |= type.GetTypeId() == NScheme::NTypeIds::Decimal;
            }
            if (hasDecimalKey) {
                TPortionLoadContext portion(rows);
                auto meta = portion.GetMetaProto();
                if (meta.GetDecimalKeyOrder()) {
                    if (!rows.Next()) {
                        return TConclusionStatus::Fail("Not ready");
                    }
                    continue;
                }
                meta.SetRebuildDecimalPrimaryKey(true);
                portions.emplace(portion.GetAddress(), std::move(meta));
                if (portions.size() == 1000) {
                    tasks.emplace_back(
                        std::make_shared<TTrivialNormalizerTask>(std::make_shared<TDecimalPrimaryKeyChanges>(std::move(portions))));
                    portions.clear();
                }
            }
            if (!rows.Next()) {
                return TConclusionStatus::Fail("Not ready");
            }
        }
        if (!portions.empty()) {
            tasks.emplace_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TDecimalPrimaryKeyChanges>(std::move(portions))));
        }
        return tasks;
    }
};

}   // namespace
}   // namespace NKikimr::NOlap
