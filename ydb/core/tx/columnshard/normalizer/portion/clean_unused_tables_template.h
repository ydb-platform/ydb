#pragma once

#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

#include <memory>
#include <vector>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;
using NIceDb::TNiceDb;

template <typename TTable>
class TCleanUnusedTablesNormalizerTemplate: public TNormalizationController::INormalizerComponent {
    using TBase = TNormalizationController::INormalizerComponent;
    static TString ClassName() {
        return "CleanUnusedTables";
    }

protected:
    virtual bool ValidateConfig() {
        return true;
    }

public:
    explicit TCleanUnusedTablesNormalizerTemplate(const TNormalizationController::TInitContext& ctx)
        : TBase(ctx) {
    }

    TString GetClassName() const override {
        return ClassName();
    }
    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

    TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(
        const TNormalizationController&, NTabletFlatExecutor::TTransactionContext& txc) override {
        AFL_VERIFY(ValidateConfig());

        TNiceDb db(txc.DB);
        std::vector<INormalizerTask::TPtr> tasks;

        if (!db.HaveTable<TTable>()) {
            return tasks;
        }
        tasks.emplace_back(MakeTask());

        return tasks;
    }

private:
    static INormalizerTask::TPtr MakeTask() {
        return std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>());
    }

    class TChanges final: public INormalizerChanges {
    public:
        bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController&) const override {
            txc.DB.Truncate(TTable::TableId);
            return true;
        }

        ui64 GetSize() const override {
            return 1;
        }
    };
};

}   // namespace NKikimr::NOlap::NCleanUnusedTables
