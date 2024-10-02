#pragma once

#include <ydb/core/tx/columnshard/normalizer/abstract/abstract.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/defs.h>


namespace NKikimr::NColumnShard {
    class TTablesManager;
}

namespace NKikimr::NOlap {

class TSchemaVersionNormalizer : public TNormalizationController::INormalizerComponent {
public:
    static TString GetClassNameStatic() {
        return "SchemaVersionCleaner";
    }

private:
    static inline TFactory::TRegistrator<TSchemaVersionNormalizer> Registrator = TFactory::TRegistrator<TSchemaVersionNormalizer>(
        GetClassNameStatic());
public:
    class TNormalizerResult;
    class TTask;

public:
    virtual std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

    virtual TString GetClassName() const override {
        return GetClassNameStatic();
    }

    TSchemaVersionNormalizer(const TNormalizationController::TInitContext&) {
    }

    virtual TConclusion<std::vector<INormalizerTask::TPtr>> DoInit(const TNormalizationController& controller, NTabletFlatExecutor::TTransactionContext& txc) override;
};

}
