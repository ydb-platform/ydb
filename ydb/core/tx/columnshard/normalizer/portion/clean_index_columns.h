#pragma once

#include "clean_unused_tables_template.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;

using TUnusedIndexColumns = TCleanUnusedTablesNormalizerTemplate<Schema::IndexColumns>;

class TCleanIndexColumnsNormalizer final: public TUnusedIndexColumns {
    using TBase = TUnusedIndexColumns;

    static TString ClassName() {
        return "CleanIndexColumns";
    }

    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TCleanIndexColumnsNormalizer>(ClassName());

public:
    explicit TCleanIndexColumnsNormalizer(const TNormalizationController::TInitContext& ctx)
        : TBase(ctx) {
    }

    TString GetClassName() const override {
        return ClassName();
    }

    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }

protected:
    virtual bool ValidateConfig() override {
        return !AppData()->ColumnShardConfig.GetColumnChunksV0Usage();
    }
};

}   // namespace NKikimr::NOlap::NCleanUnusedTables
