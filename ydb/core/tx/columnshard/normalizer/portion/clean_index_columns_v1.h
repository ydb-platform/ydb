#pragma once

#include "clean_unused_tables_template.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;

using TUnusedIndexColumnsV1 = TCleanUnusedTablesNormalizerTemplate<Schema::IndexColumnsV1>;

class TCleanIndexColumnsV1Normalizer final: public TUnusedIndexColumnsV1 {
    using TBase = TUnusedIndexColumnsV1;

    static TString ClassName() {
        return "CleanIndexColumnsV1";
    }

    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TCleanIndexColumnsV1Normalizer>(ClassName());

public:
    explicit TCleanIndexColumnsV1Normalizer(const TNormalizationController::TInitContext& ctx)
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
        return !AppData()->ColumnShardConfig.GetColumnChunksV1Usage();
    }
};

}   // namespace NKikimr::NOlap::NCleanUnusedTables
