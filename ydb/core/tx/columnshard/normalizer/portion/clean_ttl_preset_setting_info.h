#pragma once

#include "clean_unused_tables_template.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;

using TUnusedTables = TCleanUnusedTablesNormalizerTemplate<Schema::TtlPresetSettingsInfo>;

class TCleanUnusedTablesNormalizer final: public TUnusedTables {
    using TBase = TUnusedTables;

    static TString ClassName() {
        return "CleanTtlPresetSettingsInfo";
    }

    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TCleanUnusedTablesNormalizer>(ClassName());

public:
    explicit TCleanUnusedTablesNormalizer(const TNormalizationController::TInitContext& ctx)
        : TBase(ctx) {
    }

    TString GetClassName() const override {
        return ClassName();
    }

    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        return std::nullopt;
    }
};

}   // namespace NKikimr::NOlap::NCleanUnusedTables
