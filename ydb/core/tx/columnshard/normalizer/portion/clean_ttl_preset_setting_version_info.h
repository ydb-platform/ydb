#pragma once

#include "clean_unused_tables_template.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;

using TUnusedTtlPresetVersionInfo = TCleanUnusedTablesNormalizerTemplate<Schema::TtlSettingsPresetVersionInfo>;

class TCleanTtlPresetVersionInfoNormalizer final: public TUnusedTtlPresetVersionInfo {
    using TBase = TUnusedTtlPresetVersionInfo;

    static TString ClassName() {
        return "CleanTtlPresetSettingsVersionInfo";
    }

    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TCleanTtlPresetVersionInfoNormalizer>(ClassName());

public:
    explicit TCleanTtlPresetVersionInfoNormalizer(const TNormalizationController::TInitContext& ctx)
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
