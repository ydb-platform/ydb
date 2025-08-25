#pragma once

#include "clean_unused_tables_template.h"

#include <ydb/core/protos/config.pb.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

namespace NKikimr::NOlap::NCleanUnusedTables {
using namespace NColumnShard;

using TUnusedTtlPresetInfo = TCleanUnusedTablesNormalizerTemplate<Schema::TtlSettingsPresetInfo>;

class TCleanTtlPresetInfoNormalizer final: public TUnusedTtlPresetInfo {
    using TBase = TUnusedTtlPresetInfo;

    static TString ClassName() {
        return "CleanTtlPresetSettingsInfo";
    }

    static inline auto Registrator = INormalizerComponent::TFactory::TRegistrator<TCleanTtlPresetInfoNormalizer>(ClassName());

public:
    explicit TCleanTtlPresetInfoNormalizer(const TNormalizationController::TInitContext& ctx)
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
