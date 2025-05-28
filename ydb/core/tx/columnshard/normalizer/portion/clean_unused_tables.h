#pragma once

#include <ydb/core/tx/columnshard/columnshard_schema.h>
#include <ydb/core/tx/columnshard/columnshard_private_events.h>

namespace NKikimr::NOlap::NCleanUnusedTables {

class TCleanUnusedTablesNormalizer final
      : public TNormalizationController::INormalizerComponent
{
    using TBase = TNormalizationController::INormalizerComponent;

    static TString ClassName() { return "CleanUnusedTables"; }
    static inline auto Registrator =
        INormalizerComponent::TFactory::TRegistrator<TCleanUnusedTablesNormalizer>(ClassName());

public:
    using TKey = std::tuple<ui32, ui64, ui32, ui64, ui64, ui64, ui32>;

    static constexpr size_t BATCH = 1000;
    
    explicit TCleanUnusedTablesNormalizer(const TNormalizationController::TInitContext& ctx)
        : TBase(ctx) {}

    TString GetClassName() const override { return ClassName(); }
    std::optional<ENormalizerSequentialId> DoGetEnumSequentialId() const override {
        // return ENormalizerSequentialId::CleanUnusedTables;
        return std::nullopt;
    }

    TConclusion<std::vector<INormalizerTask::TPtr>>
    DoInit(const TNormalizationController&,
           NTabletFlatExecutor::TTransactionContext& txc) override;
};

} // namespace NKikimr::NOlap::NCleanUnusedTables