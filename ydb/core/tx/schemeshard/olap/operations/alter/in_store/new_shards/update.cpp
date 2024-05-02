#include "evolution.h"
#include "update.h"

#include <ydb/core/tx/schemeshard/olap/operations/alter/abstract/converter.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/in_store/transfer/evolution.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/start.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/continue.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/publish.h>
#include <ydb/core/tx/schemeshard/olap/operations/alter/operations/finish.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TInStoreReshardingUpdate::DoInitialize(const TUpdateInitializationContext& context) {
    if (!context.GetModification()->HasReshardColumnTable()) {
        return TConclusionStatus::Fail("no found resharding data");
    }
    auto description = OriginalInStoreTable->GetTableInfoVerified().Description;
    if (!description.HasSharding()) {
        return TConclusionStatus::Fail("no sharding data in original table");
    }
    if (!description.GetSharding().HasHashSharding()) {
        return TConclusionStatus::Fail("sharding data in original table is not hash");
    }

    auto layoutPolicy = OriginalInStoreTable->GetStoreInfo()->GetTablesLayoutPolicy();
    auto currentLayout = context.GetSSOperationContext()->SS->ColumnTables.GetTablesLayout(TColumnTablesLayout::ShardIdxToTabletId(OriginalInStoreTable->GetStoreInfo()->GetColumnShards(), *context.GetSSOperationContext()->SS));
    auto layoutConclusion = layoutPolicy->Layout(currentLayout, description.GetSharding().GetColumnShards().size());
    if (layoutConclusion.IsFail()) {
        return layoutConclusion;
    }
    for (auto&& i : layoutConclusion->GetTabletIds()) {
        AFL_VERIFY(NewShardIds.emplace(i).second);
    }
    

    CreateToShard.MutableSchemaPreset()->SetId(OriginalInStoreTable->GetTableInfoVerified().Description.GetSchemaPresetId());
    CreateToShard.MutableSchemaPreset()->SetName(OriginalInStoreTable->GetTableInfoVerified().Description.GetSchemaPresetName());
    *CreateToShard.MutableSchemaPreset()->MutableSchema() = OriginalInStoreTable->GetTableInfoVerified().Description.GetSchema();
    if (OriginalInStoreTable->GetTableInfoVerified().Description.HasTtlSettings()) {
        *CreateToShard.MutableTtlSettings() = OriginalInStoreTable->GetTableInfoVerified().Description.GetTtlSettings();
    }
    if (OriginalInStoreTable->GetTableInfoVerified().Description.HasSchemaPresetVersionAdj()) {
        CreateToShard.SetSchemaPresetVersionAdj(OriginalInStoreTable->GetTableInfoVerified().Description.GetSchemaPresetVersionAdj());
    }

    return TConclusionStatus::Success();
}

TConclusion<TEvolutions> TInStoreReshardingUpdate::DoBuildEvolutions() const {
    std::deque<TSSEntityEvolutionContainer> evolutions;
    evolutions.emplace_back(std::make_shared<TInStoreNewShardsEvolution>(NewShardIds, CreateToShard));
    ui32 idx = 0;
    for (auto&& to : NewShardIds) {
        const ui64 from = OriginalInStoreTable->GetTableInfoVerified().GetTabletId(idx);
        std::set<ui64> fromShardIds = { from };
        evolutions.emplace_back(std::make_shared<TInStoreShardsTransferEvolution>(fromShardIds, to));
    }
    TEvolutions result(GetRequest(), std::move(evolutions));
    return result;
}

TVector<NKikimr::NSchemeShard::ISubOperation::TPtr> TInStoreReshardingUpdate::DoBuildOperations(const TOperationId& id, const NKikimrSchemeOp::TModifyScheme& request) const {
    TVector<ISubOperation::TPtr> result;
    result.emplace_back(new TStartAlterColumnTable(id, request));
    result.emplace_back(new TEvoluteAlterColumnTable(id, request));
    for (auto&& i : NewShardIds) {
        Y_UNUSED(i);
        result.emplace_back(new TEvoluteAlterColumnTable(id, request));
        result.emplace_back(new TPublishAlterColumnTable(id, request));
    }
    result.emplace_back(new TFinishAlterColumnTable(id, request));

    return result;
}

}