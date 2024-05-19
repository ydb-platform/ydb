#include "update.h"
#include <ydb/core/tx/columnshard/bg_tasks/abstract/task.h>
#include <ydb/core/tx/schemeshard/olap/bg_tasks/tx_chain/task.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/sharding/sharding.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TInStoreShardingUpdate::DoStart(const TUpdateStartContext& context) {
    NKikimr::NOlap::NBackground::TTask task("SPLIT_SHARDS::" + context.GetObjectPath()->PathString(), std::make_shared<NKikimr::NOlap::NBackground::TFakeStatusChannel>(),
        std::make_shared<NOlap::NBackground::TTxChainTask>(TxChainData));
    auto tx = context.GetSSOperationContext()->SS->BackgroundSessionsManager->TxAddTask(task);
    if (!tx->Execute(context.GetSSOperationContext()->GetTxc(), context.GetSSOperationContext()->Ctx)) {
        return TConclusionStatus::Fail("cannot execute transaction for write task");
    }
    tx->Complete(context.GetSSOperationContext()->Ctx);
    return TConclusionStatus::Success();
}

TConclusionStatus TInStoreShardingUpdate::DoInitialize(const TUpdateInitializationContext& context) {
    auto& inStoreTable = context.GetOriginalEntityAsVerified<TInStoreTable>();
    AFL_VERIFY(context.GetModification()->GetAlterColumnTable().HasReshardColumnTable());
    const ui32 shardsCount = inStoreTable.GetTableInfoPtrVerified()->GetColumnShards().size();
    auto& storeInfo = *inStoreTable.GetStoreInfo();
    auto layoutPolicy = storeInfo.GetTablesLayoutPolicy();
    auto currentLayout = context.GetSSOperationContext()->SS->ColumnTables.GetTablesLayout(TColumnTablesLayout::ShardIdxToTabletId(
        storeInfo.GetColumnShards(), *context.GetSSOperationContext()->SS));
    auto tablePtr = context.GetSSOperationContext()->SS->ColumnTables.GetVerifiedPtr(context.GetOriginalEntity().GetPathId());
    auto layoutConclusion = layoutPolicy->Layout(currentLayout, shardsCount);
    if (layoutConclusion.IsFail()) {
        return layoutConclusion;
    }
    std::shared_ptr<NSharding::IShardingBase> sharding = inStoreTable.GetTableInfoPtrVerified()->GetShardingVerified(inStoreTable.GetTableSchemaVerified());
    TConclusion<std::vector<NKikimrSchemeOp::TAlterShards>> alters = sharding->BuildAddShardsModifiers(layoutConclusion->GetTabletIds());
    if (alters.IsFail()) {
        return alters;
    }
    for (auto&& i : *alters) {
        NKikimrSchemeOp::TModifyScheme modification;
        modification.SetWorkingDir(context.GetModification()->GetWorkingDir());
        modification.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterColumnTable);
        *modification.MutableAlterColumnTable()->MutableAlterShards() = std::move(i);
        modification.MutableAlterColumnTable()->SetName(context.GetModification()->GetAlterColumnTable().GetName());
        TxChainData.MutableTransactions().emplace_back(std::move(modification));
    }
    return TConclusionStatus::Success();
}

}