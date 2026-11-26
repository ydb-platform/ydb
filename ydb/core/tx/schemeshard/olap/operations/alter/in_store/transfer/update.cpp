#include "update.h"
#include <ydb/core/tx/columnshard/data_sharing/initiator/controller/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

TConclusionStatus TInStoreShardsTransfer::InitializeTransfers(
    const TUpdateRestoreContext& context, const NKikimrSchemeOp::TAlterColumnTable& alterTable) {
    const auto& table = *TargetInStoreTable;
    auto sharding = table.GetTableInfo()->GetShardingVerified(table.GetTableSchemaVerified());
    for (auto&& alter : alterTable.GetAlterShards().GetTransfer().GetTransfers()) {
        const auto* destinationShardInfo = sharding->GetShardInfo(alter.GetDestinationTabletId());
        if (!destinationShardInfo) {
            return TConclusionStatus::Fail(TStringBuilder() << "Unknown destination tablet id for shards transfer: "
                << alter.GetDestinationTabletId());
        }
        if (!destinationShardInfo->GetOpenForWriteSnapshot()) {
            return TConclusionStatus::Fail(TStringBuilder() << "No open snapshot for destination tablet id in shards transfer: "
                << alter.GetDestinationTabletId());
        }
        NKikimrColumnShardDataSharingProto::TDestinationSession destinationSession;
        destinationSession.SetSessionId(alter.GetSessionId());
        *destinationSession.MutableInitiatorController() = NKikimr::NOlap::NDataSharing::TInitiatorControllerContainer(
            std::make_shared<NKikimr::NOlap::NDataSharing::TSSInitiatorController>(context.GetSSOperationContext()->SS->TabletID(), 0)).SerializeToProto();
        {
            auto& pathIdRemap = *destinationSession.AddPathIds();
            pathIdRemap.SetSourcePathId(context.GetOriginalEntity().GetPathId().LocalPathId);
            pathIdRemap.SetDestPathId(context.GetOriginalEntity().GetPathId().LocalPathId);
        }
        ::NKikimr::NOlap::TSnapshot ssOpen = sharding->GetShardingOpenSnapshotVerified(alter.GetDestinationTabletId());

        destinationSession.MutableTransferContext()->SetDestinationTabletId(alter.GetDestinationTabletId());
        destinationSession.MutableTransferContext()->SetMoving(alter.GetMoving());
        destinationSession.MutableTransferContext()->SetTxId(context.GetTxId());
        *destinationSession.MutableTransferContext()->MutableSnapshotBarrier() = ssOpen.SerializeToProto();
        for (auto&& i : alter.GetSourceTabletIds()) {
            destinationSession.MutableTransferContext()->AddSourceTabletIds(i);
        }
        if (!DestinationSessions.emplace(destinationSession.GetTransferContext().GetDestinationTabletId(), destinationSession).second) {
            return TConclusionStatus::Fail(TStringBuilder() << "Duplicated destination tablet id for shards transfer: "
                << alter.GetDestinationTabletId());
        }
        if (!ShardIdsUsage.emplace(alter.GetDestinationTabletId()).second) {
            return TConclusionStatus::Fail(TStringBuilder() << "Duplicated destination tablet id for shards transfer: "
                << alter.GetDestinationTabletId());
        }
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TInStoreShardsTransfer::DoRestoreImpl(
    const TUpdateRestoreContext& context, const TColumnTableInfo::TPtr& alterData) {
    TEntityInitializationContext eContext(context.GetSSOperationContext());
    TargetInStoreTable = std::make_shared<TInStoreTable>(context.GetOriginalEntity().GetPathId(), alterData, eContext);
    return InitializeTransfers(context, *alterData->AlterBody);
}

NKikimr::TConclusionStatus TInStoreShardsTransfer::DoInitializeImpl(const TUpdateInitializationContext& context) {
    const auto& alter = context.GetModification()->GetAlterColumnTable();
    if (!alter.GetAlterShards().GetTransfer().GetTransfers().size()) {
        return TConclusionStatus::Fail("hasn't data about shards transfer");
    }

    const auto& inStoreOriginal = context.GetOriginalEntityAsVerified<TInStoreTable>();
    auto targetInfo = std::make_shared<TColumnTableInfo>(inStoreOriginal.GetTableInfoVerified().AlterVersion,
        inStoreOriginal.GetTableInfoVerified().Description, TMaybe<NKikimrSchemeOp::TColumnStoreSharding>(), context.GetModification()->GetAlterColumnTable());
    TEntityInitializationContext eContext(context.GetSSOperationContext());
    TargetInStoreTable = std::make_shared<TInStoreTable>(context.GetOriginalEntity().GetPathId(), targetInfo, eContext);
    return InitializeTransfers(context, alter);
}

}
