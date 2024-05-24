#include "update.h"
#include <ydb/core/tx/columnshard/data_sharing/initiator/controller/schemeshard.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

NKikimr::TConclusionStatus TInStoreShardsTransfer::DoInitializeImpl(const TUpdateInitializationContext& context) {
    if (!context.GetModification()->GetAlterColumnTable().GetAlterShards().GetTransfer().GetTransfers().size()) {
        return TConclusionStatus::Fail("hasn't data about shards transfer");
    }
    auto& table = context.GetOriginalEntityAsVerified<TInStoreTable>();
    auto sharding = table.GetTableInfo()->GetShardingVerified(table.GetTableSchemaVerified());
    for (auto&& alter : context.GetModification()->GetAlterColumnTable().GetAlterShards().GetTransfer().GetTransfers()) {
        NKikimrColumnShardDataSharingProto::TDestinationSession destinationSession;
        destinationSession.SetSessionId("SHARE_TO_SHARD::" + ::ToString(alter.GetDestinationTabletId()));
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
        DestinationSessions.emplace_back(destinationSession);
        AFL_VERIFY(ShardIdsUsage.emplace(alter.GetDestinationTabletId()).second);
    }
    const auto& inStoreOriginal = context.GetOriginalEntityAsVerified<TInStoreTable>();
    auto targetInfo = std::make_shared<TColumnTableInfo>(inStoreOriginal.GetTableInfoVerified().AlterVersion,
        inStoreOriginal.GetTableInfoVerified().Description, TMaybe<NKikimrSchemeOp::TColumnStoreSharding>(), context.GetModification()->GetAlterColumnTable());
    TEntityInitializationContext eContext(context.GetSSOperationContext());
    TargetInStoreTable = std::make_shared<TInStoreTable>(context.GetOriginalEntity().GetPathId(), targetInfo, eContext);

    return TConclusionStatus::Success();
}

}