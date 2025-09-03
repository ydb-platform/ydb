#include "tablet_flat_executor.h"
#include "flat_executor.h"
#include "util_fmt_abort.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace NFlatExecutorSetup {
    IActor* CreateExecutor(ITablet *owner, const TActorId& ownerActorId) {
        return new TExecutor(owner, ownerActorId);
    }

    void ITablet::SnapshotComplete(TIntrusivePtr<TTableSnapshotContext> snapContext, const TActorContext &ctx) {
        Y_UNUSED(snapContext);
        Y_UNUSED(ctx);
        Y_TABLET_ERROR("must be overriden if plan to use table snapshot completion");
    }

    void ITablet::CompactionComplete(ui32 tableId, const TActorContext &ctx) {
        Y_UNUSED(tableId);
        Y_UNUSED(ctx);
    }

    void ITablet::VacuumComplete(ui64 vacuumGeneration, const TActorContext& ctx) {
        Y_UNUSED(vacuumGeneration);
        Y_UNUSED(ctx);
    }

    void ITablet::CompletedLoansChanged(const TActorContext &ctx) {
        Y_UNUSED(ctx);
    }

    void ITablet::ScanComplete(NTable::EStatus status, TAutoPtr<IDestructable> prod, ui64 cookie, const TActorContext &ctx)
    {
        Y_UNUSED(status);
        Y_UNUSED(prod);
        Y_UNUSED(cookie);
        Y_UNUSED(ctx);
    }

    bool ITablet::ReassignChannelsEnabled() const {
        // By default channels are reassigned automatically
        return true;
    }

    void ITablet::OnYellowChannelsChanged() {
        // nothing by default
    }

    void ITablet::OnRejectProbabilityRelaxed() {
        // nothing by default
    }

    void ITablet::UpdateTabletInfo(TIntrusivePtr<TTabletStorageInfo> info, const TActorId& launcherID) {
        if (info)
            TabletInfo = info;

        if (launcherID)
            LauncherActorID = launcherID;
    }

    bool ITablet::ReadOnlyLeaseEnabled() {
        return false;
    }

    TDuration ITablet::ReadOnlyLeaseDuration() {
        return TDuration::MilliSeconds(250);
    }

    void ITablet::ReadOnlyLeaseDropped() {
        // nothing by default
    }

    void ITablet::OnFollowersCountChanged() {
        // nothing by default
    }

    void ITablet::OnFollowerSchemaUpdated() {
        // nothing by default
    }

    void ITablet::OnFollowerDataUpdated() {
        // nothing by default
    }

    bool ITablet::IsSystemTablet() const {
        switch (TabletInfo->TabletType) {
            case NKikimrTabletBase::TTabletTypes_EType_OldSchemeShard:
            case NKikimrTabletBase::TTabletTypes_EType_OldHive:
            case NKikimrTabletBase::TTabletTypes_EType_OldCoordinator:
            case NKikimrTabletBase::TTabletTypes_EType_Mediator:
            case NKikimrTabletBase::TTabletTypes_EType_OldTxProxy:
            case NKikimrTabletBase::TTabletTypes_EType_OldBSController:
            case NKikimrTabletBase::TTabletTypes_EType_Coordinator:
            case NKikimrTabletBase::TTabletTypes_EType_Hive:
            case NKikimrTabletBase::TTabletTypes_EType_BSController:
            case NKikimrTabletBase::TTabletTypes_EType_SchemeShard:
            case NKikimrTabletBase::TTabletTypes_EType_Cms:
            case NKikimrTabletBase::TTabletTypes_EType_NodeBroker:
            case NKikimrTabletBase::TTabletTypes_EType_TxAllocator:
            case NKikimrTabletBase::TTabletTypes_EType_TxProxy:
            case NKikimrTabletBase::TTabletTypes_EType_TenantSlotBroker:
            case NKikimrTabletBase::TTabletTypes_EType_Console:
            case NKikimrTabletBase::TTabletTypes_EType_StatisticsAggregator:
            case NKikimrTabletBase::TTabletTypes_EType_SysViewProcessor:
                return true;
            case NKikimrTabletBase::TTabletTypes_EType_Unknown:
            case NKikimrTabletBase::TTabletTypes_EType_OldDataShard:
            case NKikimrTabletBase::TTabletTypes_EType_Dummy:
            case NKikimrTabletBase::TTabletTypes_EType_RTMRPartition:
            case NKikimrTabletBase::TTabletTypes_EType_OldKeyValue:
            case NKikimrTabletBase::TTabletTypes_EType_KeyValue:
            case NKikimrTabletBase::TTabletTypes_EType_DataShard:
            case NKikimrTabletBase::TTabletTypes_EType_PersQueue:
            case NKikimrTabletBase::TTabletTypes_EType_PersQueueReadBalancer:
            case NKikimrTabletBase::TTabletTypes_EType_BlockStoreVolume:
            case NKikimrTabletBase::TTabletTypes_EType_BlockStorePartition:
            case NKikimrTabletBase::TTabletTypes_EType_Kesus:
            case NKikimrTabletBase::TTabletTypes_EType_BlockStorePartition2:
            case NKikimrTabletBase::TTabletTypes_EType_BlockStoreDiskRegistry:
            case NKikimrTabletBase::TTabletTypes_EType_FileStore:
            case NKikimrTabletBase::TTabletTypes_EType_ColumnShard:
            case NKikimrTabletBase::TTabletTypes_EType_TestShard:
            case NKikimrTabletBase::TTabletTypes_EType_SequenceShard:
            case NKikimrTabletBase::TTabletTypes_EType_ReplicationController:
            case NKikimrTabletBase::TTabletTypes_EType_BlobDepot:
            case NKikimrTabletBase::TTabletTypes_EType_GraphShard:
            case NKikimrTabletBase::TTabletTypes_EType_BackupController:
            case NKikimrTabletBase::TTabletTypes_EType_Reserved43:
            case NKikimrTabletBase::TTabletTypes_EType_Reserved44:
            case NKikimrTabletBase::TTabletTypes_EType_Reserved45:
            case NKikimrTabletBase::TTabletTypes_EType_Reserved46:
            case NKikimrTabletBase::TTabletTypes_EType_UserTypeStart:
            case NKikimrTabletBase::TTabletTypes_EType_TypeInvalid:
                return false;
        }
        return false;
    }

    bool ITablet::IsClusterLevelTablet() const {
        return TabletInfo->TenantPathId == TPathId();
    }
}

}}
