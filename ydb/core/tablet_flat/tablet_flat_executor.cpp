#include "tablet_flat_executor.h"
#include "flat_executor.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

namespace NFlatExecutorSetup {
    IActor* CreateExecutor(ITablet *owner, const TActorId& ownerActorId) {
        return new TExecutor(owner, ownerActorId);
    }

    void ITablet::SnapshotComplete(TIntrusivePtr<TTableSnapshotContext> snapContext, const TActorContext &ctx) {
        Y_UNUSED(snapContext);
        Y_UNUSED(ctx);
        Y_ABORT("must be overriden if plan to use table snapshot completion");
    }

    void ITablet::CompactionComplete(ui32 tableId, const TActorContext &ctx) {
        Y_UNUSED(tableId);
        Y_UNUSED(ctx);
    }

    void ITablet::CompletedLoansChanged(const TActorContext &ctx) {
        Y_UNUSED(ctx);
    }

    void ITablet::ScanComplete(NTable::EAbort status, TAutoPtr<IDestructable> prod, ui64 cookie, const TActorContext &ctx)
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
}

}}
