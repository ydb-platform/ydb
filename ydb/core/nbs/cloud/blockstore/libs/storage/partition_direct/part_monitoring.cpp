#include "fast_path_service.h"
#include "mon_render.h"
#include "partition_direct_actor.h"

#include <ydb/library/actors/core/mon.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleHttpInfo(
    NMon::TEvRemoteHttpInfo::TPtr& ev,
    const TActorContext& ctx)
{
    TMonPageData data;
    data.Page = EMonPage::Overview;
    data.TabletInfo.TabletId = TabletID();
    data.TabletInfo.Generation = Executor()->Generation();
    data.TabletInfo.DiskId = VolumeConfig.GetDiskId();
    data.TabletInfo.State = FastPathService ? "WORK" : "INIT";

    if (FastPathService) {
        data.FastPathServiceInfo = FastPathService->GetMonInfo();
    }

    ctx.Send(ev->Sender, new NMon::TEvRemoteHttpInfoRes(RenderMonPage(data)));
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
