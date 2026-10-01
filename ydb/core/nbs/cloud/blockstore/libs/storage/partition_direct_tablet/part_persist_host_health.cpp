#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/common/constants.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/fast_path_service.h>

#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NTabletFlatExecutor;

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandlePersistHostHealth(
    const TEvPartitionDirectPrivate::TEvPersistHostHealth::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    Y_ABORT_UNLESS(FastPathService);

    const auto* msg = ev->Get();
    const size_t dbgId = msg->DirectBlockGroupId;
    THostIndex hostIndex = msg->HostIndex;
    EHostHealth oldHealth = msg->OldHealth;
    EHostHealth newHealth = msg->NewHealth;

    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Not implemented PersistHostHealth %s/%s: %s -> %s",
        LogTitle.GetWithTime().c_str(),
        PrintDbgId(dbgId).c_str(),
        PrintHostIndex(hostIndex).c_str(),
        ToString(oldHealth).c_str(),
        ToString(newHealth).c_str());
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
