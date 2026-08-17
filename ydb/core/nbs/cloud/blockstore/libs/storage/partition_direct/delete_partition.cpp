#include "partition_direct_actor.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>

#include <ydb/library/services/services.pb.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

void TPartitionActor::HandleDeletePartition(
    const TEvService::TEvDeletePartitionRequest::TPtr& ev,
    const NActors::TActorContext& ctx)
{
    LOG_INFO(
        ctx,
        NKikimrServices::NBS_PARTITION,
        "%s Handle DeletePartition request",
        LogTitle.GetWithTime().c_str());

    auto response = std::make_unique<TEvService::TEvDeletePartitionResponse>();
    ctx.Send(ev->Sender, response.release(), 0, ev->Cookie);
}

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
