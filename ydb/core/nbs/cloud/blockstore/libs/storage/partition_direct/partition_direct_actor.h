#pragma once

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/direct_block_group/direct_block_group.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NActors;
using namespace NKikimr;

////////////////////////////////////////////////////////////////////////////////

class TPartitionActor
    : public TActorBootstrapped<TPartitionActor>
{
private:
    TActorId BSControllerPipeClient;

    std::unique_ptr<TDirectBlockGroup> DirectBlockGroup;

public:
    TPartitionActor() = default;

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void CreateBSControllerPipeClient(const TActorContext& ctx);

    void AllocateDDiskBlockGroup(const TActorContext& ctx);

    void HandleControllerAllocateDDiskBlockGroupResult(
        const TEvBlobStorage::TEvControllerAllocateDDiskBlockGroupResult::TPtr& ev,
        const TActorContext& ctx);
        
    void HandleWriteBlocksRequest(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlocksRequest(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    // Forward events to DirectBlockGroup
    void HandleDDiskConnectResult(
        const NDDisk::TEvDDiskConnectResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleDDiskWriteResult(
        const NDDisk::TEvDDiskWriteResult::TPtr& ev,
        const TActorContext& ctx);

    void HandleDDiskReadResult(
        const NDDisk::TEvDDiskReadResult::TPtr& ev,
        const TActorContext& ctx);
};

} // namespace NYdb::NBS::NStorage::NPartitionDirect
