#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>


namespace NYdb::NBS::NStorage::NPartitionDirect {

using namespace NActors;

class TPartitionActor
    : public TActorBootstrapped<TPartitionActor>
{
public:
    TPartitionActor() = default;
    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleWriteBlocksRequest(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlocksRequest(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

} // namespace NYdb::NBS::NStorage::NPartitionDirect
