#pragma once

#include "fast_path_service.h"

#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

////////////////////////////////////////////////////////////////////////////////

class TLoadActorAdapter: public NActors::TActorBootstrapped<TLoadActorAdapter>
{
private:
    std::shared_ptr<TFastPathService> FastPathService;

public:
    explicit TLoadActorAdapter(
        std::shared_ptr<TFastPathService> fastPathService);

    void Bootstrap(const NActors::TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleWriteBlocksRequest(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);

    void HandleReadBlocksRequest(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const NActors::TActorContext& ctx);
};

TActorId CreateLoadActorAdapter(
    const NActors::TActorId& owner,
    std::shared_ptr<TFastPathService> fastPathService);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
