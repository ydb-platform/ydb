#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/nbs/cloud/blockstore/libs/service/fast_path_service/fast_path_service.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/service.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

////////////////////////////////////////////////////////////////////////////////

class TLoadActorAdapter
    : public TActorBootstrapped<TLoadActorAdapter>
{
private:
    std::shared_ptr<TFastPathService> FastPathService;

public:
    explicit TLoadActorAdapter(
        std::shared_ptr<TFastPathService> fastPathService);

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);

    void HandleWriteBlocksRequest(
        const TEvService::TEvWriteBlocksRequest::TPtr& ev,
        const TActorContext& ctx);

    void HandleReadBlocksRequest(
        const TEvService::TEvReadBlocksRequest::TPtr& ev,
        const TActorContext& ctx);
};

TActorId CreateLoadActorAdapter(
    const NActors::TActorId& owner,
    std::shared_ptr<TFastPathService> fastPathService);

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
