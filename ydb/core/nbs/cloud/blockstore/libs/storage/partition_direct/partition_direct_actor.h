#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/log.h>


namespace NCloud::NBlockStore::NStorage::NPartitionDirect {

using namespace NActors;

class TPartitionActor
    : public TActorBootstrapped<TPartitionActor>
{
public:
    TPartitionActor() = default;
    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateWork);
};

} // namespace NCloud::NBlockStore::NStorage::NPartitionDirect
