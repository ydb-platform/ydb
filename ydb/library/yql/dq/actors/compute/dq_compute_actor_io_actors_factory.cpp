#include "dq_compute_actor_io_actors_factory.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sinks.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>
#include <ydb/library/yql/dq/common/dq_common.h>

namespace NYql::NDq {

std::pair<IDqSourceActor*, NActors::IActor*> TDqSourceFactory::CreateDqSourceActor(IDqSourceActorFactory::TArguments&& args) const
{
    const TString& type = args.InputDesc.GetSource().GetType();
    YQL_ENSURE(!type.empty(), "Attempt to create source actor of empty type");
    const TCreatorFunction* creatorFunc = CreatorsByType.FindPtr(type);
    YQL_ENSURE(creatorFunc, "Unknown type of source actor: \"" << type << "\"");
    std::pair<IDqSourceActor*, NActors::IActor*> actor = (*creatorFunc)(std::move(args));
    Y_VERIFY(actor.first);
    Y_VERIFY(actor.second);
    return actor;
}

void TDqSourceFactory::Register(const TString& type, TCreatorFunction creator)
{
    auto [_, registered] = CreatorsByType.emplace(type, std::move(creator));
    Y_VERIFY(registered);
}

std::pair<IDqSinkActor*, NActors::IActor*> TDqSinkFactory::CreateDqSinkActor(IDqSinkActorFactory::TArguments&& args) const
{
    const TString& type = args.OutputDesc.GetSink().GetType();
    YQL_ENSURE(!type.empty(), "Attempt to create sink actor of empty type");
    const TCreatorFunction* creatorFunc = CreatorsByType.FindPtr(type);
    YQL_ENSURE(creatorFunc, "Unknown type of sink actor: \"" << type << "\"");
    std::pair<IDqSinkActor*, NActors::IActor*> actor = (*creatorFunc)(std::move(args));
    Y_VERIFY(actor.first);
    Y_VERIFY(actor.second);
    return actor;
}

void TDqSinkFactory::Register(const TString& type, TCreatorFunction creator)
{
    auto [_, registered] = CreatorsByType.emplace(type, std::move(creator));
    Y_VERIFY(registered);
}

} // namespace NYql::NDq
