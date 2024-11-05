#include "dq_compute_actor_async_io_factory.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/common/dq_common.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NYql::NDq {

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> TDqAsyncIoFactory::CreateDqSource(TSourceArguments&& args) const
{
    const TString& type = args.InputDesc.GetSource().GetType();
    YQL_ENSURE(!type.empty(), "Attempt to create source of empty type");
    const TSourceCreatorFunction* creatorFunc = SourceCreatorsByType.FindPtr(type);
    YQL_ENSURE(creatorFunc, "Unknown type of source: \"" << type << "\"");
    std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> actor = (*creatorFunc)(std::move(args));
    Y_ABORT_UNLESS(actor.first);
    Y_ABORT_UNLESS(actor.second);
    return actor;
}

void TDqAsyncIoFactory::RegisterSource(const TString& type, TSourceCreatorFunction creator)
{
    auto [_, registered] = SourceCreatorsByType.emplace(type, std::move(creator));
    Y_ABORT_UNLESS(registered);
}

std::pair<IDqAsyncLookupSource*, NActors::IActor*> TDqAsyncIoFactory::CreateDqLookupSource(TStringBuf type, TLookupSourceArguments&& args) const
{
    YQL_ENSURE(!type.empty(), "Attempt to create LookupSource of empty type");
    const auto* creatorFunc = LookupSourceCreatorsByType.FindPtr(type);
    YQL_ENSURE(creatorFunc, "Unknown type of source: \"" << type << "\"");
    auto lookupSource = (*creatorFunc)(std::move(args));
    Y_ABORT_UNLESS(lookupSource.first);
    Y_ABORT_UNLESS(lookupSource.second);
    return lookupSource;
}

void TDqAsyncIoFactory::RegisterLookupSource(const TString& type, TLookupSourceCreatorFunction creator) {
    auto [_, registered] = LookupSourceCreatorsByType.emplace(type, std::move(creator));
    Y_ABORT_UNLESS(registered);
}

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> TDqAsyncIoFactory::CreateDqSink(TSinkArguments&& args) const
{
    const TString& type = args.OutputDesc.GetSink().GetType();
    YQL_ENSURE(!type.empty(), "Attempt to create sink of empty type");
    const TSinkCreatorFunction* creatorFunc = SinkCreatorsByType.FindPtr(type);
    YQL_ENSURE(creatorFunc, "Unknown type of sink: \"" << type << "\"");
    std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> actor = (*creatorFunc)(std::move(args));
    Y_ABORT_UNLESS(actor.first);
    Y_ABORT_UNLESS(actor.second);
    return actor;
}

void TDqAsyncIoFactory::RegisterSink(const TString& type, TSinkCreatorFunction creator)
{
    auto [_, registered] = SinkCreatorsByType.emplace(type, std::move(creator));
    Y_ABORT_UNLESS(registered);
}

std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> TDqAsyncIoFactory::CreateDqInputTransform(TInputTransformArguments&& args)
{
    const TString& type = args.InputDesc.GetTransform().GetType();
    YQL_ENSURE(!type.empty(), "Attempt to create input transform of empty type");
    const TInputTransformCreatorFunction* creatorFunc = InputTransformCreatorsByType.FindPtr(type);
    YQL_ENSURE(creatorFunc, "Unknown type of input transform: \"" << type << "\"");
    std::pair<IDqComputeActorAsyncInput*, NActors::IActor*> actor = (*creatorFunc)(std::move(args));
    Y_ABORT_UNLESS(actor.first);
    Y_ABORT_UNLESS(actor.second);
    return actor;
}

void TDqAsyncIoFactory::RegisterInputTransform(const TString& type, TInputTransformCreatorFunction creator)
{
    auto [_, registered] = InputTransformCreatorsByType.emplace(type, std::move(creator));
    Y_ABORT_UNLESS(registered);
}

std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> TDqAsyncIoFactory::CreateDqOutputTransform(TOutputTransformArguments&& args)
{
    const TString& type = args.OutputDesc.GetTransform().GetType();
    YQL_ENSURE(!type.empty(), "Attempt to create output transform of empty type");
    const TOutputTransformCreatorFunction* creatorFunc = OutputTransformCreatorsByType.FindPtr(type);
    YQL_ENSURE(creatorFunc, "Unknown type of output transform: \"" << type << "\"");
    std::pair<IDqComputeActorAsyncOutput*, NActors::IActor*> actor = (*creatorFunc)(std::move(args));
    Y_ABORT_UNLESS(actor.first);
    Y_ABORT_UNLESS(actor.second);
    return actor;
}

void TDqAsyncIoFactory::RegisterOutputTransform(const TString& type, TOutputTransformCreatorFunction creator)
{
    auto [_, registered] = OutputTransformCreatorsByType.emplace(type, std::move(creator));
    Y_ABORT_UNLESS(registered);
}

} // namespace NYql::NDq
