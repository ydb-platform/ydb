#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/core/protos/kqp_lookup_source.pb.h>

namespace NYql::NDq::NDqSourceLookup {
    std::pair<NYql::NDq::IDqAsyncLookupSource*, NActors::IActor*> CreateKikimrLookupActor(
        NKqpProto::TDqSourceKikimrLookupSource&& lookupSource,
        IDqAsyncIoFactory::TLookupSourceArguments&& args
    );
} // namespace NYql::NDq::NDqSourceLookup
