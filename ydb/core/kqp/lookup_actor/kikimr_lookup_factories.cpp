#include "kikimr_lookup_factories.h"

#include "kikimr_lookup_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

    void RegisterKikimrLookupProviderFactories(TDqAsyncIoFactory& factory) {
        auto lookupActorFactory = [](NKqpProto::TKikimrLookupSource&& lookupSource, IDqAsyncIoFactory::TLookupSourceArguments&& args) {
            return CreateKikimrLookupActor(
                std::move(args.ParentId),
                std::move(args.TaskCounters),
                std::move(args.Alloc),
                std::move(args.KeyTypeHelper),
                std::move(lookupSource),
                args.KeyType,
                args.PayloadType,
                args.TypeEnv,
                args.HolderFactory,
                args.MaxKeysInRequest,
                args.IsMultiMatches
            );
        };

        factory.RegisterLookupSource<NKqpProto::TKikimrLookupSource>("kikimr", lookupActorFactory);
    }

} // namespace NYql::NDq
