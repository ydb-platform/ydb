#include "kikimr_lookup_factories.h"

#include "kikimr_lookup_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

    void RegisterDqSourceKikimrLookupProviderFactories(TDqAsyncIoFactory& factory) {
        auto lookupActorFactory = [](NKqpProto::TDqSourceKikimrLookupSource&& lookupSource, IDqAsyncIoFactory::TLookupSourceArguments&& args) {
            return CreateDqSourceKikimrLookupActor(std::move(lookupSource), std::move(args));
        };

        factory.RegisterLookupSource<NKqpProto::TDqSourceKikimrLookupSource>("kikimr", lookupActorFactory);
    }

} // namespace NYql::NDq
