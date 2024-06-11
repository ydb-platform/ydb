#include "yql_yt_provider_factories.h"
#include "yql_yt_lookup_actor.h"
#include <ydb/library/yql/providers/yt/proto/source.pb.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

    void RegisterYtLookupActorFactory(TDqAsyncIoFactory& factory, NFile::TYtFileServices::TPtr ytServices, NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry) {
        auto lookupActorFactory = [&functionRegistry, ytServices](NYql::NYt::NSource::TLookupSource&& lookupSource, IDqAsyncIoFactory::TLookupSourceArguments&& args) {
            return CreateYtLookupActor(
                ytServices,
                std::move(args.ParentId),
                args.Alloc,
                functionRegistry,
                std::move(lookupSource),
                args.KeyType,
                args.PayloadType,
                args.TypeEnv,
                args.HolderFactory,
                args.MaxKeysInRequest);
        };
        factory.RegisterLookupSource<NYql::NYt::NSource::TLookupSource>("yt", lookupActorFactory);
    }

}
