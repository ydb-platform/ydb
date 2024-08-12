#include "yql_generic_provider_factories.h"

#include "yql_generic_read_actor.h"
#include "yql_generic_lookup_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

    void RegisterGenericProviderFactories(TDqAsyncIoFactory& factory,
                                          ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
                                          NYql::NConnector::IClient::TPtr genericClient) {
        auto readActorFactory = [credentialsFactory, genericClient](
                                    Generic::TSource&& settings,
                                    IDqAsyncIoFactory::TSourceArguments&& args) {
            return CreateGenericReadActor(genericClient, std::move(settings), args.InputIndex, args.StatsLevel,
                                          args.SecureParams, args.TaskParams, args.ComputeActorId, credentialsFactory, args.HolderFactory);
        };

        auto lookupActorFactory = [credentialsFactory, genericClient](NYql::Generic::TLookupSource&& lookupSource, IDqAsyncIoFactory::TLookupSourceArguments&& args) {
            return CreateGenericLookupActor(
                genericClient,
                credentialsFactory,
                std::move(args.ParentId),
                args.Alloc,
                args.KeyTypeHelper,
                std::move(lookupSource),
                args.KeyType,
                args.PayloadType,
                args.TypeEnv,
                args.HolderFactory,
                args.MaxKeysInRequest);
        };

        for (auto& name : {"ClickHouseGeneric", "PostgreSqlGeneric", "YdbGeneric", "MySqlGeneric", "GreenplumGeneric", "MsSQLServerGeneric", "OracleGeneric"}) {
            factory.RegisterSource<Generic::TSource>(name, readActorFactory);
            factory.RegisterLookupSource<Generic::TLookupSource>(name, lookupActorFactory);
        }
    }

}
