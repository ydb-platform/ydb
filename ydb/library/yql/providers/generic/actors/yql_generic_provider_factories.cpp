#include "yql_generic_provider_factories.h"

#include "yql_generic_read_actor.h"
#include "yql_generic_lookup_actor.h"
#include "yql_generic_write_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

    void RegisterGenericProviderFactories(TDqAsyncIoFactory& factory,
                                          ISecuredServiceAccountCredentialsFactory::TPtr securedServiceAccountCredentialsFactory,
                                          NYql::NConnector::IClient::TPtr genericClient) {
        auto readActorFactory = [securedServiceAccountCredentialsFactory, genericClient](
                                    Generic::TSource&& settings,
                                    IDqAsyncIoFactory::TSourceArguments&& args) {
            return CreateGenericReadActor(
                genericClient,
                std::move(settings),
                args.InputIndex,
                args.StatsLevel,
                args.SecureParams,
                args.TaskId,
                args.TaskParams,
                args.ReadRanges,
                args.ComputeActorId,
                securedServiceAccountCredentialsFactory,
                args.HolderFactory,
                std::move(args.Alloc));
        };

        auto lookupActorFactory = [securedServiceAccountCredentialsFactory, genericClient](Generic::TLookupSource&& lookupSource, IDqAsyncIoFactory::TLookupSourceArguments&& args) {
            return CreateGenericLookupActor(
                genericClient,
                securedServiceAccountCredentialsFactory,
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
                args.SecureParams,
                args.IsMultiMatches
            );
        };

        auto sinkActorFactory = [securedServiceAccountCredentialsFactory, genericClient](
                                    Generic::TSink&& settings,
                                    IDqAsyncIoFactory::TSinkArguments&& args) {
            return CreateGenericWriteActor(
                args.TypeEnv,
                *args.HolderFactory.GetFunctionRegistry(),
                genericClient,
                std::move(settings),
                args.OutputIndex,
                args.StatsLevel,
                args.TxId,
                args.SecureParams,
                args.Callback,
                securedServiceAccountCredentialsFactory);
        };

        for (auto& name : {
                 "ClickHouseGeneric",
                 "PostgreSqlGeneric",
                 "YdbGeneric",
                 "MySqlGeneric",
                 "GreenplumGeneric",
                 "MsSQLServerGeneric",
                 "OracleGeneric",
                 "LoggingGeneric",
                 "IcebergGeneric",
                 "RedisGeneric",
                 "PrometheusGeneric",
                 "MongoDBGeneric",
                 "OpenSearchGeneric",
                 "YtGeneric"}) {
            factory.RegisterSource<Generic::TSource>(name, readActorFactory);
            factory.RegisterLookupSource<Generic::TLookupSource>(name, lookupActorFactory);
            factory.RegisterSink<Generic::TSink>(name, sinkActorFactory);
        }
    }

} // namespace NYql::NDq
