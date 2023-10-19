#include "yql_ch_source_factory.h"
#include "yql_ch_read_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

void RegisterClickHouseReadActorFactory(TDqAsyncIoFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, IHTTPGateway::TPtr gateway) {
    factory.RegisterSource<NCH::TSource>("ClickHouseSource",
        [credentialsFactory, gateway](NCH::TSource&& settings, IDqAsyncIoFactory::TSourceArguments&& args) {
                return CreateClickHouseReadActor(gateway, std::move(settings), args.InputIndex, args.StatsLevel,
                args.SecureParams, args.TaskParams, args.ComputeActorId, credentialsFactory);
        });
}

}
