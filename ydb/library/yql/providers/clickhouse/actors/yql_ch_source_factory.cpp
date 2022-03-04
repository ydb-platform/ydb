#include "yql_ch_source_factory.h"
#include "yql_ch_read_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>

namespace NYql::NDq {

void RegisterClickHouseReadActorFactory(TDqSourceFactory& factory, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory, IHTTPGateway::TPtr gateway) {
    factory.Register<NCH::TSource>("ClickHouseSource",
        [credentialsFactory, gateway](NCH::TSource&& settings, IDqSourceActorFactory::TArguments&& args) {
                return CreateClickHouseReadActor(gateway, std::move(settings), args.InputIndex, args.SecureParams, args.TaskParams, args.ComputeActorId, credentialsFactory);
        });
}

}
