#include "yql_ydb_source_factory.h"
#include "yql_ydb_read_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_sources.h>

namespace NYql::NDq {

void RegisterYdbReadActorFactory(NYql::NDq::TDqSourceFactory& factory, ::NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.Register<NYql::NYdb::TSource>("YdbSource",
        [driver, credentialsFactory](NYql::NYdb::TSource&& settings, IDqSourceActorFactory::TArguments&& args) {
                return CreateYdbReadActor(std::move(settings), args.InputIndex, args.SecureParams, args.TaskParams, args.ComputeActorId, driver, credentialsFactory);
        });
}

}
