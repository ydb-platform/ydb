#include "yql_ydb_source_factory.h"
#include "yql_ydb_read_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

void RegisterYdbReadActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, ::NYdb::TDriver driver, ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.RegisterSource<NYql::NYdb::TSource>("YdbSource",
        [driver, credentialsFactory](NYql::NYdb::TSource&& settings, IDqAsyncIoFactory::TSourceArguments&& args) {
            return CreateYdbReadActor(std::move(settings), args.InputIndex, args.StatsLevel, args.SecureParams,
                args.TaskParams, args.ComputeActorId, driver, credentialsFactory);
        });
}

}
