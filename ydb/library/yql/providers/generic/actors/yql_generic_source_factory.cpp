#include "yql_generic_source_factory.h"

#include "yql_generic_read_actor.h"

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>

namespace NYql::NDq {

    void RegisterGenericReadActorFactory(TDqAsyncIoFactory& factory,
                                         ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory,
                                         NYql::NConnector::IClient::TPtr genericClient) {
        factory.RegisterSource<Generic::TSource>("GenericSource", [credentialsFactory, genericClient](
                                                                      Generic::TSource&& settings,
                                                                      IDqAsyncIoFactory::TSourceArguments&& args) {
            return CreateGenericReadActor(genericClient, std::move(settings), args.InputIndex, args.SecureParams,
                                          args.TaskParams, args.ComputeActorId, credentialsFactory, args.HolderFactory);
        });
    }

}
