#include "cloud_function_transform_factory.h"
#include "cloud_function_transform.h"

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NYql::NDq {

void RegisterTransformCloudFunction(TDqTransformActorFactory& factory, IHTTPGateway::TPtr gateway,
                                    ISecuredServiceAccountCredentialsFactory::TPtr credentialsFactory) {
    factory.Register("YandexCloudFunction", [gateway, credentialsFactory](const NDqProto::TDqTransform& transform, TDqTransformActorFactory::TArguments&& args) {
        return CreateCloudFunctionTransformActor(transform, gateway, credentialsFactory, std::move(args));
    });
}

}
