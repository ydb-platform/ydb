#include "cloud_function_transform_factory.h"
#include "cloud_function_transform.h"

#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NYql::NDq {

void RegisterTransformCloudFunction(TDqTransformActorFactory& factory, IHTTPGateway::TPtr gateway) {
    constexpr NDqProto::ETransformType type = NDqProto::ETransformType::TRANSFORM_CLOUD_FUNCTION;
    factory.Register(type, [gateway](const NDqProto::TDqTransform& transform, TDqTransformActorFactory::TArguments&& args) {
        return CreateCloudFunctionTransformActor(transform, gateway, std::move(args));
    });
}

}