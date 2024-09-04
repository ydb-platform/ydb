#include "dq_input_transform_lookup_factory.h"
#include "dq_input_transform_lookup.h"
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor_async_io.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

namespace NYql::NDq {

void RegisterDqInputTransformLookupActorFactory(NDq::TDqAsyncIoFactory& factory) {
    factory.RegisterInputTransform<NYql::NDqProto::TDqInputTransformLookupSettings>(
        "StreamLookupInputTransform",
        [factory = &factory](NDqProto::TDqInputTransformLookupSettings&& settings, IDqAsyncIoFactory::TInputTransformArguments&& args) {
            return CreateInputTransformStreamLookup(
                factory,
                std::move(settings),
                std::move(args)
            );
        }
    );
}

} // namespace NYql::NDq
