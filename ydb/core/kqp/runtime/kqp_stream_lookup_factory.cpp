#include "kqp_stream_lookup_factory.h"
#include "kqp_stream_lookup_actor.h"

namespace NKikimr {
namespace NKqp {

void RegisterStreamLookupActorFactory(NYql::NDq::TDqAsyncIoFactory& factory) {
    factory.RegisterInputTransform<NKikimrKqp::TKqpStreamLookupSettings>("StreamLookupInputTransformer", [](NKikimrKqp::TKqpStreamLookupSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TInputTransformArguments&& args) {
            return CreateStreamLookupActor(args.InputIndex, args.TransformInput, args.ComputeActorId, args.TypeEnv,
                args.HolderFactory, args.Alloc, std::move(settings));
    });
}

} // namespace NKqp
} // namespace NKikimr
