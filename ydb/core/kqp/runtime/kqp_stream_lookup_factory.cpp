#include "kqp_stream_lookup_factory.h"
#include "kqp_stream_lookup_actor.h"

namespace NKikimr {
namespace NKqp {

void RegisterStreamLookupActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterInputTransform<NKikimrKqp::TKqpStreamLookupSettings>("StreamLookupInputTransformer", [counters](NKikimrKqp::TKqpStreamLookupSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TInputTransformArguments&& args) {
            return CreateStreamLookupActor(std::move(args), std::move(settings), counters);
    });
}

} // namespace NKqp
} // namespace NKikimr
