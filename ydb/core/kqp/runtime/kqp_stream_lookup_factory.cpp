#include "kqp_stream_lookup_factory.h"
#include "kqp_stream_lookup_actor.h"

namespace NKikimr {
namespace NKqp {

void RegisterStreamLookupActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters,
    TIntrusivePtr<TVectorIndexLevelsCache> vectorIndexLevelsCache) {
    factory.RegisterInputTransform<NKikimrKqp::TKqpStreamLookupSettings>("StreamLookupInputTransformer",
        [counters, vectorIndexLevelsCache](NKikimrKqp::TKqpStreamLookupSettings&& settings,
            NYql::NDq::TDqAsyncIoFactory::TInputTransformArguments&& args) {
            return CreateStreamLookupActor(std::move(args), std::move(settings), counters, vectorIndexLevelsCache);
    });
}

} // namespace NKqp
} // namespace NKikimr
