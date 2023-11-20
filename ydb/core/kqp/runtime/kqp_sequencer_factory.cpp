#include "kqp_sequencer_factory.h"
#include "kqp_sequencer_actor.h"

namespace NKikimr {
namespace NKqp {

void RegisterSequencerActorFactory(NYql::NDq::TDqAsyncIoFactory& factory, TIntrusivePtr<TKqpCounters> counters) {
    factory.RegisterInputTransform<NKikimrKqp::TKqpSequencerSettings>("SequencerInputTransformer", [counters](NKikimrKqp::TKqpSequencerSettings&& settings,
        NYql::NDq::TDqAsyncIoFactory::TInputTransformArguments&& args) {
            return CreateSequencerActor(args.InputIndex, args.StatsLevel, args.TransformInput, args.ComputeActorId, args.TypeEnv,
                args.HolderFactory, args.Alloc, std::move(settings), counters);
    });
}

} // namespace NKqp
} // namespace NKikimr
