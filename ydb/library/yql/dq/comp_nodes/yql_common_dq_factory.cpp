#include "yql_common_dq_factory.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

namespace NYql {

using namespace NKikimr::NMiniKQL;

class TDqNotify : public TMutableComputationNode<TDqNotify> {
    typedef TMutableComputationNode<TDqNotify> TBaseComputation;
public:
    class TValue : public TComputationValue<TValue> {
    public:
        TValue(TMemoryUsageInfo* memInfo)
            : TComputationValue(memInfo)
            , ActorSystem(NActors::TActivationContext::ActorSystem())
            , CurrentActorId(NActors::TActivationContext::AsActorContext().SelfID)
        {}

    private:
        NUdf::TUnboxedValue Run(const NUdf::IValueBuilder*, const NUdf::TUnboxedValuePod*) const override {
            ActorSystem->Send(new NActors::IEventHandle(
                CurrentActorId, NActors::TActorId(), new NDq::TEvDqCompute::TEvResumeExecution()));
            return NUdf::TUnboxedValue::Void();
        }

        NActors::TActorSystem* const ActorSystem;
        NActors::TActorId const CurrentActorId;
    };

    TDqNotify(TComputationMutables& mutables)
        : TBaseComputation(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& compCtx) const {
        return compCtx.HolderFactory.Create<TValue>();
    }

private:
    void RegisterDependencies() const final {
    }
};

TComputationNodeFactory GetCommonDqFactory() {
    return [] (TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
            TStringBuf name = callable.GetType()->GetName();
            if (name == "DqNotify") {
                return new TDqNotify(ctx.Mutables);
            }

            return nullptr;
        };
}

} // NYql
