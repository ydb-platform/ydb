#include "datashard_kqp_compute.h"

#include <ydb/core/engine/mkql_keys.h>
#include <ydb/core/engine/mkql_engine_flat_host.h>
#include <ydb/core/kqp/runtime/kqp_read_table.h>

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>

#include <util/generic/cast.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NTable;
using namespace NUdf;

namespace {

class TKqpEffectsWrapper : public TMutableComputationNode<TKqpEffectsWrapper> {
    using TBase = TMutableComputationNode<TKqpEffectsWrapper>;

public:
    class TEffectsValue : public TComputationValue<TEffectsValue> {
    public:
        using TBase = TComputationValue<TEffectsValue>;

        TEffectsValue(TMemoryUsageInfo* memInfo, TUnboxedValueVector&& effects)
            : TBase(memInfo)
            , Effects(std::move(effects)) {}

        EFetchStatus Fetch(TUnboxedValue& result) final {
            if (Index >= Effects.size()) {
                return EFetchStatus::Finish;
            }

            while (Index < Effects.size()) {
                const auto status = Effects[Index].Fetch(result);
                switch (status) {
                    case EFetchStatus::Finish:
                        ++Index;
                        break;

                    default:
                        return status;
                }
            }

            result = TUnboxedValue();
            return EFetchStatus::Finish;
        }

    private:
        const TUnboxedValueVector Effects;
        ui32 Index = 0;
    };

    TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        TUnboxedValueVector values;
        values.reserve(Effects.size());
        std::transform(Effects.cbegin(), Effects.cend(), std::back_inserter(values),
            std::bind(&IComputationNode::GetValue, std::placeholders::_1, std::ref(ctx))
        );

        return ctx.HolderFactory.Create<TEffectsValue>(std::move(values));
    }

public:
    TKqpEffectsWrapper(TComputationMutables& mutables, TComputationNodePtrVector&& effects)
        : TBase(mutables)
        , Effects(std::move(effects)) {}

private:
    void RegisterDependencies() const final {
        std::for_each(Effects.cbegin(), Effects.cend(),
            std::bind(&TKqpEffectsWrapper::DependsOn, this, std::placeholders::_1));
    }

private:
    const TComputationNodePtrVector Effects;
};

} // namespace

IComputationNode* WrapKqpEffects(TCallable& callable, const TComputationNodeFactoryContext& ctx,
    TKqpDatashardComputeContext& computeCtx)
{
    Y_UNUSED(computeCtx);

    TComputationNodePtrVector effectNodes;
    effectNodes.reserve(callable.GetInputsCount());
    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
        effectNodes.emplace_back(LocateNode(ctx.NodeLocator, callable, i));
    }

    return new TKqpEffectsWrapper(ctx.Mutables, std::move(effectNodes));
}

} // namspace NMiniKQL
} // namespace NKikimr
