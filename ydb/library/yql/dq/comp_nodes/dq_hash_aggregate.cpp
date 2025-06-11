#include "dq_hash_aggregate.h"
#include "dq_hash_operator_common.h"
#include "dq_hash_operator_serdes.h"

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {


class TDqHashAggregate: public TStatefulWideFlowComputationNode<TDqHashAggregate>
{
using TBaseComputation = TStatefulWideFlowComputationNode<TDqHashAggregate>;
public:
    TDqHashAggregate(
        TComputationMutables& mutables,
        IComputationWideFlowNode* flow,
        NDqHashOperatorCommon::TCombinerNodes&& nodes,
        const TMultiType* usedInputItemType,
        TKeyTypes&& keyTypes,
        const TMultiType* keyAndStateType,
        bool allowSpilling)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow(flow)
        , Nodes(std::move(nodes))
        , KeyTypes(std::move(keyTypes))
        , UsedInputItemType(usedInputItemType)
        , KeyAndStateType(keyAndStateType)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Nodes.ItemNodes.size()))
        , AllowSpilling(allowSpilling)
    {
        Y_UNUSED(AllowSpilling, UsedInputItemType, KeyAndStateType);
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue*const* output) const {
        Y_UNUSED(state, ctx, output);
        THROW yexception() << "Not implemented yet";
    }

private:
    void RegisterDependencies() const final {
        if (const auto flow = this->FlowDependsOn(Flow)) {
            Nodes.RegisterDependencies(
                [this, flow](IComputationNode* node){ this->DependsOn(flow, node); },
                [this, flow](IComputationExternalNode* node){ this->Own(flow, node); }
            );
        }
    }

    IComputationWideFlowNode *const Flow;
    const NDqHashOperatorCommon::TCombinerNodes Nodes;
    const TKeyTypes KeyTypes;

    const TMultiType* const UsedInputItemType;
    const TMultiType* const KeyAndStateType;

    const ui32 WideFieldsIndex;

    const bool AllowSpilling;
};

}

IComputationNode* WrapDqHashAggregate(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TDqHashOperatorParams params = ParseCommonDqHashOperatorParams(callable, ctx);

    const auto flow = LocateNode(ctx.NodeLocator, callable, NDqHashOperatorParams::Flow);
    auto* wideFlow = dynamic_cast<IComputationWideFlowNode*>(flow);
    if (!wideFlow) {
        THROW yexception() << "Expected wide flow.";
    };

    const TTupleLiteral* operatorParams = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::OperatorParams));
    const bool allowSpilling = AS_VALUE(TDataLiteral, operatorParams->GetValue(NDqHashOperatorParams::AggregateParamEnableSpilling))->AsValue().Get<bool>();

    const auto inputType = AS_TYPE(TFlowType, callable.GetInput(NDqHashOperatorParams::Flow).GetStaticType());
    const auto inputItemTypes = GetWideComponents(inputType);

    return new TDqHashAggregate(ctx.Mutables, wideFlow, std::move(params.Nodes),
        TMultiType::Create(inputItemTypes.size(), inputItemTypes.data(), ctx.Env),
        std::move(params.KeyTypes),
        TMultiType::Create(params.KeyAndStateItemTypes.size(),params.KeyAndStateItemTypes.data(), ctx.Env),
        allowSpilling
    );
}

}
}
