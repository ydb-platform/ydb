#include "dq_hash_combine.h"
#include "dq_hash_operator_common.h"
#include "dq_hash_operator_serdes.h"

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/mkql_node_builder.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/defs.h>

namespace NKikimr {
namespace NMiniKQL {

class TDqHashCombine: public TStatefulWideFlowComputationNode<TDqHashCombine>
{
using TBaseComputation = TStatefulWideFlowComputationNode<TDqHashCombine>;
public:
    TDqHashCombine(TComputationMutables& mutables, IComputationWideFlowNode* flow, NDqHashOperatorCommon::TCombinerNodes&& nodes, TKeyTypes&& keyTypes, ui64 memLimit)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow(flow)
        , Nodes(std::move(nodes))
        , KeyTypes(std::move(keyTypes))
        , MemLimit(memLimit)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Nodes.ItemNodes.size()))
    {
        Y_UNUSED(MemLimit);
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state, TComputationContext& ctx, NUdf::TUnboxedValue* const* output) const {
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
    const ui64 MemLimit;

    const ui32 WideFieldsIndex;
};

IComputationNode* WrapDqHashCombine(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    TDqHashOperatorParams params = ParseCommonDqHashOperatorParams(callable, ctx);

    const auto flow = LocateNode(ctx.NodeLocator, callable, NDqHashOperatorParams::Flow);

    auto* wideFlow = dynamic_cast<IComputationWideFlowNode*>(flow);
    if (!wideFlow) {
        THROW yexception() << "Expected wide flow.";
    };

    const TTupleLiteral* operatorParams = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::OperatorParams));
    const auto memLimit = AS_VALUE(TDataLiteral, operatorParams->GetValue(NDqHashOperatorParams::CombineParamMemLimit))->AsValue().Get<ui64>();

    return new TDqHashCombine(ctx.Mutables, wideFlow, std::move(params.Nodes), std::move(params.KeyTypes), ui64(memLimit));
}

}
}
