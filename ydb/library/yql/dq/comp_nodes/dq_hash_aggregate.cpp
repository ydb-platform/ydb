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


class TDqHashAggregate: public TMutableComputationNode<TDqHashAggregate>
{
using TBaseComputation = TMutableComputationNode<TDqHashAggregate>;
public:
    TDqHashAggregate(
        TComputationMutables& mutables,
        IComputationNode* streamSource,
        NDqHashOperatorCommon::TCombinerNodes&& nodes,
        const TMultiType* usedInputItemType,
        TKeyTypes&& keyTypes,
        const TMultiType* keyAndStateType,
        bool allowSpilling)
        : TBaseComputation(mutables, EValueRepresentation::Boxed)
        , StreamSource(streamSource)
        , Nodes(std::move(nodes))
        , KeyTypes(std::move(keyTypes))
        , UsedInputItemType(usedInputItemType)
        , KeyAndStateType(keyAndStateType)
        , WideFieldsIndex(mutables.IncrementWideFieldsIndex(Nodes.ItemNodes.size()))
        , AllowSpilling(allowSpilling)
    {
        Y_UNUSED(AllowSpilling, UsedInputItemType, KeyAndStateType);
    }

    NUdf::TUnboxedValue DoCalculate([[maybe_unused]] TComputationContext& ctx) const {
        THROW yexception() << "Not implemented yet";
    }

private:
    void RegisterDependencies() const final {
        DependsOn(StreamSource);
        Nodes.RegisterDependencies(
            [this](IComputationNode* node){ this->DependsOn(node); },
            [this](IComputationExternalNode* node){ this->Own(node); }
        );
    }

    IComputationNode *const StreamSource;
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

    const auto inputNode = LocateNode(ctx.NodeLocator, callable, NDqHashOperatorParams::Input);

    const TTupleLiteral* operatorParams = AS_VALUE(TTupleLiteral, callable.GetInput(NDqHashOperatorParams::OperatorParams));
    const bool allowSpilling = AS_VALUE(TDataLiteral, operatorParams->GetValue(NDqHashOperatorParams::AggregateParamEnableSpilling))->AsValue().Get<bool>();

    const auto inputType = AS_TYPE(TStreamType, callable.GetInput(NDqHashOperatorParams::Input).GetStaticType());
    const auto inputItemTypes = GetWideComponents(inputType);

    std::vector<TType*> keyAndStateTypes;
    keyAndStateTypes.insert(keyAndStateTypes.end(), params.KeyItemTypes.begin(), params.KeyItemTypes.end());
    keyAndStateTypes.insert(keyAndStateTypes.end(), params.StateItemTypes.begin(), params.StateItemTypes.end());

    return new TDqHashAggregate(ctx.Mutables, inputNode, std::move(params.Nodes),
        TMultiType::Create(inputItemTypes.size(), inputItemTypes.data(), ctx.Env),
        std::move(params.KeyTypes),
        TMultiType::Create(keyAndStateTypes.size(),keyAndStateTypes.data(), ctx.Env),
        allowSpilling
    );
}

}
}
