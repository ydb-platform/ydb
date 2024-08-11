#include "mkql_map_join.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {


IComputationNode* WrapBlockMapJoinCore(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 4, "Expected 4 args");

    const auto joinType = callable.GetType()->GetReturnType();
    MKQL_ENSURE(joinType->IsFlow(), "Expected WideFlow as a resulting stream");
    const auto joinFlowType = AS_TYPE(TFlowType, joinType);
    MKQL_ENSURE(joinFlowType->GetItemType()->IsMulti(),
                "Expected Multi as a resulting item type");
    const auto joinComponents = GetWideComponents(joinFlowType);
    MKQL_ENSURE(joinComponents.size() > 0, "Expected at least one column");

    const auto leftFlowNode = callable.GetInput(0);
    MKQL_ENSURE(leftFlowNode.GetStaticType()->IsFlow(),
                "Expected WideFlow as a left stream");
    const auto leftFlowType = AS_TYPE(TFlowType, leftFlowNode);
    MKQL_ENSURE(leftFlowType->GetItemType()->IsMulti(),
                "Expected Multi as a left stream item type");
    const auto leftFlowComponents = GetWideComponents(leftFlowType);
    MKQL_ENSURE(leftFlowComponents.size() > 0, "Expected at least one column");

    const auto rightDictNode = callable.GetInput(1);
    const auto rightDictType = AS_TYPE(TDictType, rightDictNode);

    const auto joinKindNode = callable.GetInput(2);
    const auto rawKind = AS_VALUE(TDataLiteral, joinKindNode)->AsValue().Get<ui32>();
    const auto kind = GetJoinKind(rawKind);
    // TODO: Handle other join types.
    Y_ENSURE(kind == EJoinKind::LeftSemi);

    const auto tupleLiteral = AS_VALUE(TTupleLiteral, callable.GetInput(3));
    TVector<ui32> leftKeyColumns;
    leftKeyColumns.reserve(tupleLiteral->GetValuesCount());
    for (ui32 i = 0; i < tupleLiteral->GetValuesCount(); i++) {
        const auto item = AS_VALUE(TDataLiteral, tupleLiteral->GetValue(i));
        leftKeyColumns.emplace_back(item->AsValue().Get<ui32>());
    }
    // TODO: Handle multi keys.
    Y_ENSURE(leftKeyColumns.size() == 1);

    const auto flow = LocateNode(ctx.NodeLocator, callable, 0);
    const auto dict = LocateNode(ctx.NodeLocator, callable, 1);
    Y_UNUSED(dictType);
    Y_UNUSED(leftKeyColumns);
    Y_UNUSED(flow);
    Y_UNUSED(dict);
    return nullptr;
}

} // namespace NMiniKQL
} // namespace NKikimr
