#include "dq_setup.h"
#include "dq_factories.h"
#include "utils.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/comp_nodes/dq_scalar_hash_join.h>
#include <ydb/library/yql/dq/comp_nodes/dq_program_builder.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

NUdf::TUnboxedValue DoTestDqScalarHashJoin(
    TSetup<false>& setup,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns,
    EJoinKind joinKind
) {
    TDqProgramBuilder dpb(setup.PgmBuilder->GetTypeEnvironment(), *setup.FunctionRegistry);

    TRuntimeNode leftList = dpb.Arg(leftType);
    TRuntimeNode rightList = dpb.Arg(rightType);
    const auto leftFlow = ToWideFlow(dpb, leftList);
    const auto rightFlow = ToWideFlow(dpb, rightList);
    
    TVector<TType*> resultTypes;
    
    auto leftComponents = GetWideComponents(leftFlow.GetStaticType());
    for (auto* type : leftComponents) {
        resultTypes.push_back(type);
    }
    
    auto rightComponents = GetWideComponents(rightFlow.GetStaticType());
    for (auto* type : rightComponents) {
        resultTypes.push_back(type);
    }
    
    auto resultMultiType = dpb.NewMultiType(resultTypes);
    auto resultFlowType = dpb.NewFlowType(resultMultiType);
    
    const auto joinNode = dpb.DqScalarHashJoin(leftFlow, rightFlow, joinKind, leftKeyColumns, rightKeyColumns, resultFlowType);
    
    const auto resultNode = FromWideFlow(dpb, joinNode);

    const auto graph = setup.BuildGraph(resultNode, {leftList.GetNode(), rightList.GetNode()});
    auto& ctx = graph->GetContext();

    graph->GetEntryPoint(0, true)->SetValue(ctx, std::move(leftListValue));
    graph->GetEntryPoint(1, true)->SetValue(ctx, std::move(rightListValue));
    return graph->GetValue();
}

} // namespace

Y_UNIT_TEST_SUITE(TDqScalarHashJoinTest) {
    Y_UNIT_TEST(TestSimpleScalarHashJoin) {
        TSetup<false> setup(GetDqNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;


        const auto leftDataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto leftStringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto leftTupleType = pb.NewTupleType({leftDataType, leftStringType});
        const auto leftListType = pb.NewListType(leftTupleType);

        TVector<TRuntimeNode> leftItems;
        leftItems.emplace_back(pb.NewTuple({pb.NewDataLiteral<ui32>(1), pb.NewDataLiteral<NUdf::EDataSlot::String>("a")}));
        leftItems.emplace_back(pb.NewTuple({pb.NewDataLiteral<ui32>(2), pb.NewDataLiteral<NUdf::EDataSlot::String>("b")}));
        const auto leftListValue = pb.NewList(leftTupleType, leftItems);


        const auto rightDataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto rightStringType = pb.NewDataType(NUdf::TDataType<char*>::Id);
        const auto rightTupleType = pb.NewTupleType({rightDataType, rightStringType});
        const auto rightListType = pb.NewListType(rightTupleType);

        TVector<TRuntimeNode> rightItems;
        rightItems.emplace_back(pb.NewTuple({pb.NewDataLiteral<ui32>(1), pb.NewDataLiteral<NUdf::EDataSlot::String>("x")}));
        rightItems.emplace_back(pb.NewTuple({pb.NewDataLiteral<ui32>(3), pb.NewDataLiteral<NUdf::EDataSlot::String>("y")}));
        const auto rightListValue = pb.NewList(rightTupleType, rightItems);

        const auto leftGraph = setup.BuildGraph(leftListValue);
        const auto rightGraph = setup.BuildGraph(rightListValue);
        const auto leftValue = leftGraph->GetValue();
        const auto rightValue = rightGraph->GetValue();


        TVector<ui32> leftKeyColumns = {0};
        TVector<ui32> rightKeyColumns = {0};


        const auto result = DoTestDqScalarHashJoin(
            setup,
            leftListType, NUdf::TUnboxedValue(leftValue), leftKeyColumns,
            rightListType, NUdf::TUnboxedValue(rightValue), rightKeyColumns,
            EJoinKind::Inner
        );

        UNIT_ASSERT(result.HasValue());
        const auto resultIterator = result.GetListIterator();

        TVector<NUdf::TUnboxedValue> resultItems;
        for (NUdf::TUnboxedValue item; resultIterator.Next(item);) {
            UNIT_ASSERT(item.HasValue());
            resultItems.push_back(item);
        }

        UNIT_ASSERT_VALUES_EQUAL(resultItems.size(), 4);

        for (ui32 i = 0; i < 2; ++i) {
            auto tuple = resultItems[i];
            UNIT_ASSERT(tuple.HasValue());
            
            auto leftId = tuple.GetElement(0);
            UNIT_ASSERT(leftId.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(leftId.Get<ui32>(), i + 1);
            
            auto leftStr = tuple.GetElement(1);
            UNIT_ASSERT(leftStr.HasValue());
            ui32 expectedAscii = (i == 0) ? 97 : 98;
            UNIT_ASSERT_VALUES_EQUAL(leftStr.Get<ui32>(), expectedAscii);
            auto rightId = tuple.GetElement(2);
            auto rightStr = tuple.GetElement(3);
            UNIT_ASSERT(!rightId.HasValue());
            UNIT_ASSERT(!rightStr.HasValue());
        }
        

        for (ui32 i = 2; i < 4; ++i) {
            auto tuple = resultItems[i];
            UNIT_ASSERT(tuple.HasValue());
            
            auto leftId = tuple.GetElement(0);
            auto leftStr = tuple.GetElement(1);
            UNIT_ASSERT(leftId.HasValue());
            UNIT_ASSERT(leftStr.HasValue());
            UNIT_ASSERT_VALUES_EQUAL(leftId.Get<ui32>(), 2);
            UNIT_ASSERT_VALUES_EQUAL(leftStr.Get<ui32>(), 98);
            
            auto rightId = tuple.GetElement(2);
            auto rightStr = tuple.GetElement(3);
            UNIT_ASSERT(rightId.HasValue());
            UNIT_ASSERT(rightStr.HasValue());
            
            ui32 expectedRightId = (i == 2) ? 1 : 3;
            ui32 expectedRightStr = (i == 2) ? 120 : 121;
            UNIT_ASSERT_VALUES_EQUAL(rightId.Get<ui32>(), expectedRightId);
            UNIT_ASSERT_VALUES_EQUAL(rightStr.Get<ui32>(), expectedRightStr);
        }
    }

    Y_UNIT_TEST(TestEmptyFlows) {
        TSetup<false> setup(GetDqNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto dataType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto tupleType = pb.NewTupleType({dataType});
        const auto listType = pb.NewListType(tupleType);
        const auto emptyListValue = pb.NewEmptyList(tupleType);

        const auto leftGraph = setup.BuildGraph(emptyListValue);
        const auto rightGraph = setup.BuildGraph(emptyListValue);
        const auto leftValue = leftGraph->GetValue();
        const auto rightValue = rightGraph->GetValue();

        TVector<ui32> leftKeyColumns = {0};
        TVector<ui32> rightKeyColumns = {0};

        const auto result = DoTestDqScalarHashJoin(
            setup,
            listType, NUdf::TUnboxedValue(leftValue), leftKeyColumns,
            listType, NUdf::TUnboxedValue(rightValue), rightKeyColumns,
            EJoinKind::Inner
        );

        UNIT_ASSERT(result.HasValue());
        const auto resultIterator = result.GetListIterator();
        
        TVector<NUdf::TUnboxedValue> resultItems;
        for (NUdf::TUnboxedValue item; resultIterator.Next(item);) {
            UNIT_ASSERT(item.HasValue());
            resultItems.push_back(item);
        }
        
        UNIT_ASSERT_VALUES_EQUAL(resultItems.size(), 0);
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
