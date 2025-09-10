#include "utils/dq_setup.h"
#include "utils/dq_factories.h"
#include "utils/utils.h"

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
    TDqSetup<false>& setup,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns,
    EJoinKind joinKind
) {
    TDqProgramBuilder& pb = setup.GetDqProgramBuilder();

    TRuntimeNode leftList = pb.Arg(leftType);
    TRuntimeNode rightList = pb.Arg(rightType);
    const auto leftFlow = ToWideFlow(pb, leftList);
    const auto rightFlow = ToWideFlow(pb, rightList);
    
    TVector<TType*> resultTypes;
    
    auto leftComponents = GetWideComponents(leftFlow.GetStaticType());
    for (auto* type : leftComponents) {
        resultTypes.push_back(type);
    }
    
    auto rightComponents = GetWideComponents(rightFlow.GetStaticType());
    for (auto* type : rightComponents) {
        resultTypes.push_back(type);
    }
    
    auto resultMultiType = pb.NewMultiType(resultTypes);
    auto resultFlowType = pb.NewFlowType(resultMultiType);
    
    const auto joinNode = pb.DqScalarHashJoin(leftFlow, rightFlow, joinKind, leftKeyColumns, rightKeyColumns, resultFlowType);
    
    const auto resultNode = FromWideFlow(pb, joinNode);

    const auto graph = setup.BuildGraph(resultNode, {leftList.GetNode(), rightList.GetNode()});
    auto& ctx = graph->GetContext();

    graph->GetEntryPoint(0, true)->SetValue(ctx, std::move(leftListValue));
    graph->GetEntryPoint(1, true)->SetValue(ctx, std::move(rightListValue));
    return graph->GetValue();
}

void RunTestDqScalarHashJoin(
    TDqSetup<false>& setup, EJoinKind joinKind,
    TType* expectedType, const NUdf::TUnboxedValue& expected,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns
) {
    const auto got = DoTestDqScalarHashJoin(
        setup,
        leftType, std::move(leftListValue), leftKeyColumns,
        rightType, std::move(rightListValue), rightKeyColumns,
        joinKind
    );
    
    UNIT_ASSERT(got.HasValue());
    CompareListsIgnoringOrder(expectedType, expected, got);
}

} // namespace

Y_UNIT_TEST_SUITE(TDqScalarHashJoinBasicTest) {

    Y_UNIT_TEST(TestBasicPassthrough) {
        TDqSetup<false> setup(GetDqNodeFactory());
        
        TVector<ui64> leftKeys = {1, 2, 3, 4, 5};
        TVector<TString> leftValues = {"a", "b", "c", "d", "e"};
        
        TVector<ui64> rightKeys = {2, 3, 4, 6, 7};
        TVector<TString> rightValues = {"x", "y", "z", "u", "v"};

        TVector<ui64> expectedKeys = {1, 2, 3, 4, 5, 2, 3, 4, 6, 7};
        TVector<TString> expectedValues = {"a", "b", "c", "d", "e", "x", "y", "z", "u", "v"};

        auto [leftType, leftList] = ConvertVectorsToTuples(setup, leftKeys, leftValues);
        auto [rightType, rightList] = ConvertVectorsToTuples(setup, rightKeys, rightValues);
        auto [expectedType, expected] = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);

        RunTestDqScalarHashJoin(
            setup, EJoinKind::Inner,
            expectedType, expected,
            leftType, std::move(leftList), {0},
            rightType, std::move(rightList), {0}
        );
    }

    Y_UNIT_TEST(TestEmptyFlows) {
        TDqSetup<false> setup(GetDqNodeFactory());
        
        TVector<ui64> emptyKeys;
        TVector<TString> emptyValues;

        auto [leftType, leftList] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
        auto [rightType, rightList] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
        auto [expectedType, expected] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);

        RunTestDqScalarHashJoin(
            setup, EJoinKind::Inner,
            expectedType, expected,
            leftType, std::move(leftList), {0},
            rightType, std::move(rightList), {0}
        );
    }

    Y_UNIT_TEST(TestEmptyLeft) {
        TDqSetup<false> setup(GetDqNodeFactory());
        
        TVector<ui64> emptyKeys;
        TVector<TString> emptyValues;
        
        TVector<ui64> rightKeys = {1, 2, 3};
        TVector<TString> rightValues = {"x", "y", "z"};

        TVector<ui64> expectedKeys = {1, 2, 3};
        TVector<TString> expectedValues = {"x", "y", "z"};

        auto [leftType, leftList] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
        auto [rightType, rightList] = ConvertVectorsToTuples(setup, rightKeys, rightValues);
        auto [expectedType, expected] = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);

        RunTestDqScalarHashJoin(
            setup, EJoinKind::Inner,
            expectedType, expected,
            leftType, std::move(leftList), {0},
            rightType, std::move(rightList), {0}
        );
    }

    Y_UNIT_TEST(TestEmptyRight) {
        TDqSetup<false> setup(GetDqNodeFactory());
        
        TVector<ui64> leftKeys = {1, 2, 3};
        TVector<TString> leftValues = {"a", "b", "c"};
        
        TVector<ui64> emptyKeys;
        TVector<TString> emptyValues;

        TVector<ui64> expectedKeys = {1, 2, 3};
        TVector<TString> expectedValues = {"a", "b", "c"};

        auto [leftType, leftList] = ConvertVectorsToTuples(setup, leftKeys, leftValues);
        auto [rightType, rightList] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
        auto [expectedType, expected] = ConvertVectorsToTuples(setup, expectedKeys, expectedValues);

        RunTestDqScalarHashJoin(
            setup, EJoinKind::Inner,
            expectedType, expected,
            leftType, std::move(leftList), {0},
            rightType, std::move(rightList), {0}
        );
    }
}

} // namespace NMiniKQL
} // namespace NKikimr
