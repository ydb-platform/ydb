#include "dq_setup.h"
#include "dq_factories.h"

#include <yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/dq/comp_nodes/dq_block_hash_join.h>

namespace NKikimr {
namespace NMiniKQL {

// TODO (mfilitov): think how can we reuse the code
// Code from https://github.com/ydb-platform/ydb/blob/main/yql/essentials/minikql/comp_nodes/ut/mkql_block_map_join_ut_utils.h
namespace {

// List<Tuple<...>> -> Stream<Multi<...>>
TRuntimeNode ToWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode list) {
    auto wideFlow = pgmBuilder.ExpandMap(pgmBuilder.ToFlow(list),
        [&](TRuntimeNode tupleNode) -> TRuntimeNode::TList {
            TTupleType* tupleType = AS_TYPE(TTupleType, tupleNode.GetStaticType());
            TRuntimeNode::TList wide;
            wide.reserve(tupleType->GetElementsCount());
            for (size_t i = 0; i < tupleType->GetElementsCount(); i++) {
                wide.emplace_back(pgmBuilder.Nth(tupleNode, i));
            }
            return wide;
        }
    );
    return pgmBuilder.FromFlow(wideFlow);
}

// Stream<Multi<...>> -> List<Tuple<...>>
TRuntimeNode FromWideStream(TProgramBuilder& pgmBuilder, TRuntimeNode stream) {
    return pgmBuilder.Collect(pgmBuilder.NarrowMap(pgmBuilder.ToFlow(stream),
        [&](TRuntimeNode::TList items) -> TRuntimeNode {
            TVector<TRuntimeNode> tupleElements;
            tupleElements.reserve(items.size());
            for (size_t i = 0; i < items.size(); i++) {
                tupleElements.emplace_back(items[i]);
            }
            return pgmBuilder.NewTuple(tupleElements);
        })
    );
}

NUdf::TUnboxedValue DoTestDqBlockHashJoin(
    TDqSetup<false>& setup,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns,
    EJoinKind joinKind
) {
    TDqProgramBuilder& pb = setup.GetDqProgramBuilder();

    TRuntimeNode leftList = pb.Arg(leftType);
    TRuntimeNode rightList = pb.Arg(rightType);
    const auto leftStream = ToWideStream(pb, leftList);
    const auto rightStream = ToWideStream(pb, rightList);
    const auto joinNode = pb.DqBlockHashJoin(leftStream, rightStream, joinKind, leftKeyColumns, rightKeyColumns, leftStream.GetStaticType());
    
    const auto resultNode = FromWideStream(pb, joinNode);

    const auto graph = setup.BuildGraph(resultNode, {leftList.GetNode(), rightList.GetNode()});
    auto& ctx = graph->GetContext();

    graph->GetEntryPoint(0, true)->SetValue(ctx, std::move(leftListValue));
    graph->GetEntryPoint(1, true)->SetValue(ctx, std::move(rightListValue));
    return graph->GetValue();
}

TVector<NUdf::TUnboxedValue> ConvertListToVector(const NUdf::TUnboxedValue& list) {
    NUdf::TUnboxedValue current;
    NUdf::TUnboxedValue iterator = list.GetListIterator();
    TVector<NUdf::TUnboxedValue> items;
    while (iterator.Next(current)) {
        items.push_back(current);
    }
    return items;
}

static void CompareResults(const TType* type, const NUdf::TUnboxedValue& expected,
        const NUdf::TUnboxedValue& got
) {
    const auto itemType = AS_TYPE(TListType, type)->GetItemType();
    const NUdf::ICompare::TPtr compare = MakeCompareImpl(itemType);
    const NUdf::IEquate::TPtr equate = MakeEquateImpl(itemType);
    // XXX: Stub both keyTypes and isTuple arguments, since
    // ICompare/IEquate are used.
    TKeyTypes keyTypesStub;
    bool isTupleStub = false;
    const TValueLess valueLess(keyTypesStub, isTupleStub, compare.Get());
    const TValueEqual valueEqual(keyTypesStub, isTupleStub, equate.Get());

    auto expectedItems = ConvertListToVector(expected);
    auto gotItems = ConvertListToVector(got);
    UNIT_ASSERT_VALUES_EQUAL(expectedItems.size(), gotItems.size());
    Sort(expectedItems, valueLess);
    Sort(gotItems, valueLess);
    for (size_t i = 0; i < expectedItems.size(); i++) {
        UNIT_ASSERT(valueEqual(gotItems[i], expectedItems[i]));
    }
}

void RunTestDqBlockHashJoin(
    TDqSetup<false>& setup, EJoinKind joinKind,
    TType* expectedType, const NUdf::TUnboxedValue& expected,
    TType* leftType, NUdf::TUnboxedValue&& leftListValue, const TVector<ui32>& leftKeyColumns,
    TType* rightType, NUdf::TUnboxedValue&& rightListValue, const TVector<ui32>& rightKeyColumns
) {
    const auto got = DoTestDqBlockHashJoin(
        setup,
        leftType, std::move(leftListValue), leftKeyColumns,
        rightType, std::move(rightListValue), rightKeyColumns,
        joinKind
    );
    
    UNIT_ASSERT(got.HasValue());
    CompareResults(expectedType, expected, got);
}

//
// Auxiliary routines to build list nodes from the given vectors.
//

template<typename Type>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector
) {
    TType* itemType;
    if constexpr (std::is_same_v<Type, std::optional<TString>>) {
        itemType = pb.NewOptionalType(pb.NewDataType(NUdf::EDataSlot::String));
    } else if constexpr (std::is_same_v<Type, TString>) {
        itemType = pb.NewDataType(NUdf::EDataSlot::String);
    } else {
        itemType = pb.NewDataType(NUdf::TDataType<Type>::Id);
    }

    TRuntimeNode::TList listItems;
    std::transform(vector.cbegin(), vector.cend(), std::back_inserter(listItems),
        [&](const auto value) {
            if constexpr (std::is_same_v<Type, std::optional<TString>>) {
                if (value == std::nullopt) {
                    return pb.NewEmptyOptional(itemType);
                } else {
                    return pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>(*value));
                }
            } else if constexpr (std::is_same_v<Type, TString>) {
                return pb.NewDataLiteral<NUdf::EDataSlot::String>(value);
            } else {
                return pb.NewDataLiteral<Type>(value);
            }
        });

    return {pb.NewList(itemType, listItems)};
}

template<typename Type, typename... Tail>
const TVector<const TRuntimeNode> BuildListNodes(TProgramBuilder& pb,
    const TVector<Type>& vector, Tail... vectors
) {
    const auto frontList = BuildListNodes(pb, vector);
    const auto tailLists = BuildListNodes(pb, std::forward<Tail>(vectors)...);
    TVector<const TRuntimeNode> lists;
    lists.reserve(tailLists.size() + 1);
    lists.push_back(frontList.front());
    for (const auto& list : tailLists) {
        lists.push_back(list);
    }
    return lists;
}

template<typename... TVectors>
const std::pair<TType*, NUdf::TUnboxedValue> ConvertVectorsToTuples(
    TSetup<false>& setup, TVectors... vectors
) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto lists = BuildListNodes(pb, std::forward<TVectors>(vectors)...);
    const auto tuplesNode = pb.Zip(lists);
    const auto tuplesNodeType = tuplesNode.GetStaticType();
    const auto tuples = setup.BuildGraph(tuplesNode)->GetValue();
    return std::make_pair(tuplesNodeType, tuples);
}

} // namespace

Y_UNIT_TEST_SUITE(TDqBlockHashJoinBasicTest) {

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

        RunTestDqBlockHashJoin(
            setup, EJoinKind::Inner,
            expectedType, expected,
            leftType, std::move(leftList), {0},
            rightType, std::move(rightList), {0}
        );
    }

    Y_UNIT_TEST(TestEmptyStreams) {
        TDqSetup<false> setup(GetDqNodeFactory());
        
        TVector<ui64> emptyKeys;
        TVector<TString> emptyValues;

        auto [leftType, leftList] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
        auto [rightType, rightList] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);
        auto [expectedType, expected] = ConvertVectorsToTuples(setup, emptyKeys, emptyValues);

        RunTestDqBlockHashJoin(
            setup, EJoinKind::Inner,
            expectedType, expected,
            leftType, std::move(leftList), {0},
            rightType, std::move(rightList), {0}
        );
    }

} // Y_UNIT_TEST_SUITE

} // namespace NMiniKQL
} // namespace NKikimr 
