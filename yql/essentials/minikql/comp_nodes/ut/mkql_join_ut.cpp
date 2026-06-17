#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLCommonJoinCoreTupleTest) {
Y_UNIT_TEST_LLVM(Inner) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalType = NTest::ConvertToMinikqlType<TMaybe<i32>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>, ui32>>{
                                                               {i32(1), TMaybe<i32>{}, i32(-1), ui32(0)},
                                                               {i32(3), TMaybe<i32>{}, i32(-3), ui32(1)},
                                                               {TMaybe<i32>{}, i32(2), i32(-2), ui32(0)},
                                                               {i32(4), i32(4), TMaybe<i32>{}, ui32(1)},
                                                           });

    const auto outputType = pb.NewFlowType(pb.NewMultiType({optionalType, optionalType}));
    const auto pgmReturn = pb.Collect(pb.CommonJoinCore(pb.ToFlow(list), EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, std::nullopt, EAnyJoinSettings::None, 3U, outputType));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), TMaybe<i32>{}},
                                                          {i32(1), i32(4)},
                                                          {TMaybe<i32>{}, TMaybe<i32>{}},
                                                          {TMaybe<i32>{}, i32(4)},
                                                      });
}

Y_UNIT_TEST_LLVM(InnerOrderLeftFirst) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalType = NTest::ConvertToMinikqlType<TMaybe<i32>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>, ui32>>{
                                                               {i32(1), TMaybe<i32>{}, i32(-1), ui32(0)},
                                                               {TMaybe<i32>{}, i32(2), i32(-2), ui32(0)},
                                                               {i32(3), TMaybe<i32>{}, i32(-3), ui32(1)},
                                                               {i32(4), i32(4), TMaybe<i32>{}, ui32(1)},
                                                           });

    const auto outputType = pb.NewFlowType(pb.NewMultiType({optionalType, optionalType}));
    const auto pgmReturn = pb.Collect(pb.CommonJoinCore(pb.ToFlow(list), EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, {0U}, EAnyJoinSettings::None, 3U, outputType));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), TMaybe<i32>{}},
                                                          {TMaybe<i32>{}, TMaybe<i32>{}},
                                                          {i32(1), i32(4)},
                                                          {TMaybe<i32>{}, i32(4)},
                                                      });
}

Y_UNIT_TEST_LLVM(InnerOrderRightFirst) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalType = NTest::ConvertToMinikqlType<TMaybe<i32>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>, ui32>>{
                                                               {i32(3), TMaybe<i32>{}, i32(-3), ui32(1)},
                                                               {i32(4), i32(4), TMaybe<i32>{}, ui32(1)},
                                                               {i32(1), TMaybe<i32>{}, i32(-1), ui32(0)},
                                                               {TMaybe<i32>{}, i32(2), i32(-2), ui32(0)},
                                                           });

    const auto outputType = pb.NewFlowType(pb.NewMultiType({optionalType, optionalType}));
    const auto pgmReturn = pb.Collect(pb.CommonJoinCore(pb.ToFlow(list), EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, {1U}, EAnyJoinSettings::None, 3U, outputType));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), TMaybe<i32>{}},
                                                          {i32(1), i32(4)},
                                                          {TMaybe<i32>{}, TMaybe<i32>{}},
                                                          {TMaybe<i32>{}, i32(4)},
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLCommonJoinCoreTupleTest)

Y_UNIT_TEST_SUITE(TMiniKQLCommonJoinCoreWideTest) {
Y_UNIT_TEST_LLVM(Inner) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalType = NTest::ConvertToMinikqlType<TMaybe<i32>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>, ui32>>{
                                                               {i32(1), TMaybe<i32>{}, i32(-1), ui32(0)},
                                                               {i32(3), TMaybe<i32>{}, i32(-3), ui32(1)},
                                                               {TMaybe<i32>{}, i32(2), i32(-2), ui32(0)},
                                                               {i32(4), i32(4), TMaybe<i32>{}, ui32(1)},
                                                           });

    const auto outputType = pb.NewFlowType(pb.NewMultiType({optionalType, optionalType}));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.CommonJoinCore(pb.ExpandMap(pb.ToFlow(list),
                                                                                  [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U)}; }),
                                                                     EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, std::nullopt, EAnyJoinSettings::None, 3U, outputType),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), TMaybe<i32>{}},
                                                          {i32(1), i32(4)},
                                                          {TMaybe<i32>{}, TMaybe<i32>{}},
                                                          {TMaybe<i32>{}, i32(4)},
                                                      });
}

Y_UNIT_TEST_LLVM(InnerOrderLeftFirst) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalType = NTest::ConvertToMinikqlType<TMaybe<i32>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>, ui32>>{
                                                               {i32(1), TMaybe<i32>{}, i32(-1), ui32(0)},
                                                               {TMaybe<i32>{}, i32(2), i32(-2), ui32(0)},
                                                               {i32(3), TMaybe<i32>{}, i32(-3), ui32(1)},
                                                               {i32(4), i32(4), TMaybe<i32>{}, ui32(1)},
                                                           });

    const auto outputType = pb.NewFlowType(pb.NewMultiType({optionalType, optionalType}));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.CommonJoinCore(pb.ExpandMap(pb.ToFlow(list),
                                                                                  [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U)}; }),
                                                                     EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, {0U}, EAnyJoinSettings::None, 3U, outputType),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), TMaybe<i32>{}},
                                                          {TMaybe<i32>{}, TMaybe<i32>{}},
                                                          {i32(1), i32(4)},
                                                          {TMaybe<i32>{}, i32(4)},
                                                      });
}

Y_UNIT_TEST_LLVM(InnerOrderRightFirst) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optionalType = NTest::ConvertToMinikqlType<TMaybe<i32>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>, ui32>>{
                                                               {i32(3), TMaybe<i32>{}, i32(-3), ui32(1)},
                                                               {i32(4), i32(4), TMaybe<i32>{}, ui32(1)},
                                                               {i32(1), TMaybe<i32>{}, i32(-1), ui32(0)},
                                                               {TMaybe<i32>{}, i32(2), i32(-2), ui32(0)},
                                                           });

    const auto outputType = pb.NewFlowType(pb.NewMultiType({optionalType, optionalType}));
    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.CommonJoinCore(pb.ExpandMap(pb.ToFlow(list),
                                                                                  [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U), pb.Nth(item, 3U)}; }),
                                                                     EJoinKind::Inner, {0U, 0U}, {1U, 1U}, {}, {2U}, 0ULL, {1U}, EAnyJoinSettings::None, 3U, outputType),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<i32>, TMaybe<i32>>>{
                                                          {i32(1), TMaybe<i32>{}},
                                                          {i32(1), i32(4)},
                                                          {TMaybe<i32>{}, TMaybe<i32>{}},
                                                          {TMaybe<i32>{}, i32(4)},
                                                      });
}

Y_UNIT_TEST_LLVM(ExclusionOrderLeftFirstAny) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optStrType = NTest::ConvertToMinikqlType<TMaybe<NTest::TUtf8>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<NTest::TUtf8>, TMaybe<NTest::TUtf8>, ui32>>{
                                                               {i32(1), TMaybe<NTest::TUtf8>{}, NTest::TUtf8{"very long value 1"}, ui32(1)},
                                                               {i32(2), TMaybe<NTest::TUtf8>{}, NTest::TUtf8{"very long value 2"}, ui32(1)},
                                                               {i32(3), TMaybe<NTest::TUtf8>{}, NTest::TUtf8{"very long value 3"}, ui32(1)},
                                                               {i32(4), TMaybe<NTest::TUtf8>{}, TMaybe<NTest::TUtf8>{}, ui32(1)},
                                                           });

    const auto outputType = pb.NewFlowType(pb.NewMultiType({optStrType, optStrType}));

    const auto landmine = NTest::ConvertValueToLiteralNode(pb, NTest::TUtf8{"ACHTUNG MINEN!"});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.CommonJoinCore(pb.ExpandMap(pb.ToFlow(list),
                                                                                  [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.NewOptional(pb.Unwrap(pb.Nth(item, 2U), landmine, __FILE__, __LINE__, 0)), pb.Nth(item, 3U)}; }),
                                                                     EJoinKind::Exclusion, {1U, 0U}, {2U, 1U}, {0U}, {0U}, 0ULL, {0U}, EAnyJoinSettings::Right, 3U, outputType),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<TStringBuf>, TMaybe<TStringBuf>>>{
                                                          {{}, "very long value 1"},
                                                      });
}

Y_UNIT_TEST_LLVM(ExclusionOrderRightFirstAny) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto optStrType = NTest::ConvertToMinikqlType<TMaybe<NTest::TUtf8>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<i32>, TMaybe<NTest::TUtf8>, TMaybe<NTest::TUtf8>, ui32>>{
                                                               {i32(1), NTest::TUtf8{"very long value 1"}, TMaybe<NTest::TUtf8>{}, ui32(0)},
                                                               {i32(2), NTest::TUtf8{"very long value 2"}, TMaybe<NTest::TUtf8>{}, ui32(0)},
                                                               {i32(3), NTest::TUtf8{"very long value 3"}, TMaybe<NTest::TUtf8>{}, ui32(0)},
                                                               {i32(4), TMaybe<NTest::TUtf8>{}, TMaybe<NTest::TUtf8>{}, ui32(0)},
                                                           });

    const auto outputType = pb.NewFlowType(pb.NewMultiType({optStrType, optStrType}));

    const auto landmine = NTest::ConvertValueToLiteralNode(pb, NTest::TUtf8{"ACHTUNG MINEN!"});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.CommonJoinCore(pb.ExpandMap(pb.ToFlow(list),
                                                                                  [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.NewOptional(pb.Unwrap(pb.Nth(item, 1U), landmine, __FILE__, __LINE__, 0)), pb.Nth(item, 2U), pb.Nth(item, 3U)}; }),
                                                                     EJoinKind::Exclusion, {1U, 0U}, {2U, 1U}, {0U}, {0U}, 0ULL, {1U}, EAnyJoinSettings::Left, 3U, outputType),
                                                   [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<TStringBuf>, TMaybe<TStringBuf>>>{
                                                          {"very long value 1", {}},
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLCommonJoinCoreWideTest)

} // namespace NMiniKQL
} // namespace NKikimr
