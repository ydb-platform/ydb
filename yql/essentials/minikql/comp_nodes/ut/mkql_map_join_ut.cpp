#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {
using TKeyPayloadRow = NTest::TStructType<NTest::TStructMember<"Key", ui32>, NTest::TStructMember<"Payload", TStringBuf>>;
} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLMapJoinCoreTest) {
Y_UNIT_TEST_LLVM(TestInnerOnTuple) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TTupleKeyRow = NTest::TStructType<NTest::TStructMember<"Key", std::tuple<TMaybe<ui64>, TMaybe<ui64>>>,
                                            NTest::TStructMember<"Payload", TStringBuf>>;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TTupleKeyRow>{
                                                                {{{{ui64(1), ui64(1)}}, {"A"}}},
                                                                {{{{ui64(2), ui64(2)}}, {"B"}}},
                                                                {{{{ui64(3), {}}}, {"C"}}},
                                                            });
    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TTupleKeyRow>{
                                                                {{{{ui64(2), ui64(2)}}, {"X"}}},
                                                                {{{{ui64(3), {}}}, {"Y"}}},
                                                                {{{{ui64(4), ui64(4)}}, {"Z"}}},
                                                            });
    const auto dict2 = pb.ToSortedDict(list2, false,
                                       [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                       [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

    const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Left", TStringBuf>, NTest::TStructMember<"Right", TStringBuf>>>(pb));

    const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType));
    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                          {"B", "X"},
                                                          {"C", "Y"},
                                                      });
}

Y_UNIT_TEST_LLVM(TestInner) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(1)}, {"A"}}},
                                                                    {{{ui32(2)}, {"B"}}},
                                                                    {{{ui32(3)}, {"C"}}},
                                                                });
        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(2)}, {"X"}}},
                                                                    {{{ui32(3)}, {"Y"}}},
                                                                    {{{ui32(4)}, {"Z"}}},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, false,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

        const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Left", TStringBuf>, NTest::TStructMember<"Right", TStringBuf>>>(pb));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                              {"B", "X"},
                                                              {"C", "Y"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestInnerMulti) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(1)}, {"A"}}},
                                                                    {{{ui32(2)}, {"B"}}},
                                                                    {{{ui32(2)}, {"C"}}},
                                                                });
        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(2)}, {"X"}}},
                                                                    {{{ui32(2)}, {"Y"}}},
                                                                    {{{ui32(3)}, {"Z"}}},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

        const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Left", TStringBuf>, NTest::TStructMember<"Right", TStringBuf>>>(pb));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                              {"B", "X"},
                                                              {"B", "Y"},
                                                              {"C", "X"},
                                                              {"C", "Y"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeft) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(1)}, {"A"}}},
                                                                    {{{ui32(2)}, {"B"}}},
                                                                    {{{ui32(3)}, {"C"}}},
                                                                });
        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(2)}, {"X"}}},
                                                                    {{{ui32(3)}, {"Y"}}},
                                                                    {{{ui32(4)}, {"Z"}}},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, false,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

        const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Left", TStringBuf>, NTest::TStructMember<"Right", TStringBuf>>>(pb));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Left, {0U}, {1U, 0U}, {0U, 1U}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TMaybe<TStringBuf>>>{
                                                              {"A", {}},
                                                              {"B", "X"},
                                                              {"C", "Y"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftMulti) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(1)}, {"A"}}},
                                                                    {{{ui32(2)}, {"B"}}},
                                                                    {{{ui32(2)}, {"C"}}},
                                                                });
        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(2)}, {"X"}}},
                                                                    {{{ui32(2)}, {"Y"}}},
                                                                    {{{ui32(3)}, {"Z"}}},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

        const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Left", TStringBuf>, NTest::TStructMember<"Right", TStringBuf>>>(pb));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::Left, {0U}, {1U, 0U}, {0U, 1U}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TMaybe<TStringBuf>>>{
                                                              {"A", {}},
                                                              {"B", "X"},
                                                              {"B", "Y"},
                                                              {"C", "X"},
                                                              {"C", "Y"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftSemi) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(1)}, {"A"}}},
                                                                    {{{ui32(2)}, {"B"}}},
                                                                    {{{ui32(2)}, {"C"}}},
                                                                });
        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(2)}, {"X"}}},
                                                                    {{{ui32(2)}, {"Y"}}},
                                                                    {{{ui32(3)}, {"Z"}}},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

        const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Key", ui32>, NTest::TStructMember<"Left", TStringBuf>>>(pb));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::LeftSemi, {0U}, {1U, 1U, 0U, 0U}, {}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<ui32, TStringBuf>>{
                                                              {ui32(2), "B"},
                                                              {ui32(2), "C"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftOnly) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(1)}, {"A"}}},
                                                                    {{{ui32(2)}, {"B"}}},
                                                                    {{{ui32(2)}, {"C"}}},
                                                                });
        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TKeyPayloadRow>{
                                                                    {{{ui32(2)}, {"X"}}},
                                                                    {{{ui32(2)}, {"Y"}}},
                                                                    {{{ui32(3)}, {"Z"}}},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

        const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Key", ui32>, NTest::TStructMember<"Left", TStringBuf>>>(pb));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::LeftOnly, {0U}, {1U, 1U, 0U, 0U}, {}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<ui32, TStringBuf>>{
                                                              {ui32(1), "A"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftSemiWithNullKey) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        using TOptKeyRow = NTest::TStructType<NTest::TStructMember<"Key", TMaybe<ui32>>,
                                              NTest::TStructMember<"Payload", TStringBuf>>;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TOptKeyRow>{
                                                                    {{{{}}, {"X"}}},
                                                                    {{{{ui32(1)}}, {"A"}}},
                                                                    {{{{ui32(2)}}, {"B"}}},
                                                                    {{{{ui32(2)}}, {"C"}}},
                                                                });
        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TOptKeyRow>{
                                                                    {{{{}}, {"C"}}},
                                                                    {{{{ui32(2)}}, {"X"}}},
                                                                    {{{{ui32(2)}}, {"Y"}}},
                                                                    {{{{ui32(3)}}, {"Z"}}},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

        const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Key", ui32>, NTest::TStructMember<"Left", TStringBuf>>>(pb));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::LeftSemi, {0U}, {1U, 1U, 0U, 0U}, {}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<ui32, TStringBuf>>{
                                                              {ui32(2), "B"},
                                                              {ui32(2), "C"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftOnlyWithNullKey) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        using TOptKeyRow = NTest::TStructType<NTest::TStructMember<"Key", TMaybe<ui32>>,
                                              NTest::TStructMember<"Payload", TStringBuf>>;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TOptKeyRow>{
                                                                    {{{{}}, {"X"}}},
                                                                    {{{{ui32(1)}}, {"A"}}},
                                                                    {{{{ui32(2)}}, {"B"}}},
                                                                    {{{{ui32(2)}}, {"C"}}},
                                                                });
        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TOptKeyRow>{
                                                                    {{{{}}, {"C"}}},
                                                                    {{{{ui32(2)}}, {"X"}}},
                                                                    {{{{ui32(2)}}, {"Y"}}},
                                                                    {{{{ui32(3)}}, {"Z"}}},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.AddMember(pb.NewEmptyStruct(), "Payload", pb.Member(item, "Payload")); });

        const auto resultType = pb.NewFlowType(NTest::ConvertToMinikqlType<NTest::TStructType<NTest::TStructMember<"Key", ui32>, NTest::TStructMember<"Left", TStringBuf>>>(pb));

        const auto pgmReturn = pb.Collect(pb.MapJoinCore(pb.ToFlow(list1), dict2, EJoinKind::LeftOnly, {0U}, {1U, 1U, 0U, 0U}, {}, resultType));
        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TMaybe<ui32>, TStringBuf>>{
                                                              {{}, "X"},
                                                              {ui32(1), "A"},
                                                          });
    }
}
} // Y_UNIT_TEST_SUITE(TMiniKQLMapJoinCoreTest)

Y_UNIT_TEST_SUITE(TMiniKQLWideMapJoinCoreTest) {
Y_UNIT_TEST_LLVM(TestInner) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(1), "A"},
                                                                    {ui32(2), "B"},
                                                                    {ui32(4), "C"},
                                                                });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(2), "X"},
                                                                    {ui32(4), "Y"},
                                                                    {ui32(4), "Z"},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, false,
                                           [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
                                           [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

        const auto resultType = pb.NewFlowType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<char*>::Id),
                                                                pb.NewDataType(NUdf::TDataType<char*>::Id)}));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                      dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                              {"B", "X"},
                                                              {"C", "Y"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestInnerMulti) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(1), "A"},
                                                                    {ui32(2), "B"},
                                                                    {ui32(2), "C"},
                                                                });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(2), "X"},
                                                                    {ui32(2), "Y"},
                                                                    {ui32(3), "Z"},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
                                           [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

        const auto resultType = pb.NewFlowType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<char*>::Id),
                                                                pb.NewDataType(NUdf::TDataType<char*>::Id)}));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                      dict2, EJoinKind::Inner, {0U}, {1U, 0U}, {0U, 1U}, resultType),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                              {"B", "X"},
                                                              {"B", "Y"},
                                                              {"C", "X"},
                                                              {"C", "Y"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeft) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(1), "A"},
                                                                    {ui32(2), "B"},
                                                                    {ui32(3), "C"},
                                                                });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(2), "X"},
                                                                    {ui32(3), "Y"},
                                                                    {ui32(4), "Z"},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, false,
                                           [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
                                           [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

        const auto resultType = pb.NewFlowType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<char*>::Id),
                                                                pb.NewDataType(NUdf::TDataType<char*>::Id)}));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                      dict2, EJoinKind::Left, {0U}, {1U, 0U}, {0U, 1U}, resultType),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TMaybe<TStringBuf>>>{
                                                              {"A", {}},
                                                              {"B", "X"},
                                                              {"C", "Y"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftMulti) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(1), "A"},
                                                                    {ui32(2), "B"},
                                                                    {ui32(2), "C"},
                                                                });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(2), "X"},
                                                                    {ui32(2), "Y"},
                                                                    {ui32(3), "Z"},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
                                           [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

        const auto resultType = pb.NewFlowType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<char*>::Id),
                                                                pb.NewDataType(NUdf::TDataType<char*>::Id)}));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                      dict2, EJoinKind::Left, {0U}, {1U, 0U}, {0U, 1U}, resultType),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TMaybe<TStringBuf>>>{
                                                              {"A", {}},
                                                              {"B", "X"},
                                                              {"B", "Y"},
                                                              {"C", "X"},
                                                              {"C", "Y"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftSemi) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(1), "A"},
                                                                    {ui32(2), "B"},
                                                                    {ui32(2), "C"},
                                                                });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(2), "X"},
                                                                    {ui32(2), "Y"},
                                                                    {ui32(3), "Z"},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
                                           [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

        const auto resultType = pb.NewFlowType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<char*>::Id),
                                                                pb.NewDataType(NUdf::TDataType<ui32>::Id)}));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                      dict2, EJoinKind::LeftSemi, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, ui32>>{
                                                              {"B", ui32(2)},
                                                              {"C", ui32(2)},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftOnly) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(1), "A"},
                                                                    {ui32(2), "B"},
                                                                    {ui32(2), "C"},
                                                                });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, TStringBuf>>{
                                                                    {ui32(2), "X"},
                                                                    {ui32(2), "Y"},
                                                                    {ui32(3), "Z"},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
                                           [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

        const auto resultType = pb.NewFlowType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<char*>::Id),
                                                                pb.NewDataType(NUdf::TDataType<ui32>::Id)}));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                      dict2, EJoinKind::LeftOnly, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, ui32>>{
                                                              {"A", ui32(1)},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftSemiWithNullKey) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<ui32>, TStringBuf>>{
                                                                    {{}, "X"},
                                                                    {ui32(1), "A"},
                                                                    {ui32(2), "B"},
                                                                    {ui32(2), "C"},
                                                                });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<ui32>, TStringBuf>>{
                                                                    {{}, "C"},
                                                                    {ui32(2), "X"},
                                                                    {ui32(2), "Y"},
                                                                    {ui32(3), "Z"},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
                                           [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

        const auto resultType = pb.NewFlowType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<char*>::Id),
                                                                pb.NewDataType(NUdf::TDataType<ui32>::Id)}));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                      dict2, EJoinKind::LeftSemi, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, ui32>>{
                                                              {"B", ui32(2)},
                                                              {"C", ui32(2)},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestLeftOnlyWithNullKey) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<ui32>, TStringBuf>>{
                                                                    {{}, "X"},
                                                                    {ui32(1), "A"},
                                                                    {ui32(2), "B"},
                                                                    {ui32(2), "C"},
                                                                });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<ui32>, TStringBuf>>{
                                                                    {{}, "C"},
                                                                    {ui32(2), "X"},
                                                                    {ui32(2), "Y"},
                                                                    {ui32(3), "Z"},
                                                                });

        const auto dict2 = pb.ToHashedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
                                           [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

        const auto resultType = pb.NewFlowType(pb.NewMultiType({pb.NewDataType(NUdf::TDataType<char*>::Id),
                                                                pb.NewDataType(NUdf::TDataType<ui32>::Id)}));

        const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.MapJoinCore(pb.ExpandMap(pb.ToFlow(list1),
                                                                                   [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                      dict2, EJoinKind::LeftOnly, {0U}, {1U, 0U, 0U, 1U}, {}, resultType),
                                                       [&](TRuntimeNode::TList items) -> TRuntimeNode { return pb.NewTuple(items); }));

        const auto graph = setup.BuildGraph(pgmReturn);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<TStringBuf, TMaybe<ui32>>>{
                                                              {"X", {}},
                                                              {"A", ui32(1)},
                                                          });
    }
}
} // Y_UNIT_TEST_SUITE(TMiniKQLWideMapJoinCoreTest)

} // namespace NMiniKQL
} // namespace NKikimr
