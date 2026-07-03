#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLJoinDictNodeTest) {
Y_UNIT_TEST_LLVM(TestInner) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        using TInRow = NTest::TStructType<NTest::TStructMember<"Key", ui32>,
                                          NTest::TStructMember<"Payload", TStringBuf>>;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                    {{{1U}, {"A"}}},
                                                                    {{{2U}, {"B"}}},
                                                                    {{{2U}, {"C"}}},
                                                                });
        const auto dict1 = pb.ToSortedDict(list1, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.Member(item, "Payload"); });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                    {{{2U}, {"X"}}},
                                                                    {{{2U}, {"Y"}}},
                                                                    {{{3U}, {"Z"}}},
                                                                });
        const auto dict2 = pb.ToSortedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.Member(item, "Payload"); });

        const auto pgmReturn = pb.JoinDict(dict1, true, dict2, true, EJoinKind::Inner);
        const auto graph = setup.BuildGraph(pgmReturn);

        using TOutRow = std::tuple<TString, TString>;

        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TOutRow>{
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

        using TInRow = NTest::TStructType<NTest::TStructMember<"Key", ui32>,
                                          NTest::TStructMember<"Payload", TStringBuf>>;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                    {{{1U}, {"A"}}},
                                                                    {{{2U}, {"B"}}},
                                                                    {{{2U}, {"C"}}},
                                                                });
        const auto dict1 = pb.ToSortedDict(list1, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.Member(item, "Payload"); });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                    {{{2U}, {"X"}}},
                                                                    {{{2U}, {"Y"}}},
                                                                    {{{3U}, {"Z"}}},
                                                                });
        const auto dict2 = pb.ToSortedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.Member(item, "Payload"); });

        const auto pgmReturn = pb.JoinDict(dict1, true, dict2, true, EJoinKind::Left);
        const auto graph = setup.BuildGraph(pgmReturn);

        using TOutRow = std::tuple<TString, TMaybe<TString>>;

        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TOutRow>{
                                                              {"A", {}},
                                                              {"B", TString("X")},
                                                              {"B", TString("Y")},
                                                              {"C", TString("X")},
                                                              {"C", TString("Y")},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestRight) {
    for (ui32 pass = 0; pass < 1; ++pass) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        using TInRow = NTest::TStructType<NTest::TStructMember<"Key", ui32>,
                                          NTest::TStructMember<"Payload", TStringBuf>>;

        const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                    {{{1U}, {"A"}}},
                                                                    {{{2U}, {"B"}}},
                                                                    {{{2U}, {"C"}}},
                                                                });
        const auto dict1 = pb.ToSortedDict(list1, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.Member(item, "Payload"); });

        const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                    {{{2U}, {"X"}}},
                                                                    {{{2U}, {"Y"}}},
                                                                    {{{3U}, {"Z"}}},
                                                                });
        const auto dict2 = pb.ToSortedDict(list2, true,
                                           [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                           [&](TRuntimeNode item) { return pb.Member(item, "Payload"); });

        const auto pgmReturn = pb.JoinDict(dict1, true, dict2, true, EJoinKind::Right);
        const auto graph = setup.BuildGraph(pgmReturn);

        using TOutRow = std::tuple<TMaybe<TString>, TString>;

        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TOutRow>{
                                                              {TString("B"), "X"},
                                                              {TString("B"), "Y"},
                                                              {TString("C"), "X"},
                                                              {TString("C"), "Y"},
                                                              {{}, "Z"},
                                                          });
    }
}

Y_UNIT_TEST_LLVM(TestFull) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TInRow = NTest::TStructType<NTest::TStructMember<"Key", ui32>,
                                      NTest::TStructMember<"Payload", TStringBuf>>;

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                {{{1U}, {"A"}}},
                                                                {{{2U}, {"B"}}},
                                                                {{{2U}, {"C"}}},
                                                            });
    const auto dict1 = pb.ToSortedDict(list1, true,
                                       [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                       [&](TRuntimeNode item) { return pb.Member(item, "Payload"); });

    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                {{{2U}, {"X"}}},
                                                                {{{2U}, {"Y"}}},
                                                                {{{3U}, {"Z"}}},
                                                            });
    const auto dict2 = pb.ToSortedDict(list2, true,
                                       [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                       [&](TRuntimeNode item) { return pb.Member(item, "Payload"); });

    const auto pgmReturn = pb.JoinDict(dict1, true, dict2, true, EJoinKind::Full);
    const auto graph = setup.BuildGraph(pgmReturn);

    using TOutRow = std::tuple<TMaybe<TString>, TMaybe<TString>>;

    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TOutRow>{
                                                          {TString("A"), {}},
                                                          {TString("B"), TString("X")},
                                                          {TString("B"), TString("Y")},
                                                          {TString("C"), TString("X")},
                                                          {TString("C"), TString("Y")},
                                                          {{}, TString("Z")},
                                                      });
}

Y_UNIT_TEST_LLVM(TestInnerFlat) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TInRow = NTest::TStructType<NTest::TStructMember<"Key", ui32>,
                                      NTest::TStructMember<"Payload", TStringBuf>>;
    const auto structType = NTest::ConvertValueToLiteralNode(pb, TInRow{}).GetStaticType();

    const auto list1 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                {{{1U}, {"A"}}},
                                                                {{{2U}, {"B"}}},
                                                                {{{3U}, {"C"}}},
                                                                {{{4U}, {"D"}}},
                                                            });

    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                                {{{2U}, {"H"}}},
                                                                {{{3U}, {"G"}}},
                                                                {{{4U}, {"F"}}},
                                                                {{{5U}, {"E"}}},
                                                            });

    const auto listList = pb.NewList(pb.NewListType(structType), {list1, list2});

    const auto pgmReturn = pb.FlatMap(listList,
                                      [&](TRuntimeNode left) {
                                          const auto dict1 = pb.ToSortedDict(left, false,
                                                                             [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                                                             [&](TRuntimeNode item) { return pb.Member(item, "Payload"); }, false, 0);
                                          return pb.FlatMap(listList,
                                                            [&](TRuntimeNode right) {
                                                                const auto dict2 = pb.ToSortedDict(right, false,
                                                                                                   [&](TRuntimeNode item) { return pb.Member(item, "Key"); },
                                                                                                   [&](TRuntimeNode item) { return pb.Member(item, "Payload"); }, false, 0);
                                                                return pb.JoinDict(dict1, false, dict2, false, EJoinKind::Inner);
                                                            });
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto list = graph->GetValue();

    using TOutRow = std::tuple<TString, TString>;

    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TOutRow>{
                                                          {"A", "A"},
                                                          {"B", "B"},
                                                          {"C", "C"},
                                                          {"D", "D"},
                                                          {"B", "H"},
                                                          {"C", "G"},
                                                          {"D", "F"},
                                                          {"H", "B"},
                                                          {"G", "C"},
                                                          {"F", "D"},
                                                          {"H", "H"},
                                                          {"G", "G"},
                                                          {"F", "F"},
                                                          {"E", "E"},
                                                      });
}
} // Y_UNIT_TEST_SUITE(TMiniKQLJoinDictNodeTest)

} // namespace NMiniKQL
} // namespace NKikimr
