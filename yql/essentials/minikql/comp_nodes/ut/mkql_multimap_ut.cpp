#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr::NMiniKQL {

using namespace NTest;

Y_UNIT_TEST_SUITE(TMiniKQLMultiMapTest) {
Y_UNIT_TEST_LLVM(TestOverList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<ui32>(2);
    const auto list = ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2, 3});
    const auto pgmReturn = pb.MultiMap(list,
                                       [&](TRuntimeNode item) {
                                           return TRuntimeNode::TList{pb.Add(item, data1), item, pb.Mul(item, data2)};
                                       });

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{2, 1, 2, 3, 2, 4, 4, 3, 6});
}

Y_UNIT_TEST_LLVM(TestOverLazyList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<ui32>(2);
    const auto list = ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2, 3});
    const auto pgmReturn = pb.MultiMap(pb.LazyList(list),
                                       [&](TRuntimeNode item) {
                                           return TRuntimeNode::TList{pb.Add(item, data1), item, pb.Mul(item, data2)};
                                       });

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{2, 1, 2, 3, 2, 4, 4, 3, 6});
}

Y_UNIT_TEST_LLVM(TestOverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto data1 = pb.NewDataLiteral<ui32>(1);
    const auto data2 = pb.NewDataLiteral<ui32>(2);
    const auto list = ConvertValueToLiteralNode(pb, TVector<ui32>{1, 2, 3});
    const auto pgmReturn = pb.Collect(pb.MultiMap(pb.ToFlow(list),
                                                  [&](TRuntimeNode item) {
                                                      return TRuntimeNode::TList{pb.Add(item, data1), item, pb.Mul(item, data2)};
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui32>{2, 1, 2, 3, 2, 4, 4, 3, 6});
}

Y_UNIT_TEST_LLVM(TestFlattenByNarrow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TRow = std::tuple<TMaybe<i32>, TMaybe<i32>, TMaybe<i32>>;
    const auto list = ConvertValueToLiteralNode(pb, TVector<TRow>{
                                                        {TMaybe<i32>{1}, TMaybe<i32>{}, TMaybe<i32>{-1}},
                                                        {TMaybe<i32>{}, TMaybe<i32>{2}, TMaybe<i32>{-2}},
                                                        {TMaybe<i32>{3}, TMaybe<i32>{}, TMaybe<i32>{-3}},
                                                    });

    const auto pgmReturn = pb.Collect(pb.NarrowMultiMap(pb.ExpandMap(pb.ToFlow(list),
                                                                     [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                        [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[2U], items[1U], items[0U]}; }));

    const auto graph = setup.BuildGraph(pgmReturn);
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               TVector<TMaybe<i32>>{-1, {}, 1, -2, 2, {}, -3, {}, 3});
}
} // Y_UNIT_TEST_SUITE(TMiniKQLMultiMapTest)

} // namespace NKikimr::NMiniKQL
