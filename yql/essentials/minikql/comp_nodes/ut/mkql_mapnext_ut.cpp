#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLMapNextTest) {
Y_UNIT_TEST_LLVM(OverStream) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<ui16, TMaybe<ui16>>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui16>{10, 20, 30});
    const auto pgmReturn = pb.MapNext(pb.Iterator(list, {}),
                                      [&](TRuntimeNode item, TRuntimeNode nextItem) {
                                          return pb.NewTuple(tupleType, {item, nextItem});
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<ui16, TMaybe<ui16>>>({
                                                 {ui16(10), ui16(20)},
                                                 {ui16(20), ui16(30)},
                                                 {ui16(30), {}},
                                             }));
}

Y_UNIT_TEST_LLVM(OverSingleElementStream) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<ui16, TMaybe<ui16>>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui16>{10});
    const auto pgmReturn = pb.MapNext(pb.Iterator(list, {}),
                                      [&](TRuntimeNode item, TRuntimeNode nextItem) {
                                          return pb.NewTuple(tupleType, {item, nextItem});
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<ui16, TMaybe<ui16>>>({
                                                 {ui16(10), {}},
                                             }));
}

Y_UNIT_TEST_LLVM(OverEmptyStream) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<ui16, TMaybe<ui16>>>(pb);

    const auto list = pb.NewList(NTest::ConvertToMinikqlType<ui16>(pb), {});
    const auto pgmReturn = pb.MapNext(pb.Iterator(list, {}),
                                      [&](TRuntimeNode item, TRuntimeNode nextItem) {
                                          return pb.NewTuple(tupleType, {item, nextItem});
                                      });

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<ui16, TMaybe<ui16>>>({}));
}

Y_UNIT_TEST_LLVM(OverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<ui16, TMaybe<ui16>>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui16>{10, 20, 30});
    const auto pgmReturn = pb.FromFlow(pb.MapNext(pb.ToFlow(pb.Iterator(list, {})),
                                                  [&](TRuntimeNode item, TRuntimeNode nextItem) {
                                                      return pb.NewTuple(tupleType, {item, nextItem});
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<ui16, TMaybe<ui16>>>({
                                                 {ui16(10), ui16(20)},
                                                 {ui16(20), ui16(30)},
                                                 {ui16(30), {}},
                                             }));
}

Y_UNIT_TEST_LLVM(OverSingleElementFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<ui16, TMaybe<ui16>>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<ui16>{10});
    const auto pgmReturn = pb.FromFlow(pb.MapNext(pb.ToFlow(pb.Iterator(list, {})),
                                                  [&](TRuntimeNode item, TRuntimeNode nextItem) {
                                                      return pb.NewTuple(tupleType, {item, nextItem});
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<ui16, TMaybe<ui16>>>({
                                                 {ui16(10), {}},
                                             }));
}

Y_UNIT_TEST_LLVM(OverEmptyFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<ui16, TMaybe<ui16>>>(pb);

    const auto list = pb.NewList(NTest::ConvertToMinikqlType<ui16>(pb), {});
    const auto pgmReturn = pb.FromFlow(pb.MapNext(pb.ToFlow(pb.Iterator(list, {})),
                                                  [&](TRuntimeNode item, TRuntimeNode nextItem) {
                                                      return pb.NewTuple(tupleType, {item, nextItem});
                                                  }));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::tuple<ui16, TMaybe<ui16>>>({}));
}
} // Y_UNIT_TEST_SUITE(TMiniKQLMapNextTest)

} // namespace NMiniKQL
} // namespace NKikimr
