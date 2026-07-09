#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLSwitchTest) {
Y_UNIT_TEST_LLVM(TestStreamOfVariantsSwap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);
    const auto varOutType = pb.NewVariantType(pb.NewTupleType({strType, intType}));

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::variant<ui32, TStringBuf>>{
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 1U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 2U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 3U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("123")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("456")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("789")},
                                                           });

    const auto pgmReturn = pb.Switch(pb.Iterator(list, {}),
                                     {{{0U}, pb.NewStreamType(intType), std::nullopt}, {{1U}, pb.NewStreamType(strType), std::nullopt}},
                                     [&](ui32 index, TRuntimeNode stream) {
                                         switch (index) {
                                             case 0U:
                                                 return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.ToString(item), 0U, varOutType); });
                                             case 1U:
                                                 return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.StrictFromString(item, intType), 1U, varOutType); });
                                         }
                                         Y_ABORT("Wrong case!");
                                     },
                                     0ULL,
                                     pb.NewStreamType(varOutType));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::variant<TStringBuf, ui32>>({
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<0>, TStringBuf("1")},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<0>, TStringBuf("2")},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<0>, TStringBuf("3")},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 123U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 456U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 789U},
                                             }));
}

Y_UNIT_TEST_LLVM(TestStreamOfVariantsTwoInOne) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);

    const auto varOutType = pb.NewVariantType(pb.NewTupleType({strType, intType}));

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::variant<ui32, TStringBuf>>{
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 1U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 2U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 3U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("123")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("456")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("789")},
                                                           });

    const auto pgmReturn = pb.Switch(pb.Iterator(list, {}),
                                     {{{0U}, pb.NewStreamType(intType), 1U}, {{1U}, pb.NewStreamType(strType), std::nullopt}},
                                     [&](ui32 index, TRuntimeNode stream) {
                                         switch (index) {
                                             case 0U:
                                                 return pb.Map(stream, [&](TRuntimeNode item) { return item; });
                                             case 1U:
                                                 return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.StrictFromString(item, intType), 1U, varOutType); });
                                         }
                                         Y_ABORT("Wrong case!");
                                     },
                                     0ULL,
                                     pb.NewStreamType(varOutType));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::variant<TStringBuf, ui32>>({
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 1U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 2U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 3U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 123U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 456U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 789U},
                                             }));
}

Y_UNIT_TEST_LLVM(TestFlowOfVariantsSwap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);

    const auto varOutType = pb.NewVariantType(pb.NewTupleType({strType, intType}));

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::variant<ui32, TStringBuf>>{
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 1U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 2U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 3U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("123")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("456")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("789")},
                                                           });

    const auto pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(list),
                                                 {{{0U}, pb.NewFlowType(intType), std::nullopt}, {{1U}, pb.NewFlowType(strType), std::nullopt}},
                                                 [&](ui32 index, TRuntimeNode stream) {
                                                     switch (index) {
                                                         case 0U:
                                                             return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.ToString(item), 0U, varOutType); });
                                                         case 1U:
                                                             return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.StrictFromString(item, intType), 1U, varOutType); });
                                                     }
                                                     Y_ABORT("Wrong case!");
                                                 },
                                                 0ULL,
                                                 pb.NewFlowType(varOutType)));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::variant<TStringBuf, ui32>>({
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<0>, TStringBuf("1")},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<0>, TStringBuf("2")},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<0>, TStringBuf("3")},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 123U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 456U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 789U},
                                             }));
}

Y_UNIT_TEST_LLVM(TestFlowOfVariantsTwoInOne) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);

    const auto varOutType = pb.NewVariantType(pb.NewTupleType({strType, intType}));

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::variant<ui32, TStringBuf>>{
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 1U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 2U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 3U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("123")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("456")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("789")},
                                                           });

    const auto pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(list),
                                                 {{{0U}, pb.NewFlowType(intType), 1U}, {{1U}, pb.NewFlowType(strType), std::nullopt}},
                                                 [&](ui32 index, TRuntimeNode stream) {
                                                     switch (index) {
                                                         case 0U:
                                                             return pb.Map(stream, [&](TRuntimeNode item) { return item; });
                                                         case 1U:
                                                             return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.StrictFromString(item, intType), 1U, varOutType); });
                                                     }
                                                     Y_ABORT("Wrong case!");
                                                 },
                                                 0ULL,
                                                 pb.NewFlowType(varOutType)));

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();
    AssertUnboxedValueElementEqual(iterator, NYql::NUdf::TUnboxedValueComparatorStreamView<std::variant<TStringBuf, ui32>>({
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 1U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 2U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 3U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 123U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 456U},
                                                 std::variant<TStringBuf, ui32>{std::in_place_index<1>, 789U},
                                             }));
}
} // Y_UNIT_TEST_SUITE(TMiniKQLSwitchTest)

} // namespace NMiniKQL
} // namespace NKikimr
