#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLSwitchTest) {
    Y_UNIT_TEST_LLVM(TestStreamOfVariantsSwap) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1U);
        const auto data2 = pb.NewDataLiteral<ui32>(2U);
        const auto data3 = pb.NewDataLiteral<ui32>(3U);

        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("123");
        const auto data5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("456");
        const auto data6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("789");

        const auto intType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);

        const auto varInType = pb.NewVariantType(pb.NewTupleType({intType, strType}));

        const auto var1 = pb.NewVariant(data1, 0U, varInType);
        const auto var2 = pb.NewVariant(data2, 0U, varInType);
        const auto var3 = pb.NewVariant(data3, 0U, varInType);
        const auto var4 = pb.NewVariant(data4, 1U, varInType);
        const auto var5 = pb.NewVariant(data5, 1U, varInType);
        const auto var6 = pb.NewVariant(data6, 1U, varInType);

        const auto varOutType = pb.NewVariantType(pb.NewTupleType({strType, intType}));

        const auto list = pb.NewList(varInType, {var1, var2, var3, var4, var5, var6});

        const auto pgmReturn = pb.Switch(pb.Iterator(list, {}),
            {{{0U}, pb.NewStreamType(intType), std::nullopt}, {{1U}, pb.NewStreamType(strType), std::nullopt}},
            [&](ui32 index, TRuntimeNode stream) {
                switch (index) {
                    case 0U: return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.ToString(item), 0U, varOutType); });
                    case 1U: return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.StrictFromString(item, intType), 1U, varOutType); });
                }
                Y_ABORT("Wrong case!");
            },
            0ULL,
            pb.NewStreamType(varOutType)
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 0U);
        UNBOXED_VALUE_STR_EQUAL(item, "1");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 0U);
        UNBOXED_VALUE_STR_EQUAL(item, "2");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 0U);
        UNBOXED_VALUE_STR_EQUAL(item, "3");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 123U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 456U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 789U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestStreamOfVariantsTwoInOne) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1U);
        const auto data2 = pb.NewDataLiteral<ui32>(2U);
        const auto data3 = pb.NewDataLiteral<ui32>(3U);

        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("123");
        const auto data5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("456");
        const auto data6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("789");

        const auto intType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);

        const auto varInType = pb.NewVariantType(pb.NewTupleType({intType, strType}));

        const auto var1 = pb.NewVariant(data1, 0U, varInType);
        const auto var2 = pb.NewVariant(data2, 0U, varInType);
        const auto var3 = pb.NewVariant(data3, 0U, varInType);
        const auto var4 = pb.NewVariant(data4, 1U, varInType);
        const auto var5 = pb.NewVariant(data5, 1U, varInType);
        const auto var6 = pb.NewVariant(data6, 1U, varInType);

        const auto varOutType = pb.NewVariantType(pb.NewTupleType({strType, intType}));

        const auto list = pb.NewList(varInType, {var1, var2, var3, var4, var5, var6});

        const auto pgmReturn = pb.Switch(pb.Iterator(list, {}),
            {{{0U}, pb.NewStreamType(intType), 1U}, {{1U}, pb.NewStreamType(strType), std::nullopt}},
            [&](ui32 index, TRuntimeNode stream) {
                switch (index) {
                    case 0U: return pb.Map(stream, [&](TRuntimeNode item) { return item; });
                    case 1U: return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.StrictFromString(item, intType), 1U, varOutType); });
                }
                Y_ABORT("Wrong case!");
            },
            0ULL,
            pb.NewStreamType(varOutType)
        );

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 2U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 3U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 123U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 456U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 789U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFlowOfVariantsSwap) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1U);
        const auto data2 = pb.NewDataLiteral<ui32>(2U);
        const auto data3 = pb.NewDataLiteral<ui32>(3U);

        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("123");
        const auto data5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("456");
        const auto data6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("789");

        const auto intType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);

        const auto varInType = pb.NewVariantType(pb.NewTupleType({intType, strType}));

        const auto var1 = pb.NewVariant(data1, 0U, varInType);
        const auto var2 = pb.NewVariant(data2, 0U, varInType);
        const auto var3 = pb.NewVariant(data3, 0U, varInType);
        const auto var4 = pb.NewVariant(data4, 1U, varInType);
        const auto var5 = pb.NewVariant(data5, 1U, varInType);
        const auto var6 = pb.NewVariant(data6, 1U, varInType);

        const auto varOutType = pb.NewVariantType(pb.NewTupleType({strType, intType}));

        const auto list = pb.NewList(varInType, {var1, var2, var3, var4, var5, var6});

        const auto pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(list),
            {{{0U}, pb.NewFlowType(intType), std::nullopt}, {{1U}, pb.NewFlowType(strType), std::nullopt}},
            [&](ui32 index, TRuntimeNode stream) {
                switch (index) {
                    case 0U: return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.ToString(item), 0U, varOutType); });
                    case 1U: return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.StrictFromString(item, intType), 1U, varOutType); });
                }
                Y_ABORT("Wrong case!");
            },
            0ULL,
            pb.NewFlowType(varOutType)
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 0U);
        UNBOXED_VALUE_STR_EQUAL(item, "1");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 0U);
        UNBOXED_VALUE_STR_EQUAL(item, "2");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 0U);
        UNBOXED_VALUE_STR_EQUAL(item, "3");
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 123U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 456U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 789U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }

    Y_UNIT_TEST_LLVM(TestFlowOfVariantsTwoInOne) {
        TSetup<LLVM> setup;
        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto data1 = pb.NewDataLiteral<ui32>(1U);
        const auto data2 = pb.NewDataLiteral<ui32>(2U);
        const auto data3 = pb.NewDataLiteral<ui32>(3U);

        const auto data4 = pb.NewDataLiteral<NUdf::EDataSlot::String>("123");
        const auto data5 = pb.NewDataLiteral<NUdf::EDataSlot::String>("456");
        const auto data6 = pb.NewDataLiteral<NUdf::EDataSlot::String>("789");

        const auto intType = pb.NewDataType(NUdf::TDataType<ui32>::Id);
        const auto strType = pb.NewDataType(NUdf::TDataType<char*>::Id);

        const auto varInType = pb.NewVariantType(pb.NewTupleType({intType, strType}));

        const auto var1 = pb.NewVariant(data1, 0U, varInType);
        const auto var2 = pb.NewVariant(data2, 0U, varInType);
        const auto var3 = pb.NewVariant(data3, 0U, varInType);
        const auto var4 = pb.NewVariant(data4, 1U, varInType);
        const auto var5 = pb.NewVariant(data5, 1U, varInType);
        const auto var6 = pb.NewVariant(data6, 1U, varInType);

        const auto varOutType = pb.NewVariantType(pb.NewTupleType({strType, intType}));

        const auto list = pb.NewList(varInType, {var1, var2, var3, var4, var5, var6});

        const auto pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(list),
            {{{0U}, pb.NewFlowType(intType), 1U}, {{1U}, pb.NewFlowType(strType), std::nullopt}},
            [&](ui32 index, TRuntimeNode stream) {
                switch (index) {
                    case 0U: return pb.Map(stream, [&](TRuntimeNode item) { return item; });
                    case 1U: return pb.Map(stream, [&](TRuntimeNode item) { return pb.NewVariant(pb.StrictFromString(item, intType), 1U, varOutType); });
                }
                Y_ABORT("Wrong case!");
            },
            0ULL,
            pb.NewFlowType(varOutType)
        ));

        const auto graph = setup.BuildGraph(pgmReturn);
        const auto iterator = graph->GetValue();
        NUdf::TUnboxedValue item;
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 2U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 3U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 123U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 456U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(item.GetVariantIndex(), 1U);
        UNIT_ASSERT_VALUES_EQUAL(item.Get<ui32>(), 789U);
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
        UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    }
}

}
}
