#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr::NMiniKQL {

namespace {

class TSwitchYieldStreamWrapper: public TMutableComputationNode<TSwitchYieldStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TSwitchYieldStreamWrapper>;

    using TData = std::variant<ui32, TString>;
    using TYield = std::monostate;
    using TInput = std::variant<TData, TYield>;

    inline static const std::array<TInput, 7> SwitchYieldData = {
        TData{std::in_place_type<ui32>, 1},
        TData{std::in_place_type<ui32>, 2},
        TYield{},
        TData{std::in_place_type<ui32>, 3},
        TData{std::in_place_type<TString>, "hello"},
        TYield{},
        TData{std::in_place_type<ui32>, 4},
    };

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx)
            : TBase(memInfo)
            , CompCtx(compCtx)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Index == SwitchYieldData.size()) {
                return NUdf::EFetchStatus::Finish;
            }

            const auto& item = SwitchYieldData[Index];
            if (std::holds_alternative<TYield>(item)) {
                ++Index;
                return NUdf::EFetchStatus::Yield;
            }

            const auto& data = std::get<TData>(item);
            if (std::holds_alternative<ui32>(data)) {
                result = CompCtx.HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(std::get<ui32>(data)), 0);
            } else {
                result = CompCtx.HolderFactory.CreateVariantHolder(MakeString(std::get<TString>(data)), 1);
            }

            ++Index;
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx;
        ui64 Index = 0;
    };

    explicit TSwitchYieldStreamWrapper(TComputationMutables& mutables)
        : TBaseComputation(mutables)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx);
    }

private:
    void RegisterDependencies() const final {
    }
};

TComputationNodeFactory GetSwitchYieldNodeFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestSwitchYieldStream") {
            MKQL_ENSURE(!callable.GetInputsCount(), "Expected no args");
            return new TSwitchYieldStreamWrapper(ctx.Mutables);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

} // anonymous namespace

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

Y_UNIT_TEST_QUAD(TestFlowSwitchDoesNotPropagateStaleYield, LLVM, StreamInput) {
    TSetup<LLVM> setup(GetSwitchYieldNodeFactory());
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);

    const auto varInType = pb.NewVariantType(pb.NewTupleType({intType, strType}));
    const auto streamType = pb.NewStreamType(varInType);

    TCallableBuilder callableBuilder(*setup.Env, "TestSwitchYieldStream", streamType);
    auto stream = TRuntimeNode(callableBuilder.Build(), false);

    const auto varOutType = pb.NewVariantType(pb.NewTupleType({intType, strType}));
    const auto handler = [&](ui32 index, TRuntimeNode handlerStream) {
        switch (index) {
            case 0U:
                return pb.Map(handlerStream, [&](TRuntimeNode item) { return item; });
            case 1U:
                return pb.Map(handlerStream, [&](TRuntimeNode item) { return item; });
        }
        Y_ABORT("Wrong case!");
    };

    TRuntimeNode pgmReturn;
    if constexpr (StreamInput) {
        pgmReturn = pb.Switch(stream,
                              {{{0U}, pb.NewStreamType(intType), 0U}, {{1U}, pb.NewStreamType(strType), 1U}},
                              handler,
                              0ULL,
                              pb.NewStreamType(varOutType));
    } else {
        pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(stream),
                                          {{{0U}, pb.NewFlowType(intType), 0U}, {{1U}, pb.NewFlowType(strType), 1U}},
                                          handler,
                                          0ULL,
                                          pb.NewFlowType(varOutType)));
    }

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();

    NUdf::TUnboxedValue result;
    ui32 yieldCount = 0;
    ui32 okCount = 0;
    auto status = NUdf::EFetchStatus::Ok;
    while (status != NUdf::EFetchStatus::Finish) {
        status = iterator.Fetch(result);
        if (status == NUdf::EFetchStatus::Yield) {
            ++yieldCount;
        } else if (status == NUdf::EFetchStatus::Ok) {
            ++okCount;
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(okCount, 5U);
    UNIT_ASSERT_VALUES_EQUAL(yieldCount, 0U);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLSwitchTest)

} // namespace NKikimr::NMiniKQL
