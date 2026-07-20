#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr::NMiniKQL {

namespace {

using TVarValue = std::variant<ui32, TString>;
using TYieldMark = std::monostate;
using TStreamScript = std::variant<TVarValue, TYieldMark>;

class TScriptedStreamWrapper: public TMutableComputationNode<TScriptedStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TScriptedStreamWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const TVector<TStreamScript>& script)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Script(script)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Index == Script.size()) {
                return NUdf::EFetchStatus::Finish;
            }

            const auto& item = Script[Index++];
            if (std::holds_alternative<TYieldMark>(item)) {
                return NUdf::EFetchStatus::Yield;
            }

            const auto& data = std::get<TVarValue>(item);
            if (std::holds_alternative<ui32>(data)) {
                result = CompCtx.HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(std::get<ui32>(data)), 0);
            } else {
                result = CompCtx.HolderFactory.CreateVariantHolder(MakeString(std::get<TString>(data)), 1);
            }

            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx;
        const TVector<TStreamScript>& Script;
        ui64 Index = 0;
    };

    TScriptedStreamWrapper(TComputationMutables& mutables, const TVector<TStreamScript>& script)
        : TBaseComputation(mutables)
        , Script(script)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Script);
    }

private:
    void RegisterDependencies() const final {
    }

    const TVector<TStreamScript>& Script;
};

class TYieldMidBufferHandlerWrapper: public TMutableComputationNode<TYieldMidBufferHandlerWrapper> {
    using TBaseComputation = TMutableComputationNode<TYieldMidBufferHandlerWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input, ui32 yieldAfter)
            : TBase(memInfo)
            , Input(std::move(input))
            , YieldAfter(yieldAfter)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (!Yielded && Forwarded == YieldAfter) {
                Yielded = true;
                return NUdf::EFetchStatus::Yield;
            }

            const auto status = Input.Fetch(result);
            if (status == NUdf::EFetchStatus::Ok) {
                ++Forwarded;
            }

            return status;
        }

        const NUdf::TUnboxedValue Input;
        const ui32 YieldAfter;
        ui32 Forwarded = 0;
        bool Yielded = false;
    };

    TYieldMidBufferHandlerWrapper(TComputationMutables& mutables, IComputationNode* input, ui32 yieldAfter)
        : TBaseComputation(mutables)
        , Input(input)
        , YieldAfter(yieldAfter)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(Input->GetValue(ctx), YieldAfter);
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Input);
    }

    IComputationNode* const Input;
    const ui32 YieldAfter;
};

class TYieldOnFinishHandlerWrapper: public TMutableComputationNode<TYieldOnFinishHandlerWrapper> {
    using TBaseComputation = TMutableComputationNode<TYieldOnFinishHandlerWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input)
            : TBase(memInfo)
            , Input(std::move(input))
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Done) {
                return NUdf::EFetchStatus::Finish;
            }

            if (!Collected) {
                for (;;) {
                    NUdf::TUnboxedValue item;
                    const auto status = Input.Fetch(item);
                    if (status == NUdf::EFetchStatus::Yield) {
                        return NUdf::EFetchStatus::Yield;
                    }
                    if (status == NUdf::EFetchStatus::Finish) {
                        break;
                    }

                    Buffer.push_back(std::move(item));
                }

                Collected = true;
                return NUdf::EFetchStatus::Yield;
            }

            if (ReplayIndex < Buffer.size()) {
                result = Buffer[ReplayIndex++];
                return NUdf::EFetchStatus::Ok;
            }

            Done = true;
            return NUdf::EFetchStatus::Finish;
        }

        const NUdf::TUnboxedValue Input;
        TUnboxedValueVector Buffer;
        ui64 ReplayIndex = 0;
        bool Collected = false;
        bool Done = false;
    };

    TYieldOnFinishHandlerWrapper(TComputationMutables& mutables, IComputationNode* input)
        : TBaseComputation(mutables)
        , Input(input)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(Input->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Input);
    }

    IComputationNode* const Input;
};

class TYieldSkipHandlerWrapper: public TMutableComputationNode<TYieldSkipHandlerWrapper> {
    using TBaseComputation = TMutableComputationNode<TYieldSkipHandlerWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue&& input)
            : TBase(memInfo)
            , Input(std::move(input))
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (const auto status = Input.Fetch(result); status != NUdf::EFetchStatus::Yield) {
                return status;
            }

            if (!SkippedYield) {
                SkippedYield = true;
                return Input.Fetch(result);
            }

            return NUdf::EFetchStatus::Yield;
        }

        const NUdf::TUnboxedValue Input;
        bool SkippedYield = false;
    };

    TYieldSkipHandlerWrapper(TComputationMutables& mutables, IComputationNode* input)
        : TBaseComputation(mutables)
        , Input(input)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(Input->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        this->DependsOn(Input);
    }

    IComputationNode* const Input;
};

class TCountingStreamWrapper: public TMutableComputationNode<TCountingStreamWrapper> {
    using TBaseComputation = TMutableComputationNode<TCountingStreamWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, ui64 count)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Count(count)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (Index >= Count) {
                return NUdf::EFetchStatus::Finish;
            }

            result = CompCtx.HolderFactory.CreateVariantHolder(NUdf::TUnboxedValuePod(static_cast<ui32>(Index++)), 0);
            return NUdf::EFetchStatus::Ok;
        }

        TComputationContext& CompCtx;
        const ui64 Count;
        ui64 Index = 0;
    };

    TCountingStreamWrapper(TComputationMutables& mutables, ui64 count)
        : TBaseComputation(mutables)
        , Count(count)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, Count);
    }

private:
    void RegisterDependencies() const final {
    }

    const ui64 Count;
};

TComputationNodeFactory GetCountingStreamFactory(ui64 count) {
    return [count](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestCountingStream") {
            MKQL_ENSURE(!callable.GetInputsCount(), "Expected no args");
            return new TCountingStreamWrapper(ctx.Mutables, count);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TComputationNodeFactory GetSwitchTestFactory(const TVector<TStreamScript>& script, ui32 midBufferYieldAfter) {
    return [&script, midBufferYieldAfter](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        const auto& name = callable.GetType()->GetName();
        if (name == "TestScriptedStream") {
            MKQL_ENSURE(!callable.GetInputsCount(), "Expected no args");
            return new TScriptedStreamWrapper(ctx.Mutables, script);
        }
        if (name == "TestYieldMidBufferHandler") {
            return new TYieldMidBufferHandlerWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0), midBufferYieldAfter);
        }
        if (name == "TestYieldOnFinishHandler") {
            return new TYieldOnFinishHandlerWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
        }
        if (name == "TestYieldSkipHandler") {
            return new TYieldSkipHandlerWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(TMiniKQLSwitchTest) {

Y_UNIT_TEST_LLVM(TestStreamOfVariantsSwap) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);
    const auto varOutType = NTest::ConvertToMinikqlType<std::variant<TStringBuf, ui32>>(pb);

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

    const auto varOutType = NTest::ConvertToMinikqlType<std::variant<TStringBuf, ui32>>(pb);

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

    const auto varOutType = NTest::ConvertToMinikqlType<std::variant<TStringBuf, ui32>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::variant<ui32, TStringBuf>>{
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 1U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 2U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 3U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("123")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("456")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("789")},
                                                           });

    const auto pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(list, {}),
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

    const auto varOutType = NTest::ConvertToMinikqlType<std::variant<TStringBuf, ui32>>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::variant<ui32, TStringBuf>>{
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 1U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 2U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<0>, 3U},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("123")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("456")},
                                                               std::variant<ui32, TStringBuf>{std::in_place_index<1>, TStringBuf("789")},
                                                           });

    const auto pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(list, {}),
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

Y_UNIT_TEST_QUAD(TestSwitchDoesNotPropagateStaleYield, LLVM, StreamInput) {
    const TVector<TStreamScript> script = {
        TVarValue{std::in_place_type<ui32>, 1},
        TVarValue{std::in_place_type<ui32>, 2},
        TYieldMark{},
        TVarValue{std::in_place_type<ui32>, 3},
        TVarValue{std::in_place_type<TString>, "hello"},
        TYieldMark{},
        TVarValue{std::in_place_type<ui32>, 4},
    };
    TSetup<LLVM> setup(GetSwitchTestFactory(script, 0U));
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);

    const auto varInType = NTest::ConvertToMinikqlType<std::variant<ui32, TStringBuf>>(pb);
    const auto streamType = pb.NewStreamType(varInType);

    TCallableBuilder callableBuilder(*setup.Env, "TestScriptedStream", streamType);
    auto stream = TRuntimeNode(callableBuilder.Build(), false);

    const auto varOutType = NTest::ConvertToMinikqlType<std::variant<ui32, TStringBuf>>(pb);
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
        pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(stream, {}),
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

Y_UNIT_TEST_QUAD(TestSwitchFinishesWhenHandlerStops, LLVM, StreamInput) {
    const TVector<TStreamScript> script = {
        TVarValue{std::in_place_type<ui32>, 1U},
        TVarValue{std::in_place_type<ui32>, 2U},
        TYieldMark{},
        TVarValue{std::in_place_type<ui32>, 3U},
        TVarValue{std::in_place_type<ui32>, 4U},
        TYieldMark{},
    };
    TSetup<LLVM> setup(GetSwitchTestFactory(script, 0U));
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto varInType = NTest::ConvertToMinikqlType<std::variant<ui32, TStringBuf>>(pb);
    const auto streamType = pb.NewStreamType(varInType);

    TCallableBuilder inputBuilder(*setup.Env, "TestScriptedStream", streamType);
    const auto input = TRuntimeNode(inputBuilder.Build(), false);

    const auto handler = [&](ui32, TRuntimeNode handlerStream) {
        return pb.Take(handlerStream, pb.NewDataLiteral<ui64>(2ULL));
    };

    TRuntimeNode pgmReturn;
    if constexpr (StreamInput) {
        pgmReturn = pb.Switch(input,
                              {{{0U}, pb.NewStreamType(intType), std::nullopt}},
                              handler, 0ULL, pb.NewStreamType(intType));
    } else {
        pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(input, {}),
                                          {{{0U}, pb.NewFlowType(intType), std::nullopt}},
                                          handler, 0ULL, pb.NewFlowType(intType)));
    }

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue();

    TVector<ui32> got;
    ui32 yieldCount = 0;
    NUdf::TUnboxedValue result;
    auto status = NUdf::EFetchStatus::Ok;
    while (status != NUdf::EFetchStatus::Finish) {
        status = iterator.Fetch(result);
        if (status == NUdf::EFetchStatus::Ok) {
            got.push_back(result.Get<ui32>());
        } else if (status == NUdf::EFetchStatus::Yield) {
            ++yieldCount;
        }
    }

    UNIT_ASSERT_VALUES_EQUAL(got.size(), 2U);
    UNIT_ASSERT_VALUES_EQUAL(got[0], 1U);
    UNIT_ASSERT_VALUES_EQUAL(got[1], 2U);
    UNIT_ASSERT_VALUES_EQUAL(yieldCount, 0U);
}

Y_UNIT_TEST_QUAD(TestSwitchWaitsForHandlerDataAfterInputFinish, LLVM, StreamInput) {
    const TVector<TStreamScript> script = {
        TVarValue{std::in_place_type<ui32>, 1U},
        TVarValue{std::in_place_type<ui32>, 2U},
        TVarValue{std::in_place_type<ui32>, 3U},
    };
    TSetup<LLVM> setup(GetSwitchTestFactory(script, 0U));
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto varInType = NTest::ConvertToMinikqlType<std::variant<ui32, TStringBuf>>(pb);
    const auto streamType = pb.NewStreamType(varInType);

    TCallableBuilder inputBuilder(*setup.Env, "TestScriptedStream", streamType);
    const auto input = TRuntimeNode(inputBuilder.Build(), false);

    const auto buildHandler = [&](TRuntimeNode handlerStream) {
        TCallableBuilder cb(*setup.Env, "TestYieldOnFinishHandler", pb.NewStreamType(intType));
        cb.Add(handlerStream);
        return TRuntimeNode(cb.Build(), false);
    };
    const auto handler = [&](ui32, TRuntimeNode handlerStream) -> TRuntimeNode {
        if constexpr (StreamInput) {
            return buildHandler(handlerStream);
        } else {
            return pb.ToFlow(buildHandler(pb.FromFlow(handlerStream)), {});
        }
    };

    TRuntimeNode pgmReturn;
    if constexpr (StreamInput) {
        pgmReturn = pb.Switch(input,
                              {{{0U}, pb.NewStreamType(intType), std::nullopt}},
                              handler, 0ULL, pb.NewStreamType(intType));
    } else {
        pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(input, {}),
                                          {{{0U}, pb.NewFlowType(intType), std::nullopt}},
                                          handler, 0ULL, pb.NewFlowType(intType)));
    }

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<ui32>({1U, 2U, 3U}));
}

Y_UNIT_TEST_QUAD(TestSwitchKeepsBufferOnHandlerYield, LLVM, StreamInput) {
    const TVector<TStreamScript> script = {
        TVarValue{std::in_place_type<ui32>, 1U},
        TVarValue{std::in_place_type<ui32>, 2U},
        TVarValue{std::in_place_type<ui32>, 3U},
    };
    TSetup<LLVM> setup(GetSwitchTestFactory(script, 1U));
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto varInType = NTest::ConvertToMinikqlType<std::variant<ui32, TStringBuf>>(pb);
    const auto streamType = pb.NewStreamType(varInType);

    TCallableBuilder inputBuilder(*setup.Env, "TestScriptedStream", streamType);
    const auto input = TRuntimeNode(inputBuilder.Build(), false);

    const auto buildHandler = [&](TRuntimeNode handlerStream) {
        TCallableBuilder cb(*setup.Env, "TestYieldMidBufferHandler", pb.NewStreamType(intType));
        cb.Add(handlerStream);
        return TRuntimeNode(cb.Build(), false);
    };
    const auto handler = [&](ui32, TRuntimeNode handlerStream) -> TRuntimeNode {
        if constexpr (StreamInput) {
            return buildHandler(handlerStream);
        } else {
            return pb.ToFlow(buildHandler(pb.FromFlow(handlerStream)), {});
        }
    };

    TRuntimeNode pgmReturn;
    if constexpr (StreamInput) {
        pgmReturn = pb.Switch(input,
                              {{{0U}, pb.NewStreamType(intType), std::nullopt}},
                              handler, 0ULL, pb.NewStreamType(intType));
    } else {
        pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(input, {}),
                                          {{{0U}, pb.NewFlowType(intType), std::nullopt}},
                                          handler, 0ULL, pb.NewFlowType(intType)));
    }

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<ui32>({1U, 2U, 3U}));
}

Y_UNIT_TEST_QUAD(TestSwitchDoesNotRereadBufferOnYield, LLVM, StreamInput) {
    const TVector<TStreamScript> script = {
        TVarValue{std::in_place_type<ui32>, 1U},
        TVarValue{std::in_place_type<TString>, "a"},
        TYieldMark{},
        TVarValue{std::in_place_type<ui32>, 2U},
        TVarValue{std::in_place_type<TString>, "b"},
    };
    TSetup<LLVM> setup(GetSwitchTestFactory(script, 0U));
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);
    const auto varType = NTest::ConvertToMinikqlType<std::variant<ui32, TStringBuf>>(pb);
    const auto streamType = pb.NewStreamType(varType);

    TCallableBuilder inputBuilder(*setup.Env, "TestScriptedStream", streamType);
    const auto input = TRuntimeNode(inputBuilder.Build(), false);

    const auto buildHandler = [&](ui32 index, TRuntimeNode handlerStream) {
        TType* type = nullptr;
        switch (index) {
            case 0U:
                type = intType;
                break;
            case 1U:
                type = strType;
                break;
        }

        TCallableBuilder cb(*setup.Env, "TestYieldSkipHandler", pb.NewStreamType(type));
        cb.Add(handlerStream);
        return TRuntimeNode(cb.Build(), false);
    };
    const auto handler = [&](ui32 index, TRuntimeNode handlerStream) -> TRuntimeNode {
        if constexpr (StreamInput) {
            return buildHandler(index, handlerStream);
        } else {
            return pb.ToFlow(buildHandler(index, pb.FromFlow(handlerStream)), {});
        }
    };

    TRuntimeNode pgmReturn;
    if constexpr (StreamInput) {
        pgmReturn = pb.Switch(input,
                              {{{0U}, pb.NewStreamType(intType), 0U}, {{1U}, pb.NewStreamType(strType), 1U}},
                              handler, 0ULL, pb.NewStreamType(varType));
    } else {
        pgmReturn = pb.FromFlow(pb.Switch(pb.ToFlow(input, {}),
                                          {{{0U}, pb.NewFlowType(intType), 0U}, {{1U}, pb.NewFlowType(strType), 1U}},
                                          handler, 0ULL, pb.NewFlowType(varType)));
    }

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TVarValue>({TVarValue{std::in_place_type<ui32>, 1U},
                                                                                                                TVarValue{std::in_place_type<TString>, "a"},
                                                                                                                TVarValue{std::in_place_type<ui32>, 2U},
                                                                                                                TVarValue{std::in_place_type<TString>, "b"}}));
}

Y_UNIT_TEST_QUAD(TestNestedSwitch, LLVM, StreamInput) {
    TVector<TStreamScript> script;
    TVector<TString> expected;
    for (ui32 i = 0; i < 64U; ++i) {
        const TString value = "long_boxed_string_value_payload_#" + ToString(i);
        script.push_back(TVarValue{std::in_place_type<TString>, value});
        expected.push_back(value);
    }

    TSetup<LLVM> setup(GetSwitchTestFactory(script, 0U));
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto strType = NTest::ConvertToMinikqlType<TStringBuf>(pb);
    const auto varType = NTest::ConvertToMinikqlType<std::variant<ui32, TStringBuf>>(pb);

    TCallableBuilder inputBuilder(*setup.Env, "TestScriptedStream", pb.NewStreamType(varType));
    const auto input = TRuntimeNode(inputBuilder.Build(), /* isImmediate */ false);

    const auto identity = [&](ui32, TRuntimeNode s) { return pb.Map(s, [&](TRuntimeNode item) { return item; }); };

    constexpr ui64 smallMemLimit = 1ULL;
    constexpr ui64 noMemLimit = 0ULL;

    TRuntimeNode pgmReturn;
    if constexpr (StreamInput) {
        const auto inner = pb.Switch(input,
                                     {{{1U}, pb.NewStreamType(strType), std::nullopt}},
                                     identity, smallMemLimit, pb.NewStreamType(strType));
        pgmReturn = pb.Switch(inner,
                              {{{0U}, pb.NewStreamType(strType), std::nullopt}},
                              identity, noMemLimit, pb.NewStreamType(strType));
    } else {
        const auto inner = pb.Switch(pb.ToFlow(input, {}),
                                     {{{1U}, pb.NewFlowType(strType), std::nullopt}},
                                     identity, smallMemLimit, pb.NewFlowType(strType));
        pgmReturn = pb.FromFlow(pb.Switch(inner,
                                          {{{0U}, pb.NewFlowType(strType), std::nullopt}},
                                          identity, noMemLimit, pb.NewFlowType(strType)));
    }

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), NYql::NUdf::TUnboxedValueComparatorStreamView<TString>(expected));
}

Y_UNIT_TEST_QUAD(TestSwitchOverDiscard, LLVM, StreamInput) {
    constexpr ui64 rowCount = 1'000'000ULL;
    constexpr ui64 memLimit = 1ULL << 20;

    TSetup<LLVM> setup(GetCountingStreamFactory(rowCount));
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto intType = NTest::ConvertToMinikqlType<ui32>(pb);
    const auto varInType = NTest::ConvertToMinikqlType<std::variant<ui32, TStringBuf>>(pb);
    const auto streamType = pb.NewStreamType(varInType);

    TCallableBuilder inputBuilder(*setup.Env, "TestCountingStream", streamType);
    const auto input = TRuntimeNode(inputBuilder.Build(), /* isImmediate */ false);

    const auto handler = [&](ui32, TRuntimeNode handlerStream) { return handlerStream; };

    TRuntimeNode pgmReturn;
    if constexpr (StreamInput) {
        pgmReturn = pb.Discard(pb.Switch(input,
                                         {{{0U}, pb.NewStreamType(intType), std::nullopt}},
                                         handler, memLimit, pb.NewStreamType(intType)));
    } else {
        pgmReturn = pb.FromFlow(pb.Discard(pb.Switch(pb.ToFlow(input, {}),
                                                     {{{0U}, pb.NewFlowType(intType), std::nullopt}},
                                                     handler, memLimit, pb.NewFlowType(intType))));
    }

    const auto graph = setup.BuildGraph(pgmReturn);

    const auto iterator = graph->GetValue();
    NUdf::TUnboxedValue result;
    auto status = NUdf::EFetchStatus::Yield;
    while (status == NUdf::EFetchStatus::Yield) {
        status = iterator.Fetch(result);
    }

    UNIT_ASSERT_VALUES_EQUAL(status, NUdf::EFetchStatus::Finish);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLSwitchTest)

} // namespace NKikimr::NMiniKQL
