#include "mkql_computation_node_ut.h"

#include <ydb/library/yql/minikql/mkql_runtime_version.h>
#include <ydb/library/yql/minikql/mkql_node_cast.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>

#include <cstring>
#include <random>
#include <ctime>
#include <algorithm>

namespace NKikimr {
namespace NMiniKQL {

namespace {

ui64 g_Yield = std::numeric_limits<ui64>::max();
ui64 g_TestStreamData[] = {0, 1, 2, 0, 1, 2, 0, 1, 2, 0, 1, 2};
ui64 g_TestYieldStreamData[] = {0, 1, 2, g_Yield, 0, g_Yield, 1, 2, 0, 1, 2, 0, g_Yield, 1, 2};

template <bool WithYields>
class TTestStreamWrapper: public TMutableComputationNode<TTestStreamWrapper<WithYields>> {
    typedef TMutableComputationNode<TTestStreamWrapper<WithYields>> TBaseComputation;
public:
    class TStreamValue : public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, TComputationContext& compCtx, const TTestStreamWrapper* parent)
            : TBase(memInfo)
            , CompCtx(compCtx)
            , Parent(parent)
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            constexpr auto size = WithYields ? Y_ARRAY_SIZE(g_TestYieldStreamData) : Y_ARRAY_SIZE(g_TestStreamData);
            if (Index == size) {
                return NUdf::EFetchStatus::Finish;
            }

            const auto val = WithYields ? g_TestYieldStreamData[Index] : g_TestStreamData[Index];
            if (g_Yield == val) {
                ++Index;
                return NUdf::EFetchStatus::Yield;
            }

            NUdf::TUnboxedValue* items = nullptr;
            result = CompCtx.HolderFactory.CreateDirectArrayHolder(2, items);
            items[0] = NUdf::TUnboxedValuePod(val);
            if (((Index + 1) % Parent->PeakStep) == 0) {
                auto str = MakeStringNotFilled(64ul << 20);
                const auto& buf = str.AsStringRef();
                memset(buf.Data(), ' ', buf.Size());
                items[1] = std::move(str);
            } else {
                items[1] = NUdf::TUnboxedValuePod::Zero();
            }

            ++Index;
            return NUdf::EFetchStatus::Ok;
        }

    private:
        TComputationContext& CompCtx;
        const TTestStreamWrapper* const Parent;
        ui64 Index = 0;
    };

    TTestStreamWrapper(TComputationMutables& mutables, ui64 peakStep)
        : TBaseComputation(mutables)
        , PeakStep(peakStep)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(ctx, this);
    }

private:
    void RegisterDependencies() const final {
    }

private:
    const ui64 PeakStep;
};

template <bool WithYields>
IComputationNode* WrapTestStream(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 args");
    const ui64 peakStep = AS_VALUE(TDataLiteral, callable.GetInput(0))->AsValue().Get<ui64>();
    return new TTestStreamWrapper<WithYields>(ctx.Mutables, peakStep);
}

TComputationNodeFactory GetNodeFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestList") {
            return new TExternalComputationNode(ctx.Mutables);
        }
        if (callable.GetType()->GetName() == "TestStream") {
            return WrapTestStream<false>(callable, ctx);
        }
        if (callable.GetType()->GetName() == "TestYieldStream") {
            return WrapTestStream<true>(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

template <bool LLVM, bool WithYields = false>
TRuntimeNode MakeStream(TSetup<LLVM>& setup, ui64 peakStep) {
    TProgramBuilder& pb = *setup.PgmBuilder;

    TCallableBuilder callableBuilder(*setup.Env, WithYields ? "TestYieldStream" : "TestStream",
        pb.NewStreamType(
            pb.NewStructType({
                {TStringBuf("a"), pb.NewDataType(NUdf::EDataSlot::Uint64)},
                {TStringBuf("b"), pb.NewDataType(NUdf::EDataSlot::String)}
            })
        )
    );
    callableBuilder.Add(pb.NewDataLiteral(peakStep));

    return TRuntimeNode(callableBuilder.Build(), false);
}

template <bool OverFlow>
TRuntimeNode Combine(TProgramBuilder& pb, TRuntimeNode stream, std::function<TRuntimeNode(TRuntimeNode, TRuntimeNode)> finishLambda) {
    const auto keyExtractor = [&](TRuntimeNode item) {
        return pb.Member(item, "a");
    };
    const auto init = [&](TRuntimeNode /*key*/, TRuntimeNode item) {
        return item;
    };
    const auto update = [&](TRuntimeNode /*key*/, TRuntimeNode item, TRuntimeNode state) {
        const auto a = pb.Add(pb.Member(item, "a"), pb.Member(state, "a"));
        const auto b = pb.Concat(pb.Member(item, "b"), pb.Member(state, "b"));
        return pb.NewStruct({
            {TStringBuf("a"), a},
            {TStringBuf("b"), b},
        });
    };

    return OverFlow ?
        pb.FromFlow(pb.CombineCore(pb.ToFlow(stream), keyExtractor, init, update, finishLambda, 64ul << 20)):
        pb.CombineCore(stream, keyExtractor, init, update, finishLambda, 64ul << 20);
}

TRuntimeNode Reduce(TProgramBuilder& pb, TRuntimeNode stream) {
    return pb.Condense(stream, pb.NewDataLiteral<ui64>(0),
        [&] (TRuntimeNode, TRuntimeNode) { return pb.NewDataLiteral<bool>(false); },
        [&] (TRuntimeNode item, TRuntimeNode state) { return pb.Add(state, item); }
    );
}

TRuntimeNode StreamToString(TProgramBuilder& pb, TRuntimeNode stream) {
    const auto sorted = pb.Sort(stream, pb.NewDataLiteral(true),
        [&](TRuntimeNode item) {
        return item;
    });

    return pb.Condense(sorted, pb.NewDataLiteral<NUdf::EDataSlot::String>("|"),
        [&] (TRuntimeNode, TRuntimeNode) { return pb.NewDataLiteral<bool>(false); },
        [&] (TRuntimeNode item, TRuntimeNode state) {
            return pb.Concat(pb.Concat(state, pb.ToString(item)), pb.NewDataLiteral<NUdf::EDataSlot::String>("|"));
        }
    );
}

} // unnamed

Y_UNIT_TEST_SUITE(TMiniKQLCombineStreamTest) {
    Y_UNIT_TEST_LLVM(TestFullCombineWithOptOut) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        const auto stream = MakeStream(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<false>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|4|8|");
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithListOut) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            const auto item = pb.Member(state, "a");
            const auto itemType = item.GetStaticType();
            auto list = pb.NewEmptyList(itemType);
            list = pb.Append(list, item);
            list = pb.Append(list, item);
            return list;
        };

        const auto stream = MakeStream(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<false>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|4|4|8|8|");
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithStreamOut) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            const auto item = pb.Member(state, "a");
            const auto itemType = item.GetStaticType();
            auto list = pb.NewEmptyList(itemType);
            list = pb.Append(list, item);
            list = pb.Append(list, item);
            return pb.Iterator(list, MakeArrayRef(&state, 1));
        };

        const auto stream = MakeStream(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<false>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|4|4|8|8|");
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithOptOutAndYields) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        const auto stream = MakeStream<LLVM, true>(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<false>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|1|1|2|2|2|4|");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithListAndYields) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            const auto item = pb.Member(state, "a");
            const auto itemType = item.GetStaticType();
            auto list = pb.NewEmptyList(itemType);
            list = pb.Append(list, item);
            list = pb.Append(list, item);
            return list;
        };

        const auto stream = MakeStream<LLVM, true>(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<false>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|0|1|1|1|1|2|2|2|2|2|2|4|4|");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithStreamAndYields) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            const auto item = pb.Member(state, "a");
            const auto itemType = item.GetStaticType();
            auto list = pb.NewEmptyList(itemType);
            list = pb.Append(list, item);
            list = pb.Append(list, item);
            return pb.Iterator(list, MakeArrayRef(&state, 1));
        };

        const auto stream = MakeStream<LLVM, true>(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<false>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|0|1|1|1|1|2|2|2|2|2|2|4|4|");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_LLVM(TestPartialFlush) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        const auto stream = MakeStream(setup, 6ul);
        const auto combine = Combine<false>(pb, stream, finish);
        {
            const auto pgm = Reduce(pb, combine);
            const auto graph = setup.BuildGraph(pgm);
            const auto streamVal = graph->GetValue();
            NUdf::TUnboxedValue result;
            UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

            UNIT_ASSERT_VALUES_EQUAL(result.Get<ui64>(), 12ul);
        }
        {
            const auto pgm = StreamToString(pb, combine);
            const auto graph = setup.BuildGraph(pgm);
            const auto streamVal = graph->GetValue();
            NUdf::TUnboxedValue result;
            UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|2|2|4|4|");
        }
    }

    Y_UNIT_TEST_LLVM(TestCombineInSingleProc) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        const auto stream = MakeStream(setup, 6ul);
        const auto pgm = Reduce(pb, Combine<false>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm, EGraphPerProcess::Single);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(result.Get<ui64>(), 12ul);
    }

    Y_UNIT_TEST_LLVM(TestCombineSwithYield) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        auto stream = MakeStream(setup, Max<ui64>());
        TSwitchInput switchInput;
        switchInput.Indicies.push_back(0);
        switchInput.InputType = stream.GetStaticType();

        stream = pb.Switch(stream,
                MakeArrayRef(&switchInput, 1),
                [&](ui32 /*index*/, TRuntimeNode item) { return Combine<false>(pb, item, finish); },
                1,
                pb.NewStreamType(pb.NewDataType(NUdf::EDataSlot::Uint64))
                );

        const auto pgm = StreamToString(pb, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|1|1|1|1|2|2|2|2|");
    }
}

Y_UNIT_TEST_SUITE(TMiniKQLCombineStreamPerfTest) {
    Y_UNIT_TEST_LLVM(TestSumDoubleBooleanKeys) {
        TSetup<LLVM> setup(GetNodeFactory());

        double positive = 0.0, negative = 0.0;
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            (sample.second > 0.0 ? positive : negative) += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.CombineCore(pb.Iterator(TRuntimeNode(list, false), {}),
            [&](TRuntimeNode item) { return pb.AggrGreater(item, pb.NewDataLiteral(0.0)); },
            [&](TRuntimeNode, TRuntimeNode item) { return item; },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); },
            [&](TRuntimeNode, TRuntimeNode state) { return pb.NewOptional(state); },
            0ULL
        );

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        std::transform(I8Samples.cbegin(), I8Samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

        NUdf::TUnboxedValue first, second;
        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        UNIT_ASSERT_EQUAL(value.Fetch(first), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_EQUAL(value.Fetch(second), NUdf::EFetchStatus::Ok);
        const auto t2 = TInstant::Now();

        if (first.template Get<double>() > 0.0) {
            UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), positive);
            UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), negative);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), negative);
            UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), positive);
        }

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleBooleanKeys) {
        TSetup<LLVM> setup(GetNodeFactory());

        double pSum = 0.0, nSum = 0.0, pMax = 0.0, nMax = -1000.0, pMin = 1000.0, nMin = 0.0;
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            if (sample.second > 0.0) {
                pSum += sample.second;
                pMax = std::max(pMax, sample.second);
                pMin = std::min(pMin, sample.second);
            } else {
                nSum += sample.second;
                nMax = std::max(nMax, sample.second);
                nMin = std::min(nMin, sample.second);
            }
        }

        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.CombineCore(pb.Iterator(TRuntimeNode(list, false), {}),
            [&](TRuntimeNode item) { return pb.AggrGreater(item, pb.NewDataLiteral(0.0)); },
            [&](TRuntimeNode, TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), item), pb.AggrMin(pb.Nth(state, 1U), item), pb.AggrMax(pb.Nth(state, 2U), item) }); },
            [&](TRuntimeNode, TRuntimeNode state) { return pb.NewOptional(state); },
            0ULL
        );

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        std::transform(I8Samples.cbegin(), I8Samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

        NUdf::TUnboxedValue first, second;
        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        UNIT_ASSERT_EQUAL(value.Fetch(first), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_EQUAL(value.Fetch(second), NUdf::EFetchStatus::Ok);
        const auto t2 = TInstant::Now();

        if (first.GetElement(0).template Get<double>() > 0.0) {
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(0).template Get<double>(), pSum);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(1).template Get<double>(), pMin);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(2).template Get<double>(), pMax);

            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(0).template Get<double>(), nSum);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(1).template Get<double>(), nMin);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(2).template Get<double>(), nMax);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(0).template Get<double>(), nSum);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(1).template Get<double>(), nMin);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(2).template Get<double>(), nMax);

            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(0).template Get<double>(), pSum);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(1).template Get<double>(), pMin);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(2).template Get<double>(), pMax);
        }

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleSmallKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::unordered_map<i8, double> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            expects.emplace(sample.first, 0.0).first->second += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<i8, double>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<i8, double> l, const std::pair<i8, double> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.Iterator(TRuntimeNode(list, false), {}),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { return pb.Nth(item, 1U); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, pb.Nth(item, 1U)); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, state})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        for (const auto& sample : I8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElement(0).template Get<i8>(), ptr[i].GetElement(1).template Get<double>());
        }

        std::sort(two.begin(), two.end(), [](const std::pair<i8, double> l, const std::pair<i8, double> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleSmallKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::unordered_map<i8, std::array<double, 3U>> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, std::numeric_limits<double>::max(), std::numeric_limits<double>::min()}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<i8, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<i8, std::array<double, 3U>> l, const std::pair<i8, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.Iterator(TRuntimeNode(list, false), {}),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({v, v, v}); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), v), pb.AggrMin(pb.Nth(state, 1U), v), pb.AggrMax(pb.Nth(state, 2U), v)}); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, pb.Nth(state, 0U), pb.Nth(state, 1U), pb.Nth(state, 2U)})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        for (const auto& sample : I8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElement(0).template Get<i8>(), std::array<double, 3U>{ptr[i].GetElement(1).template Get<double>(), ptr[i].GetElement(2).template Get<double>(), ptr[i].GetElement(3).template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<i8, std::array<double, 3U>> l, const std::pair<i8, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleStringKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::vector<std::pair<std::string, double>> stringI8Samples(I8Samples.size());
        std::transform(I8Samples.cbegin(), I8Samples.cend(), stringI8Samples.begin(), [](std::pair<i8, double> src){ return std::make_pair(ToString(src.first), src.second); });

        std::unordered_map<std::string, double> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : stringI8Samples) {
            expects.emplace(sample.first, 0.0).first->second += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::string_view, double>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::string_view, double> l, const std::pair<std::string_view, double> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<const char*>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.Iterator(TRuntimeNode(list, false), {}),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { return pb.Nth(item, 1U); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, pb.Nth(item, 1U)); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, state})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(stringI8Samples.size(), items));
        for (const auto& sample : stringI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod::Embedded(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElements()->AsStringRef(), ptr[i].GetElement(1).template Get<double>());
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::string_view, double> l, const std::pair<std::string_view, double> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleStringKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::vector<std::pair<std::string, double>> stringI8Samples(I8Samples.size());
        std::transform(I8Samples.cbegin(), I8Samples.cend(), stringI8Samples.begin(), [](std::pair<i8, double> src){ return std::make_pair(ToString(src.first), src.second); });

        std::unordered_map<std::string, std::array<double, 3U>> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : stringI8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, +1E7, -1E7}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::string_view, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::string_view, std::array<double, 3U>> l, const std::pair<std::string_view, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<const char*>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.Iterator(TRuntimeNode(list, false), {}),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({v, v, v}); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), v), pb.AggrMin(pb.Nth(state, 1U), v), pb.AggrMax(pb.Nth(state, 2U), v)}); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, pb.Nth(state, 0U), pb.Nth(state, 1U), pb.Nth(state, 2U)})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(stringI8Samples.size(), items));
        for (const auto& sample : stringI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod::Embedded(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElements()->AsStringRef(), std::array<double, 3U>{ptr[i].GetElement(1).template Get<double>(), ptr[i].GetElement(2).template Get<double>(), ptr[i].GetElement(3).template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::string_view, std::array<double, 3U>> l, const std::pair<std::string_view, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumTupleKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::vector<std::pair<std::pair<ui32, std::string>, double>> pairI8Samples(Ui16Samples.size());
        std::transform(Ui16Samples.cbegin(), Ui16Samples.cend(), pairI8Samples.begin(), [](std::pair<ui32, double> src){ return std::make_pair(std::make_pair(ui32(src.first / 10U % 100U), ToString(src.first % 10U)), src.second); });

        struct TPairHash { size_t operator()(const std::pair<ui16, std::string>& p) const { return CombineHashes(std::hash<ui32>()(p.first), std::hash<std::string_view>()(p.second)); } };

        std::unordered_map<std::pair<ui32, std::string>, std::array<double, 3U>, TPairHash> expects;
        const auto t = TInstant::Now();
        for (const auto& sample : pairI8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, +1E7, -1E7}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::pair<ui32, std::string>, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> l, const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<const char*>::Id)}), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.Iterator(TRuntimeNode(list, false), {}),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({v, v, v}); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), v), pb.AggrMin(pb.Nth(state, 1U), v), pb.AggrMax(pb.Nth(state, 2U), v)}); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, pb.Nth(state, 0U), pb.Nth(state, 1U), pb.Nth(state, 2U)})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(pairI8Samples.size(), items));
        for (const auto& sample : pairI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
            NUdf::TUnboxedValue* keys = nullptr;
            pair[0] =  graph->GetHolderFactory().CreateDirectArrayHolder(2U, keys);
            keys[0] = NUdf::TUnboxedValuePod(sample.first.first);
            keys[1] = NUdf::TUnboxedValuePod::Embedded(sample.first.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            const auto elements = ptr[i].GetElements();
            two.emplace_back(std::make_pair(elements[0].GetElement(0).template Get<ui32>(), (elements[0].GetElements()[1]).AsStringRef()), std::array<double, 3U>{elements[1].template Get<double>(), elements[2].template Get<double>(), elements[3].template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> l, const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }
}
#if !defined(MKQL_RUNTIME_VERSION) || MKQL_RUNTIME_VERSION >= 3u
Y_UNIT_TEST_SUITE(TMiniKQLCombineFlowTest) {
    Y_UNIT_TEST_LLVM(TestFullCombineWithOptOut) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        const auto stream = MakeStream(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<true>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|4|8|");
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithListOut) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            const auto item = pb.Member(state, "a");
            const auto itemType = item.GetStaticType();
            auto list = pb.NewEmptyList(itemType);
            list = pb.Append(list, item);
            list = pb.Append(list, item);
            return list;
        };

        const auto stream = MakeStream(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<true>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|4|4|8|8|");
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithStreamOut) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            const auto item = pb.Member(state, "a");
            const auto itemType = item.GetStaticType();
            auto list = pb.NewEmptyList(itemType);
            list = pb.Append(list, item);
            list = pb.Append(list, item);
            return pb.Iterator(list, MakeArrayRef(&state, 1));
        };

        const auto stream = MakeStream(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<true>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|4|4|8|8|");
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithOptOutAndYields) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        const auto stream = MakeStream<LLVM, true>(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<true>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|1|1|2|2|2|4|");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithListAndYields) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            const auto item = pb.Member(state, "a");
            const auto itemType = item.GetStaticType();
            auto list = pb.NewEmptyList(itemType);
            list = pb.Append(list, item);
            list = pb.Append(list, item);
            return list;
        };

        const auto stream = MakeStream<LLVM, true>(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<true>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|0|1|1|1|1|2|2|2|2|2|2|4|4|");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_LLVM(TestFullCombineWithStreamAndYields) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            const auto item = pb.Member(state, "a");
            const auto itemType = item.GetStaticType();
            auto list = pb.NewEmptyList(itemType);
            list = pb.Append(list, item);
            list = pb.Append(list, item);
            return pb.Iterator(list, MakeArrayRef(&state, 1));
        };

        const auto stream = MakeStream<LLVM, true>(setup, Max<ui64>());
        const auto pgm = StreamToString(pb, Combine<true>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Yield);
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|0|0|1|1|1|1|2|2|2|2|2|2|4|4|");
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Finish);
    }

    Y_UNIT_TEST_LLVM(TestPartialFlush) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        const auto stream = MakeStream(setup, 6ul);
        const auto combine = Combine<true>(pb, stream, finish);
        {
            const auto pgm = Reduce(pb, combine);
            const auto graph = setup.BuildGraph(pgm);
            const auto streamVal = graph->GetValue();
            NUdf::TUnboxedValue result;
            UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

            UNIT_ASSERT_VALUES_EQUAL(result.Get<ui64>(), 12ul);
        }
        {
            const auto pgm = StreamToString(pb, combine);
            const auto graph = setup.BuildGraph(pgm);
            const auto streamVal = graph->GetValue();
            NUdf::TUnboxedValue result;
            UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

            UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|2|2|4|4|");
        }
    }

    Y_UNIT_TEST_LLVM(TestCombineInSingleProc) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        const auto stream = MakeStream(setup, 6ul);
        const auto pgm = Reduce(pb, Combine<true>(pb, stream, finish));
        const auto graph = setup.BuildGraph(pgm, EGraphPerProcess::Single);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(result.Get<ui64>(), 12ul);
    }

    Y_UNIT_TEST_LLVM(TestCombineSwithYield) {
        TSetup<LLVM> setup(GetNodeFactory());
        TProgramBuilder& pb = *setup.PgmBuilder;
        const auto finish = [&](TRuntimeNode /*key*/, TRuntimeNode state) {
            return pb.NewOptional(pb.Member(state, "a"));
        };

        auto stream = MakeStream(setup, Max<ui64>());
        TSwitchInput switchInput;
        switchInput.Indicies.push_back(0);
        switchInput.InputType = stream.GetStaticType();

        stream = pb.Switch(stream,
                MakeArrayRef(&switchInput, 1),
                [&](ui32 /*index*/, TRuntimeNode item) { return Combine<true>(pb, item, finish); },
                1,
                pb.NewStreamType(pb.NewDataType(NUdf::EDataSlot::Uint64))
                );

        const auto pgm = StreamToString(pb, stream);
        const auto graph = setup.BuildGraph(pgm);
        const auto streamVal = graph->GetValue();
        NUdf::TUnboxedValue result;
        UNIT_ASSERT_EQUAL(streamVal.Fetch(result), NUdf::EFetchStatus::Ok);

        UNIT_ASSERT_VALUES_EQUAL(TStringBuf(result.AsStringRef()), "|0|0|0|0|1|1|1|1|2|2|2|2|");
    }
}

Y_UNIT_TEST_SUITE(TMiniKQLCombineFlowPerfTest) {
    Y_UNIT_TEST_LLVM(TestSumDoubleBooleanKeys) {
        TSetup<LLVM> setup(GetNodeFactory());

        double positive = 0.0, negative = 0.0;
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            (sample.second > 0.0 ? positive : negative) += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.FromFlow(pb.CombineCore(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.AggrGreater(item, pb.NewDataLiteral(0.0)); },
            [&](TRuntimeNode, TRuntimeNode item) { return item; },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, item); },
            [&](TRuntimeNode, TRuntimeNode state) { return pb.NewOptional(state); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        std::transform(I8Samples.cbegin(), I8Samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

        NUdf::TUnboxedValue first, second;
        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        UNIT_ASSERT_EQUAL(value.Fetch(first), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_EQUAL(value.Fetch(second), NUdf::EFetchStatus::Ok);
        const auto t2 = TInstant::Now();

        if (first.template Get<double>() > 0.0) {
            UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), positive);
            UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), negative);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(first.template Get<double>(), negative);
            UNIT_ASSERT_VALUES_EQUAL(second.template Get<double>(), positive);
        }

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleBooleanKeys) {
        TSetup<LLVM> setup(GetNodeFactory());

        double pSum = 0.0, nSum = 0.0, pMax = 0.0, nMax = -1000.0, pMin = 1000.0, nMin = 0.0;
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            if (sample.second > 0.0) {
                pSum += sample.second;
                pMax = std::max(pMax, sample.second);
                pMin = std::min(pMin, sample.second);
            } else {
                nSum += sample.second;
                nMax = std::max(nMax, sample.second);
                nMin = std::min(nMin, sample.second);
            }
        }

        const auto cppTime = TInstant::Now() - t;

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<double>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.FromFlow(pb.CombineCore(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.AggrGreater(item, pb.NewDataLiteral(0.0)); },
            [&](TRuntimeNode, TRuntimeNode item) { return pb.NewTuple({item, item, item}); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), item), pb.AggrMin(pb.Nth(state, 1U), item), pb.AggrMax(pb.Nth(state, 2U), item) }); },
            [&](TRuntimeNode, TRuntimeNode state) { return pb.NewOptional(state); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        std::transform(I8Samples.cbegin(), I8Samples.cend(), items, [](const std::pair<i8, double> s){ return ToValue<double>(s.second); });

        NUdf::TUnboxedValue first, second;
        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        UNIT_ASSERT_EQUAL(value.Fetch(first), NUdf::EFetchStatus::Ok);
        UNIT_ASSERT_EQUAL(value.Fetch(second), NUdf::EFetchStatus::Ok);
        const auto t2 = TInstant::Now();

        if (first.GetElement(0).template Get<double>() > 0.0) {
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(0).template Get<double>(), pSum);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(1).template Get<double>(), pMin);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(2).template Get<double>(), pMax);

            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(0).template Get<double>(), nSum);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(1).template Get<double>(), nMin);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(2).template Get<double>(), nMax);
        } else {
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(0).template Get<double>(), nSum);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(1).template Get<double>(), nMin);
            UNIT_ASSERT_VALUES_EQUAL(first.GetElement(2).template Get<double>(), nMax);

            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(0).template Get<double>(), pSum);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(1).template Get<double>(), pMin);
            UNIT_ASSERT_VALUES_EQUAL(second.GetElement(2).template Get<double>(), pMax);
        }

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleSmallKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::unordered_map<i8, double> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            expects.emplace(sample.first, 0.0).first->second += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<i8, double>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<i8, double> l, const std::pair<i8, double> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { return pb.Nth(item, 1U); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, pb.Nth(item, 1U)); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, state})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        for (const auto& sample : I8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElement(0).template Get<i8>(), ptr[i].GetElement(1).template Get<double>());
        }

        std::sort(two.begin(), two.end(), [](const std::pair<i8, double> l, const std::pair<i8, double> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleSmallKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::unordered_map<i8, std::array<double, 3U>> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : I8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, std::numeric_limits<double>::max(), std::numeric_limits<double>::min()}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<i8, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<i8, std::array<double, 3U>> l, const std::pair<i8, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i8>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({v, v, v}); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), v), pb.AggrMin(pb.Nth(state, 1U), v), pb.AggrMax(pb.Nth(state, 2U), v)}); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, pb.Nth(state, 0U), pb.Nth(state, 1U), pb.Nth(state, 2U)})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(I8Samples.size(), items));
        for (const auto& sample : I8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElement(0).template Get<i8>(), std::array<double, 3U>{ptr[i].GetElement(1).template Get<double>(), ptr[i].GetElement(2).template Get<double>(), ptr[i].GetElement(3).template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<i8, std::array<double, 3U>> l, const std::pair<i8, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestSumDoubleStringKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::vector<std::pair<std::string, double>> stringI8Samples(I8Samples.size());
        std::transform(I8Samples.cbegin(), I8Samples.cend(), stringI8Samples.begin(), [](std::pair<i8, double> src){ return std::make_pair(ToString(src.first), src.second); });

        std::unordered_map<std::string, double> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : stringI8Samples) {
            expects.emplace(sample.first, 0.0).first->second += sample.second;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::string_view, double>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::string_view, double> l, const std::pair<std::string_view, double> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<const char*>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { return pb.Nth(item, 1U); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { return pb.AggrAdd(state, pb.Nth(item, 1U)); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, state})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(stringI8Samples.size(), items));
        for (const auto& sample : stringI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod::Embedded(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElements()->AsStringRef(), ptr[i].GetElement(1).template Get<double>());
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::string_view, double> l, const std::pair<std::string_view, double> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumDoubleStringKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::vector<std::pair<std::string, double>> stringI8Samples(I8Samples.size());
        std::transform(I8Samples.cbegin(), I8Samples.cend(), stringI8Samples.begin(), [](std::pair<i8, double> src){ return std::make_pair(ToString(src.first), src.second); });

        std::unordered_map<std::string, std::array<double, 3U>> expects(201);
        const auto t = TInstant::Now();
        for (const auto& sample : stringI8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, +1E7, -1E7}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::string_view, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::string_view, std::array<double, 3U>> l, const std::pair<std::string_view, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<const char*>::Id), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({v, v, v}); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), v), pb.AggrMin(pb.Nth(state, 1U), v), pb.AggrMax(pb.Nth(state, 2U), v)}); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, pb.Nth(state, 0U), pb.Nth(state, 1U), pb.Nth(state, 2U)})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(stringI8Samples.size(), items));
        for (const auto& sample : stringI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[0] = NUdf::TUnboxedValuePod::Embedded(sample.first);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            two.emplace_back(ptr[i].GetElements()->AsStringRef(), std::array<double, 3U>{ptr[i].GetElement(1).template Get<double>(), ptr[i].GetElement(2).template Get<double>(), ptr[i].GetElement(3).template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::string_view, std::array<double, 3U>> l, const std::pair<std::string_view, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    Y_UNIT_TEST_LLVM(TestMinMaxSumTupleKey) {
        TSetup<LLVM> setup(GetNodeFactory());

        std::vector<std::pair<std::pair<ui32, std::string>, double>> pairI8Samples(Ui16Samples.size());
        std::transform(Ui16Samples.cbegin(), Ui16Samples.cend(), pairI8Samples.begin(), [](std::pair<ui16, double> src){ return std::make_pair(std::make_pair(ui32(src.first / 10U % 100U), ToString(src.first % 10U)), src.second); });

        struct TPairHash { size_t operator()(const std::pair<ui32, std::string>& p) const { return CombineHashes(std::hash<ui32>()(p.first), std::hash<std::string_view>()(p.second)); } };

        std::unordered_map<std::pair<ui32, std::string>, std::array<double, 3U>, TPairHash> expects;
        const auto t = TInstant::Now();
        for (const auto& sample : pairI8Samples) {
            auto& item = expects.emplace(sample.first, std::array<double, 3U>{0.0, +1E7, -1E7}).first->second;
            std::get<0U>(item) += sample.second;
            std::get<1U>(item) = std::min(std::get<1U>(item), sample.second);
            std::get<2U>(item) = std::max(std::get<2U>(item), sample.second);
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::pair<ui32, std::string>, std::array<double, 3U>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> l, const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({pb.NewTupleType({pb.NewDataType(NUdf::TDataType<ui32>::Id), pb.NewDataType(NUdf::TDataType<const char*>::Id)}), pb.NewDataType(NUdf::TDataType<double>::Id)}));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(pb.ToFlow(TRuntimeNode(list, false)),
            [&](TRuntimeNode item) { return pb.Nth(item, 0U); },
            [&](TRuntimeNode, TRuntimeNode item) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({v, v, v}); },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) { const auto v = pb.Nth(item, 1U); return pb.NewTuple({pb.AggrAdd(pb.Nth(state, 0U), v), pb.AggrMin(pb.Nth(state, 1U), v), pb.AggrMax(pb.Nth(state, 2U), v)}); },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({key, pb.Nth(state, 0U), pb.Nth(state, 1U), pb.Nth(state, 2U)})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(pairI8Samples.size(), items));
        for (const auto& sample : pairI8Samples) {
            NUdf::TUnboxedValue* pair = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(2U, pair);
            pair[1] = NUdf::TUnboxedValuePod(sample.second);
            NUdf::TUnboxedValue* keys = nullptr;
            pair[0] =  graph->GetHolderFactory().CreateDirectArrayHolder(2U, keys);
            keys[0] = NUdf::TUnboxedValuePod(sample.first.first);
            keys[1] = NUdf::TUnboxedValuePod::Embedded(sample.first.second);
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            const auto elements = ptr[i].GetElements();
            two.emplace_back(std::make_pair(elements[0].GetElement(0).template Get<ui32>(), (elements[0].GetElements()[1]).AsStringRef()), std::array<double, 3U>{elements[1].template Get<double>(), elements[2].template Get<double>(), elements[3].template Get<double>()});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> l, const std::pair<std::pair<ui32, std::string_view>, std::array<double, 3U>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }

    const auto border = 9124596000000000ULL;

    Y_UNIT_TEST_LLVM(TestTpch) {
        TSetup<LLVM> setup(GetNodeFactory());

        struct TPairHash { size_t operator()(const std::pair<std::string_view, std::string_view>& p) const { return CombineHashes(std::hash<std::string_view>()(p.first), std::hash<std::string_view>()(p.second)); } };

        std::unordered_map<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>, TPairHash> expects;
        const auto t = TInstant::Now();
        for (auto& sample : TpchSamples) {
            if (std::get<0U>(sample) <= border) {
                const auto& ins = expects.emplace(std::pair<std::string_view, std::string_view>{std::get<1U>(sample), std::get<2U>(sample)}, std::pair<ui64, std::array<double, 5U>>{0ULL, {0., 0., 0., 0., 0.}});
                auto& item = ins.first->second;
                ++item.first;
                std::get<0U>(item.second) += std::get<3U>(sample);
                std::get<1U>(item.second) += std::get<5U>(sample);
                std::get<2U>(item.second) += std::get<6U>(sample);
                const auto v = std::get<3U>(sample) * (1. - std::get<5U>(sample));
                std::get<3U>(item.second) += v;
                std::get<4U>(item.second) += v * (1. + std::get<4U>(sample));
            }
        }
        for (auto& item : expects) {
            std::get<1U>(item.second.second) /= item.second.first;
        }
        const auto cppTime = TInstant::Now() - t;

        std::vector<std::pair<std::pair<std::string, std::string>, std::pair<ui64, std::array<double, 5U>>>> one, two;
        one.reserve(expects.size());
        two.reserve(expects.size());

        one.insert(one.cend(), expects.cbegin(), expects.cend());
        std::sort(one.begin(), one.end(), [](const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> l, const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> r){ return l.first < r.first; });

        TProgramBuilder& pb = *setup.PgmBuilder;

        const auto listType = pb.NewListType(pb.NewTupleType({
            pb.NewDataType(NUdf::TDataType<ui64>::Id),
            pb.NewDataType(NUdf::TDataType<const char*>::Id),
            pb.NewDataType(NUdf::TDataType<const char*>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id),
            pb.NewDataType(NUdf::TDataType<double>::Id)
        }));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();

        const auto pgmReturn = pb.Collect(pb.CombineCore(
            pb.Map(pb.Filter(pb.ToFlow(TRuntimeNode(list, false)),
                [&](TRuntimeNode item) { return pb.AggrLessOrEqual(pb.Nth(item, 0U), pb.NewDataLiteral<ui64>(border)); }
                      ),
            [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U), pb.Nth(item, 2U),pb.Nth(item, 3U),pb.Nth(item, 4U),pb.Nth(item, 5U),pb.Nth(item, 6U)}); } ),
            [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 0U), pb.Nth(item, 1U)}); },
            [&](TRuntimeNode, TRuntimeNode item) {
                const auto price = pb.Nth(item, 2U);
                const auto disco = pb.Nth(item, 4U);
                const auto v = pb.Mul(price, pb.Sub(pb.NewDataLiteral<double>(1.), disco));
                return pb.NewTuple({pb.NewDataLiteral<ui64>(1ULL), price, disco, pb.Nth(item, 5U), v, pb.Mul(v, pb.Add(pb.NewDataLiteral<double>(1.), pb.Nth(item, 3U))) });
            },
            [&](TRuntimeNode, TRuntimeNode item, TRuntimeNode state) {
                const auto price = pb.Nth(item, 2U);
                const auto disco = pb.Nth(item, 4U);
                const auto v = pb.Mul(price, pb.Sub(pb.NewDataLiteral<double>(1.), disco));
                return pb.NewTuple({pb.Increment(pb.Nth(state, 0U)), pb.AggrAdd(pb.Nth(state, 1U), price), pb.AggrAdd(pb.Nth(state, 2U), disco), pb.AggrAdd(pb.Nth(state, 3U), pb.Nth(item, 5U)), pb.AggrAdd(pb.Nth(state, 4U), v), pb.AggrAdd(pb.Nth(state, 5U), pb.Mul(v, pb.Add(pb.NewDataLiteral<double>(1.), pb.Nth(item, 3U)))) });
            },
            [&](TRuntimeNode key, TRuntimeNode state) { return pb.NewOptional(pb.NewTuple({pb.Nth(key, 0U), pb.Nth(key, 1U), pb.Nth(state, 0U), pb.Nth(state, 1U), pb.Div(pb.Nth(state, 2U), pb.Nth(state, 0U)), pb.Nth(state, 3U), pb.Nth(state, 4U), pb.Nth(state, 5U)})); },
            0ULL
        ));

        const auto graph = setup.BuildGraph(pgmReturn, EGraphPerProcess::Multi, {list});
        NUdf::TUnboxedValue* items = nullptr;
        graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(TpchSamples.size(), items));
        for (const auto& sample : TpchSamples) {
            NUdf::TUnboxedValue* elements = nullptr;
            *items++ = graph->GetHolderFactory().CreateDirectArrayHolder(7U, elements);
            elements[0] = NUdf::TUnboxedValuePod(std::get<0U>(sample));
            elements[1] = NUdf::TUnboxedValuePod::Embedded(std::get<1U>(sample));
            elements[2] = NUdf::TUnboxedValuePod::Embedded(std::get<2U>(sample));
            elements[3] = NUdf::TUnboxedValuePod(std::get<3U>(sample));
            elements[4] = NUdf::TUnboxedValuePod(std::get<4U>(sample));
            elements[5] = NUdf::TUnboxedValuePod(std::get<5U>(sample));
            elements[6] = NUdf::TUnboxedValuePod(std::get<6U>(sample));
        }

        const auto t1 = TInstant::Now();
        const auto& value = graph->GetValue();
        const auto t2 = TInstant::Now();

        UNIT_ASSERT_VALUES_EQUAL(value.GetListLength(), expects.size());

        const auto ptr = value.GetElements();
        for (size_t i = 0ULL; i < expects.size(); ++i) {
            const auto elements = ptr[i].GetElements();
            two.emplace_back(std::make_pair(elements[0].AsStringRef(), elements[1].AsStringRef()), std::pair<ui64, std::array<double, 5U>>{elements[2].template Get<ui64>(), {elements[3].template Get<double>(), elements[4].template Get<double>(), elements[5].template Get<double>(), elements[6].template Get<double>(), elements[7].template Get<double>()}});
        }

        std::sort(two.begin(), two.end(), [](const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> l, const std::pair<std::pair<std::string_view, std::string_view>, std::pair<ui64, std::array<double, 5U>>> r){ return l.first < r.first; });
        UNIT_ASSERT_VALUES_EQUAL(one, two);

        Cerr << "Runtime is " << t2 - t1 << " vs C++ " << cppTime << Endl;
    }
}
#endif
} // NMiniKQL
} // NKikimr
