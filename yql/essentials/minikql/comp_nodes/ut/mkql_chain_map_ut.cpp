#include "mkql_computation_node_ut.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>

namespace NKikimr {
namespace NMiniKQL {

namespace {

class TStreamThrottlerWrapper: public TMutableComputationNode<TStreamThrottlerWrapper> {
    using TBaseComputation = TMutableComputationNode<TStreamThrottlerWrapper>;

public:
    class TStreamValue: public TComputationValue<TStreamValue> {
    public:
        using TBase = TComputationValue<TStreamValue>;

        TStreamValue(TMemoryUsageInfo* memInfo, NUdf::TUnboxedValue origStream)
            : TBase(memInfo)
            , OrigStream_(std::move(origStream))
        {
        }

    private:
        NUdf::EFetchStatus Fetch(NUdf::TUnboxedValue& result) override {
            if (YieldsRemaining_ > 0) {
                --YieldsRemaining_;
                return NUdf::EFetchStatus::Yield;
            }
            return OrigStream_.Fetch(result);
        }

        NUdf::TUnboxedValue OrigStream_;
        size_t YieldsRemaining_ = 3;
    };

    TStreamThrottlerWrapper(TComputationMutables& mutables, IComputationNode* origStream)
        : TBaseComputation(mutables)
        , OrigStream_(origStream)
    {
    }

    NUdf::TUnboxedValuePod DoCalculate(TComputationContext& ctx) const {
        return ctx.HolderFactory.Create<TStreamValue>(OrigStream_->GetValue(ctx));
    }

private:
    void RegisterDependencies() const final {
        DependsOn(OrigStream_);
    }

    IComputationNode* OrigStream_;
};

IComputationNode* WrapStreamThrottler(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    MKQL_ENSURE(callable.GetInputsCount() == 1, "Expected 1 arg");
    return new TStreamThrottlerWrapper(ctx.Mutables, LocateNode(ctx.NodeLocator, callable, 0));
}

TComputationNodeFactory GetChain1MapThrottleFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "StreamThrottler") {
            return WrapStreamThrottler(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TRuntimeNode ThrottleNarrowStream(TProgramBuilder& pb, TRuntimeNode stream) {
    TCallableBuilder callableBuilder(pb.GetTypeEnvironment(), "StreamThrottler", stream.GetStaticType());
    callableBuilder.Add(stream);
    return TRuntimeNode(callableBuilder.Build(), false);
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLChainMapNodeTest) {
Y_UNIT_TEST_LLVM(TestOverList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto dataType = pb.NewOptionalType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}));

    auto data0 = pb.NewEmptyOptional(dataType);

    auto data2 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(7),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("A")}));
    auto data3 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(1),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("D")}));

    auto list = pb.NewList(dataType, {data2, data0, data3});

    auto init = pb.NewTuple({pb.NewOptional(pb.NewDataLiteral<i32>(3)),
                             pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("B"))});

    auto pgmReturn = pb.ChainMap(list, init,
                                 [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                                     auto key = pb.Nth(item, 0);
                                     auto val = pb.Nth(item, 1);
                                     auto skey = pb.AggrAdd(pb.Nth(state, 0), key);
                                     auto sval = pb.AggrConcat(pb.Nth(state, 1), val);
                                     return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})};
                                 });

    auto graph = setup.BuildGraph(pgmReturn);
    auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 7);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item.GetElement(0));
    UNIT_ASSERT(!item.GetElement(1));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 11);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BAD");
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(Test1OverList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto dataType = pb.NewOptionalType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}));

    auto data0 = pb.NewEmptyOptional(dataType);

    auto data1 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(3),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("B")}));
    auto data2 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(7),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("A")}));
    auto data3 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(1),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("D")}));

    auto list = pb.NewList(dataType, {data1, data2, data3, data0});

    auto pgmReturn = pb.Chain1Map(list,
                                  [&](TRuntimeNode item) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                return {pb.NewTuple({key, val, key, val}), pb.NewTuple({key, val})}; },
                                  [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                auto skey = pb.Add(pb.Nth(state, 0), key);
                auto sval = pb.Concat(pb.Nth(state, 1), val);
                return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})}; });

    auto graph = setup.BuildGraph(pgmReturn);
    auto iterator = graph->GetValue().GetListIterator();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "B");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 3);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "B");
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 7);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 11);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BAD");
    UNIT_ASSERT(iterator.Next(item));
    UNIT_ASSERT(!item.GetElement(0));
    UNIT_ASSERT(!item.GetElement(1));
    UNIT_ASSERT(!item.GetElement(2));
    UNIT_ASSERT(!item.GetElement(3));
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

Y_UNIT_TEST_LLVM(TestOverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto dataType = pb.NewOptionalType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}));

    auto data0 = pb.NewEmptyOptional(dataType);

    auto data2 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(7),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("A")}));
    auto data3 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(1),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("D")}));

    auto list = pb.NewList(dataType, {data2, data0, data3});

    auto init = pb.NewTuple({pb.NewOptional(pb.NewDataLiteral<i32>(3)),
                             pb.NewOptional(pb.NewDataLiteral<NUdf::EDataSlot::String>("B"))});

    auto pgmReturn = pb.FromFlow(pb.ChainMap(pb.ToFlow(list), init,
                                             [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                                                 auto key = pb.Nth(item, 0);
                                                 auto val = pb.Nth(item, 1);
                                                 auto skey = pb.AggrAdd(pb.Nth(state, 0), key);
                                                 auto sval = pb.AggrConcat(pb.Nth(state, 1), val);
                                                 return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})};
                                             }));

    auto graph = setup.BuildGraph(pgmReturn);
    auto iterator = graph->GetValue();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 7);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT(!item.GetElement(0));
    UNIT_ASSERT(!item.GetElement(1));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 11);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BAD");
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
}

Y_UNIT_TEST_LLVM(Test1OverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto dataType = pb.NewOptionalType(pb.NewTupleType({pb.NewDataType(NUdf::TDataType<i32>::Id), pb.NewDataType(NUdf::TDataType<char*>::Id)}));

    auto data0 = pb.NewEmptyOptional(dataType);

    auto data1 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(3),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("B")}));
    auto data2 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(7),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("A")}));
    auto data3 = pb.NewOptional(pb.NewTuple({pb.NewDataLiteral<i32>(1),
                                             pb.NewDataLiteral<NUdf::EDataSlot::String>("D")}));

    auto list = pb.NewList(dataType, {data1, data2, data3, data0});

    auto pgmReturn = pb.FromFlow(pb.Chain1Map(pb.ToFlow(list),
                                              [&](TRuntimeNode item) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                return {pb.NewTuple({key, val, key, val}), pb.NewTuple({key, val})}; },
                                              [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                auto key = pb.Nth(item, 0);
                auto val = pb.Nth(item, 1);
                auto skey = pb.Add(pb.Nth(state, 0), key);
                auto sval = pb.Concat(pb.Nth(state, 1), val);
                return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})}; }));

    auto graph = setup.BuildGraph(pgmReturn);
    auto iterator = graph->GetValue();
    NUdf::TUnboxedValue item;
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 3);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "B");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 3);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "B");
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 7);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "A");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 10);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BA");
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(0).template Get<i32>(), 1);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), "D");
    UNIT_ASSERT_VALUES_EQUAL(item.GetElement(2).template Get<i32>(), 11);
    UNBOXED_VALUE_STR_EQUAL(item.GetElement(3), "BAD");
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Ok, iterator.Fetch(item));
    UNIT_ASSERT(!item.GetElement(0));
    UNIT_ASSERT(!item.GetElement(1));
    UNIT_ASSERT(!item.GetElement(2));
    UNIT_ASSERT(!item.GetElement(3));
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
    UNIT_ASSERT_VALUES_EQUAL(NUdf::EFetchStatus::Finish, iterator.Fetch(item));
}

using TChainMapBuilder = TRuntimeNode (*)(TProgramBuilder&, TRuntimeNode, TRuntimeNode);

template <bool LLVM>
void TestMultiUsage(bool WithCollect, TChainMapBuilder chainMapBuilder) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    constexpr ui64 from = 1;
    constexpr ui64 to = 5;
    const TString prefix("Long prefix to prevent SSO: 0");

    const auto list = pb.ListFromRange(pb.NewDataLiteral(from),
                                       pb.NewDataLiteral(to),
                                       pb.NewDataLiteral<ui64>(1));

    const auto fold = chainMapBuilder(pb, WithCollect ? pb.Collect(list) : list,
                                      pb.NewDataLiteral<NUdf::EDataSlot::String>(prefix));

    const auto pgmReturn = pb.Zip({fold, fold});

    const auto graph = setup.BuildGraph(pgmReturn);
    const auto iterator = graph->GetValue().GetListIterator();

    NUdf::TUnboxedValue item;
    TString value(prefix);
    for (ui64 i = from; i < to; i++) {
        const auto expected = NYql::NUdf::TStringRef(value.data(), value.size());
        UNIT_ASSERT(iterator.Next(item));
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(0), expected);
        UNBOXED_VALUE_STR_EQUAL(item.GetElement(1), expected);
        value += ::ToString(i);
    }
    UNIT_ASSERT(!iterator.Next(item));
    UNIT_ASSERT(!iterator.Next(item));
}

template <bool LLVM>
void TestChainMapMultiUsage(bool WithCollect) {
    TestMultiUsage<LLVM>(WithCollect, [](TProgramBuilder& pb, TRuntimeNode list, TRuntimeNode init) {
        return pb.ChainMap(list, init,
                           [&pb](const TRuntimeNode item, const TRuntimeNode state) -> TRuntimeNodePair {
                               return {state, pb.Concat(state, pb.ToString(item))};
                           });
    });
}

template <bool LLVM>
void TestChain1MapMultiUsage(bool WithCollect) {
    TestMultiUsage<LLVM>(WithCollect, [](TProgramBuilder& pb, TRuntimeNode list, TRuntimeNode init) {
        return pb.Chain1Map(list,
                            [&pb, init](const TRuntimeNode item) -> TRuntimeNodePair { return {init, pb.Concat(init, pb.ToString(item))}; },
                            [&pb](const TRuntimeNode item, const TRuntimeNode state) -> TRuntimeNodePair { return {state, pb.Concat(state, pb.ToString(item))}; });
    });
}

Y_UNIT_TEST_LLVM(TestChainMapMultiUsageWithCollect) {
    TestChainMapMultiUsage<LLVM>(true);
}
Y_UNIT_TEST_LLVM(TestChainMapMultiUsageWithoutCollect) {
    TestChainMapMultiUsage<LLVM>(false);
}
Y_UNIT_TEST_LLVM(TestChain1MapMultiUsageWithCollect) {
    TestChain1MapMultiUsage<LLVM>(true);
}
Y_UNIT_TEST_LLVM(TestChain1MapMultiUsageWithoutCollect) {
    TestChain1MapMultiUsage<LLVM>(false);
}
} // Y_UNIT_TEST_SUITE(TMiniKQLChainMapNodeTest)

Y_UNIT_TEST_SUITE(TMiniKQLChain1MapThrottleTest) {

Y_UNIT_TEST_LLVM(TestChain1MapWithThrottledStream) {
    TSetup<LLVM> setup(GetChain1MapThrottleFactory());
    TProgramBuilder& pb = *setup.PgmBuilder;

    auto item1 = pb.NewStruct({{"dt", pb.NewDataLiteral<ui64>(10)}});
    auto item2 = pb.NewStruct({{"dt", pb.NewDataLiteral<ui64>(20)}});
    auto item3 = pb.NewStruct({{"dt", pb.NewDataLiteral<ui64>(30)}});
    auto item4 = pb.NewStruct({{"dt", pb.NewDataLiteral<ui64>(40)}});
    auto item5 = pb.NewStruct({{"dt", pb.NewDataLiteral<ui64>(50)}});
    auto itemType = item1.GetStaticType();
    auto list = pb.NewList(itemType, {item1, item2, item3, item4, item5});

    auto throttledStream = ThrottleNarrowStream(pb, pb.Iterator(list, {}));

    auto zero = pb.NewDataLiteral<ui64>(0);

    // clang-format off
    auto chain1 = pb.Chain1Map(throttledStream,
        [&](TRuntimeNode item) -> TRuntimeNode {
            auto dt = pb.Member(item, "dt");
            auto count0 = pb.AggrCountInit(dt);
            auto resultStruct = pb.NewStruct({{"Count0", count0}, {"Count1", zero}, {"dt", dt}});
            auto stateStruct = pb.NewStruct({{"Count0", count0}, {"Count1", pb.NewVoid()}});
            return pb.NewTuple({resultStruct, stateStruct});
        },
        [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNode {
            auto prevState = pb.Nth(state, 1);
            auto dt = pb.Member(item, "dt");
            auto count0 = pb.AggrCountUpdate(dt, pb.Member(prevState, "Count0"));
            auto resultStruct = pb.NewStruct({{"Count0", count0}, {"Count1", zero}, {"dt", dt}});
            auto stateStruct = pb.NewStruct({{"Count0", count0}, {"Count1", pb.Member(prevState, "Count1")}});
            return pb.NewTuple({resultStruct, stateStruct});
        });
    // clang-format on

    auto pgmReturn = pb.FromFlow(pb.ToFlow(
        pb.OrderedMap(chain1, [&](TRuntimeNode tuple) -> TRuntimeNode {
            return pb.Nth(tuple, 0);
        })));

    auto graph = setup.BuildGraph(pgmReturn);
    auto iterator = graph->GetValue();

    NUdf::TUnboxedValue item;
    ui32 resultCount = 0;
    for (;;) {
        const auto status = iterator.Fetch(item);
        if (status == NUdf::EFetchStatus::Finish) {
            break;
        }
        if (status == NUdf::EFetchStatus::Yield) {
            continue;
        }
        ++resultCount;
    }
    UNIT_ASSERT_VALUES_EQUAL(resultCount, 5u);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLChain1MapThrottleTest)

} // namespace NMiniKQL
} // namespace NKikimr
