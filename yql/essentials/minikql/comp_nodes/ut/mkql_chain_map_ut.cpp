#include "mkql_computation_node_ut.h"
#include "mkql_program_builder_test_utils.h"

#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/mkql_string_util.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

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

    using TItem = TMaybe<std::tuple<i32, TStringBuf>>;
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TItem>{
                                                         TItem{std::tuple<i32, TStringBuf>{7, "A"}},
                                                         TItem{},
                                                         TItem{std::tuple<i32, TStringBuf>{1, "D"}},
                                                     });
    auto init = NTest::ConvertValueToLiteralNode(pb, std::make_tuple(TMaybe<i32>{3}, TMaybe<TStringBuf>{"B"}));

    auto pgmReturn = pb.ChainMap(list, init,
                                 [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                                     auto key = pb.Nth(item, 0);
                                     auto val = pb.Nth(item, 1);
                                     auto skey = pb.AggrAdd(pb.Nth(state, 0), key);
                                     auto sval = pb.AggrConcat(pb.Nth(state, 1), val);
                                     return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})};
                                 });

    auto graph = setup.BuildGraph(pgmReturn);

    using TRow = std::tuple<TMaybe<i32>, TMaybe<TString>, TMaybe<i32>, TMaybe<TString>>;
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                                      {TMaybe<i32>{7}, TMaybe<TString>{"A"}, TMaybe<i32>{10}, TMaybe<TString>{"BA"}},
                                                                      {TMaybe<i32>{}, TMaybe<TString>{}, TMaybe<i32>{10}, TMaybe<TString>{"BA"}},
                                                                      {TMaybe<i32>{1}, TMaybe<TString>{"D"}, TMaybe<i32>{11}, TMaybe<TString>{"BAD"}},
                                                                  });
}

Y_UNIT_TEST_LLVM(Test1OverList) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TItem = TMaybe<std::tuple<i32, TStringBuf>>;
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TItem>{
                                                         TItem{std::tuple<i32, TStringBuf>{3, "B"}},
                                                         TItem{std::tuple<i32, TStringBuf>{7, "A"}},
                                                         TItem{std::tuple<i32, TStringBuf>{1, "D"}},
                                                         TItem{},
                                                     });

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

    using TRow = std::tuple<TMaybe<i32>, TMaybe<TString>, TMaybe<i32>, TMaybe<TString>>;
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TRow>{
                                                                      {TMaybe<i32>{3}, TMaybe<TString>{"B"}, TMaybe<i32>{3}, TMaybe<TString>{"B"}},
                                                                      {TMaybe<i32>{7}, TMaybe<TString>{"A"}, TMaybe<i32>{10}, TMaybe<TString>{"BA"}},
                                                                      {TMaybe<i32>{1}, TMaybe<TString>{"D"}, TMaybe<i32>{11}, TMaybe<TString>{"BAD"}},
                                                                      {TMaybe<i32>{}, TMaybe<TString>{}, TMaybe<i32>{}, TMaybe<TString>{}},
                                                                  });
}

Y_UNIT_TEST_LLVM(TestOverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TItem = TMaybe<std::tuple<i32, TStringBuf>>;
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TItem>{
                                                         TItem{std::tuple<i32, TStringBuf>{7, "A"}},
                                                         TItem{},
                                                         TItem{std::tuple<i32, TStringBuf>{1, "D"}},
                                                     });
    auto init = NTest::ConvertValueToLiteralNode(pb, std::make_tuple(TMaybe<i32>{3}, TMaybe<TStringBuf>{"B"}));

    auto pgmReturn = pb.FromFlow(pb.ChainMap(pb.ToFlow(list), init,
                                             [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNodePair {
                                                 auto key = pb.Nth(item, 0);
                                                 auto val = pb.Nth(item, 1);
                                                 auto skey = pb.AggrAdd(pb.Nth(state, 0), key);
                                                 auto sval = pb.AggrConcat(pb.Nth(state, 1), val);
                                                 return {pb.NewTuple({key, val, skey, sval}), pb.NewTuple({skey, sval})};
                                             }));

    auto graph = setup.BuildGraph(pgmReturn);

    using TRow = std::tuple<TMaybe<i32>, TMaybe<TString>, TMaybe<i32>, TMaybe<TString>>;
    const TVector<TRow> expected{
        {TMaybe<i32>{7}, TMaybe<TString>{"A"}, TMaybe<i32>{10}, TMaybe<TString>{"BA"}},
        {TMaybe<i32>{}, TMaybe<TString>{}, TMaybe<i32>{10}, TMaybe<TString>{"BA"}},
        {TMaybe<i32>{1}, TMaybe<TString>{"D"}, TMaybe<i32>{11}, TMaybe<TString>{"BAD"}},
    };
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               NYql::NUdf::TUnboxedValueComparatorStreamView<TRow>(expected));
}

Y_UNIT_TEST_LLVM(Test1OverFlow) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    using TItem = TMaybe<std::tuple<i32, TStringBuf>>;
    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TItem>{
                                                         TItem{std::tuple<i32, TStringBuf>{3, "B"}},
                                                         TItem{std::tuple<i32, TStringBuf>{7, "A"}},
                                                         TItem{std::tuple<i32, TStringBuf>{1, "D"}},
                                                         TItem{},
                                                     });

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

    using TRow = std::tuple<TMaybe<i32>, TMaybe<TString>, TMaybe<i32>, TMaybe<TString>>;
    const TVector<TRow> expected{
        {TMaybe<i32>{3}, TMaybe<TString>{"B"}, TMaybe<i32>{3}, TMaybe<TString>{"B"}},
        {TMaybe<i32>{7}, TMaybe<TString>{"A"}, TMaybe<i32>{10}, TMaybe<TString>{"BA"}},
        {TMaybe<i32>{1}, TMaybe<TString>{"D"}, TMaybe<i32>{11}, TMaybe<TString>{"BAD"}},
        {TMaybe<i32>{}, TMaybe<TString>{}, TMaybe<i32>{}, TMaybe<TString>{}},
    };
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               NYql::NUdf::TUnboxedValueComparatorStreamView<TRow>(expected));
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

    TVector<std::tuple<TString, TString>> expectedItems;
    TString value(prefix);
    for (ui64 i = from; i < to; i++) {
        expectedItems.emplace_back(value, value);
        value += ::ToString(i);
    }
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(), expectedItems);
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

    using TInRow = NTest::TStructType<NTest::TStructMember<"dt", ui64>>;

    auto list = NTest::ConvertValueToLiteralNode(pb, TVector<TInRow>{
                                                         {{{10ULL}}},
                                                         {{{20ULL}}},
                                                         {{{30ULL}}},
                                                         {{{40ULL}}},
                                                         {{{50ULL}}},
                                                     });

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

    // Struct fields are sorted alphabetically: Count0 (0), Count1 (1), dt (2)
    using TRow = std::tuple<ui64, ui64, ui64>;
    const TVector<TRow> expected{
        {ui64{1}, ui64{0}, ui64{10}},
        {ui64{2}, ui64{0}, ui64{20}},
        {ui64{3}, ui64{0}, ui64{30}},
        {ui64{4}, ui64{0}, ui64{40}},
        {ui64{5}, ui64{0}, ui64{50}},
    };
    NYql::NUdf::AssertUnboxedValueElementEqual(graph->GetValue(),
                                               NYql::NUdf::TUnboxedValueComparatorStreamView<TRow>(expected));
}

} // Y_UNIT_TEST_SUITE(TMiniKQLChain1MapThrottleTest)

} // namespace NMiniKQL
} // namespace NKikimr
