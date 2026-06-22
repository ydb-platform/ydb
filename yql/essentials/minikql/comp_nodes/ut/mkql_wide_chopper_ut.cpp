#include "mkql_computation_node_ut.h"
#include <yql/essentials/minikql/mkql_runtime_version.h>
#include <yql/essentials/minikql/mkql_node_cast.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_codegen.h> // Y_IGNORE
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>

namespace NKikimr {
namespace NMiniKQL {

Y_UNIT_TEST_SUITE(TMiniKQLWideChopperTest) {
Y_UNIT_TEST_LLVM(TestConcatKeyToItems) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                               {"key one", "very long value 1"},
                                                               {"key two", "very long value 2"},
                                                               {"key two", "very long value 3"},
                                                               {"very long key one", "very long value 4"},
                                                               {"very long key two", "very long value 5"},
                                                               {"very long key two", "very long value 6"},
                                                               {"very long key two", "very long value 7"},
                                                               {"very long key two", "very long value 8"},
                                                               {"very long key two", "very long value 9"},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.Substring(items.front(), pb.Sub(pb.Size(items.front()), pb.NewDataLiteral<ui32>(4U)), pb.NewDataLiteral<ui32>(4U))}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.AggrNotEquals(keys.front(), items.front()); },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode input) { return pb.WideMap(input, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                                                                                                                          return {pb.AggrConcat(items.back(), keys.front())};
                                                                                                                      }); }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "very long value 1 one",
                                                          "very long value 2 two",
                                                          "very long value 3 two",
                                                          "very long value 4 one",
                                                          "very long value 5 two",
                                                          "very long value 6 two",
                                                          "very long value 7 two",
                                                          "very long value 8 two",
                                                          "very long value 9 two",
                                                      });
}

Y_UNIT_TEST_LLVM(TestCollectKeysOnly) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                               {"key one", "value 1"},
                                                               {"key two", "value 2"},
                                                               {"key two", "value 3"},
                                                               {"very long key one", "value 4"},
                                                               {"very long key two", "value 5"},
                                                               {"very long key two", "value 6"},
                                                               {"very long key two", "value 7"},
                                                               {"very long key two", "value 8"},
                                                               {"very long key two", "value 9"},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.AggrNotEquals(keys.front(), items.front()); },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode) { return pb.ExpandMap(pb.ToFlow(pb.NewOptional(keys.front())), [&](TRuntimeNode item) -> TRuntimeNode::TList {
                                                                                                                    return {item};
                                                                                                                }); }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "key one", "key two", "very long key one", "very long key two"});
}

Y_UNIT_TEST_LLVM(TestGetPart) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TStringBuf, TStringBuf>>{
                                                               {"key one", "value 1"},
                                                               {"key two", "value 2"},
                                                               {"key two", "value 3"},
                                                               {"very long key one", "value 4"},
                                                               {"very long key one", "value 5"},
                                                               {"very long key one", "value 6"},
                                                               {"very long key one", "value 7"},
                                                               {"very long key one", "value 8"},
                                                               {"very long key two", "value 9"},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.AggrNotEquals(keys.front(), items.front()); },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode input) { return pb.Take(pb.Skip(input, pb.NewDataLiteral<ui64>(1ULL)), pb.NewDataLiteral<ui64>(3ULL)); }),
                                                   [&](TRuntimeNode::TList items) { return items.back(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "value 3", "value 5", "value 6", "value 7"});
}

Y_UNIT_TEST_LLVM(TestSwitchByBoolFieldAndDontUseKey) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<TStringBuf>, TStringBuf, bool>>{
                                                               {{}, "value 1", true},
                                                               {"one", "value 2", false},
                                                               {"two", "value 3", false},
                                                               {{}, "value 4", true},
                                                               {"one", "value 5", false},
                                                               {"two", "value 6", false},
                                                               {{}, "value 7", false},
                                                               {"one", "value 8", false},
                                                               {"two", "value 9", true},
                                                           });

    const auto landmine = NTest::ConvertValueToLiteralNode(pb, TStringBuf("ACHTUNG MINEN!"));

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {pb.Unwrap(items.front(), landmine, __FILE__, __LINE__, 0)}; },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode::TList items) { return items.back(); },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode input) { return pb.Take(input, NTest::ConvertValueToLiteralNode(pb, ui64(2ULL))); }),
                                                   [&](TRuntimeNode::TList items) { return items[1U]; }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "value 1", "value 2", "value 4", "value 5", "value 9"});
}

Y_UNIT_TEST_LLVM(TestCollectKeysIfPresent) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto dataType = NTest::ConvertToMinikqlType<TStringBuf>(pb);

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<TStringBuf>, TStringBuf, bool>>{
                                                               {"one", "value 1", true},
                                                               {"one", "value 2", false},
                                                               {"one", "value 3", false},
                                                               {{}, "value 4", true},
                                                               {{}, "value 5", false},
                                                               {"two", "value 6", false},
                                                               {{}, "value 7", false},
                                                               {{}, "value 8", false},
                                                               {{}, "value 9", true},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front()}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.AggrNotEquals(keys.front(), items.front()); },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode part) { return pb.IfPresent(keys,
                                                                                                                                         [&](TRuntimeNode::TList keys) { return pb.ExpandMap(pb.ToFlow(pb.NewList(dataType, keys)), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }); },
                                                                                                                                         pb.WideMap(part, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U]}; })); }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "one", "value 4", "value 5", "two", "value 7", "value 8", "value 9"});
}

Y_UNIT_TEST_LLVM(TestConditionalByKeyPart) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TMaybe<TStringBuf>, TStringBuf, bool>>{
                                                               {"one", "value 1", true},
                                                               {"one", "value 2", true},
                                                               {"one", "value 3", false},
                                                               {"one", "value 4", false},
                                                               {"two", "value 5", false},
                                                               {"two", "value 6", false},
                                                               {"two", "value 7", true},
                                                               {{}, "value 8", true},
                                                               {{}, "value 9", false},
                                                           });

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U), pb.Nth(item, 2U)}; }),
                                                                  [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items.front(), items.back()}; },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode::TList items) { return pb.Or({pb.AggrNotEquals(keys.front(), items.front()), pb.AggrNotEquals(keys.back(), items.back())}); },
                                                                  [&](TRuntimeNode::TList keys, TRuntimeNode part) { return pb.If(keys.back(),
                                                                                                                                  pb.ExpandMap(pb.ToFlow(keys.front()), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
                                                                                                                                  pb.WideMap(part, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return {items[1U]}; })); }),
                                                   [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{
                                                          "one", "value 3", "value 4", "value 5", "value 6", "two", "value 9"});
}

Y_UNIT_TEST_LLVM(TestThinAllLambdas) {
    TSetup<LLVM> setup;
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto tupleType = NTest::ConvertToMinikqlType<std::tuple<>>(pb);
    const auto data = pb.NewTuple({});
    const auto list = pb.NewList(tupleType, {data, data, data, data});

    const auto pgmReturn = pb.Collect(pb.NarrowMap(pb.WideChopper(pb.ExpandMap(pb.ToFlow(list),
                                                                               [](TRuntimeNode) -> TRuntimeNode::TList { return {}; }),
                                                                  [](TRuntimeNode::TList items) { return items; },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode::TList) { return pb.NewDataLiteral<bool>(true); },
                                                                  [&](TRuntimeNode::TList, TRuntimeNode input) { return pb.WideMap(input, [](TRuntimeNode::TList items) { return items; }); }),
                                                   [&](TRuntimeNode::TList) { return pb.NewTuple({}); }));

    const auto graph = setup.BuildGraph(pgmReturn);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<std::tuple<>>{{}, {}, {}, {}});
}

class TTestProxyFlowWrapper: public TStatefulWideFlowCodegeneratorNode<TTestProxyFlowWrapper> {
    using TBaseComputation = TStatefulWideFlowCodegeneratorNode<TTestProxyFlowWrapper>;

public:
    TTestProxyFlowWrapper(TComputationMutables& mutables,
                          IComputationWideFlowNode* flow,
                          size_t width)
        : TBaseComputation(mutables, flow, EValueRepresentation::Boxed)
        , Flow_(flow)
        , Width_(width)
    {
    }

    EFetchResult DoCalculate(NUdf::TUnboxedValue& state,
                             TComputationContext& ctx,
                             NUdf::TUnboxedValue* const* output) const {
        return GetState(state, ctx)->ProxyFetch(ctx, output);
    }

#ifndef MKQL_DISABLE_CODEGEN
    ICodegeneratorInlineWideNode::TGenerateResult DoGenGetValues(const TCodegenContext& ctx, Value* statePtr, BasicBlock*& block) const {
        auto& context = ctx.Codegen.GetContext();
        // Call GetState.
        const auto ptrType = PointerType::getUnqual(StructType::get(context));
        const auto self = CastInst::Create(Instruction::IntToPtr, ConstantInt::get(Type::getInt64Ty(context), uintptr_t(this)), ptrType, "self", block);
        const auto state = EmitFunctionCall<&TTestProxyFlowWrapper::GetState>(ptrType, {self, statePtr, ctx.Ctx}, ctx, block);

        // Initialize temporary storage.
        const auto statusType = Type::getInt32Ty(context);
        const auto indexType = Type::getInt64Ty(context);
        const auto valueType = Type::getInt128Ty(context);
        const auto arrayValueType = ArrayType::get(valueType, Width_);
        const auto ptrValueType = PointerType::getUnqual(valueType);
        const auto arrayPtrValueType = ArrayType::get(ptrValueType, Width_);

        const auto values = new AllocaInst(arrayValueType, 0, "storage", block);
        const auto output = new AllocaInst(arrayPtrValueType, 0, "output", block);
        for (size_t idx = 0; idx < Width_; idx++) {
            const auto storagePtr = GetElementPtrInst::CreateInBounds(arrayValueType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, "storagePtr", block);
            const auto outputPtr = GetElementPtrInst::CreateInBounds(arrayPtrValueType, output, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, "outputPtr", block);
            new StoreInst(storagePtr, outputPtr, block);
        }

        // Call ProxyFetch.
        const auto result = EmitFunctionCall<&TTestProxyFlowState::ProxyFetch>(statusType, {state, ctx.Ctx, output}, ctx, block);

        // Return the result.
        ICodegeneratorInlineWideNode::TGettersList getters(Width_);
        for (size_t idx = 0; idx < getters.size(); idx++) {
            getters[idx] = [idx, values, arrayValueType, indexType, valueType](const TCodegenContext& ctx, BasicBlock*& block) {
                const auto valuePtr = GetElementPtrInst::CreateInBounds(arrayValueType, values, {ConstantInt::get(indexType, 0), ConstantInt::get(indexType, idx)}, "valuePtr", block);
                const auto value = new LoadInst(valueType, valuePtr, "value", block);
                // XXX: As a result of <ProxyFetch> call, a vector
                // of unboxed values is obtained. Hence, if these
                // values are not released here, no code later
                // will do it, leading to UV payload leakage.
                ValueRelease(EValueRepresentation::Any, value, ctx, block);
                return value;
            };
        }
        return {result, getters};
    }
#endif

private:
    class TTestProxyFlowState: public TComputationValue<TTestProxyFlowState> {
    public:
        TTestProxyFlowState(TMemoryUsageInfo* memInfo, IComputationWideFlowNode* flow)
            : TComputationValue(memInfo)
            , Flow_(flow)
        {
        }
        EFetchResult ProxyFetch(TComputationContext& ctx, NUdf::TUnboxedValue* const* output) {
            return Flow_->FetchValues(ctx, output);
        }

    private:
        IComputationWideFlowNode* const Flow_;
    };

    TTestProxyFlowState* GetState(NUdf::TUnboxedValue& state, TComputationContext& ctx) const {
        if (state.IsInvalid()) {
            state = ctx.HolderFactory.Create<TTestProxyFlowState>(Flow_);
        }
        return static_cast<TTestProxyFlowState*>(state.AsBoxed().Get());
    }

    void RegisterDependencies() const final {
        FlowDependsOn(Flow_);
    }

    IComputationWideFlowNode* const Flow_;
    const size_t Width_;
};

IComputationNode* WrapTestProxyFlow(TCallable& callable, const TComputationNodeFactoryContext& ctx) {
    const auto wideComponents = GetWideComponents(AS_TYPE(TFlowType, callable.GetInput(0U).GetStaticType()));
    const auto wideFlow = dynamic_cast<IComputationWideFlowNode*>(LocateNode(ctx.NodeLocator, callable, 0));
    return new TTestProxyFlowWrapper(ctx.Mutables, wideFlow, wideComponents.size());
}

constexpr std::string_view TestProxyFlowName = "TestProxyFlow";

TComputationNodeFactory GetNodeFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == TestProxyFlowName) {
            return WrapTestProxyFlow(callable, ctx);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

template <bool LLVM>
TRuntimeNode MakeTestProxyFlow(TSetup<LLVM>& setup, TRuntimeNode inputWideFlow) {
    TCallableBuilder callableBuilder(*setup.Env, TestProxyFlowName, inputWideFlow.GetStaticType());
    callableBuilder.Add(inputWideFlow);
    return TRuntimeNode(callableBuilder.Build(), false);
}

Y_UNIT_TEST_LLVM(TestCodegenWithProxyFlow) {
    TSetup<LLVM> setup(GetNodeFactory());
    TProgramBuilder& pb = *setup.PgmBuilder;

    const auto list = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<TStringBuf, TStringBuf>>{{"key one", "very long value 1"}});

    const auto wideFlow = pb.ExpandMap(pb.ToFlow(list), [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0U), pb.Nth(item, 1U)}; });
    const auto wideChoppedFlow = pb.WideChopper(wideFlow,
                                                [&](TRuntimeNode::TList items) -> TRuntimeNode::TList { return items; },
                                                [&](TRuntimeNode::TList, TRuntimeNode::TList) { return pb.NewDataLiteral<bool>(true); },
                                                [&](TRuntimeNode::TList keys, TRuntimeNode input) { return pb.ToFlow(pb.FromFlow(pb.WideMap(input, [&](TRuntimeNode::TList items) -> TRuntimeNode::TList {
                                                                                                        return {pb.AggrConcat(pb.AggrConcat(keys.front(), NTest::ConvertValueToLiteralNode(pb, TStringBuf(": "))), items.back())};
                                                                                                    }))); });
    const auto wideProxyFlow = MakeTestProxyFlow(setup, wideChoppedFlow);
    const auto root = pb.Collect(pb.NarrowMap(wideProxyFlow, [&](TRuntimeNode::TList items) { return items.front(); }));

    const auto graph = setup.BuildGraph(root);
    AssertUnboxedValueElementEqual(graph->GetValue(), TVector<TStringBuf>{"key one: very long value 1"});
}

} // Y_UNIT_TEST_SUITE(TMiniKQLWideChopperTest)

} // namespace NMiniKQL
} // namespace NKikimr
