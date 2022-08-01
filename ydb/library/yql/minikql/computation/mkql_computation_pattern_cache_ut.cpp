#include "library/cpp/threading/local_executor/local_executor.h"
#include "ydb/library/yql/minikql/comp_nodes/ut/mkql_computation_node_ut.h"
#include <ydb/library/yql/minikql/computation/mkql_computation_node_holders.h>
#include <ydb/library/yql/minikql/mkql_type_builder.h>
#include <ydb/library/yql/minikql/mkql_node_serialization.h>
#include <ydb/library/yql/utils/yql_panic.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/dq/proto/dq_tasks.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NKikimr {
namespace NMiniKQL {

using namespace NYql::NUdf;

TComputationNodeFactory GetListTestFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestList") {
            return new TExternalComputationNode(ctx.Mutables);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TRuntimeNode CreateFlow(TProgramBuilder& pb, size_t vecSize, TCallable *list = nullptr) {
    if (list) {
        return pb.ToFlow(TRuntimeNode(list, false));
    } else {
        std::vector<const TRuntimeNode> arr;
        arr.reserve(vecSize);
        for (ui64 i = 0; i < vecSize; ++i) {
            arr.push_back(pb.NewDataLiteral<ui64>((i + 124515) % 6740234));
        }
        TArrayRef<const TRuntimeNode> arrRef(std::move(arr));
        return pb.ToFlow(pb.AsList(arrRef));
    }
}

template<bool Wide>
TRuntimeNode CreateFilter(TProgramBuilder& pb, size_t vecSize, TCallable *list);

template<>
TRuntimeNode CreateFilter<false>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode node) -> TRuntimeNode {
        return pb.AggrEquals(
                    pb.Mod(node, pb.NewOptional(pb.NewDataLiteral<ui64>(128))),
                pb.NewOptional(pb.NewDataLiteral<ui64>(0)));
    };
    return pb.Filter(flow, handler);
}

template<>
TRuntimeNode CreateFilter<true>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode::TList node) -> TRuntimeNode {
        return pb.AggrEquals(
                    pb.Mod(node.front(), pb.NewOptional(pb.NewDataLiteral<ui64>(128))),
                pb.NewOptional(pb.NewDataLiteral<ui64>(0)));
    };
    return pb.NarrowMap(
        pb.WideFilter(
            pb.ExpandMap(flow,
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }
            ),
            handler
        ),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }
    );
}

template<bool Wide>
TRuntimeNode CreateMap(TProgramBuilder& pb, size_t vecSize, TCallable *list = nullptr);

template<>
TRuntimeNode CreateMap<false>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode node) -> TRuntimeNode {
        return pb.AggrEquals(
                    pb.Mod(node, pb.NewOptional(pb.NewDataLiteral<ui64>(128))),
                pb.NewOptional(pb.NewDataLiteral<ui64>(0)));
    };
    return pb.Map(flow, handler);
}

template<>
TRuntimeNode CreateMap<true>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode::TList node) -> TRuntimeNode::TList {
        return {pb.AggrEquals(
                    pb.Mod(node.front(), pb.NewOptional(pb.NewDataLiteral<ui64>(128))),
                pb.NewOptional(pb.NewDataLiteral<ui64>(0)))};
    };
    return pb.NarrowMap(
        pb.WideMap(
            pb.ExpandMap(flow,
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }
            ),
            handler
        ),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }
    );
}

template<bool Wide>
TRuntimeNode CreateCondense(TProgramBuilder& pb, size_t vecSize, TCallable *list = nullptr);

template<>
TRuntimeNode CreateCondense<false>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto switcherHandler = [&](TRuntimeNode, TRuntimeNode) -> TRuntimeNode {
        return pb.NewDataLiteral<bool>(false);
    };
    auto updateHandler = [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNode {
        return pb.Add(item, state);
    };
    TRuntimeNode state = pb.NewDataLiteral<ui64>(0);
    return pb.Condense(flow, state, switcherHandler, updateHandler);
}

template<>
TRuntimeNode CreateCondense<true>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    TRuntimeNode state = pb.NewDataLiteral<ui64>(0);
    return pb.NarrowMap(
        pb.WideCondense1(
            /* stream */
            pb.ExpandMap(flow,
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }
            ),
            /* init */
            [&](TRuntimeNode::TList item) -> TRuntimeNode::TList { return {item}; },
            /* switcher */
            [&](TRuntimeNode::TList, TRuntimeNode::TList) -> TRuntimeNode { return pb.NewDataLiteral<bool>(false); },
            /* handler */
            [&](TRuntimeNode::TList item, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.Add(item.front(), state.front())}; }
        ),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }
    );
}

template<bool Wide>
TRuntimeNode CreateChopper(TProgramBuilder& pb, size_t vecSize, TCallable *list = nullptr);

template<>
TRuntimeNode CreateChopper<false>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.Chopper(flow,
        /* keyExtractor */
        [&](TRuntimeNode item) -> TRuntimeNode { return item; },
        /* groupSwitch */
        [&](TRuntimeNode key, TRuntimeNode /*item*/) -> TRuntimeNode {
            return pb.AggrEquals(
                    pb.Mod(key, pb.NewOptional(pb.NewDataLiteral<ui64>(128))),
                pb.NewOptional(pb.NewDataLiteral<ui64>(0)));
        },
        /* groupHandler */
        [&](TRuntimeNode, TRuntimeNode list) -> TRuntimeNode { return list; }
    );
};

template<>
TRuntimeNode CreateChopper<true>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowMap(
        pb.WideChopper(
            /* stream */
            pb.ExpandMap(flow,
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }
            ),
            /* keyExtractor */
            [&](TRuntimeNode::TList item) -> TRuntimeNode::TList { return item; },
            /* groupSwitch */
            [&](TRuntimeNode::TList key, TRuntimeNode::TList /*item*/) -> TRuntimeNode {
                return pb.AggrEquals(
                        pb.Mod(key.front(), pb.NewOptional(pb.NewDataLiteral<ui64>(128))),
                    pb.NewOptional(pb.NewDataLiteral<ui64>(0)));
            },
            /* groupHandler */
            [&](TRuntimeNode::TList, TRuntimeNode input) { return pb.WideMap(input, [](TRuntimeNode::TList items) { return items; }); }
        ),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }
    );
};

template<bool Wide>
TRuntimeNode CreateCombine(TProgramBuilder& pb, size_t vecSize, TCallable *list = nullptr);

template<>
TRuntimeNode CreateCombine<false>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.CombineCore(
        /* stream */
        flow,
        /* keyExtractor */
        [&] (TRuntimeNode /*item*/) -> TRuntimeNode { return pb.NewDataLiteral<ui64>(0);},
        /* init */
        [&] (TRuntimeNode /* key */, TRuntimeNode item) -> TRuntimeNode { return item; },
        /* update */
        [&] (TRuntimeNode /* key */, TRuntimeNode item, TRuntimeNode state) -> TRuntimeNode { return pb.Add(item, state); },
        /* finish */
        [&] (TRuntimeNode /* key */, TRuntimeNode item) -> TRuntimeNode { return pb.NewOptional(item); },
        /* memlimit */
        64 << 20
    );
};

template<>
TRuntimeNode CreateCombine<true>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {

    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowMap(
        pb.WideCombiner(
            /* stream */
            pb.ExpandMap(flow,
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }
            ),
            /* memlimit */
            64 << 20,
            /* keyExtractor */
            [&] (TRuntimeNode::TList /*item*/) -> TRuntimeNode::TList { return {pb.NewDataLiteral<ui64>(0)};},
            /* init */
            [&] (TRuntimeNode::TList /* key */, TRuntimeNode::TList item) -> TRuntimeNode::TList { return {item}; },
            /* update */
            [&] (TRuntimeNode::TList /* key */, TRuntimeNode::TList item, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.Add(item.front(), state.front())};
            },
            /* finish */
            [&] (TRuntimeNode::TList /* key */, TRuntimeNode::TList item) -> TRuntimeNode::TList { return {pb.NewOptional(item.front())}; }
        ),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); }
    );
};

template<bool Wide>
TRuntimeNode CreateChain1Map(TProgramBuilder& pb, size_t vecSize, TCallable *list = nullptr);

template<>
TRuntimeNode CreateChain1Map<false>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.Chain1Map(
        flow,
        /* init */
        [&] (TRuntimeNode item) -> TRuntimeNode { return item; },
        /* update */
        [&] (TRuntimeNode item, TRuntimeNode state) -> TRuntimeNode { return pb.Add(item, state); }
    );
}

template<>
TRuntimeNode CreateChain1Map<true>(TProgramBuilder& pb, size_t vecSize, TCallable *list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowMap(
        pb.WideChain1Map(
            /* stream */
            pb.ExpandMap(flow,
                [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }
            ),
            /* init */
            [&] (TRuntimeNode::TList item) -> TRuntimeNode::TList { return item; },
            /* update */
            [&] (TRuntimeNode::TList item, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.Add(item.front(), state.front())}; }
        ),
        [&] (TRuntimeNode::TList item) -> TRuntimeNode { return item.front(); }
    );
}

template<bool Wide>
TRuntimeNode CreateDiscard(TProgramBuilder& pb, size_t vecSize, TCallable *list = nullptr) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    if (Wide) {
        return pb.Discard(
                pb.ExpandMap(flow,
                    [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }
                )
            );
    } else {
        return pb.Discard(flow);
    }
}

template<bool Wide>
TRuntimeNode CreateSkip(TProgramBuilder& pb, size_t vecSize, TCallable *list = nullptr) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto count = pb.NewDataLiteral<ui64>(500);
    if (Wide) {
        return pb.NarrowMap(
            pb.Skip(
                pb.ExpandMap(flow,
                    [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }
                ),
                count
            ),
            [&] (TRuntimeNode::TList item) -> TRuntimeNode { return item.front(); }
        );
    } else {
        return pb.Skip(flow, count);
    }
}

Y_UNIT_TEST_SUITE(ComputationGraphDataRace) {
    template<class T>
    void ParallelProgTest(T f, bool useLLVM, ui64 testResult) {
        const ui32 cacheSize = 10;
        const ui32 inFlight = 3;
        TComputationPatternLRUCache cache(cacheSize);

        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
        std::shared_ptr<TPatternWithEnv> patternEnv = cache.CreateEnv();
        TScopedAlloc &alloc = patternEnv->Alloc;
        TTypeEnvironment &typeEnv = patternEnv->Env;

        TProgramBuilder pb(typeEnv, *functionRegistry);

        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<ui64>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();
        const ui32 vecSize = 100'000;
        auto progReturn = f(pb, vecSize, list);

        TExploringNodeVisitor explorer;
        explorer.Walk(progReturn.GetNode(), typeEnv);

        TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetListTestFactory(), functionRegistry.Get(),
            NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception, useLLVM ? "" : "OFF", EGraphPerProcess::Multi);

        {
            auto guard = patternEnv->Env.BindAllocator();
            patternEnv->Pattern = MakeComputationPattern(explorer, progReturn, {list}, opts);
        }
        cache.EmplacePattern("a", patternEnv);
        auto genData = [&]() {
            std::vector<ui64> data;
            data.reserve(vecSize);
            for (ui64 i = 0; i < vecSize; ++i) {
                data.push_back((i + 124515) % 6740234);
            }
            return data;
        };

        const auto data = genData();

        std::vector<std::vector<ui64>> results(inFlight);

        NPar::LocalExecutor().RunAdditionalThreads(inFlight);
        NPar::LocalExecutor().ExecRange([&](int id) {
            for (ui32 i = 0; i < 100; ++i) {
                auto key = "a";

                auto randomProvider = CreateDeterministicRandomProvider(1);
                auto timeProvider = CreateDeterministicTimeProvider(10000000);
                TScopedAlloc graphAlloc;

                auto patternEnv = cache.Find(key);

                TComputationPatternOpts opts(patternEnv->Alloc.Ref(), patternEnv->Env, GetListTestFactory(),
                    functionRegistry.Get(), NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception,
                    useLLVM ? "" : "OFF", EGraphPerProcess::Multi);

                auto graph = patternEnv->Pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider, &graphAlloc.Ref()));
                TUnboxedValue* items = nullptr;
                graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(data.size(), items));

                std::transform(data.cbegin(), data.cend(), items,
                [](const auto s) {
                    return ToValue<ui64>(s);
                });

                ui64 acc = 0;
                TUnboxedValue v = graph->GetValue();
                while (v.HasValue()) {
                    acc += v.Get<ui64>();
                    v = graph->GetValue();
                }
                results[id].push_back(acc);
            }
        }, 0, inFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

        for (auto threadResults : results) {
            for (auto res : threadResults) {
              UNIT_ASSERT_VALUES_EQUAL(res, testResult);
            }
        }
    }

    Y_UNIT_TEST_QUAD(Filter, Wide, UseLLVM) {
        ParallelProgTest(CreateFilter<Wide>, UseLLVM, 136480896);
    }

    Y_UNIT_TEST_QUAD(Map, Wide, UseLLVM) {
        ParallelProgTest(CreateMap<Wide>, UseLLVM, 782);
    }

    Y_UNIT_TEST_QUAD(Condense, Wide, UseLLVM) {
        ParallelProgTest(CreateCondense<Wide>, UseLLVM, 17451450000);
    }

    Y_UNIT_TEST_QUAD(Chopper, Wide, UseLLVM) {
        ParallelProgTest(CreateChopper<Wide>, UseLLVM, 17451450000);
    }

    Y_UNIT_TEST_QUAD(Combine, Wide, UseLLVM) {
        ParallelProgTest(CreateCombine<Wide>, UseLLVM, 17451450000);
    }

    Y_UNIT_TEST_QUAD(Chain1Map, Wide, UseLLVM) {
        ParallelProgTest(CreateChain1Map<Wide>, UseLLVM, 789247892400000);
    }

    Y_UNIT_TEST_QUAD(Discard, Wide, UseLLVM) {
        ParallelProgTest(CreateDiscard<Wide>, UseLLVM, 0);
    }

    Y_UNIT_TEST_QUAD(Skip, Wide, UseLLVM) {
        ParallelProgTest(CreateSkip<Wide>, UseLLVM, 17389067750);
    }
}

Y_UNIT_TEST_SUITE(ComputationPatternCache) {
    Y_UNIT_TEST(Smoke) {
        const ui32 cacheSize = 10;
        TComputationPatternLRUCache cache(cacheSize);

        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

        for (ui32 i = 0; i < cacheSize; ++i) {
            std::shared_ptr<TPatternWithEnv> patternEnv = cache.CreateEnv();
            TScopedAlloc& alloc = patternEnv->Alloc;
            TTypeEnvironment& typeEnv = patternEnv->Env;

            TProgramBuilder pb(typeEnv, *functionRegistry);

            auto progReturn  = pb.NewDataLiteral<NYql::NUdf::EDataSlot::String>("qwerty");

            TExploringNodeVisitor explorer;
            explorer.Walk(progReturn.GetNode(), typeEnv);

            TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetBuiltinFactory(),
                functionRegistry.Get(), NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception,
                "OFF", EGraphPerProcess::Multi);

            {
                auto guard = patternEnv->Env.BindAllocator();
                patternEnv->Pattern = MakeComputationPattern(explorer, progReturn, {}, opts);
            }
            cache.EmplacePattern(TString((char)('a' + i)), patternEnv);
        }

        for (ui32 i = 0; i < cacheSize; ++i) {
            auto key = TString((char)('a' + i));

            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(10000000);
            TScopedAlloc graphAlloc;
            auto patternEnv = cache.Find(key);
            UNIT_ASSERT(patternEnv);
            TComputationPatternOpts opts(patternEnv->Alloc.Ref(), patternEnv->Env, GetBuiltinFactory(),
                functionRegistry.Get(), NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception,
                "OFF", EGraphPerProcess::Multi);

            auto graph = patternEnv->Pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider, &graphAlloc.Ref()));
            auto value = graph->GetValue();
            UNIT_ASSERT_EQUAL(value.AsStringRef(), NYql::NUdf::TStringRef("qwerty"));
        }
    }

    Y_UNIT_TEST(AddPerf) {
        TTimer t("all: ");
        TScopedAlloc alloc;
        TTypeEnvironment typeEnv(alloc);

        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

        TProgramBuilder pb(typeEnv, *functionRegistry);
        auto prog1 = pb.NewDataLiteral<ui64>(123591592ULL);
        auto prog2 = pb.NewDataLiteral<ui64>(323591592ULL);
        auto progReturn = pb.Add(prog1, prog2);

        TExploringNodeVisitor explorer;
        explorer.Walk(progReturn.GetNode(), typeEnv);

        NUdf::EValidateMode validateMode = NUdf::EValidateMode::Lazy;
        TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetBuiltinFactory(),
            functionRegistry.Get(), validateMode, NUdf::EValidatePolicy::Exception,
            "OFF", EGraphPerProcess::Multi);

        auto t_make_pattern = std::make_unique<TTimer>("make_pattern: ");
        auto pattern = MakeComputationPattern(explorer, progReturn, {}, opts);
        t_make_pattern.reset();
        auto randomProvider = CreateDeterministicRandomProvider(1);
        auto timeProvider = CreateDeterministicTimeProvider(10000000);

        auto t_clone = std::make_unique<TTimer>("clone: ");
        auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
        t_clone.reset();

        {
            TTimer t("graph: ");
            ui64 acc = 0;
            for (ui32 i = 0; i < 100'000'000; ++i) {
                acc += graph->GetValue().Get<ui64>();
            }
            Y_DO_NOT_OPTIMIZE_AWAY(acc);
        }
        {
            std::function<ui64(ui64, ui64)> add = [](ui64 a, ui64 b) {
                return a + b;
            };

            TTimer t("lambda: ");
            ui64 acc = 0;
            for (ui32 i = 0; i < 100'000'000; ++i) {
                acc += add(123591592ULL, 323591592ULL);
            }
            Y_DO_NOT_OPTIMIZE_AWAY(acc);
        }
        {
            std::function<TUnboxedValue(TUnboxedValue&, TUnboxedValue&)> add =
                [](TUnboxedValue& a, TUnboxedValue& b) {
                    return TUnboxedValuePod(a.Get<ui64>() + b.Get<ui64>());
            };
            Y_DO_NOT_OPTIMIZE_AWAY(add);

            TTimer t("lambda unboxed value: ");
            TUnboxedValue acc(TUnboxedValuePod(0));
            TUnboxedValue v1(TUnboxedValuePod(ui64{123591592UL}));
            TUnboxedValue v2(TUnboxedValuePod(ui64{323591592UL}));
            for (ui32 i = 0; i < 100'000'000; ++i) {
                auto r = add(v1, v2);
                acc = add(r, acc);
            }
            Y_DO_NOT_OPTIMIZE_AWAY(acc.Get<ui64>());
        }
    }

    Y_UNIT_TEST_TWIN(FilterPerf, Wide) {
        TScopedAlloc alloc;
        TTypeEnvironment typeEnv(alloc);

        auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

        TProgramBuilder pb(typeEnv, *functionRegistry);
        const ui64 vecSize = 100'000'000;
        Cerr << "vecSize: " << vecSize << Endl;
        const auto listType = pb.NewListType(pb.NewDataType(NUdf::TDataType<ui64>::Id));
        const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();
        auto progReturn = CreateFilter<Wide>(pb, vecSize, list);

        TExploringNodeVisitor explorer;
        explorer.Walk(progReturn.GetNode(), typeEnv);

        NUdf::EValidateMode validateMode = NUdf::EValidateMode::Max;
        TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetListTestFactory(),
            functionRegistry.Get(), validateMode, NUdf::EValidatePolicy::Exception,
            "OFF", EGraphPerProcess::Multi);

        auto t_make_pattern = std::make_unique<TTimer>("make_pattern: ");
        auto pattern = MakeComputationPattern(explorer, progReturn, {list}, opts);
        t_make_pattern.reset();

        auto randomProvider = CreateDeterministicRandomProvider(1);
        auto timeProvider = CreateDeterministicTimeProvider(10000000);

        auto t_clone = std::make_unique<TTimer>("clone: ");
        auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));

        t_clone.reset();

        auto genData = [&]() {
            std::vector<ui64> data;
            data.reserve(vecSize);
            for (ui64 i = 0; i < vecSize; ++i) {
                data.push_back((i + 124515) % 6740234);
            }
            return data;
        };

        auto testResult = [&] (ui64 acc, ui64 count) {
            if (vecSize == 100'000'000) {
                UNIT_ASSERT_VALUES_EQUAL(acc, 2614128386688);
                UNIT_ASSERT_VALUES_EQUAL(count, 781263);
            } else if (vecSize == 10'000'000) {
                UNIT_ASSERT_VALUES_EQUAL(acc, 222145217664);
            } else {
                UNIT_FAIL("result is not checked");
            }
        };

        ui64 kIter = 2;
        {
            TDuration total;
            for (ui64 i = 0; i < kIter; ++i) {
                ui64 acc = 0;
                ui64 count = 0;

                auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
                auto data = genData();
                TUnboxedValue* items = nullptr;
                graph->GetEntryPoint(0, true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(data.size(), items));

                std::transform(data.cbegin(), data.cend(), items,
                [](const auto s) {
                    return ToValue<ui64>(s);
                });

                TSimpleTimer t;
                TUnboxedValue v = graph->GetValue();
                while (v.HasValue()) {
                  acc += v.Get<ui64>();
                  ++count;
                  v = graph->GetValue();
                }
                testResult(acc, count);

                total += t.Get();
            }
            Cerr << "graph: " << Sprintf("%.3f", total.SecondsFloat()) << "s" << Endl;
        }

        {
            auto data = genData();
            std::function<bool(ui64)> predicate = [](ui64 a) {
                return a % 128 == 0;
            };
            Y_DO_NOT_OPTIMIZE_AWAY(predicate);

            TDuration total;

            for (ui64 i = 0; i < kIter; ++i) {
                TSimpleTimer t;
                ui64 acc = 0;
                ui64 count = 0;
                for (ui64 j = 0; j < data.size(); ++j) {
                    if (predicate(data[j])) {
                        acc += data[j];
                        ++count;
                    }
                }

                total += t.Get();

                testResult(acc, count);
            }
            Cerr << "std::function: " << Sprintf("%.3f", total.SecondsFloat()) << "s" << Endl;
        }

        {
            auto data = genData();
            auto predicate = [](ui64 a) {
                return a % 128 == 0;
            };
            Y_DO_NOT_OPTIMIZE_AWAY(predicate);

            TDuration total;
            for (ui64 i = 0; i < kIter; ++i) {
                TSimpleTimer t;
                ui64 acc = 0;
                ui64 count = 0;
                for (ui64 j = 0; j < data.size(); ++j) {
                    if (predicate(data[j])) {
                        acc += data[j];
                        ++count;
                    }
                }

                total += t.Get();

                testResult(acc, count);
            }
            Cerr << "lambda: " << Sprintf("%.3f", total.SecondsFloat()) << "s" << Endl;
        }
    }
}

}
}
