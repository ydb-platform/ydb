#include "library/cpp/threading/local_executor/local_executor.h"
#include "yql/essentials/minikql/comp_nodes/ut/mkql_computation_node_ut.h"
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>
#include <yql/essentials/minikql/udf_value_test_support/udf_value_comparator_utils.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_holders.h>
#include <yql/essentials/minikql/computation/mkql_computation_pattern_cache.h>
#include <yql/essentials/minikql/mkql_type_builder.h>
#include <yql/essentials/minikql/mkql_node_serialization.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/minikql/mkql_node.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/computation/mkql_computation_node_impl.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/cputimer.h>

namespace NKikimr::NMiniKQL {

using namespace NYql::NUdf;
namespace {

class TMockComputationPattern final: public IComputationPattern {
public:
    explicit TMockComputationPattern(size_t codeSize)
        : Size_(codeSize)
    {
    }

    void Compile(TString, IStatsRegistry*) override {
        Compiled_ = true;
    }
    bool IsCompiled() const override {
        return Compiled_;
    }
    size_t CompiledCodeSize() const override {
        return Size_;
    }
    void RemoveCompiledCode() override {
        Compiled_ = false;
    }
    THolder<IComputationGraph> Clone(const TComputationOptsFull&) override {
        return {};
    }
    bool GetSuitableForCache() const override {
        return true;
    }

private:
    const size_t Size_;
    bool Compiled_ = false;
};

TPatternCacheEntryPtr MakeMockEntry(size_t codeSize = 1) {
    auto entry = std::make_shared<TPatternCacheEntry>();
    entry->Pattern = MakeIntrusive<TMockComputationPattern>(codeSize);
    return entry;
}

NYql::TRuntimeSettingsStableHash MakeStableHash(ui8 fill) {
    NYql::TRuntimeSettingsStableHash hash;
    constexpr size_t arbitraryLength = 12;
    hash.resize(arbitraryLength, fill);
    return hash;
}

} // namespace

TComputationNodeFactory GetListTestFactory() {
    return [](TCallable& callable, const TComputationNodeFactoryContext& ctx) -> IComputationNode* {
        if (callable.GetType()->GetName() == "TestList") {
            return new TExternalComputationNode(ctx.Mutables);
        }
        return GetBuiltinFactory()(callable, ctx);
    };
}

TRuntimeNode CreateFlow(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    if (list) {
        return pb.ToFlow(TRuntimeNode(list, /*isImmediate=*/false));
    } else {
        std::vector<const TRuntimeNode> arr;
        arr.reserve(vecSize);
        for (ui64 i = 0; i < vecSize; ++i) {
            arr.push_back(NTest::ConvertValueToLiteralNode(pb, ui64((i + 124515) % 6740234)));
        }
        TArrayRef<const TRuntimeNode> arrRef(std::move(arr));
        return pb.ToFlow(pb.AsList(arrRef));
    }
}

template <bool Wide>
TRuntimeNode CreateFilter(TProgramBuilder& pb, size_t vecSize, TCallable* list);

template <>
TRuntimeNode CreateFilter<false>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode node) -> TRuntimeNode {
        return pb.AggrEquals(
            pb.Mod(node, NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(128))),
            NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(0)));
    };
    return pb.Filter(flow, handler);
}

template <>
TRuntimeNode CreateFilter<true>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode::TList node) -> TRuntimeNode {
        return pb.AggrEquals(
            pb.Mod(node.front(), NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(128))),
            NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(0)));
    };
    return pb.NarrowMap(
        pb.WideFilter(
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            handler),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });
}

template <bool Wide>
TRuntimeNode CreateMap(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr);

template <>
TRuntimeNode CreateMap<false>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode node) -> TRuntimeNode {
        return pb.AggrEquals(
            pb.Mod(node, NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(128))),
            NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(0)));
    };
    return pb.Map(flow, handler);
}

template <>
TRuntimeNode CreateMap<true>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto handler = [&](TRuntimeNode::TList node) -> TRuntimeNode::TList {
        return {pb.AggrEquals(
            pb.Mod(node.front(), NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(128))),
            NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(0)))};
    };
    return pb.NarrowMap(
        pb.WideMap(
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            handler),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });
}

template <bool Wide>
TRuntimeNode CreateCondense(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr);

template <>
TRuntimeNode CreateCondense<false>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto switcherHandler = [&](TRuntimeNode, TRuntimeNode) -> TRuntimeNode {
        return NTest::ConvertValueToLiteralNode(pb, /*simpleNode=*/false);
    };
    auto updateHandler = [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNode {
        return pb.Add(item, state);
    };
    TRuntimeNode state = NTest::ConvertValueToLiteralNode(pb, ui64(0));
    return pb.Condense(flow, state, switcherHandler, updateHandler);
}

template <>
TRuntimeNode CreateCondense<true>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowMap(
        pb.WideCondense1(
            /* stream */
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            /* init */
            [&](TRuntimeNode::TList item) -> TRuntimeNode::TList { return {item}; },
            /* switcher */
            [&](TRuntimeNode::TList, TRuntimeNode::TList) -> TRuntimeNode { return NTest::ConvertValueToLiteralNode(pb, /*simpleNode=*/false); },
            /* handler */
            [&](TRuntimeNode::TList item, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.Add(item.front(), state.front())}; }),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });
}

template <bool Wide>
TRuntimeNode CreateChopper(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr);

template <>
TRuntimeNode CreateChopper<false>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.Chopper(
        flow,
        /* keyExtractor */
        [&](TRuntimeNode item) -> TRuntimeNode { return item; },
        /* groupSwitch */
        [&](TRuntimeNode key, TRuntimeNode /*item*/) -> TRuntimeNode { return pb.AggrEquals(
                                                                           pb.Mod(key, NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(128))),
                                                                           NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(0))); },
        /* groupHandler */
        [&](TRuntimeNode, TRuntimeNode list) -> TRuntimeNode { return list; });
};

template <>
TRuntimeNode CreateChopper<true>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowMap(
        pb.WideChopper(
            /* stream */
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            /* keyExtractor */
            [&](TRuntimeNode::TList item) -> TRuntimeNode::TList { return item; },
            /* groupSwitch */
            [&](TRuntimeNode::TList key, TRuntimeNode::TList /*item*/) -> TRuntimeNode {
                return pb.AggrEquals(
                    pb.Mod(key.front(), NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(128))),
                    NTest::ConvertValueToLiteralNode(pb, TMaybe<ui64>(0)));
            },
            /* groupHandler */
            [&](TRuntimeNode::TList, TRuntimeNode input) { return pb.WideMap(input, [](TRuntimeNode::TList items) { return items; }); }),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });
};

template <bool Wide>
TRuntimeNode CreateCombine(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr);

template <>
TRuntimeNode CreateCombine<false>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.CombineCore(
        /* stream */
        flow,
        /* keyExtractor */
        [&](TRuntimeNode /*item*/) -> TRuntimeNode { return NTest::ConvertValueToLiteralNode(pb, ui64(0)); },
        /* init */
        [&](TRuntimeNode /* key */, TRuntimeNode item) -> TRuntimeNode { return item; },
        /* update */
        [&](TRuntimeNode /* key */, TRuntimeNode item, TRuntimeNode state) -> TRuntimeNode { return pb.Add(item, state); },
        /* finish */
        [&](TRuntimeNode /* key */, TRuntimeNode item) -> TRuntimeNode { return pb.NewOptional(item); },
        /* memlimit */
        64 << 20);
};

template <>
TRuntimeNode CreateCombine<true>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowMap(
        pb.WideCombiner(
            /* stream */
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            /* memlimit */
            64 << 20,
            /* keyExtractor */
            [&](TRuntimeNode::TList /*item*/) -> TRuntimeNode::TList { return {NTest::ConvertValueToLiteralNode(pb, ui64(0))}; },
            /* init */
            [&](TRuntimeNode::TList /* key */, TRuntimeNode::TList item) -> TRuntimeNode::TList { return {item}; },
            /* update */
            [&](TRuntimeNode::TList /* key */, TRuntimeNode::TList item, TRuntimeNode::TList state) -> TRuntimeNode::TList {
                return {pb.Add(item.front(), state.front())};
            },
            /* finish */
            [&](TRuntimeNode::TList /* key */, TRuntimeNode::TList item) -> TRuntimeNode::TList { return {pb.NewOptional(item.front())}; }),
        [&](TRuntimeNode::TList items) -> TRuntimeNode { return items.front(); });
};

template <bool Wide>
TRuntimeNode CreateChain1Map(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr);

template <>
TRuntimeNode CreateChain1Map<false>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.Chain1Map(
        flow,
        /* init */
        [&](TRuntimeNode item) -> TRuntimeNode { return item; },
        /* update */
        [&](TRuntimeNode item, TRuntimeNode state) -> TRuntimeNode { return pb.Add(item, state); });
}

template <>
TRuntimeNode CreateChain1Map<true>(TProgramBuilder& pb, size_t vecSize, TCallable* list) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowMap(
        pb.WideChain1Map(
            /* stream */
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            /* init */
            [&](TRuntimeNode::TList item) -> TRuntimeNode::TList { return item; },
            /* update */
            [&](TRuntimeNode::TList item, TRuntimeNode::TList state) -> TRuntimeNode::TList { return {pb.Add(item.front(), state.front())}; }),
        [&](TRuntimeNode::TList item) -> TRuntimeNode { return item.front(); });
}

template <bool Wide>
TRuntimeNode CreateDiscard(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    if (Wide) {
        return pb.Discard(
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }));
    } else {
        return pb.Discard(flow);
    }
}

template <bool Wide>
TRuntimeNode CreateSkip(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    auto count = NTest::ConvertValueToLiteralNode(pb, ui64(500));
    if (Wide) {
        return pb.NarrowMap(
            pb.Skip(
                pb.ExpandMap(flow,
                             [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
                count),
            [&](TRuntimeNode::TList item) -> TRuntimeNode { return item.front(); });
    } else {
        return pb.Skip(flow, count);
    }
}

template <bool Flow>
TRuntimeNode CreateNarrowFlatMap(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowFlatMap(
        pb.ExpandMap(flow,
                     [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
        [&](TRuntimeNode::TList item) -> TRuntimeNode {
            auto x = pb.NewOptional(item.front());
            return Flow ? pb.ToFlow(x) : x;
        });
}

TRuntimeNode CreateNarrowMultiMap(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.NarrowMultiMap(
        pb.ExpandMap(flow,
                     [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
        [&](TRuntimeNode::TList item) -> TRuntimeNode::TList {
            return {item.front(), item.front()};
        });
}

template <bool WithPayload>
TRuntimeNode CreateSqueezeToSortedDict(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    return pb.FlatMap(
        pb.NarrowSqueezeToSortedDict(
            pb.ExpandMap(flow,
                         [&](TRuntimeNode item) -> TRuntimeNode::TList { return {item}; }),
            /*all*/ false,
            /*keySelector*/ [&](TRuntimeNode::TList item) { return item.front(); },
            /*payloadSelector*/ [&](TRuntimeNode::TList) { return WithPayload ? NTest::ConvertValueToLiteralNode(pb, ui64(0)) : NTest::ConvertValueToLiteralNode(pb, NTest::TSingularVoid{}); }),
        [&](TRuntimeNode item) { return pb.DictKeys(item); });
}

TRuntimeNode CreateMapJoin(TProgramBuilder& pb, size_t vecSize, TCallable* list = nullptr) {
    TTimer t(TString(__func__) + ": ");
    auto flow = CreateFlow(pb, vecSize, list);

    const auto list1 = pb.Map(flow, [&](TRuntimeNode item) {
        return pb.NewTuple({pb.Mod(item, NTest::ConvertValueToLiteralNode(pb, ui64(1000))), NTest::ConvertValueToLiteralNode(pb, ui32(1))});
    });

    const auto list2 = NTest::ConvertValueToLiteralNode(pb, TVector<std::tuple<ui32, ui64>>{{1, 3000}, {2, 4000}, {3, 5000}});

    const auto dict = pb.ToSortedDict(list2, /*all=*/false,
                                      [&](TRuntimeNode item) { return pb.Nth(item, 0); },
                                      [&](TRuntimeNode item) { return pb.NewTuple({pb.Nth(item, 1U)}); });

    const auto resultType = pb.NewFlowType(pb.NewMultiType({
        NTest::ConvertToMinikqlType<TStringBuf>(pb),
        NTest::ConvertToMinikqlType<TStringBuf>(pb),
    }));

    return pb.Map(
        pb.NarrowMap(pb.MapJoinCore(
                         pb.ExpandMap(list1, [&](TRuntimeNode item) -> TRuntimeNode::TList { return {pb.Nth(item, 0), pb.Nth(item, 1)}; }),
                         dict,
                         EJoinKind::Inner,
                         {0U},
                         {1U, 0U},
                         {0U, 1U},
                         resultType),
                     [&](TRuntimeNode::TList items) { return pb.NewTuple(items); }),
        [&](TRuntimeNode item) { return pb.Nth(item, 1); });
}

Y_UNIT_TEST_SUITE(ComputationGraphDataRace) {
template <class T>
void ParallelProgTest(T f, bool useLLVM, ui64 testResult, size_t vecSize = 10'000) {
#if defined(_ubsan_enabled_)
    if (useLLVM) {
        // There is an issue with UBSan and codegen that occurs in various tests, including this one.
        // The problem is likely caused by calling LLVM-generated functions.
        // Moreover, the test fails in CI but does not fail locally, so it was decided to disable it
        // and treat the failure as a false positive.
        return;
    }
#endif
    TTimer t("total: ");
    const ui32 cacheSizeInBytes = 104857600; // 100 MiB
    const ui32 inFlight = 7;
    TComputationPatternLRUCache cache({cacheSizeInBytes, cacheSizeInBytes});

    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();
    auto entry = std::make_shared<TPatternCacheEntry>();
    TScopedAlloc& alloc = entry->Alloc;
    TTypeEnvironment& typeEnv = entry->Env;

    TProgramBuilder pb(typeEnv, *functionRegistry);

    const auto listType = NTest::ConvertToMinikqlType<TVector<ui64>>(pb);
    const auto list = TCallableBuilder(pb.GetTypeEnvironment(), "TestList", listType).Build();
    TRuntimeNode progReturn;
    with_lock (alloc) {
        progReturn = f(pb, vecSize, list);
    }

    TExploringNodeVisitor explorer;
    explorer.Walk(progReturn.GetNode(), typeEnv);

    TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetListTestFactory(), functionRegistry.Get(),
                                 NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception, useLLVM ? "" : "OFF", EGraphPerProcess::Multi);

    {
        auto guard = entry->Env.BindAllocator();
        entry->Pattern = MakeComputationPattern(explorer, progReturn, {list}, opts);
    }
    cache.EmplacePattern(TProgramKey{NYql::UnknownLangVersion, {}, "a"}, entry);
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
            TProgramKey key{NYql::UnknownLangVersion, {}, "a"};

            auto randomProvider = CreateDeterministicRandomProvider(1);
            auto timeProvider = CreateDeterministicTimeProvider(10000000);
            TScopedAlloc graphAlloc(__LOCATION__);

            auto entry = cache.Find(key);

            TComputationPatternOpts opts(entry->Alloc.Ref(), entry->Env, GetListTestFactory(),
                                         functionRegistry.Get(), NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception,
                                         useLLVM ? "" : "OFF", EGraphPerProcess::Multi);

            auto graph = entry->Pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider, &graphAlloc.Ref()));
            TUnboxedValue* items = nullptr;
            graph->GetEntryPoint(0, /*require=*/true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(data.size(), items));

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
    ParallelProgTest(CreateFilter<Wide>, UseLLVM, 10098816);
}

Y_UNIT_TEST_QUAD(Map, Wide, UseLLVM) {
    ParallelProgTest(CreateMap<Wide>, UseLLVM, 78);
}

Y_UNIT_TEST_QUAD(Condense, Wide, UseLLVM) {
    ParallelProgTest(CreateCondense<Wide>, UseLLVM, 1295145000);
}

Y_UNIT_TEST_QUAD(Chopper, Wide, UseLLVM) {
    ParallelProgTest(CreateChopper<Wide>, UseLLVM, 1295145000);
}

Y_UNIT_TEST_QUAD(Combine, Wide, UseLLVM) {
    ParallelProgTest(CreateCombine<Wide>, UseLLVM, 1295145000);
}

Y_UNIT_TEST_QUAD(Chain1Map, Wide, UseLLVM) {
    ParallelProgTest(CreateChain1Map<Wide>, UseLLVM, 6393039240000);
}

Y_UNIT_TEST_QUAD(Discard, Wide, UseLLVM) {
    ParallelProgTest(CreateDiscard<Wide>, UseLLVM, 0);
}

Y_UNIT_TEST_QUAD(Skip, Wide, UseLLVM) {
    ParallelProgTest(CreateSkip<Wide>, UseLLVM, 1232762750);
}

Y_UNIT_TEST_QUAD(NarrowFlatMap, Flow, UseLLVM) {
    ParallelProgTest(CreateNarrowFlatMap<Flow>, UseLLVM, 1295145000);
}

Y_UNIT_TEST_TWIN(NarrowMultiMap, UseLLVM) {
    ParallelProgTest(CreateNarrowMultiMap, UseLLVM, 1295145000ULL * 2);
}

Y_UNIT_TEST_QUAD(SqueezeToSortedDict, WithPayload, UseLLVM) {
    ParallelProgTest(CreateSqueezeToSortedDict<WithPayload>, UseLLVM, 125014500, 1000);
}

Y_UNIT_TEST_TWIN(MapJoin, UseLLVM) {
    ParallelProgTest(CreateMapJoin, UseLLVM, 120000, 10'000);
}
} // Y_UNIT_TEST_SUITE(ComputationGraphDataRace)

Y_UNIT_TEST_SUITE(ComputationPatternCache) {
Y_UNIT_TEST(Smoke) {
    const ui32 cacheSize = 10'000'000;
    const ui32 cacheItems = 10;
    TComputationPatternLRUCache cache({cacheSize, cacheSize});

    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

    for (ui32 i = 0; i < cacheItems; ++i) {
        auto entry = std::make_shared<TPatternCacheEntry>();
        TScopedAlloc& alloc = entry->Alloc;
        TTypeEnvironment& typeEnv = entry->Env;

        TProgramBuilder pb(typeEnv, *functionRegistry);

        TRuntimeNode progReturn;
        with_lock (alloc) {
            progReturn = NTest::ConvertValueToLiteralNode(pb, TStringBuf("qwerty"));
        }

        TExploringNodeVisitor explorer;
        explorer.Walk(progReturn.GetNode(), typeEnv);

        TComputationPatternOpts opts(alloc.Ref(), typeEnv, GetBuiltinFactory(),
                                     functionRegistry.Get(), NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception,
                                     "OFF", EGraphPerProcess::Multi);

        {
            auto guard = entry->Env.BindAllocator();
            entry->Pattern = MakeComputationPattern(explorer, progReturn, {}, opts);
        }

        // XXX: There is no way to accurately define how the entry's
        // allocator obtains the memory pages: using the free ones from the
        // global page pool or the ones directly requested by <mmap>. At the
        // same time, it is the total allocated bytes (not just the number
        // of the borrowed pages) that is a good estimate of the memory
        // consumed by the pattern cache entry for real life workload.
        // Hence, to avoid undesired cache flushes, release the free pages
        // of the allocator of the particular entry.
        alloc.ReleaseFreePages();
        cache.EmplacePattern(TProgramKey{NYql::UnknownLangVersion, {}, TString((char)('a' + i))}, entry);
    }

    for (ui32 i = 0; i < cacheItems; ++i) {
        TProgramKey key{NYql::UnknownLangVersion, {}, TString((char)('a' + i))};

        auto randomProvider = CreateDeterministicRandomProvider(1);
        auto timeProvider = CreateDeterministicTimeProvider(10000000);
        TScopedAlloc graphAlloc(__LOCATION__);
        auto entry = cache.Find(key);
        UNIT_ASSERT(entry);
        TComputationPatternOpts opts(entry->Alloc.Ref(), entry->Env, GetBuiltinFactory(),
                                     functionRegistry.Get(), NUdf::EValidateMode::Lazy, NUdf::EValidatePolicy::Exception,
                                     "OFF", EGraphPerProcess::Multi);

        auto graph = entry->Pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider, &graphAlloc.Ref()));
        auto value = graph->GetValue();
        AssertUnboxedValueElementEqual(value, TStringBuf("qwerty"));
    }
}

Y_UNIT_TEST(DoubleNotifyPatternCompiled) {
    const TProgramKey key{NYql::UnknownLangVersion, {}, "program"};
    const ui32 cacheSize = 2;
    TComputationPatternLRUCache cache({cacheSize, cacheSize});

    auto entry = std::make_shared<TPatternCacheEntry>();
    entry->Pattern = MakeIntrusive<TMockComputationPattern>(1U);
    cache.EmplacePattern(key, entry);

    for (ui32 i = 0; i < cacheSize + 1; ++i) {
        entry->Pattern->Compile("", /*stats=*/nullptr);
        cache.NotifyPatternCompiled(key);
    }

    entry = std::make_shared<TPatternCacheEntry>();
    entry->Pattern = MakeIntrusive<TMockComputationPattern>(cacheSize + 1);
    entry->Pattern->Compile("", /*stats=*/nullptr);
    cache.EmplacePattern(key, entry);
}

Y_UNIT_TEST(AddPerf) {
    TTimer t("all: ");
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment typeEnv(alloc);

    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

    TProgramBuilder pb(typeEnv, *functionRegistry);
    auto prog1 = NTest::ConvertValueToLiteralNode(pb, ui64(123591592ULL));
    auto prog2 = NTest::ConvertValueToLiteralNode(pb, ui64(323591592ULL));
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

    const ui64 repeats = 100'000;

    {
        TTimer t("graph: ");
        ui64 acc = 0;
        for (ui64 i = 0; i < repeats; ++i) {
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
        for (ui64 i = 0; i < repeats; ++i) {
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
        for (ui64 i = 0; i < repeats; ++i) {
            auto r = add(v1, v2);
            acc = add(r, acc);
        }
        Y_DO_NOT_OPTIMIZE_AWAY(acc.Get<ui64>());
    }
}

Y_UNIT_TEST_TWIN(FilterPerf, Wide) {
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment typeEnv(alloc);

    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry())->Clone();

    TProgramBuilder pb(typeEnv, *functionRegistry);
    const ui64 vecSize = 100'000;
    Cerr << "vecSize: " << vecSize << Endl;
    const auto listType = NTest::ConvertToMinikqlType<TVector<ui64>>(pb);
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

    auto testResult = [&](ui64 acc, ui64 count) {
        if (vecSize == 100'000'000) {
            UNIT_ASSERT_VALUES_EQUAL(acc, 2614128386688);
            UNIT_ASSERT_VALUES_EQUAL(count, 781263);
        } else if (vecSize == 10'000'000) {
            UNIT_ASSERT_VALUES_EQUAL(acc, 222145217664);
        } else if (vecSize == 100'000) {
            UNIT_ASSERT_VALUES_EQUAL(acc, 136480896);
            UNIT_ASSERT_VALUES_EQUAL(count, 782);
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
            graph->GetEntryPoint(0, /*require=*/true)->SetValue(graph->GetContext(), graph->GetHolderFactory().CreateDirectArrayHolder(data.size(), items));

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
        static auto Predicate = [](ui64 a) {
            return a % 128 == 0;
        };
        Y_DO_NOT_OPTIMIZE_AWAY(Predicate);

        TDuration total;
        for (ui64 i = 0; i < kIter; ++i) {
            TSimpleTimer t;
            ui64 acc = 0;
            ui64 count = 0;
            for (ui64 j = 0; j < data.size(); ++j) {
                if (Predicate(data[j])) {
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

Y_UNIT_TEST(UpdateConfigurationResize) {
    constexpr size_t patternSize = 100;
    constexpr size_t patternCount = 4;
    constexpr size_t initialMaxBytes = patternSize * patternCount;

    auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
    auto maxSizeBytes = counters->GetCounter("PatternCache/MaxSizeBytes", /*derivative=*/false);
    auto maxCompiledSizeBytes = counters->GetCounter("PatternCache/MaxCompiledSizeBytes", /*derivative=*/false);
    auto sizeCompiledBytes = counters->GetCounter("PatternCache/SizeCompiledBytes", /*derivative=*/false);

    TComputationPatternLRUCache cache({initialMaxBytes, initialMaxBytes, 0}, counters);

    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxSizeBytes), initialMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxCompiledSizeBytes), initialMaxBytes);

    TVector<TProgramKey> keys;
    for (size_t i = 0; i < patternCount; ++i) {
        TProgramKey key{NYql::UnknownLangVersion, {}, "p" + ToString(i)};
        keys.push_back(key);
        auto entry = std::make_shared<TPatternCacheEntry>();
        entry->Pattern = MakeIntrusive<TMockComputationPattern>(patternSize);
        entry->Pattern->Compile("", /*stats=*/nullptr);
        cache.EmplacePattern(key, entry);
    }
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*sizeCompiledBytes), patternSize * patternCount);

    // Resize up: entries preserved, sensors reflect new limit.
    constexpr size_t expandedMaxBytes = initialMaxBytes * 2;
    cache.UpdateConfiguration({expandedMaxBytes, expandedMaxBytes, 0});
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxSizeBytes), expandedMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxCompiledSizeBytes), expandedMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(cache.GetConfiguration().MaxSizeBytes, expandedMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*sizeCompiledBytes), patternSize * patternCount);
    for (const auto& key : keys) {
        auto entry = cache.Find(key);
        UNIT_ASSERT(entry);
        UNIT_ASSERT(entry->Pattern->IsCompiled());
    }

    // Resize down past current compiled usage: oldest compiled code evicted.
    constexpr size_t shrunkMaxBytes = patternSize * 2;
    cache.UpdateConfiguration({expandedMaxBytes, shrunkMaxBytes, 0});
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxSizeBytes), expandedMaxBytes);
    UNIT_ASSERT_VALUES_EQUAL(static_cast<size_t>(*maxCompiledSizeBytes), shrunkMaxBytes);
    UNIT_ASSERT_LE(static_cast<size_t>(*sizeCompiledBytes), shrunkMaxBytes);
    // Entries themselves still resolvable; only the compiled code of the LRU
    // entries was dropped.
    size_t stillCompiled = 0;
    for (const auto& key : keys) {
        auto entry = cache.Find(key);
        UNIT_ASSERT(entry);
        if (entry->Pattern->IsCompiled()) {
            ++stillCompiled;
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(stillCompiled, shrunkMaxBytes / patternSize);
}

Y_UNIT_TEST(TripletKeyFieldsDistinguishEntries) {
    // Four entries that differ from each other in exactly one field at a time
    const NYql::TLangVersion ver1 = NYql::MakeLangVersion(2025, 1);
    const NYql::TLangVersion ver2 = NYql::MakeLangVersion(2025, 2);
    const NYql::TRuntimeSettingsStableHash hash1 = MakeStableHash(0xAA);
    const NYql::TRuntimeSettingsStableHash hash2 = MakeStableHash(0xBB);

    const TProgramKey keyBase{ver1, hash1, "prog"};
    const TProgramKey keyDiffVer{ver2, hash1, "prog"};   // only lang version differs
    const TProgramKey keyDiffHash{ver1, hash2, "prog"};  // only stable hash differs
    const TProgramKey keyDiffProg{ver1, hash1, "other"}; // only program differs

    TComputationPatternLRUCache cache({1'000'000, 1'000'000});

    auto entryBase = MakeMockEntry();
    auto entryDiffVer = MakeMockEntry();
    auto entryDiffHash = MakeMockEntry();
    auto entryDiffProg = MakeMockEntry();

    cache.EmplacePattern(keyBase, entryBase);
    cache.EmplacePattern(keyDiffVer, entryDiffVer);
    cache.EmplacePattern(keyDiffHash, entryDiffHash);
    cache.EmplacePattern(keyDiffProg, entryDiffProg);

    UNIT_ASSERT_VALUES_EQUAL(cache.GetSize(), 4);

    UNIT_ASSERT_EQUAL(cache.Find(keyBase), entryBase);
    UNIT_ASSERT_EQUAL(cache.Find(keyDiffVer), entryDiffVer);
    UNIT_ASSERT_EQUAL(cache.Find(keyDiffHash), entryDiffHash);
    UNIT_ASSERT_EQUAL(cache.Find(keyDiffProg), entryDiffProg);

    UNIT_ASSERT(!cache.Find(TProgramKey{ver2, hash2, "missing"}));
}

Y_UNIT_TEST(TripletKeyNotifyPatternCompiled) {
    const TProgramKey key{NYql::MakeLangVersion(2025, 1), MakeStableHash(0x10), "prog"};
    TComputationPatternLRUCache cache({1'000'000, 1'000'000});

    auto entry = MakeMockEntry(512);
    cache.EmplacePattern(key, entry);

    entry->Pattern->Compile("", /*stats=*/nullptr);
    cache.NotifyPatternCompiled(key);

    auto found = cache.Find(key);
    UNIT_ASSERT_EQUAL(found, entry);
    UNIT_ASSERT(found->Pattern->IsCompiled());
}

Y_UNIT_TEST(TripletKeyNotifyPatternMissing) {
    // NotifyPatternMissing releases waiters for the specific triplet
    const TProgramKey key{NYql::MakeLangVersion(2025, 1), MakeStableHash(0x20), "prog"};
    TComputationPatternLRUCache cache({1'000'000, 1'000'000});

    // Register as the first subscriber (gets an empty future to trigger creation)
    auto firstFuture = cache.FindOrSubscribe(key);
    UNIT_ASSERT(!firstFuture.Initialized());

    // Register a second subscriber (gets a promise future)
    auto secondFuture = cache.FindOrSubscribe(key);
    UNIT_ASSERT(secondFuture.Initialized());
    UNIT_ASSERT(!secondFuture.HasValue());

    // Notify missing - second subscriber should receive nullptr
    cache.NotifyPatternMissing(key);
    UNIT_ASSERT(secondFuture.HasValue());
    UNIT_ASSERT(!secondFuture.GetValue());
}

Y_UNIT_TEST(TripletKeyFindOrSubscribeDistinctKeys) {
    // FindOrSubscribe distinguishes entries by full triplet
    const NYql::TLangVersion ver1 = NYql::MakeLangVersion(2025, 1);
    const NYql::TLangVersion ver2 = NYql::MakeLangVersion(2025, 2);
    const NYql::TRuntimeSettingsStableHash hash = {};
    const TString program = "prog";

    TComputationPatternLRUCache cache({1'000'000, 1'000'000});

    auto entry1 = MakeMockEntry();
    auto entry2 = MakeMockEntry();

    // Emplace both entries
    cache.EmplacePattern(TProgramKey{ver1, hash, program}, entry1);
    cache.EmplacePattern(TProgramKey{ver2, hash, program}, entry2);

    // FindOrSubscribe should find the correct entry for each triplet
    auto future1 = cache.FindOrSubscribe(TProgramKey{ver1, hash, program});
    auto future2 = cache.FindOrSubscribe(TProgramKey{ver2, hash, program});

    UNIT_ASSERT(future1.Initialized() && future1.HasValue());
    UNIT_ASSERT(future2.Initialized() && future2.HasValue());
    UNIT_ASSERT_EQUAL(future1.GetValue(), entry1);
    UNIT_ASSERT_EQUAL(future2.GetValue(), entry2);
}

} // Y_UNIT_TEST_SUITE(ComputationPatternCache)

} // namespace NKikimr::NMiniKQL
