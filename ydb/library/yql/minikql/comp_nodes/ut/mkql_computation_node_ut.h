#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_terminator.h>
#include "../mkql_factories.h"

#include <library/cpp/testing/unittest/registar.h>

#define UNBOXED_VALUE_STR_EQUAL(unboxed, expected) \
    do { \
        const auto v = (unboxed); \
        if (!(v.AsStringRef() == (expected))) { \
            UNIT_FAIL_IMPL( \
                    "equal assertion failed", \
                    Sprintf("%s %s == %s", #unboxed, TString(v.AsStringRef()).c_str(), #expected)); \
        } \
    } while (0)



#if defined(_msan_enabled_) || defined(_ubsan_enabled_) || defined(WITH_VALGRIND)
#define Y_UNIT_TEST_TWIN_IMPL_REGISTER(N, OPT)                                       \
    template<bool OPT> void N(NUnitTest::TTestContext&);                                   \
    struct TTestRegistration##N {                                                              \
        TTestRegistration##N() {                                                               \
            TCurrentTest::AddTest(#N "-" #OPT, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false>), false); \
        }                                                                                      \
    };                                                                                         \
    static TTestRegistration##N testRegistration##N;
#else
#define Y_UNIT_TEST_TWIN_IMPL_REGISTER(N, OPT)                                       \
    template<bool OPT> void N(NUnitTest::TTestContext&);                                   \
    struct TTestRegistration##N {                                                              \
        TTestRegistration##N() {                                                               \
            TCurrentTest::AddTest(#N "-" #OPT, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false>), false); \
            TCurrentTest::AddTest(#N "+" #OPT, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true>), false);  \
        }                                                                                      \
    };                                                                                         \
    static TTestRegistration##N testRegistration##N;
#endif

#define Y_UNIT_TEST_TWIN(N, OPT)      \
    Y_UNIT_TEST_TWIN_IMPL_REGISTER(N, OPT) \
    template<bool OPT> \
    void N(NUnitTest::TTestContext&)

#define Y_UNIT_TEST_LLVM(N) Y_UNIT_TEST_TWIN(N, LLVM)
#define Y_UNIT_TEST_LLVM_SPILLING(N) Y_UNIT_TEST_QUAD(N, LLVM, SPILLING)

#define Y_UNIT_TEST_QUAD(N, OPT1, OPT2)                                                                                              \
    template<bool OPT1, bool OPT2> void N(NUnitTest::TTestContext&);                                                                 \
    struct TTestRegistration##N {                                                                                                    \
        TTestRegistration##N() {                                                                                                     \
            TCurrentTest::AddTest(#N "-" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, false>), false); \
            TCurrentTest::AddTest(#N "-" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, true>), false);  \
            TCurrentTest::AddTest(#N "+" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, false>), false);  \
            TCurrentTest::AddTest(#N "+" #OPT1 "+" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, true>), false);   \
        }                                                                                                                            \
    };                                                                                                                               \
    static TTestRegistration##N testRegistration##N;                                                                                 \
    template<bool OPT1, bool OPT2>                                                                                                   \
    void N(NUnitTest::TTestContext&)

namespace NKikimr {
namespace NMiniKQL {

TComputationNodeFactory GetTestFactory(TComputationNodeFactory customFactory = {});

template<typename T>
NUdf::TUnboxedValuePod ToValue(T value) {
    return NUdf::TUnboxedValuePod(value);
}

struct TUdfModuleInfo {
    TString LibraryPath;
    TString ModuleName;
    NUdf::TUniquePtr<NUdf::IUdfModule> Module;
};

template<bool UseLLVM, bool EnableSpilling = false>
struct TSetup {
    explicit TSetup(TComputationNodeFactory nodeFactory = GetTestFactory(), TVector<TUdfModuleInfo>&& modules = {})
        : Alloc(__LOCATION__)
        , StatsRegistry(CreateDefaultStatsRegistry())
    {
        NodeFactory = nodeFactory;
        FunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
        if (!modules.empty()) {
            auto mutableRegistry = FunctionRegistry->Clone();
            for (auto& m : modules) {
                mutableRegistry->AddModule(m.LibraryPath, m.ModuleName, std::move(m.Module));
            }

            FunctionRegistry = mutableRegistry;
        }

        Alloc.Ref().ForcefullySetMemoryYellowZone(EnableSpilling);

        RandomProvider = CreateDeterministicRandomProvider(1);
        TimeProvider = CreateDeterministicTimeProvider(10000000);

        Env.Reset(new TTypeEnvironment(Alloc));
        PgmBuilder.Reset(new TProgramBuilder(*Env, *FunctionRegistry));
    }

    THolder<IComputationGraph> BuildGraph(TRuntimeNode pgm, const std::vector<TNode*>& entryPoints = std::vector<TNode*>()) {
       return BuildGraph(pgm, EGraphPerProcess::Multi, entryPoints);
    }

    THolder<IComputationGraph> BuildGraph(TRuntimeNode pgm, EGraphPerProcess graphPerProcess) {
        return BuildGraph(pgm, graphPerProcess, {});
    }

    TAutoPtr<IComputationGraph> BuildGraph(TRuntimeNode pgm, EGraphPerProcess graphPerProcess, const std::vector<TNode*>& entryPoints) {
        Reset();
        Explorer.Walk(pgm.GetNode(), *Env);
        TComputationPatternOpts opts(Alloc.Ref(), *Env, NodeFactory,
            FunctionRegistry.Get(), NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception,
             UseLLVM ? "" : "OFF", graphPerProcess, StatsRegistry.Get(), nullptr, nullptr);
        Pattern = MakeComputationPattern(Explorer, pgm, entryPoints, opts);
        auto graph = Pattern->Clone(opts.ToComputationOptions(*RandomProvider, *TimeProvider));
        Terminator.Reset(new TBindTerminator(graph->GetTerminator()));
        return graph;
    }

    void RenameCallable(TRuntimeNode pgm, TString originalName, TString newName) {
        const auto renameProvider = [originalName = std::move(originalName), newName = std::move(newName)](TInternName name) -> TCallableVisitFunc {
            if (name == originalName) {
                return [name, newName = std::move(newName)](TCallable& callable, const TTypeEnvironment& env) {
                    TCallableBuilder callableBuilder(env, newName,
                        callable.GetType()->GetReturnType(), false);
                    for (ui32 i = 0; i < callable.GetInputsCount(); ++i) {
                        callableBuilder.Add(callable.GetInput(i));
                    }
                    return TRuntimeNode(callableBuilder.Build(), false);
                };
            } else {
                return TCallableVisitFunc();
            }
        };
        TExploringNodeVisitor explorer;
        explorer.Walk(pgm.GetNode(), *Env);
        bool wereChanges = false;
        SinglePassVisitCallables(pgm, explorer, renameProvider, *Env, true, wereChanges);
    }

    void Reset() {
        Terminator.Destroy();
        Pattern.Reset();
    }

    TScopedAlloc Alloc;
    TComputationNodeFactory NodeFactory;
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry;
    TIntrusivePtr<IRandomProvider> RandomProvider;
    TIntrusivePtr<ITimeProvider> TimeProvider;
    IStatsRegistryPtr StatsRegistry;

    THolder<TTypeEnvironment> Env;
    THolder<TProgramBuilder> PgmBuilder;

    TExploringNodeVisitor Explorer;
    IComputationPattern::TPtr Pattern;
    THolder<TBindTerminator> Terminator;
};

extern const std::vector<std::pair<i8, double>> I8Samples;
extern const std::vector<std::pair<ui16, double>> Ui16Samples;
extern const std::vector<std::tuple<ui64, std::string, std::string, double, double, double, double>> TpchSamples;

}
}
