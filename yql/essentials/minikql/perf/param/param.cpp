#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

#include <benchmark/benchmark.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdf;

namespace {
void Run(benchmark::State& state, bool useParameter) {
    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
    auto randomProvider = CreateDefaultRandomProvider();
    auto timeProvider = CreateDefaultTimeProvider();
    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TProgramBuilder pgmBuilder(env, *functionRegistry);
    auto argType = pgmBuilder.NewDataType(EDataSlot::Uint32);
    auto arg = pgmBuilder.Arg(argType);
    auto pgm = useParameter ? arg : pgmBuilder.NewDataLiteral<ui32>(1);
    TExploringNodeVisitor explorer;
    explorer.Walk(pgm.GetNode(), env);
    TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(),
                                 functionRegistry.Get(), NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi);
    auto pattern = MakeComputationPattern(explorer, pgm, {arg.GetNode(), pgm.GetNode()}, opts);
    auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
    TBindTerminator terminator(graph->GetTerminator());
    auto param = graph->GetEntryPoint(0, /*require=*/false);
    auto& ctx = graph->GetContext();
    ui32 i = 0;
    for (auto _ : state) {
        if (param) {
            param->SetValue(ctx, NUdf::TUnboxedValuePod(i));
        }
        const auto result = graph->GetValue().Get<ui32>();
        const ui32 expected = useParameter ? i : 1;
        Y_ENSURE(result == expected, "At " << i << ": got " << result << ", expected " << expected);
        ++i;
    }
    state.SetItemsProcessed(state.iterations());
}

void Const(benchmark::State& state) {
    Run(state, /*useParameter=*/false);
}
void Param(benchmark::State& state) {
    Run(state, /*useParameter=*/true);
}
BENCHMARK(Const);
BENCHMARK(Param);
} // namespace
