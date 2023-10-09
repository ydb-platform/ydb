#include <util/datetime/cputimer.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

#include <util/datetime/cputimer.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdf;

int main(int, char**) {

    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
    auto randomProvider = CreateDefaultRandomProvider();
    auto timeProvider = CreateDefaultTimeProvider();

    for (ui32 pass = 0; pass < 2; ++pass) {
        TString name = pass ? "param" : "const";
        TScopedAlloc alloc(__LOCATION__);
        TTypeEnvironment env(alloc);
        TProgramBuilder pgmBuilder(env, *functionRegistry);

        auto argType = pgmBuilder.NewDataType(EDataSlot::Uint32);
        auto arg = pgmBuilder.Arg(argType);
        auto pgm = pass ? arg : pgmBuilder.NewDataLiteral<ui32>(1);
        TExploringNodeVisitor explorer;
        explorer.Walk(pgm.GetNode(), env);
        TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(),
            functionRegistry.Get(), NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi);
        auto pattern = MakeComputationPattern(explorer, pgm, { arg.GetNode(), pgm.GetNode() }, opts);
        auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider));
        TBindTerminator terminator(graph->GetTerminator());

        auto param = graph->GetEntryPoint(0, false);
        auto& ctx = graph->GetContext();

        TSimpleTimer timer;
        for (ui32 i = 0; i < 100000000; ++i) {
            if (param) {
                param->SetValue(ctx, NUdf::TUnboxedValuePod(i));
            }

            auto resVal = graph->GetValue().Get<ui32>();
            Y_ABORT_UNLESS(resVal == (pass ? i : 1));
        }

        Cerr << "[" << name << "] Elapsed: " << timer.Get() << "\n";
    }

    return 0;
}
