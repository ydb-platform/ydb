#include <util/datetime/cputimer.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/comp_nodes/mkql_factories.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>
#include <ydb/library/yql/minikql/mkql_program_builder.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/minikql/mkql_string_util.h>

#include <util/datetime/cputimer.h>
#include <util/system/thread.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdf;

const ui32 threadsCount = 10;
const TString prefix = "VERRRRRRRRY LONG STRING";

int main(int, char**) {
    auto functionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
    auto randomProvider = CreateDefaultRandomProvider();
    auto timeProvider = CreateDefaultTimeProvider();

    TScopedAlloc alloc(__LOCATION__);
    TTypeEnvironment env(alloc);
    TProgramBuilder pgmBuilder(env, *functionRegistry);

    auto argType = pgmBuilder.NewDataType(EDataSlot::String);
    auto arg = pgmBuilder.Arg(argType);
    auto prefixNode = pgmBuilder.NewDataLiteral<NUdf::EDataSlot::String>(prefix);
    auto concat = pgmBuilder.Concat(prefixNode, arg);
    auto pgm = pgmBuilder.NewList(argType, { prefixNode, prefixNode, concat, concat });
    TExploringNodeVisitor explorer;
    explorer.Walk(pgm.GetNode(), env);
    TComputationPatternOpts opts(alloc.Ref(), env, GetBuiltinFactory(),
        functionRegistry.Get(), NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi);
    auto pattern = MakeComputationPattern(explorer, pgm, { arg.GetNode(), pgm.GetNode() }, opts);

    TSimpleTimer timer;
    TVector<THolder<TThread>> threads;
    for (ui32 i = 0; i < threadsCount; ++i) {
        threads.emplace_back(MakeHolder<TThread>([pattern, opts, randomProvider, timeProvider]() {
            ui32 iters = 1000000;
#if defined(_tsan_enabled_)
            iters /= 100;
#endif
            for (ui32 i = 0; i < iters; ++i) {
                TScopedAlloc runAlloc(__LOCATION__);
                auto graph = pattern->Clone(opts.ToComputationOptions(*randomProvider, *timeProvider, &runAlloc.Ref()));
                TBindTerminator terminator(graph->GetTerminator());

                auto param = graph->GetEntryPoint(0, false);
                auto& ctx = graph->GetContext();

                TString s = ToString(i);
                param->SetValue(ctx, MakeString(s));

                auto resVal = graph->GetValue();
                auto val0 = resVal.GetElement(0);
                Y_ABORT_UNLESS(TStringBuf(val0.AsStringRef()) == prefix);
                Y_ABORT_UNLESS(val0.AsStringValue().RefCount() < 0);
                auto val1 = resVal.GetElement(1);
                Y_ABORT_UNLESS(TStringBuf(val1.AsStringRef()) == prefix);
                Y_ABORT_UNLESS(val1.AsStringValue().RefCount() < 0);
                auto val2 = resVal.GetElement(2);
                Y_ABORT_UNLESS(TStringBuf(val2.AsStringRef()) == prefix + s);
                Y_ABORT_UNLESS(val2.AsStringValue().RefCount() > 0);
                auto val3 = resVal.GetElement(3);
                Y_ABORT_UNLESS(TStringBuf(val3.AsStringRef()) == prefix + s);
                Y_ABORT_UNLESS(val3.AsStringValue().RefCount() > 0);
            }
        }));
    }

    for (ui32 i = 0; i < threadsCount; ++i) {
        threads[i]->Start();
    }

    for (ui32 i = 0; i < threadsCount; ++i) {
        threads[i]->Join();
    }

    Cerr << "Elapsed: " << timer.Get() << "\n";

    return 0;
}
