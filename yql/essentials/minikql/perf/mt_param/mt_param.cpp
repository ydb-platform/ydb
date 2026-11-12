#include <benchmark/benchmark.h>

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/minikql/invoke_builtins/mkql_builtins.h>
#include <yql/essentials/minikql/mkql_program_builder.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/minikql/mkql_string_util.h>

#include <util/generic/ptr.h>

using namespace NKikimr;
using namespace NKikimr::NMiniKQL;
using namespace NKikimr::NUdf;

namespace {

void CheckResult(const TUnboxedValue& result, const TString& prefix, const TString& suffix) {
    // With ref locking disabled, immutable string literals are copied into owned values.
    auto val0 = result.GetElement(0);
    Y_ENSURE(TStringBuf(val0.AsStringRef()) == prefix);
    Y_ENSURE(val0.AsStringValue().RefCount() > 0);
    auto val1 = result.GetElement(1);
    Y_ENSURE(TStringBuf(val1.AsStringRef()) == prefix);
    Y_ENSURE(val1.AsStringValue().RefCount() > 0);
    auto val2 = result.GetElement(2);
    Y_ENSURE(TStringBuf(val2.AsStringRef()) == prefix + suffix);
    Y_ENSURE(val2.AsStringValue().RefCount() > 0);
    auto val3 = result.GetElement(3);
    Y_ENSURE(TStringBuf(val3.AsStringRef()) == prefix + suffix);
    Y_ENSURE(val3.AsStringValue().RefCount() > 0);
}

struct TSharedPattern {
    const TString Prefix = "VERRRRRRRRY LONG STRING";
    TIntrusivePtr<IFunctionRegistry> FunctionRegistry = CreateFunctionRegistry(CreateBuiltinRegistry());
    TIntrusivePtr<IRandomProvider> RandomProvider = CreateDefaultRandomProvider();
    TIntrusivePtr<ITimeProvider> TimeProvider = CreateDefaultTimeProvider();
    TScopedAlloc Alloc{__LOCATION__};
    TTypeEnvironment Env{Alloc};
    TComputationPatternOpts Opts{Alloc.Ref(), Env, GetBuiltinFactory(), FunctionRegistry.Get(),
                                 NUdf::EValidateMode::None, NUdf::EValidatePolicy::Exception, "OFF", EGraphPerProcess::Multi};
    IComputationPattern::TPtr Pattern;

    TSharedPattern() {
        TProgramBuilder builder(Env, *FunctionRegistry);
        auto argType = builder.NewDataType(EDataSlot::String);
        auto arg = builder.Arg(argType);
        auto prefix = builder.NewDataLiteral<NUdf::EDataSlot::String>(Prefix);
        auto concat = builder.Concat(prefix, arg);
        auto program = builder.NewList(argType, {prefix, prefix, concat, concat});
        TExploringNodeVisitor explorer;
        explorer.Walk(program.GetNode(), Env);
        Pattern = MakeComputationPattern(explorer, program, {arg.GetNode(), program.GetNode()}, Opts);
    }
};

void RunOperation(TSharedPattern& sharedPattern, ui32 index) {
    TScopedAlloc runAlloc(__LOCATION__);
    auto graph = sharedPattern.Pattern->Clone(sharedPattern.Opts.ToComputationOptions(
        *sharedPattern.RandomProvider, *sharedPattern.TimeProvider, &runAlloc.Ref()));
    TBindTerminator terminator(graph->GetTerminator());
    auto param = graph->GetEntryPoint(0, /*require=*/false);
    auto& ctx = graph->GetContext();
    TString suffix = ToString(index);
    param->SetValue(ctx, MakeString(suffix));
    auto result = graph->GetValue();
    CheckResult(result, sharedPattern.Prefix, suffix);
}

class TMtParamFixture: public benchmark::Fixture {
public:
    void SetUp(benchmark::State& state) override {
        if (state.thread_index() == 0) {
            try {
                SetupError_.clear();
                SharedPattern_ = MakeHolder<TSharedPattern>();
            } catch (...) {
                SetupError_ = CurrentExceptionMessage();
            }
        }
    }

    void TearDown(benchmark::State& state) override {
        if (state.thread_index() == 0) {
            SharedPattern_.Reset();
        }
    }

protected:
    THolder<TSharedPattern> SharedPattern_;
    TString SetupError_;
};

BENCHMARK_DEFINE_F(TMtParamFixture, MeasureMtParam)
(benchmark::State& state) {
    ui32 index = 0;
    // KeepRunning synchronizes setup and teardown across all benchmark workers.
    while (state.KeepRunning()) {
        try {
            Y_ENSURE(SharedPattern_, SetupError_);
            RunOperation(*SharedPattern_, index++);
        } catch (...) {
            state.SkipWithError(CurrentExceptionMessage());
        }
    }
}
BENCHMARK_REGISTER_F(TMtParamFixture, MeasureMtParam)->Name("MeasureMtParam")->UseRealTime()->Threads(10);

} // namespace
