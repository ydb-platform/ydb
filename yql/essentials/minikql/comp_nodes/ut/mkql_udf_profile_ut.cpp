#include "mkql_computation_node_ut.h"
#include <yql/essentials/public/udf/udf_helpers.h>
#include <yql/essentials/minikql/comp_nodes/ut/mkql_program_builder_test_utils.h>

#include <util/generic/hash.h>

namespace NKikimr::NMiniKQL {

namespace {

SIMPLE_UDF(TSleep, ui64(ui64)) {
    Y_UNUSED(valueBuilder);
    Sleep(TDuration::MicroSeconds(args[0].Get<ui64>()));
    return NYql::NUdf::TUnboxedValuePod(static_cast<ui64>(0));
}

class TSleeperClosure: public NYql::NUdf::TBoxedValue {
private:
    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder*, const NYql::NUdf::TUnboxedValuePod* args) const override {
        Sleep(TDuration::MicroSeconds(args[0].Get<ui64>()));
        return NYql::NUdf::TUnboxedValuePod(static_cast<ui64>(0));
    }
};

class TMakeSleeper: public NYql::NUdf::TBoxedValue {
public:
    static const NYql::NUdf::TStringRef& Name() {
        static auto Name = NYql::NUdf::TStringRef::Of("MakeSleeper");
        return Name;
    }

    static bool DeclareSignature(
        const NYql::NUdf::TStringRef& name,
        NYql::NUdf::TType*,
        NYql::NUdf::IFunctionTypeInfoBuilder& builder,
        bool typesOnly)
    {
        if (Name() != name) {
            return false;
        }

        builder.Returns(builder.Callable()->Returns<ui64>().Arg<ui64>());
        if (!typesOnly) {
            builder.Implementation(new TMakeSleeper());
        }

        return true;
    }

private:
    NYql::NUdf::TUnboxedValue Run(const NYql::NUdf::IValueBuilder*, const NYql::NUdf::TUnboxedValuePod*) const override {
        return NYql::NUdf::TUnboxedValuePod(new TSleeperClosure());
    }
};

SIMPLE_MODULE(TProfileUTModule, TSleep, TMakeSleeper)

// Minimal ICountersProvider test double: hands out TCounter handles backed
// by a plain map, so the test can assert on final values after the graph
// (and hence TUdfProfileState) is torn down.
class TTestCountersProvider: public NYql::NUdf::ICountersProvider {
public:
    NYql::NUdf::TCounter GetCounter(const NYql::NUdf::TStringRef& module, const NYql::NUdf::TStringRef& name, bool) override {
        return NYql::NUdf::TCounter(&Counters[TString(module) + "_" + TString(name)]);
    }

    NYql::NUdf::TScopedProbe GetScopedProbe(const NYql::NUdf::TStringRef&, const NYql::NUdf::TStringRef&) override {
        return NYql::NUdf::TScopedProbe();
    }

    THashMap<TString, i64> Counters;
};

template <bool LLVM>
THolder<IComputationGraph> BuildSleepGraph(TSetup<LLVM>& setup, const TVector<ui64>& sleepUs) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto list = NTest::ConvertValueToLiteralNode(pb, sleepUs);

    const auto udf = pb.Udf("TestModule.Sleep");
    const auto pgmReturn = pb.Map(list, [&pb, udf](const TRuntimeNode item) {
        return pb.Apply(udf, {item});
    });

    return setup.BuildGraph(pgmReturn);
}

TVector<TUdfModuleInfo> MakeProfileModules() {
    TVector<TUdfModuleInfo> modules;
    modules.emplace_back(TUdfModuleInfo{
        .LibraryPath = "",
        .ModuleName = "TestModule",
        .Module = new TProfileUTModule(),
    });
    return modules;
}

template <bool LLVM>
THolder<IComputationGraph> BuildClosureSleepGraph(TSetup<LLVM>& setup, const TVector<ui64>& sleepUs) {
    TProgramBuilder& pb = *setup.PgmBuilder;
    const auto list = NTest::ConvertValueToLiteralNode(pb, sleepUs);

    // "TestModule.MakeSleeper()" is the zero-arg factory call producing the
    // closure; each row then invokes the closure itself with the real arg.
    const auto factory = pb.Udf("TestModule.MakeSleeper");
    const auto closure = pb.Apply(factory, {});
    const auto pgmReturn = pb.Map(list, [&pb, closure](const TRuntimeNode item) {
        return pb.Apply(closure, {item});
    });

    return setup.BuildGraph(pgmReturn);
}

} // namespace

Y_UNIT_TEST_SUITE(TMiniKQLUdfProfileTest) {

Y_UNIT_TEST_LLVM(ReportsCountersWhenSlow) {
    TSetup<LLVM> setup(GetTestFactory(), MakeProfileModules());
    TTestCountersProvider countersProvider;
    setup.CountersProvider = &countersProvider;
    setup.RuntimeSettings->UdfProfileEnable.Set(true);
    // Wide margins to keep this test stable: sleeps are either far below
    // or far above the threshold, never close to it.
    setup.RuntimeSettings->UdfProfileMinTimeUs.Set(TDuration::MilliSeconds(10));
    setup.RuntimeSettings->UdfProfileGraceCount.Set(3);
    setup.RuntimeSettings->UdfProfileHLLPrecision.Set(10);

    const TVector<ui64> sleepUs = {0, 0, 20000, 0, 20000, 30000};
    const ui64 slowCount = 3; // the three non-zero sleeps

    {
        const auto graph = BuildSleepGraph(setup, sleepUs);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>(sleepUs.size(), 0));
    }
    // The graph (and its TComputationContext) is destroyed by now, so the
    // profiling state's destructor has already flushed the counters.

    UNIT_ASSERT_VALUES_EQUAL(countersProvider.Counters["_UdfProfile_TestModule.Sleep_CallCount"], (i64)sleepUs.size());
    // It would be great to have an equality here, but sometimes the process can get interrupted, which increases the slow counter
    UNIT_ASSERT(countersProvider.Counters["_UdfProfile_TestModule.Sleep_SlowCallCount"] >= (i64)slowCount);
    UNIT_ASSERT(countersProvider.Counters["_UdfProfile_TestModule.Sleep_Duration"] > 0);
    UNIT_ASSERT(countersProvider.Counters["_UdfProfile_TestModule.Sleep_Cardinality"] >= 1);
}

Y_UNIT_TEST_LLVM(DoesNotReportWhenNeverSlow) {
    TSetup<LLVM> setup(GetTestFactory(), MakeProfileModules());
    TTestCountersProvider countersProvider;
    setup.CountersProvider = &countersProvider;
    setup.RuntimeSettings->UdfProfileEnable.Set(true);
    // Threshold is far above any sleep used below, so the call site never
    // becomes "interesting" and nothing gets reported.
    setup.RuntimeSettings->UdfProfileMinTimeUs.Set(TDuration::Seconds(10));
    setup.RuntimeSettings->UdfProfileGraceCount.Set(3);
    setup.RuntimeSettings->UdfProfileHLLPrecision.Set(10);

    const TVector<ui64> sleepUs = {0, 0, 0, 0};

    {
        const auto graph = BuildSleepGraph(setup, sleepUs);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>(sleepUs.size(), 0));
    }

    UNIT_ASSERT(!countersProvider.Counters.contains("_UdfProfile_TestModule.Sleep_CallCount"));
}

Y_UNIT_TEST_LLVM(DoesNotReportWhenModuleExcluded) {
    TSetup<LLVM> setup(GetTestFactory(), MakeProfileModules());
    TTestCountersProvider countersProvider;
    setup.CountersProvider = &countersProvider;
    setup.RuntimeSettings->UdfProfileEnable.Set(true);
    setup.RuntimeSettings->UdfProfileMinTimeUs.Set(TDuration::MilliSeconds(1));
    setup.RuntimeSettings->UdfProfileGraceCount.Set(1);
    setup.RuntimeSettings->UdfProfileHLLPrecision.Set(10);
    setup.RuntimeSettings->UdfProfileExcludeModules.Set({"TestModule"});

    const TVector<ui64> sleepUs = {20000, 20000};

    {
        const auto graph = BuildSleepGraph(setup, sleepUs);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>(sleepUs.size(), 0));
    }

    UNIT_ASSERT(!countersProvider.Counters.contains("_UdfProfile_TestModule.Sleep_CallCount"));
}

Y_UNIT_TEST_LLVM(ReportsCountersForClosureLeaf) {
    TSetup<LLVM> setup(GetTestFactory(), MakeProfileModules());
    TTestCountersProvider countersProvider;
    setup.CountersProvider = &countersProvider;
    setup.RuntimeSettings->UdfProfileEnable.Set(true);
    setup.RuntimeSettings->UdfProfileMinTimeUs.Set(TDuration::MilliSeconds(10));
    setup.RuntimeSettings->UdfProfileGraceCount.Set(3);
    setup.RuntimeSettings->UdfProfileHLLPrecision.Set(10);

    const TVector<ui64> sleepUs = {0, 0, 20000, 0, 20000, 30000};
    const ui64 slowCount = 3; // the three non-zero sleeps

    {
        const auto graph = BuildClosureSleepGraph(setup, sleepUs);
        AssertUnboxedValueElementEqual(graph->GetValue(), TVector<ui64>(sleepUs.size(), 0));
    }

    // Profiling must attach to the leaf callable (TSleeperClosure, invoked
    // once per row) and not to the zero-arg TestModule.MakeSleeper() factory
    // call, which only runs once and never sleeps.
    UNIT_ASSERT_VALUES_EQUAL(countersProvider.Counters["_UdfProfile_TestModule.MakeSleeper_CallCount"], (i64)sleepUs.size());
    // It would be great to have an equality here, but sometimes the process can get interrupted, which increases the slow counter
    UNIT_ASSERT(countersProvider.Counters["_UdfProfile_TestModule.MakeSleeper_SlowCallCount"] >= (i64)slowCount);
    UNIT_ASSERT(countersProvider.Counters["_UdfProfile_TestModule.MakeSleeper_Duration"] > 0);
    UNIT_ASSERT(countersProvider.Counters["_UdfProfile_TestModule.MakeSleeper_Cardinality"] >= 1);
}

} // Y_UNIT_TEST_SUITE(TMiniKQLUdfProfileTest)

} // namespace NKikimr::NMiniKQL
