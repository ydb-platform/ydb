#include <ydb/library/yql/dq/comp_nodes/ut/join_perf/benchmark_settings.h>
#include <ydb/library/yql/dq/comp_nodes/ut/join_perf/joins.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/datetime/base.h>
#include <util/stream/file.h>
#include <util/stream/output.h>
#include <util/string/builder.h>
#include <util/system/env.h>

#include <cstdlib>

namespace NKikimr::NMiniKQL {
namespace {

TBenchmarkSettings MakeLookupAnyPerfSettings() {
    TBenchmarkSettings params;
    params.Seed = 123;
    params.Scale = 1;
    params.Samples = 7;
    params.Warmup = 1;
    params.MinSampleMs = 40;
    params.MaxItersPerSample = 16;
    params.StringBytes = DefaultStringBytes;
    params.BlockSizes = {8192, 2048, 512, 128, 4096, 1024, 64, 256};
    params.Preset = TPreset{.Sizes = {{65536, 65536}}, .PresetName = "Daily"};
    params.KeySchemas = {{ETestedJoinKeyType::kString}};
    params.Payloads = {ETestedPayload::kNarrow};
    params.Algorithms = {ETestedJoinAlgo::kBlockHash};
    params.Flavours = {ETestedInputFlavour::kSameSizeTable};
    params.Filters = {ETestedFilter::kNone};
    return params;
}

struct TNamedCase {
    TString Scenario;
    EJoinKind Kind;
    TSelectivity Selectivity;
    i64 ExpectedOutputRows = -1;
};

void RunLookupAnyPerfSuite() {
    const TVector<TNamedCase> cases = {
        {.Scenario = "left-semi-string-dup8",
         .Kind = EJoinKind::LeftSemi,
         .Selectivity = {.MatchRate = 1.0, .DupsPerKey = 8},
         .ExpectedOutputRows = 65536},
        {.Scenario = "left-only-string-dup8",
         .Kind = EJoinKind::LeftOnly,
         .Selectivity = {.MatchRate = 1.0, .DupsPerKey = 8},
         .ExpectedOutputRows = 0},
        {.Scenario = "inner-string-miss",
         .Kind = EJoinKind::Inner,
         .Selectivity = {.MatchRate = 0.05, .DupsPerKey = 1},
         .ExpectedOutputRows = -1},
    };

    TStringBuilder json;
    json << "{\n  \"suite\": \"lookup-any-early-exit\",\n  \"cases\": [\n";
    bool first = true;

    for (const auto& named : cases) {
        auto params = MakeLookupAnyPerfSettings();
        params.JoinKinds = {named.Kind};
        params.Selectivities = {named.Selectivity};

        TVector<TBenchmarkCaseResult> results;
        RunJoinsBench(params, [&](const TBenchmarkCaseResult& result) { results.push_back(result); });
        UNIT_ASSERT_VALUES_EQUAL_C(results.size(), 1, named.Scenario);

        const auto& result = results.front();
        if (named.ExpectedOutputRows >= 0) {
            UNIT_ASSERT_VALUES_EQUAL_C(result.OutputRows, named.ExpectedOutputRows, named.Scenario);
        } else {
            UNIT_ASSERT_C(result.OutputRows > 0, named.Scenario);
        }

        Cout << named.Scenario << " medianCpuUs=" << result.RunDuration.MicroSeconds()
             << " cv=" << result.CvPercent << "% rows=" << result.OutputRows << Endl;

        if (!first) {
            json << ",\n";
        }
        first = false;
        json << "    {"
             << "\"scenario\":\"" << named.Scenario << "\","
             << "\"joinKind\":\"" << JoinKindOptionName(result.JoinKind) << "\","
             << "\"medianCpuUs\":" << result.RunDuration.MicroSeconds() << ","
             << "\"minCpuUs\":" << result.MinCpu.MicroSeconds() << ","
             << "\"maxCpuUs\":" << result.MaxCpu.MicroSeconds() << ","
             << "\"meanCpuUs\":" << result.MeanCpu.MicroSeconds() << ","
             << "\"cvPercent\":" << result.CvPercent << ","
             << "\"samples\":" << result.Samples << ","
             << "\"outputRows\":" << result.OutputRows << ","
             << "\"leftRows\":" << result.Sizes.Left << ","
             << "\"rightRows\":" << result.Sizes.Right
             << "}";
    }

    json << "\n  ]\n}\n";

    if (const TString outPath = GetEnv("JOIN_PERF_OUT")) {
        TFileOutput(outPath).Write(json);
        Cout << "wrote " << outPath << Endl;
    } else {
        Cout << json;
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TDqBlockHashLookupAnyPerf) {
    Y_UNIT_TEST(StringDuplicateProbeSuite) {
        RunLookupAnyPerfSuite();
    }
}

} // namespace NKikimr::NMiniKQL
