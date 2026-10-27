#include <ydb/core/kqp/counters/kqp_counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <library/cpp/threading/local_executor/local_executor.h>
#include <util/generic/size_literals.h>
#include <util/system/sanitizers.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpService) {
    void ConfigureSettings(TKikimrSettings & settings, bool useCache, bool useAsyncPatternCompilation, bool useCompiledCapacityBytesLimit) {
        size_t cacheSize = 0;
        if (useCache) {
            cacheSize = useAsyncPatternCompilation ? 10_MB : 1_MB;
        }

        auto * tableServiceConfig = settings.AppConfig.MutableTableServiceConfig();
        tableServiceConfig->MutableResourceManager()->SetKqpPatternCacheCapacityBytes(cacheSize);
        if (useCompiledCapacityBytesLimit) {
            tableServiceConfig->MutableResourceManager()->SetKqpPatternCacheCompiledCapacityBytes(1_MB * 0.1);
        }

        tableServiceConfig->SetEnableAsyncComputationPatternCompilation(useAsyncPatternCompilation);

        if (useAsyncPatternCompilation) {
            tableServiceConfig->MutableCompileComputationPatternServiceConfig()->SetWakeupIntervalMs(1);
            tableServiceConfig->MutableResourceManager()->SetKqpPatternCachePatternAccessTimesBeforeTryToCompile(0);
        }
    }

    enum AsyncPatternCompilationStrategy {
        Off,
        On,
        OnWithLimit,
    };

    void PatternCacheImpl(bool useCache, AsyncPatternCompilationStrategy asyncPatternCompilationStrategy) {
        bool useAsyncPatternCompilation = asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::On ||
            asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::OnWithLimit;
        bool useCompiledCapacityBytesLimit = asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::OnWithLimit;

        auto settings = TKikimrSettings()
            .SetWithSampleTables(false);
        ConfigureSettings(settings, useCache, useAsyncPatternCompilation, useCompiledCapacityBytesLimit);

        auto kikimr = TKikimrRunner{settings};
        auto driver = kikimr.GetDriver();

        NKqp::TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
        auto kqpCounters = counters.GetKqpCounters();
        auto patternEvictions = kqpCounters->GetCounter("PatternCache/Evictions", /*derivative=*/true);
        auto compiledCodeEvictions = kqpCounters->GetCounter("PatternCache/CompiledCodeEvictions", /*derivative=*/true);

        static constexpr i64 AsyncPatternCompilationUniqueRequestsSize = 5;

        // A pattern that has lost its compiled code is not queued for compilation over and over again, so waiting for
        // more compilations than there are unique programs would never finish.
        auto async_compilation_condition = [&]() {
            if (useCache) {
                if (asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::On) {
                    return *counters.CompiledComputationPatterns != AsyncPatternCompilationUniqueRequestsSize;
                } else if (asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::OnWithLimit) {
                    return *counters.CompiledComputationPatterns < AsyncPatternCompilationUniqueRequestsSize;
                }
            }

            return false;
        };

        size_t InFlight = NSan::PlainOrUnderSanitizer(10, 4);
        const ui32 IterationCount = NSan::PlainOrUnderSanitizer(500u, 50u);
        NPar::LocalExecutor().RunAdditionalThreads(InFlight);
        NPar::LocalExecutor().ExecRange([&](int /*id*/) {
            NYdb::NTable::TTableClient db(driver);
            auto session = db.CreateSession().GetValueSync().GetSession();

            for (ui32 i = 0; i < IterationCount || async_compilation_condition(); ++i) {
                ui32 value = useCache && useAsyncPatternCompilation ? i % AsyncPatternCompilationUniqueRequestsSize : i / 5;
                ui64 total = 100500;
                TString request = (TStringBuilder() << R"_(
                    $data = AsList(
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),

                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("aaa" AS Key,)_" << value << R"_(u AS Value),

                        AsStruct("aaa" AS Key,)_" << total - 10 * value << R"_(u AS Value),

                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),

                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value),
                        AsStruct("bbb" AS Key,)_" << value << R"_(u AS Value)
                    );

                    SELECT * FROM (
                        SELECT Key, SUM(Value) as Sum FROM (
                            SELECT * FROM AS_TABLE($data)
                        ) GROUP BY Key
                    ) WHERE Key == "aaa";
                )_");

                NYdb::NTable::TExecDataQuerySettings execSettings;
                execSettings.KeepInQueryCache(true);

                auto result = session.ExecuteDataQuery(request,
                    TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx(), execSettings).ExtractValueSync();
                AssertSuccessResult(result);

                CompareYson(R"( [ ["aaa";100500u] ])", FormatResultSetYson(result.GetResultSet(0)));
            }
        }, 0, InFlight, NPar::TLocalExecutor::WAIT_COMPLETE | NPar::TLocalExecutor::MED_PRIORITY);

        if (useCache && useAsyncPatternCompilation) {
            // Only the same few programs are executed over and over, and 10 MB is plenty of room for their patterns,
            // so all of them settle in the cache: none is ever pushed out and built again from scratch.
            UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(*patternEvictions), 0);

            if (asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::On) {
                // Nothing takes the compiled code away, so every pattern is compiled exactly once.
                UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(*counters.CompiledComputationPatterns),
                                         AsyncPatternCompilationUniqueRequestsSize);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<i64>(*compiledCodeEvictions), 0);
            } else if (asyncPatternCompilationStrategy == AsyncPatternCompilationStrategy::OnWithLimit) {
                const i64 compilations = *counters.CompiledComputationPatterns;
                const i64 codeEvictions = *compiledCodeEvictions;

                // The compiled code budget is deliberately too small to hold all of the patterns, so some of them do
                // lose their code ...
                UNIT_ASSERT_GE(codeEvictions, 1);

                // ... but giving the code up is the whole point of the limit, so it is not earned back again and
                // again: every pattern is compiled exactly once, however long the test runs.
                UNIT_ASSERT_VALUES_EQUAL(compilations, AsyncPatternCompilationUniqueRequestsSize);

                // Which bounds the churn as well: a pattern can only lose the code it has earned, and it earns it
                // exactly once per compilation. Both numbers settle instead of growing with the iteration count.
                UNIT_ASSERT_LE(codeEvictions, compilations);
            }
        }
    }

    Y_UNIT_TEST(PatternCache) {
        PatternCacheImpl(false, AsyncPatternCompilationStrategy::Off);
        PatternCacheImpl(false, AsyncPatternCompilationStrategy::On);
        PatternCacheImpl(false, AsyncPatternCompilationStrategy::OnWithLimit);
        PatternCacheImpl(true, AsyncPatternCompilationStrategy::Off);
        PatternCacheImpl(true, AsyncPatternCompilationStrategy::On);
        PatternCacheImpl(true, AsyncPatternCompilationStrategy::OnWithLimit);
    }

}

} // namespace NKqp
} // namespace NKikimr
