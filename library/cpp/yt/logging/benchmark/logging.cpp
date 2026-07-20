#include <benchmark/benchmark.h>

#include <library/cpp/yt/logging/logger.h>

#include <atomic>

namespace NYT::NLogging {
namespace {

////////////////////////////////////////////////////////////////////////////////

// A log manager whose Enqueue is essentially free, so that the benchmark measures
// only the producer-side cost (building the event payload), not the logging thread.
class TBenchmarkLogManager
    : public ILogManager
{
public:
    void RegisterStaticAnchor(TLoggingAnchor* anchor, ::TSourceLocation /*sourceLocation*/, TStringBuf /*message*/) override
    {
        anchor->Registered.store(true);
    }

    void UpdateAnchor(TLoggingAnchor* anchor) override
    {
        anchor->CurrentVersion.store(ActualVersion_.load());
    }

    void Enqueue(TLogEvent&& event) override
    {
        // Keep the produced payload observable so the build is not optimized away;
        // the event is then destroyed (freeing its chunk slice) -- same for both APIs.
        benchmark::DoNotOptimize(std::get<TTaggedLogEventPayload>(event.Payload).Underlying().Begin());
    }

    const TLoggingCategory* GetCategory(TStringBuf /*categoryName*/) override
    {
        return &Category_;
    }

    void UpdateCategory(TLoggingCategory* category) override
    {
        category->CurrentVersion.store(ActualVersion_.load());
    }

    bool GetAbortOnAlert() const override
    {
        return false;
    }

private:
    std::atomic<int> ActualVersion_ = 1;
    TLoggingCategory Category_ = {
        .Name = "Bench",
        .MinPlainTextLevel = ELogLevel::Minimum,
        .CurrentVersion = 0,
        .ActualVersion = &ActualVersion_,
    };
};

////////////////////////////////////////////////////////////////////////////////
// No tags.

void BM_Log_NoTags(benchmark::State& state)
{
    TBenchmarkLogManager manager;
    TLogger Logger(&manager, "Bench");
    for (auto _ : state) {
        YT_LOG_INFO("This is a log message of a typical length without tags");
    }
}
BENCHMARK(BM_Log_NoTags);

void BM_TLog_NoTags(benchmark::State& state)
{
    TBenchmarkLogManager manager;
    TLogger Logger(&manager, "Bench");
    for (auto _ : state) {
        YT_TLOG_INFO("This is a log message of a typical length without tags");
    }
}
BENCHMARK(BM_TLog_NoTags);

////////////////////////////////////////////////////////////////////////////////
// One int tag.

void BM_Log_OneTag(benchmark::State& state)
{
    TBenchmarkLogManager manager;
    TLogger Logger(&manager, "Bench");
    for (auto _ : state) {
        YT_LOG_INFO("This is a log message of a typical length (TagName: %v)",
            123);
    }
}
BENCHMARK(BM_Log_OneTag);

void BM_TLog_OneTag(benchmark::State& state)
{
    TBenchmarkLogManager manager;
    TLogger Logger(&manager, "Bench");
    for (auto _ : state) {
        YT_TLOG_INFO("This is a log message of a typical length")
            .With("TagName", 123);
    }
}
BENCHMARK(BM_TLog_OneTag);

////////////////////////////////////////////////////////////////////////////////
// Two tags (int + string).

void BM_Log_TwoTags(benchmark::State& state)
{
    TBenchmarkLogManager manager;
    TLogger Logger(&manager, "Bench");
    for (auto _ : state) {
        YT_LOG_INFO("This is a log message of a typical length (TagName1: %v, TagName2: %v)",
            123,
            "this-is-a-string");
    }
}
BENCHMARK(BM_Log_TwoTags);

void BM_TLog_TwoTags(benchmark::State& state)
{
    TBenchmarkLogManager manager;
    TLogger Logger(&manager, "Bench");
    for (auto _ : state) {
        YT_TLOG_INFO("This is a log message of a typical length")
            .With("TagName1", 123)
            .With("TagName2", "this-is-a-string");
    }
}
BENCHMARK(BM_TLog_TwoTags);

////////////////////////////////////////////////////////////////////////////////
// Three tags (int + string + double).

void BM_Log_ThreeTags(benchmark::State& state)
{
    TBenchmarkLogManager manager;
    TLogger Logger(&manager, "Bench");
    for (auto _ : state) {
        YT_LOG_INFO("This is a log message of a typical length (TagName1: %v, TagName2: %v, TagName3: %v)",
            123,
            "this-is-a-string",
            3.14);
    }
}
BENCHMARK(BM_Log_ThreeTags);

void BM_TLog_ThreeTags(benchmark::State& state)
{
    TBenchmarkLogManager manager;
    TLogger Logger(&manager, "Bench");
    for (auto _ : state) {
        YT_TLOG_INFO("This is a log message of a typical length")
            .With("TagName1", 123)
            .With("TagName2", "this-is-a-string")
            .With("TagName3", 3.14);
    }
}
BENCHMARK(BM_TLog_ThreeTags);

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NLogging
