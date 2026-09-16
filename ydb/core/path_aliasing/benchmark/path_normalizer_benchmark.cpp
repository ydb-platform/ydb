#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/protos/config.pb.h>

#include <benchmark/benchmark.h>

#include <util/string/builder.h>
#include <util/system/rusage.h>

#include <array>
#include <chrono>

namespace NKikimr::NPathAliasing {
    namespace {

        enum class ERuleKind {
            Prefix,
            Complex,
        };

        enum class EMatchPosition {
            First,
            Last,
            None,
        };

        constexpr std::array<size_t, 6> RuleCounts = {0, 1, 10, 100, 1000, 10000};

        NKikimrConfig::TPathRewriteConfig MakeConfig(size_t ruleCount, ERuleKind kind) {
            NKikimrConfig::TPathRewriteConfig config;
            for (size_t index = 0; index < ruleCount; ++index) {
                auto* rule = config.AddRules();
                if (kind == ERuleKind::Prefix) {
                    rule->SetPattern(TStringBuilder() << "^/alias" << index << "(/|$)");
                    rule->SetReplacement(TStringBuilder() << "/target" << index << R"(\1)");
                } else {
                    rule->SetPattern(TStringBuilder() << "^/(?:alias|legacy)" << index
                                                      << R"(/((?:[a-z]+[0-9]*-?)+)/(table|view)(/.*)?$)");
                    rule->SetReplacement(TStringBuilder() << "/target" << index << R"(/\1/\2\3)");
                }
            }
            return config;
        }

        void NormalizePaths(benchmark::State& state, ERuleKind kind, EMatchPosition position, size_t suffixLength) {
            const size_t ruleCount = state.range(0);
            const auto config = MakeConfig(ruleCount, kind);
            const size_t rssBefore = TRusage::GetCurrentRSS();
            const auto buildStart = std::chrono::steady_clock::now();
            const TPathNormalizer normalizer(config);
            const double buildMicroseconds = std::chrono::duration<double, std::micro>(
                                                 std::chrono::steady_clock::now() - buildStart)
                                                 .count();

            const size_t index = position == EMatchPosition::Last && ruleCount ? ruleCount - 1 : 0;
            const TString prefix = position == EMatchPosition::None
                                       ? TString("/unmatched")
                                       : TString(TStringBuilder() << "/alias" << index);
            const TString suffix = TString("/team12/table/") + TString(suffixLength, 'x');
            const TString path = prefix + suffix;
            const TString expected = !ruleCount || position == EMatchPosition::None
                                         ? path
                                         : TString(TStringBuilder() << "/target" << index << suffix);

            // Validate and warm RE2's lazy matching state before measuring steady state.
            if (normalizer.NormalizePath(path) != expected) {
                state.SkipWithError("Path normalizer produced an incorrect result");
                return;
            }
            const size_t rssAfter = TRusage::GetCurrentRSS();

            for ([[maybe_unused]] auto _ : state) {
                TString resolved = normalizer.NormalizePath(path);
                benchmark::DoNotOptimize(resolved);
            }
            state.SetItemsProcessed(state.iterations());
            state.SetBytesProcessed(state.iterations() * path.size());
            state.counters["build_us"] = buildMicroseconds;
            state.counters["input_bytes"] = path.size();
            // RSS is process-wide and includes allocator caches. These are observations,
            // not exact rule sizes or allocation counts; compare fresh filtered runs.
            state.counters["rss_total_bytes"] = rssAfter;
            state.counters["rss_growth_bytes"] = rssAfter > rssBefore ? rssAfter - rssBefore : 0;
        }

        void ConstructRules(benchmark::State& state, ERuleKind kind) {
            const auto config = MakeConfig(state.range(0), kind);
            for ([[maybe_unused]] auto _ : state) {
                TPathNormalizer normalizer(config);
                benchmark::DoNotOptimize(normalizer);
            }
            // Includes immutable-state construction, regex compilation, fingerprinting,
            // and destruction. Input protobuf creation is outside the timed loop.
            state.SetItemsProcessed(state.iterations());
        }

        void AddRuleCounts(benchmark::Benchmark* benchmark) {
            benchmark->ArgName("rules");
            for (const size_t count : RuleCounts) {
                benchmark->Arg(count);
            }
        }

        const auto RegisteredBenchmarks = [] {
            for (const auto kind : {ERuleKind::Prefix, ERuleKind::Complex}) {
                const TString kindName = kind == ERuleKind::Prefix ? "prefix" : "complex";
                const TString constructName = TString("ConstructRules/") + kindName;
                AddRuleCounts(benchmark::RegisterBenchmark(
                    constructName.c_str(), ConstructRules, kind));

                for (const auto position : {EMatchPosition::First, EMatchPosition::Last, EMatchPosition::None}) {
                    const TString positionName = position == EMatchPosition::First  ? "first"
                                                 : position == EMatchPosition::Last ? "last"
                                                                                    : "none";
                    for (const size_t suffixLength : {size_t{16}, size_t{64 * 1024}}) {
                        const TString name = TStringBuilder() << "NormalizePaths/" << kindName << "/"
                                                              << positionName << "/suffix_bytes:" << suffixLength;
                        AddRuleCounts(benchmark::RegisterBenchmark(
                            name.c_str(), NormalizePaths, kind, position, suffixLength));
                    }
                }
            }
            return 0;
        }();

    } // namespace
} // namespace NKikimr::NPathAliasing
