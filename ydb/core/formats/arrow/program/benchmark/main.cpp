#include <ydb/core/formats/arrow/program/ascii_contains/ascii_contains.h>

#include <benchmark/benchmark.h>

#include <util/generic/string.h>
#include <util/generic/strbuf.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/random/random.h>
#include <util/string/ascii.h>

#include <algorithm>

using NKikimr::NArrow::NSSA::AsciiContainsIgnoreCaseMemchr;

namespace {

bool IgnoreCaseComparator(char a, char b) {
    return AsciiToUpper(a) == AsciiToUpper(b);
}

// Same algorithm the String._yql_AsciiContainsIgnoreCase UDF used before the Memchr kernel.
bool AsciiContainsIgnoreCaseScalar(const TStringBuf haystack, const TStringBuf needle) noexcept {
    if (needle.empty()) {
        return true;
    }
    return std::search(haystack.begin(), haystack.end(), needle.begin(), needle.end(), IgnoreCaseComparator) != haystack.end();
}

TString MakeNoMatchHaystack(size_t n) {
    SetRandomSeed(n * 2654435761ull + 1);
    TString s;
    s.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        s.push_back('a' + RandomNumber<unsigned char>(25)); // 'a'..'y'
    }
    return s;
}

TString MakeNeedle(size_t n, bool upperCase) {
    static const TStringBuf pattern = "needlepatternforbenchmarkingcontains";
    TString s;
    s.reserve(n);
    for (size_t i = 0; i < n; ++i) {
        s.push_back(pattern[i % pattern.size()]);
    }
    if (upperCase) {
        for (char& c : s) {
            c = AsciiToUpper(c);
        }
    }
    return s;
}

TString MakeMatchAtEndHaystack(size_t n, const TStringBuf needle) {
    if (n < needle.size()) {
        return MakeNoMatchHaystack(n);
    }
    TString s = MakeNoMatchHaystack(n - needle.size());
    s += needle;
    return s;
}

constexpr size_t kCorpusSize = 15000;
constexpr size_t kMinRowLen = 100;
constexpr size_t kMaxRowLen = 200;
constexpr double kMatchRate = 0.05;

TVector<TString> MakeCorpus(const TStringBuf plantedNeedle) {
    SetRandomSeed(20260819);
    TVector<TString> corpus;
    corpus.reserve(kCorpusSize);
    for (size_t i = 0; i < kCorpusSize; ++i) {
        const size_t len = kMinRowLen + RandomNumber<size_t>(kMaxRowLen - kMinRowLen + 1);
        TString s;
        s.reserve(len);
        for (size_t j = 0; j < len; ++j) {
            s.push_back('a' + RandomNumber<unsigned char>(25));
        }
        if (len >= plantedNeedle.size() && RandomNumber<double>() < kMatchRate) {
            const size_t pos = RandomNumber<size_t>(len - plantedNeedle.size() + 1);
            s.replace(pos, plantedNeedle.size(), plantedNeedle);
        }
        corpus.push_back(std::move(s));
    }
    return corpus;
}

size_t CorpusBytes(const TVector<TString>& corpus) noexcept {
    size_t total = 0;
    for (const auto& row : corpus) {
        total += row.size();
    }
    return total;
}

TString MakeRowsNeedle(size_t n) {
    TString needle = "z";
    needle += MakeNeedle(n - 1, /*upperCase*/ false);
    return needle;
}

template <auto Impl>
void BenchNoMatch(benchmark::State& state) {
    const size_t haystackLen = state.range(0);
    const size_t needleLen = state.range(1);
    const TString haystack = MakeNoMatchHaystack(haystackLen);
    const TString needle = MakeNeedle(needleLen, /*upperCase*/ true);
    for (auto _ : state) {
        bool found = Impl(haystack, needle);
        benchmark::DoNotOptimize(found);
    }
    state.SetBytesProcessed(state.iterations() * haystack.size());
}

template <auto Impl>
void BenchMatchAtEnd(benchmark::State& state) {
    const size_t haystackLen = state.range(0);
    const size_t needleLen = state.range(1);
    const TString needle = MakeNeedle(needleLen, /*upperCase*/ true);
    TString planted = needle;
    for (char& c : planted) {
        c = AsciiToLower(c);
    }
    const TString haystack = MakeMatchAtEndHaystack(haystackLen, planted);
    for (auto _ : state) {
        bool found = Impl(haystack, needle);
        benchmark::DoNotOptimize(found);
    }
    state.SetBytesProcessed(state.iterations() * haystack.size());
}

template <auto Impl>
void BenchManyRows(benchmark::State& state) {
    const size_t needleLen = state.range(0);
    const TString needle = MakeRowsNeedle(needleLen);
    TString planted = needle;
    for (char& c : planted) {
        c = AsciiToUpper(c);
    }
    const TVector<TString> corpus = MakeCorpus(planted);
    for (auto _ : state) {
        size_t hits = 0;
        for (const auto& row : corpus) {
            hits += Impl(row, needle) ? 1 : 0;
        }
        benchmark::DoNotOptimize(hits);
    }
    state.SetItemsProcessed(state.iterations() * corpus.size());
    state.SetBytesProcessed(state.iterations() * CorpusBytes(corpus));
}

} // namespace

#define CONTAINS_ARGS \
    ArgNames({"haystack", "needle"})->ArgsProduct({{8, 32, 256, 4096, 65536}, {1, 8, 32}})

BENCHMARK(BenchNoMatch<AsciiContainsIgnoreCaseScalar>)->CONTAINS_ARGS;
BENCHMARK(BenchNoMatch<AsciiContainsIgnoreCaseMemchr>)->CONTAINS_ARGS;

BENCHMARK(BenchMatchAtEnd<AsciiContainsIgnoreCaseScalar>)->CONTAINS_ARGS;
BENCHMARK(BenchMatchAtEnd<AsciiContainsIgnoreCaseMemchr>)->CONTAINS_ARGS;

#undef CONTAINS_ARGS

#define ROWS_ARGS ArgName("needleLen")->Arg(5)->Arg(10)->Arg(20)

BENCHMARK(BenchManyRows<AsciiContainsIgnoreCaseScalar>)->ROWS_ARGS;
BENCHMARK(BenchManyRows<AsciiContainsIgnoreCaseMemchr>)->ROWS_ARGS;

#undef ROWS_ARGS
