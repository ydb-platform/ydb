#include <library/cpp/containers/comptrie/pattern_searcher.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/stream/str.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <utility>

namespace {

using TBuilder = TCompactPatternSearcherBuilder<char, ui32>;
using TSearcher = TCompactPatternSearcher<char, ui32>;
using TPatternModel = TMap<TString, ui32>;
using TMatchSet = TSet<std::pair<ui64, ui32>>;

constexpr size_t MaxPatterns = 48;
constexpr size_t MaxSamples = 32;
constexpr size_t MaxPatternSize = 24;
constexpr size_t MaxSampleSize = 96;

TString MakeBytes(FuzzedDataProvider& fdp, size_t maxSize) {
    auto bytes = fdp.ConsumeRandomLengthString(maxSize);
    TString value(bytes.data(), bytes.size());
    for (char& ch : value) {
        if (ch == '\0') {
            ch = '\1';
        }
    }
    return value;
}

TMatchSet SearchNaively(const TPatternModel& patterns, const TString& sample) {
    TMatchSet expected;
    for (const auto& [pattern, value] : patterns) {
        if (pattern.empty() || pattern.size() > sample.size()) {
            continue;
        }

        for (size_t start = 0; start + pattern.size() <= sample.size(); ++start) {
            if (std::equal(pattern.begin(), pattern.end(), sample.begin() + start)) {
                expected.insert({start + pattern.size() - 1, value});
            }
        }
    }
    return expected;
}

void ValidateSearcher(const TSearcher& searcher, const TPatternModel& patterns, const TVector<TString>& samples) {
    TMap<ui32, TString> valueToPattern;
    for (const auto& [pattern, value] : patterns) {
        valueToPattern[value] = pattern;
    }

    for (const TString& sample : samples) {
        TMatchSet actual;
        for (const auto& match : searcher.SearchMatches(sample.data(), sample.size())) {
            actual.insert({match.End, match.Data});

            const auto patternIt = valueToPattern.find(match.Data);
            Y_ENSURE(patternIt != valueToPattern.end());
            const TString& pattern = patternIt->second;
            Y_ENSURE(match.End + 1 >= pattern.size());
            Y_ENSURE(match.End < sample.size());
            Y_ENSURE(sample.substr(match.End + 1 - pattern.size(), pattern.size()) == pattern);
        }

        Y_ENSURE(actual == SearchNaively(patterns, sample));
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    TBuilder builder;
    TPatternModel patterns;
    TVector<TString> knownPatterns;
    TVector<TString> samples;

    const size_t patternCount = fdp.ConsumeIntegralInRange<size_t>(0, MaxPatterns);
    for (size_t i = 0; i < patternCount; ++i) {
        TString pattern = (fdp.ConsumeBool() && !knownPatterns.empty())
            ? knownPatterns[fdp.ConsumeIntegralInRange<size_t>(0, knownPatterns.size() - 1)]
            : MakeBytes(fdp, MaxPatternSize);

        if (pattern.empty()) {
            samples.push_back(MakeBytes(fdp, MaxSampleSize));
            continue;
        }

        const ui32 value = static_cast<ui32>(i);
        const auto [patternIt, expectedNew] = patterns.emplace(pattern, value);
        const bool added = builder.Add(pattern.data(), pattern.size(), value);
        Y_ENSURE(added == expectedNew);
        if (added) {
            knownPatterns.push_back(pattern);
        }

        ui32 foundValue = Max<ui32>();
        Y_ENSURE(builder.Find(pattern.data(), pattern.size(), &foundValue));
        Y_ENSURE(foundValue == patternIt->second);

        if (fdp.ConsumeBool()) {
            samples.push_back(pattern);
        }
        if (fdp.ConsumeBool()) {
            samples.push_back(MakeBytes(fdp, MaxSampleSize) + pattern + MakeBytes(fdp, MaxSampleSize));
        }
    }

    const size_t sampleCount = fdp.ConsumeIntegralInRange<size_t>(0, MaxSamples);
    for (size_t i = 0; i < sampleCount; ++i) {
        samples.push_back(MakeBytes(fdp, MaxSampleSize));
    }

    TBufferOutput serialized;
    builder.Save(serialized);

    TSearcher searcher(serialized.Buffer().Data(), serialized.Buffer().Size());
    ValidateSearcher(searcher, patterns, samples);

    const TBlob blob = builder.Save();
    TSearcher blobSearcher(blob);
    ValidateSearcher(blobSearcher, patterns, samples);

    return 0;
}
