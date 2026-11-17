#include <ydb/core/formats/arrow/accessor/dictionary/accessor.h>

#include <benchmark/benchmark.h>

#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_binary.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/array/builder_primitive.h>
#include <contrib/libs/apache/arrow/cpp/src/arrow/compute/api.h>

#include <array>

namespace {

constexpr ui32 DictionarySize = 1000;
constexpr ui32 RecordsCount = 1000000;

struct TStqAgentBucket {
    ui32 MaxLength;
    ui32 RowsCount;
    ui32 RowsBytes;
    ui32 ValuesCount;
    ui32 ValuesBytes;
};

constexpr std::array StqAgentBuckets = {
    TStqAgentBucket{ 64, 453090, 10345256, 11387, 584220 },
    TStqAgentBucket{ 128, 485634, 44203212, 36841, 3327006 },
    TStqAgentBucket{ 256, 52981, 8373617, 27126, 4569321 },
    TStqAgentBucket{ 512, 5381, 1862034, 4844, 1676989 },
    TStqAgentBucket{ 1024, 1851, 1310992, 1848, 1309204 },
    TStqAgentBucket{ 60000, 1063, 2264121, 1063, 2264121 },
};

template <typename TBuilder, typename TValueGenerator, typename TPositionGenerator>
std::shared_ptr<NKikimr::NArrow::NAccessor::TDictionaryArray> BuildDictionary(
    TValueGenerator&& valueGenerator, TPositionGenerator&& positionGenerator) {
    TBuilder dictionaryBuilder;
    NKikimr::NArrow::TStatusValidator::Validate(dictionaryBuilder.Reserve(DictionarySize));
    for (ui32 i = 0; i < DictionarySize; ++i) {
        NKikimr::NArrow::TStatusValidator::Validate(dictionaryBuilder.Append(valueGenerator(i)));
    }
    std::shared_ptr<arrow::Array> dictionary;
    NKikimr::NArrow::TStatusValidator::Validate(dictionaryBuilder.Finish(&dictionary));

    arrow::UInt16Builder positionsBuilder;
    NKikimr::NArrow::TStatusValidator::Validate(positionsBuilder.Reserve(RecordsCount));
    for (ui32 i = 0; i < RecordsCount; ++i) {
        NKikimr::NArrow::TStatusValidator::Validate(positionsBuilder.Append(positionGenerator(i)));
    }
    std::shared_ptr<arrow::Array> positions;
    NKikimr::NArrow::TStatusValidator::Validate(positionsBuilder.Finish(&positions));

    return std::make_shared<NKikimr::NArrow::NAccessor::TDictionaryArray>(dictionary, positions);
}

std::shared_ptr<NKikimr::NArrow::NAccessor::TDictionaryArray> BuildStringDictionary() {
    return BuildDictionary<arrow::StringBuilder>(
        [](const ui32 index) {
            return index ? arrow::util::string_view("x") : arrow::util::string_view("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        },
        [](const ui32 /*position*/) { return 0; });
}

std::shared_ptr<NKikimr::NArrow::NAccessor::TDictionaryArray> BuildStqAgentDictionary() {
    std::vector<ui32> valueLengths;
    std::vector<ui32> positions;
    valueLengths.reserve(83109);
    positions.reserve(RecordsCount);
    for (const auto& bucket : StqAgentBuckets) {
        const ui32 minLength = Min(bucket.ValuesBytes / bucket.ValuesCount, bucket.RowsBytes / bucket.RowsCount);
        const ui32 valuesStart = valueLengths.size();
        valueLengths.insert(valueLengths.end(), bucket.ValuesCount, minLength);
        ui32 bytesRemaining = bucket.ValuesBytes - minLength * bucket.ValuesCount;
        for (ui32 i = 0; bytesRemaining; ++i) {
            const ui32 increment = Min(bucket.MaxLength - minLength, bytesRemaining);
            valueLengths[valuesStart + i] += increment;
            bytesRemaining -= increment;
        }
        AFL_VERIFY(!bytesRemaining);

        std::vector<ui32> shortValues;
        std::vector<ui32> longValues;
        for (ui32 i = 0; i < bucket.ValuesCount; ++i) {
            if (valueLengths[valuesStart + i] == minLength) {
                shortValues.emplace_back(valuesStart + i);
            } else if (valueLengths[valuesStart + i] == valueLengths[valuesStart]) {
                longValues.emplace_back(valuesStart + i);
            }
        }
        const ui64 extraRowBytes = bucket.RowsBytes - minLength * bucket.RowsCount;
        const ui32 longLength = valueLengths[valuesStart];
        const ui32 longRowsCount = longLength == minLength ? 0 : extraRowBytes / (longLength - minLength);
        ui32 shortCursor = 0;
        ui32 longCursor = 0;
        for (ui32 i = 0; i < bucket.RowsCount; ++i) {
            if (ui64(i + 1) * longRowsCount / bucket.RowsCount > ui64(i) * longRowsCount / bucket.RowsCount) {
                positions.emplace_back(longValues[longCursor++ % longValues.size()]);
            } else {
                positions.emplace_back(shortValues[shortCursor++ % shortValues.size()]);
            }
        }
    }
    AFL_VERIFY(valueLengths.size() == 83109);
    AFL_VERIFY(positions.size() == RecordsCount);

    arrow::StringBuilder dictionaryBuilder;
    NKikimr::NArrow::TStatusValidator::Validate(dictionaryBuilder.Reserve(valueLengths.size()));
    for (const ui32 length : valueLengths) {
        const TString value(length, 'x');
        NKikimr::NArrow::TStatusValidator::Validate(dictionaryBuilder.Append(value.data(), value.size()));
    }
    std::shared_ptr<arrow::Array> dictionary;
    NKikimr::NArrow::TStatusValidator::Validate(dictionaryBuilder.Finish(&dictionary));

    arrow::UInt32Builder positionsBuilder;
    NKikimr::NArrow::TStatusValidator::Validate(positionsBuilder.Reserve(positions.size()));
    for (const ui32 position : positions) {
        NKikimr::NArrow::TStatusValidator::Validate(positionsBuilder.Append(position));
    }
    std::shared_ptr<arrow::Array> indices;
    NKikimr::NArrow::TStatusValidator::Validate(positionsBuilder.Finish(&indices));
    return std::make_shared<NKikimr::NArrow::NAccessor::TDictionaryArray>(dictionary, indices);
}

std::shared_ptr<arrow::Array> TakeWithExactBinaryPreallocation(
    const NKikimr::NArrow::NAccessor::TDictionaryArray& dictionary) {
    auto options = arrow::compute::TakeOptions::Defaults();
    options.preallocate_exact_binary_data = true;
    return NKikimr::NArrow::TStatusValidator::GetValid(
        arrow::compute::Take(*dictionary.GetDictionary(), *dictionary.GetPositions(), options));
}

std::shared_ptr<arrow::Array> ExpandBaseline(const NKikimr::NArrow::NAccessor::TDictionaryArray& dictionary) {
    std::unique_ptr<arrow::ArrayBuilder> builder = NKikimr::NArrow::MakeBuilder(dictionary.GetDictionary()->type());
    AFL_VERIFY(NKikimr::NArrow::SwitchType(dictionary.GetDictionary()->type()->id(), [&](const auto typeVariant) {
        const auto* values = typeVariant.CastArray(dictionary.GetDictionary().get());
        auto* valuesBuilder = typeVariant.CastBuilder(builder.get());
        if constexpr (typeVariant.IsAppropriate) {
            AFL_VERIFY(NKikimr::NArrow::SwitchType(dictionary.GetPositions()->type()->id(), [&](const auto positionsType) {
                const auto* positions = positionsType.CastArray(dictionary.GetPositions().get());
                if constexpr (positionsType.IsIndexType()) {
                    for (int64_t i = 0; i < positions->length(); ++i) {
                        if (positions->IsNull(i)) {
                            NKikimr::NArrow::TStatusValidator::Validate(valuesBuilder->AppendNull());
                        } else {
                            NKikimr::NArrow::TStatusValidator::Validate(
                                valuesBuilder->Append(typeVariant.GetValue(*values, positions->Value(i))));
                        }
                    }
                    return true;
                }
                return false;
            }));
            return true;
        }
        return false;
    }));
    return NKikimr::NArrow::FinishBuilder(std::move(builder));
}

void BenchBaselineBool(benchmark::State& state) {
    const auto dictionary = BuildDictionary<arrow::BooleanBuilder>(
        [](const ui32 index) { return index % 2 == 0; },
        [](const ui32 position) { return position % DictionarySize; });
    for (auto _ : state) {
        const auto data = ExpandBaseline(*dictionary);
        benchmark::DoNotOptimize(data.get());
    }
    state.SetItemsProcessed(state.iterations() * RecordsCount);
}

void BenchTakeBool(benchmark::State& state) {
    const auto dictionary = BuildDictionary<arrow::BooleanBuilder>(
        [](const ui32 index) { return index % 2 == 0; },
        [](const ui32 position) { return position % DictionarySize; });
    for (auto _ : state) {
        const auto data = dictionary->GetLocalData(std::nullopt, 0).GetArray();
        benchmark::DoNotOptimize(data.get());
    }
    state.SetItemsProcessed(state.iterations() * RecordsCount);
}

void BenchBaselineUInt32(benchmark::State& state) {
    const auto dictionary = BuildDictionary<arrow::UInt32Builder>(
        [](const ui32 index) { return index; },
        [](const ui32 position) { return position % DictionarySize; });
    for (auto _ : state) {
        const auto data = ExpandBaseline(*dictionary);
        benchmark::DoNotOptimize(data.get());
    }
    state.SetItemsProcessed(state.iterations() * RecordsCount);
}

void BenchTakeUInt32(benchmark::State& state) {
    const auto dictionary = BuildDictionary<arrow::UInt32Builder>(
        [](const ui32 index) { return index; },
        [](const ui32 position) { return position % DictionarySize; });
    for (auto _ : state) {
        const auto data = dictionary->GetLocalData(std::nullopt, 0).GetArray();
        benchmark::DoNotOptimize(data.get());
    }
    state.SetItemsProcessed(state.iterations() * RecordsCount);
}

void BenchTakeStringHeuristic(benchmark::State& state) {
    const auto dictionary = BuildStringDictionary();
    for (auto _ : state) {
        const auto data = dictionary->GetLocalData(std::nullopt, 0).GetArray();
        benchmark::DoNotOptimize(data.get());
    }
    state.SetItemsProcessed(state.iterations() * RecordsCount);
}

void BenchTakeStringExactPreallocation(benchmark::State& state) {
    const auto dictionary = BuildStringDictionary();
    AFL_VERIFY(dictionary->GetLocalData(std::nullopt, 0).GetArray()->Equals(
        *TakeWithExactBinaryPreallocation(*dictionary)));
    for (auto _ : state) {
        const auto data = TakeWithExactBinaryPreallocation(*dictionary);
        benchmark::DoNotOptimize(data.get());
    }
    state.SetItemsProcessed(state.iterations() * RecordsCount);
}

void BenchTakeStqAgentMessageHeuristic(benchmark::State& state) {
    const auto dictionary = BuildStqAgentDictionary();
    for (auto _ : state) {
        const auto data = dictionary->GetLocalData(std::nullopt, 0).GetArray();
        benchmark::DoNotOptimize(data.get());
    }
    state.SetItemsProcessed(state.iterations() * RecordsCount);
}

void BenchTakeStqAgentMessageExactPreallocation(benchmark::State& state) {
    const auto dictionary = BuildStqAgentDictionary();
    AFL_VERIFY(dictionary->GetLocalData(std::nullopt, 0).GetArray()->Equals(
        *TakeWithExactBinaryPreallocation(*dictionary)));
    for (auto _ : state) {
        const auto data = TakeWithExactBinaryPreallocation(*dictionary);
        benchmark::DoNotOptimize(data.get());
    }
    state.SetItemsProcessed(state.iterations() * RecordsCount);
}

}   // namespace

BENCHMARK(BenchBaselineBool);
BENCHMARK(BenchTakeBool);
BENCHMARK(BenchBaselineUInt32);
BENCHMARK(BenchTakeUInt32);
BENCHMARK(BenchTakeStringHeuristic);
BENCHMARK(BenchTakeStringExactPreallocation);
BENCHMARK(BenchTakeStqAgentMessageHeuristic);
BENCHMARK(BenchTakeStqAgentMessageExactPreallocation);
