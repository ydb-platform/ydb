#include <ydb/core/erasure/erasure.h>

#include <library/cpp/testing/gbenchmark/benchmark.h>

#include <util/generic/yexception.h>
#include <util/random/mersenne64.h>

namespace NKikimr {
namespace {

constexpr auto CrcMode = TErasureType::CrcModeNone;

TRope CopyToRope(const TString& data) {
    TRope result(TRcBuf::Uninitialized(data.size()));
    if (!data.empty()) {
        memcpy(result.GetContiguousSpanMut().data(), data.data(), data.size());
    }
    return result;
}

template <TErasureType::EErasureSpecies Species>
struct TFixture {
    static constexpr ui32 DataParts = Species == TErasureType::Erasure4Plus2Block ? 4 : 8;
    static constexpr ui32 TotalParts = DataParts + 2;
    using TParts = std::array<TRope, TotalParts>;

    const TErasureType Type{Species};
    TString Data;
    TRope Whole;
    std::array<TString, TotalParts> ExpectedParts;
    TParts Encoded;

    explicit TFixture(size_t size)
        : Data(TString::Uninitialized(size))
    {
        NPrivate::TMersenne64 random(82011);
        for (char& byte : Data) {
            byte = random.GenRand();
        }
        Whole = CopyToRope(Data);

        TDataPartSet reference;
        Type.SplitData(CrcMode, Whole, reference);
        for (ui32 i = 0; i < TotalParts; ++i) {
            ExpectedParts[i] = reference.Parts[i].OwnedString.ConvertToString();
            Encoded[i] = CopyToRope(ExpectedParts[i]);
        }
    }

    TParts Split() const {
        TParts parts;
        ErasureSplit(CrcMode, Type, Whole, parts, nullptr, GetDefaultRcBufAllocator());
        return parts;
    }

    template <ui32 MissingDataParts>
    TParts RestoreInput() const {
        static_assert(MissingDataParts <= 2);
        TParts parts;
        // Missing D0 (or D0 and D1); supply only the required parity parts.
        for (ui32 i = MissingDataParts; i < DataParts + MissingDataParts; ++i) {
            parts[i] = Encoded[i];
        }
        return parts;
    }

    TRope Restore(const TParts& input) const {
        auto parts = input;
        TRope whole;
        ErasureRestore(CrcMode, Type, Data.size(), &whole, parts, 0);
        return whole;
    }

    void CheckSplit(const TParts& parts) const {
        for (ui32 i = 0; i < TotalParts; ++i) {
            Y_ENSURE(parts[i].ConvertToString() == ExpectedParts[i], "Split mismatch in part " << i);
        }
    }

    void CheckWhole(const TRope& whole) const {
        Y_ENSURE(whole.ConvertToString() == Data, "Restored blob mismatch");
    }

    void CheckInputs() const {
        Y_ENSURE(Whole.ConvertToString() == Data, "Split modified its input");
        CheckSplit(Encoded);
    }
};

template <TErasureType::EErasureSpecies Species>
void Split(benchmark::State& state) {
    const TFixture<Species> fixture(state.range(0));
    fixture.CheckSplit(fixture.Split());

    for (auto _ : state) {
        auto parts = fixture.Split();
        benchmark::DoNotOptimize(parts);
        benchmark::ClobberMemory();
    }

    fixture.CheckSplit(fixture.Split());
    fixture.CheckInputs();
    state.SetBytesProcessed(state.iterations() * fixture.Data.size());
    state.SetItemsProcessed(state.iterations());
}

template <TErasureType::EErasureSpecies Species, ui32 MissingDataParts>
void Restore(benchmark::State& state) {
    const TFixture<Species> fixture(state.range(0));
    const auto input = fixture.template RestoreInput<MissingDataParts>();
    fixture.CheckWhole(fixture.Restore(input));

    for (auto _ : state) {
        auto whole = fixture.Restore(input);
        benchmark::DoNotOptimize(whole);
        benchmark::ClobberMemory();
    }

    fixture.CheckWhole(fixture.Restore(input));
    fixture.CheckInputs();
    state.SetBytesProcessed(state.iterations() * fixture.Data.size());
    state.SetItemsProcessed(state.iterations());
}

void Sizes(benchmark::Benchmark* bench) {
    bench->ArgName("bytes")
        ->Arg(4 << 10)
        ->Arg(64 << 10)
        ->Arg(1 << 20)
        ->Arg(4 << 20)
        ->Unit(benchmark::kMicrosecond);
}

BENCHMARK_TEMPLATE(Split, TErasureType::Erasure4Plus2Block)
    ->Name("Block42/Split")->Apply(Sizes);
BENCHMARK_TEMPLATE(Restore, TErasureType::Erasure4Plus2Block, 0)
    ->Name("Block42/RestoreDataWithoutMissingParts")->Apply(Sizes);
BENCHMARK_TEMPLATE(Restore, TErasureType::Erasure4Plus2Block, 1)
    ->Name("Block42/RestoreOneMissingData")->Apply(Sizes);
BENCHMARK_TEMPLATE(Restore, TErasureType::Erasure4Plus2Block, 2)
    ->Name("Block42/RestoreTwoMissingData")->Apply(Sizes);

BENCHMARK_TEMPLATE(Split, TErasureType::Erasure8Plus2Block)
    ->Name("Block82/Split")->Apply(Sizes);
BENCHMARK_TEMPLATE(Restore, TErasureType::Erasure8Plus2Block, 0)
    ->Name("Block82/RestoreDataWithoutMissingParts")->Apply(Sizes);
BENCHMARK_TEMPLATE(Restore, TErasureType::Erasure8Plus2Block, 1)
    ->Name("Block82/RestoreOneMissingData")->Apply(Sizes);
BENCHMARK_TEMPLATE(Restore, TErasureType::Erasure8Plus2Block, 2)
    ->Name("Block82/RestoreTwoMissingData")->Apply(Sizes);

} // namespace
} // namespace NKikimr
