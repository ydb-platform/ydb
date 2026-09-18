#include "production_kernels.h"
#include "counters.h"
#include "isa_dispatch.h"

#include <ydb/core/erasure/erasure_isa.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/testing/benchmark/bench.h>
#include <util/generic/yexception.h>
#include <util/stream/file.h>
#include <util/string/cast.h>
#include <util/system/env.h>
#include <util/system/datetime.h>

#include <array>
#include <bit>
#include <ctime>

namespace NKikimr {
namespace {

ui64 Nanos(clockid_t clock) {
    timespec ts;
    Y_ENSURE(clock_gettime(clock, &ts) == 0);
    return ui64(ts.tv_sec) * 1'000'000'000 + ts.tv_nsec;
}

size_t Parameter(const char* name, size_t value) {
    return FromString<size_t>(GetEnv(name, ToString(value)));
}

struct TOptions {
    const size_t BlobSize = Parameter("ERASURE_BENCH_SIZE", 1 << 20);
    const size_t RingBytes = Parameter("ERASURE_BENCH_RING_BYTES", 0);
    const size_t FragmentBytes = Parameter("ERASURE_BENCH_FRAGMENT_BYTES", 4093);
    const ui64 MinimumNs = Parameter("ERASURE_BENCH_MIN_NS", 50'000'000);
    const TString Level = GetEnv("ERASURE_BENCH_LEVEL", "api");
    const TString Operation = GetEnv("ERASURE_BENCH_OPERATION", "encode");
    const TString Loss = GetEnv("ERASURE_BENCH_LOSS", "DD");
    const TString Output = GetEnv("ERASURE_BENCH_OUTPUT", "parts");
    const TString Availability = GetEnv("ERASURE_BENCH_AVAILABILITY", "all");
    const bool Fragmented = Parameter("ERASURE_BENCH_FRAGMENTED", 0);
    const bool Incremental = Parameter("ERASURE_BENCH_INCREMENTAL", 0);
    const TErasureType::ECrcMode Crc = Parameter("ERASURE_BENCH_CRC", 0)
        ? TErasureType::CrcModeWholePart : TErasureType::CrcModeNone;
};

TRope MakeRope(const TString& value, size_t fragmentBytes) {
    TRope result;
    for (size_t offset = 0; offset < value.size(); offset += fragmentBytes) {
        result.Insert(result.End(), TRope(value.substr(offset, fragmentBytes)));
    }
    return result;
}

class TFixture {
    struct TItem {
        TRope Input;
        TVector<TRope> Original;
        TVector<TRope> Parts;
        TVector<TString> Expected;
        TRope Whole;
        TString Logical;
        std::array<const ui8*, 8> Sources{};
        std::array<ui8*, 10> Slots{};
        std::array<ui8*, 2> Parity{};
    };

    const TOptions Options;
    const TErasureType Erasure;
    TBenchCounters Counters;
    TVector<TItem> Items;
    size_t Next = 0;
    ui16 Missing = 0;
    ui16 Requested = 0;
    size_t Offset = 0;
    size_t Length = 0;
    ui64 Iterations = 0;
    ui64 WallNs = 0;
    ui64 CpuNs = 0;
    ui64 TimerTicks = 0;

public:
    explicit TFixture(TErasureType erasure)
        : Erasure(erasure)
    {
        Y_ENSURE(Options.BlobSize && Options.FragmentBytes);
        Y_ENSURE(Options.Level == "kernel" || Options.Level == "api");
        Y_ENSURE(Options.Operation == "encode" || Options.Operation == "restore"
            || Options.Operation == "fragment" || Options.Operation == "glue");
        Y_ENSURE(Options.Output == "whole" || Options.Output == "parts"
            || Options.Output == "both" || Options.Output == "first");
        Y_ENSURE(Options.Availability == "all" || Options.Availability == "k");
        Y_ENSURE(Options.Level != "kernel" || (Options.Crc == TErasureType::CrcModeNone
            && !Options.Fragmented && !Options.Incremental));
        Y_ENSURE(Options.Level != "kernel" || (Options.Operation != "glue"
            && Options.Output != "whole" && Options.Output != "both"));
        Y_ENSURE(Options.Operation != "fragment" || (Options.Crc == TErasureType::CrcModeNone
            && Options.Output != "whole" && Options.Output != "both"));

        const ui32 k = Erasure.DataParts();
        if (Options.Loss == "D") Missing = 1;
        else if (Options.Loss == "P") Missing = 1 << k;
        else if (Options.Loss == "DD") Missing = 3;
        else if (Options.Loss == "DP") Missing = 1 | (1 << k);
        else if (Options.Loss == "PP") Missing = 3 << k;
        else Missing = FromString<ui16>(Options.Loss);
        Y_ENSURE(Missing < (1 << Erasure.TotalPartCount()) && std::popcount(Missing) <= 2);
        Requested = Options.Output == "whole" ? 0 : Options.Output == "first" ? Missing & -Missing : Missing;
        if (Options.Availability == "k") {
            for (ui32 i = Erasure.TotalPartCount(); std::popcount(Missing) < 2 && i--;) {
                Missing |= 1 << i;
            }
        }
        if (Options.Operation == "glue") Missing = Requested = 0;

        Length = Erasure.PartSize(Options.Crc, Options.BlobSize);
        if (Options.Operation == "fragment") {
            Offset = Length >= 96 ? 32 : 0;
            Length = Min<size_t>(Length - Offset, 4096);
            Length -= Length % 32;
            Y_ENSURE(Length);
        }
        GetErasureIsaL(); // Eager initialization and dispatcher warm-up, outside timing.
        const size_t count = Max<size_t>(1, (Options.RingBytes + Options.BlobSize - 1) / Options.BlobSize);
        Y_ENSURE(count <= 8192, "streaming ring is too large for diagnostic blob size");
        Items.resize(count);
        for (size_t index = 0; index != count; ++index) {
            auto& item = Items[index];
            item.Logical.resize(Options.BlobSize);
            ui64 state = 0x9e3779b97f4a7c15ULL ^ index;
            for (char& byte : item.Logical) {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                byte = state;
            }
            item.Input = MakeRope(item.Logical, Options.Fragmented ? Options.FragmentBytes : Options.BlobSize);
            item.Original.resize(Erasure.TotalPartCount());
            ErasureSplit(Options.Crc, Erasure, item.Input, item.Original, nullptr, GetDefaultRcBufAllocator());
            item.Expected.resize(Erasure.TotalPartCount());
            item.Parts.resize(Erasure.TotalPartCount());
            for (ui32 i = 0; i != Erasure.TotalPartCount(); ++i) {
                item.Expected[i] = item.Original[i].ConvertToString();
                if (Options.Operation == "fragment") {
                    item.Expected[i] = item.Expected[i].substr(Offset, Length);
                    item.Original[i] = MakeRope(item.Expected[i], Options.Fragmented ? Options.FragmentBytes : Length);
                }
                if (Options.Level == "kernel") {
                    item.Parts[i] = TRope(TString(item.Expected[i]));
                    item.Slots[i] = reinterpret_cast<ui8*>(item.Parts[i].GetContiguousSpanMut().data());
                    if (i < k) item.Sources[i] = item.Slots[i];
                    else item.Parity[i - k] = item.Slots[i];
                }
            }
            if (Options.Level == "kernel" && Options.Operation != "encode") {
                for (ui32 i = 0; i != Erasure.TotalPartCount(); ++i) {
                    if ((Missing >> i & 1) && !(Requested >> i & 1)) {
                        item.Parts[i] = {};
                        item.Slots[i] = nullptr;
                    }
                }
            }
            RunOne(item);
            Verify(item);
        }
        // Identical full-ring warm-up also faults all outputs before measurement.
        for (auto& item : Items) RunOne(item);
    }

    ~TFixture() {
        // Check the final state of the complete corpus outside every timed call.
        for (const auto& item : Items) Verify(item);
        const TString output = GetEnv("ERASURE_BENCH_RESULT");
        if (!output || !Iterations) return;
        NJson::TJsonValue row;
        row["species"] = Erasure.ToString();
        row["level"] = Options.Level;
        row["operation"] = Options.Operation;
        row["logical_bytes"] = Options.BlobSize;
        row["part_bytes"] = Erasure.PartSize(Options.Crc, Options.BlobSize);
        row["fragment_offset"] = Offset;
        row["kernel_bytes"] = Length;
        row["missing_mask"] = Missing;
        row["requested_mask"] = Requested;
        row["output"] = Options.Output;
        row["availability"] = Options.Availability;
        row["ring_items"] = Items.size();
        row["ring_logical_bytes"] = Items.size() * Options.BlobSize;
        row["fragmented"] = Options.Fragmented;
        row["incremental"] = Options.Incremental;
        row["crc"] = ui32(Options.Crc);
        row["iterations"] = Iterations;
        row["wall_ns"] = WallNs;
        row["thread_cpu_ns"] = CpuNs;
        row["timer_ticks"] = TimerTicks;
        row["perf_events"] = Counters.Report(Iterations, Options.BlobSize);
        row["isa_l_encode_dispatcher"] = ObservedIsaLEncodeDispatcher();
        row["ns_per_blob"] = double(WallNs) / Iterations;
        row["cpu_ns_per_blob"] = double(CpuNs) / Iterations;
        row["logical_bytes_per_second"] = double(Options.BlobSize) * Iterations * 1e9 / WallNs;
        row["cpu_seconds_per_logical_gib"] = double(CpuNs) / Iterations / Options.BlobSize * (1ULL << 30) / 1e9;
        row["recovered_bytes_per_blob"] = Options.Operation == "encode" || Options.Operation == "glue"
            ? 0 : Length * std::popcount(ui16(Requested | ((Options.Output == "whole" || Options.Output == "both")
                ? Missing & ((1 << Erasure.DataParts()) - 1) : 0)));
        row["physical_to_logical_ratio"] = double(Erasure.TotalPartCount()) * Erasure.PartSize(Options.Crc, Options.BlobSize)
            / Options.BlobSize;
        row["allocated_bytes"] = NJson::TJsonValue();
        row["copy_bytes"] = NJson::TJsonValue();
        row["zero_bytes"] = NJson::TJsonValue();
        TFileOutput file(output);
        file << row << Endl;
    }

    void Run(NBench::NCpu::TParams& params) {
        Counters.Start();
        const ui64 cpu = Nanos(CLOCK_THREAD_CPUTIME_ID);
        const ui64 wall = Nanos(CLOCK_MONOTONIC_RAW);
        const ui64 ticks = GetCycleCount();
        // The framework starts its budget before constructing this fixture.
        // Ensure a real steady-state interval even when initialization exhausts
        // that outer budget. Check the clock only once per batch, not per blob.
        const size_t batch = Iterations ? params.Iterations() : Max<size_t>(64, params.Iterations());
        size_t completed = 0;
        do {
            for (size_t i = 0; i != batch; ++i) {
                RunOne(Items[Next]);
                if (++Next == Items.size()) Next = 0;
            }
            completed += batch;
        } while (!Iterations && Nanos(CLOCK_MONOTONIC_RAW) - wall < Options.MinimumNs);
        TimerTicks += GetCycleCount() - ticks;
        WallNs += Nanos(CLOCK_MONOTONIC_RAW) - wall;
        CpuNs += Nanos(CLOCK_THREAD_CPUTIME_ID) - cpu;
        Counters.Stop();
        Iterations += completed;
    }

private:
    void RunOne(TItem& item) {
        if (Options.Level == "kernel") {
            if (Options.Operation == "encode") {
                if (Erasure.DataParts() == 4) ErasureSplitBlock42(item.Parts, 0, Length);
                else GetErasureIsaL().Encode(Length, item.Sources.data(), item.Parity.data());
            } else if (Erasure.DataParts() == 4) {
                ErasureRestoreBlock42ForBenchmark(item.Parts, Missing);
            } else {
                GetErasureIsaL().Restore(Length, Missing, Requested, item.Slots.data());
            }
            NBench::Clobber();
        } else if (Options.Operation == "encode") {
            for (auto& part : item.Parts) part = {};
            auto context = TErasureSplitContext::Init(256 << 10);
            while (!ErasureSplit(Options.Crc, Erasure, item.Input, item.Parts,
                Options.Incremental ? &context : nullptr, GetDefaultRcBufAllocator())) {}
        } else {
            item.Parts = item.Original;
            for (ui32 i = 0; i != Erasure.TotalPartCount(); ++i) {
                if (Missing >> i & 1) item.Parts[i] = {};
            }
            item.Whole = {};
            TRope* whole = Options.Output == "whole" || Options.Output == "both"
                || Options.Operation == "glue" ? &item.Whole : nullptr;
            ErasureRestore(Options.Crc, Erasure, Options.BlobSize, whole, item.Parts,
                Options.Output == "whole" ? 0 : Requested, Offset, Options.Operation == "fragment");
        }
    }

    void Verify(const TItem& item) const {
        if (item.Whole) Y_ENSURE(item.Whole.ConvertToString() == item.Logical, "whole mismatch");
        for (ui32 i = 0; i != Erasure.TotalPartCount(); ++i) {
            if (Options.Operation == "encode" || (Requested >> i & 1)) {
                Y_ENSURE(item.Parts[i].ConvertToString() == item.Expected[i], "part mismatch: " << i);
            }
        }
    }
};

} // namespace
} // namespace NKikimr

Y_CPU_BENCHMARK(Block42, params) {
    thread_local NKikimr::TFixture fixture(NKikimr::TErasureType::Erasure4Plus2Block);
    fixture.Run(params);
}

Y_CPU_BENCHMARK(Block82, params) {
    thread_local NKikimr::TFixture fixture(NKikimr::TErasureType::Erasure8Plus2Block);
    fixture.Run(params);
}
