#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_partlayout.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <library/cpp/json/json_value.h>
#include <util/generic/strbuf.h>
#include <util/generic/yexception.h>
#include <util/stream/output.h>
#include <util/string/cast.h>
#include <util/system/datetime.h>

#include <array>
#include <ctime>
#include <type_traits>

namespace NKikimr {
namespace {

using TLayout = TSubgroupPartLayout;
constexpr size_t CorpusSize = 256;

// Escape the complete aggregate through memory: the generic register operand
// helper cannot represent a 16-byte bitmap with this Clang toolchain.
Y_FORCE_INLINE void EscapeLayout(TLayout& layout) {
    asm volatile("" : "+m"(layout) : : "memory");
}

struct TCase {
    TLayout Layout;
    TLayout Other;
    TLayout Boundary;
    std::array<ui32, 10> Rows{};
    ui32 BoundaryBits = 0;
    ui32 Mask = 0;
};

ui64 Nanos(clockid_t id) {
    timespec time;
    Y_ENSURE(clock_gettime(id, &time) == 0);
    return ui64(time.tv_sec) * 1'000'000'000 + time.tv_nsec;
}

ui32 MatchingOracle(const TCase& test, ui32 parts, ui32 disks) {
    std::array<int, 12> owner;
    owner.fill(-1);
    const auto augment = [&](auto&& self, ui32 part, ui32& visited) -> bool {
        for (ui32 disk = 0; disk < disks; ++disk) {
            if (!(test.Rows[part] >> disk & 1) || (visited >> disk & 1)) continue;
            visited |= 1u << disk;
            if (owner[disk] < 0 || self(self, owner[disk], visited)) {
                owner[disk] = part;
                return true;
            }
        }
        return false;
    };
    ui32 count = 0;
    for (ui32 part = 0; part < parts; ++part) {
        ui32 visited = 0;
        count += augment(augment, part, visited);
    }
    return count;
}

std::array<TCase, CorpusSize> MakeCorpus(const TBlobStorageGroupType& type) {
    std::array<TCase, CorpusSize> corpus;
    ui64 state = 0x820420128ULL;
    const auto random = [&] {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        return state;
    };
    const ui32 parts = type.TotalPartCount(), disks = type.BlobSubgroupSize();
    for (auto& test : corpus) {
        for (ui32 part = 0; part < parts; ++part) {
            for (ui32 disk : {part, parts, parts + 1}) {
                if (random() & 1) {
                    test.Layout.AddItem(disk, part, type);
                    test.Rows[part] |= 1u << disk;
                }
                if (random() & 1) test.Other.AddItem(disk, part, type);
            }
        }
        // Arbitrary cells are valid for primitive bitmap operations. This
        // boundary-only layout is never fed to the valid-placement matcher.
        test.BoundaryBits = (ui32(random()) & ((1u << disks) - 1)) | (1u << 3) | (1u << 4);
        for (ui32 disk = 0; disk < disks; ++disk) {
            if (test.BoundaryBits >> disk & 1) test.Boundary.AddItem(disk, 5, type);
        }
        test.Mask = ui32(random()) & ((1u << disks) - 1);
        Y_ENSURE(test.Boundary.GetDisksWithPart(5) == test.BoundaryBits);
        Y_ENSURE(test.Layout.CountEffectiveReplicas(type) == MatchingOracle(test, parts, disks));
        auto masked = test.Boundary;
        masked.Mask(5, test.Mask);
        Y_ENSURE(masked.GetDisksWithPart(5) == (test.BoundaryBits & test.Mask));
        auto merged = test.Layout;
        merged.Merge(test.Other, type);
        for (ui32 part = 0; part < parts; ++part) {
            Y_ENSURE(merged.GetDisksWithPart(part) == (test.Rows[part] | test.Other.GetDisksWithPart(part)));
        }
    }
    return corpus;
}

template <typename TOperation>
NJson::TJsonValue Measure(const char* name, size_t iterations,
        const std::array<TCase, CorpusSize>& corpus, TOperation&& operation) {
    for (size_t i = 0; i < CorpusSize * 4; ++i) operation(corpus[i % CorpusSize]);
    const ui64 cpu = Nanos(CLOCK_THREAD_CPUTIME_ID);
    const ui64 wall = Nanos(CLOCK_MONOTONIC_RAW);
    const ui64 ticks = GetCycleCount();
    for (size_t i = 0; i < iterations; ++i) operation(corpus[i % CorpusSize]);
    const ui64 elapsedTicks = GetCycleCount() - ticks;
    const ui64 elapsed = Nanos(CLOCK_MONOTONIC_RAW) - wall;
    const ui64 elapsedCpu = Nanos(CLOCK_THREAD_CPUTIME_ID) - cpu;
    NJson::TJsonValue row;
    row["operation"] = name;
    row["iterations"] = iterations;
    row["wall_ns"] = elapsed;
    row["thread_cpu_ns"] = elapsedCpu;
    row["timer_ticks"] = elapsedTicks;
    row["ns_per_operation"] = double(elapsed) / iterations;
    row["cpu_ns_per_operation"] = double(elapsedCpu) / iterations;
    return row;
}

void Run(bool wide, size_t iterations) {
    const TBlobStorageGroupType type(wide ? TErasureType::Erasure8Plus2Block : TErasureType::Erasure4Plus2Block);
    Y_ENSURE(!wide || sizeof(TLayout) * 8 >= 120, "the baseline layout cannot represent block-8-2");
    const auto corpus = MakeCorpus(type);
    NJson::TJsonValue report;
    report["species"] = type.ToString();
    report["sizeof_layout"] = sizeof(TLayout);
    report["alignof_layout"] = alignof(TLayout);
    report["sizeof_ingress"] = sizeof(TIngress);
    report["sizeof_parts_vector"] = sizeof(NMatrix::TVectorType);
    report["trivially_destructible_layout"] = std::is_trivially_destructible_v<TLayout>;
    report["corpus_cases"] = CorpusSize;
    report["corpus_seed"] = ui64(0x820420128ULL);
    report["primitive_boundary_row"] = 5;
    report["heap_storage_evidence"] = "source inspection: fixed inline fields; no allocation counter is claimed";
    auto& rows = report["measurements"];
    rows.AppendValue(Measure("CopyControl", iterations, corpus, [](const TCase& test) {
        auto layout = test.Layout;
        EscapeLayout(layout);
    }));
    rows.AppendValue(Measure("AddItem", iterations, corpus, [&](const TCase& test) {
        auto layout = test.Layout;
        layout.AddItem(type.TotalPartCount(), 5, type);
        EscapeLayout(layout);
    }));
    rows.AppendValue(Measure("ClearItem", iterations, corpus, [&](const TCase& test) {
        auto layout = test.Layout;
        layout.ClearItem(type.TotalPartCount(), 5, type);
        EscapeLayout(layout);
    }));
    rows.AppendValue(Measure("GetRow5", iterations, corpus, [](const TCase& test) {
        auto value = test.Boundary.GetDisksWithPart(5);
        DoNotOptimizeAway(value);
    }));
    rows.AppendValue(Measure("MaskRow5", iterations, corpus, [](const TCase& test) {
        auto layout = test.Boundary;
        layout.Mask(5, test.Mask);
        EscapeLayout(layout);
    }));
    rows.AppendValue(Measure("Merge", iterations, corpus, [&](const TCase& test) {
        auto layout = test.Layout;
        layout.Merge(test.Other, type);
        EscapeLayout(layout);
    }));
    rows.AppendValue(Measure("CountDistinctParts", iterations, corpus, [&](const TCase& test) {
        auto value = test.Layout.CountDistinctParts(type);
        DoNotOptimizeAway(value);
    }));
    rows.AppendValue(Measure("CountEffectiveReplicas", iterations, corpus, [&](const TCase& test) {
        auto value = test.Layout.CountEffectiveReplicas(type);
        DoNotOptimizeAway(value);
    }));
    // Verify again after all measurements, outside timed regions.
    for (const auto& test : corpus) {
        Y_ENSURE(test.Layout.CountEffectiveReplicas(type)
            == MatchingOracle(test, type.TotalPartCount(), type.BlobSubgroupSize()));
    }
    Cout << report << Endl;
}

} // namespace
} // namespace NKikimr

int main(int argc, char** argv) {
    try {
        Y_ENSURE(argc >= 2 && (TStringBuf(argv[1]) == "42" || TStringBuf(argv[1]) == "82"),
            "usage: layout_bench 42|82 [iterations]");
        const size_t iterations = argc > 2 ? FromString<size_t>(argv[2]) : 1 << 20;
        Y_ENSURE(iterations >= 256);
        NKikimr::Run(TStringBuf(argv[1]) == "82", iterations);
        return 0;
    } catch (...) {
        Cerr << CurrentExceptionMessage() << Endl;
        return 1;
    }
}
