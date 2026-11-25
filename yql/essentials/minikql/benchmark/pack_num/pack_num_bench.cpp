#include "pack.h"

#include <yql/essentials/minikql/pack_num.h>

#include <benchmark/benchmark.h>
#include <library/cpp/packedtypes/longs.h>

#include <util/generic/xrange.h>
#include <util/random/random.h>

namespace {

template <ui32 UPPER, size_t MAX_BYTE_SIZE = (1 << 15) - (1 << 10)>
struct TSamples32 {
    constexpr static size_t COUNT = MAX_BYTE_SIZE / sizeof(ui32);
    TSamples32() {
        for (size_t i : xrange(COUNT)) {
            Data[i] = RandomNumber<ui32>(UPPER);
        }
    }

    ui32 Data[COUNT]; // NOLINT(modernize-avoid-c-arrays)
};

template <ui64 UPPER, size_t MAX_BYTE_SIZE = (1 << 15) - (1 << 10)>
struct TSamples64 {
    constexpr static size_t COUNT = MAX_BYTE_SIZE / sizeof(ui64);
    TSamples64() {
        for (size_t i : xrange(COUNT)) {
            Data[i] = RandomNumber<ui64>(UPPER);
        }
    }

    ui64 Data[COUNT]; // NOLINT(modernize-avoid-c-arrays)
};

template <ui32 UPPER, typename TCoder, size_t MAX_BYTE_SIZE = (1 << 15) - (1 << 10)>
struct TCodedData32 {
    constexpr static size_t BYTES_PER_NUM = sizeof(ui32) + 2;
    constexpr static size_t COUNT = MAX_BYTE_SIZE / BYTES_PER_NUM;
    TCodedData32() {
        for (size_t i : xrange(COUNT)) {
            Length[i] = TCoder()(RandomNumber<ui32>(UPPER), Data + i * BYTES_PER_NUM);
        }
    }

    char Data[MAX_BYTE_SIZE + BYTES_PER_NUM]; // NOLINT(modernize-avoid-c-arrays)
    size_t Length[COUNT];                     // NOLINT(modernize-avoid-c-arrays)
};

template <ui64 UPPER, typename TCoder, size_t MAX_BYTE_SIZE = (1 << 15) - (1 << 10)>
struct TCodedData64 {
    constexpr static size_t BYTES_PER_NUM = sizeof(ui64) + 2;
    constexpr static size_t COUNT = MAX_BYTE_SIZE / BYTES_PER_NUM;
    TCodedData64() {
        for (size_t i : xrange(COUNT)) {
            Length[i] = TCoder()(RandomNumber<ui64>(UPPER), Data + i * BYTES_PER_NUM);
        }
    }

    char Data[MAX_BYTE_SIZE + BYTES_PER_NUM]; // NOLINT(modernize-avoid-c-arrays)
    size_t Length[COUNT];                     // NOLINT(modernize-avoid-c-arrays)
};

struct TKikimrCoder32 {
    size_t operator()(ui32 num, char* buf) const {
        return NKikimr::Pack32(num, buf);
    }
};

struct TKikimrCoder64 {
    size_t operator()(ui64 num, char* buf) const {
        return NKikimr::Pack64(num, buf);
    }
};

struct TPackedTypesCoder32 {
    size_t operator()(ui32 num, char* buf) const {
        return Pack32(num, buf) - buf;
    }
};

struct TPackedTypesCoder64 {
    size_t operator()(ui64 num, char* buf) const {
        return Pack64(num, buf) - buf;
    }
};

struct TDictUtilsCoder32 {
    size_t operator()(ui32 num, char* buf) const {
        return PackU32(num, buf);
    }
};

struct TDictUtilsCoder64 {
    size_t operator()(ui64 num, char* buf) const {
        return PackU64(num, buf);
    }
};

template <ui32 LIMIT>
void Write32_Kikimr(benchmark::State& state) {
    char buffer[sizeof(ui32) + 1]; // NOLINT(modernize-avoid-c-arrays)
    benchmark::DoNotOptimize(static_cast<char*>(buffer));
    const TSamples32<LIMIT> data;
    size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(NKikimr::Pack32(data.Data[i % data.COUNT], buffer));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui32 LIMIT>
void Write32_PackedTypes(benchmark::State& state) {
    char buffer[sizeof(ui32) + 1]; // NOLINT(modernize-avoid-c-arrays)
    benchmark::DoNotOptimize(static_cast<char*>(buffer));
    const TSamples32<LIMIT> data;
    size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(Pack32(data.Data[i % data.COUNT], buffer));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui32 LIMIT>
void Write32_DictUtils(benchmark::State& state) {
    char buffer[sizeof(ui32) + 1]; // NOLINT(modernize-avoid-c-arrays)
    benchmark::DoNotOptimize(static_cast<char*>(buffer));
    const TSamples32<LIMIT> data;
    size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(PackU32(data.Data[i % data.COUNT], buffer));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui32 LIMIT>
void Read32_KikimrLong(benchmark::State& state) {
    ui32 num = 0;
    benchmark::DoNotOptimize(&num);
    const TCodedData32<LIMIT, TKikimrCoder32> data;
    size_t i = 0;
    for (auto _ : state) {
        const size_t pos = i % data.COUNT;
        benchmark::DoNotOptimize(NKikimr::Unpack32(data.Data + pos * data.BYTES_PER_NUM, data.BYTES_PER_NUM, num));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui32 LIMIT>
void Read32_KikimrShort(benchmark::State& state) {
    ui32 num = 0;
    benchmark::DoNotOptimize(&num);
    const TCodedData32<LIMIT, TKikimrCoder32> data;
    size_t i = 0;
    for (auto _ : state) {
        const size_t pos = i % data.COUNT;
        benchmark::DoNotOptimize(NKikimr::Unpack32(data.Data + pos * data.BYTES_PER_NUM, data.Length[pos], num));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui32 LIMIT>
void Read32_PackedTypes(benchmark::State& state) {
    ui32 num = 0;
    benchmark::DoNotOptimize(&num);
    const TCodedData32<LIMIT, TPackedTypesCoder32> data;
    size_t i = 0;
    for (auto _ : state) {
        const size_t pos = i % data.COUNT;
        benchmark::DoNotOptimize(Unpack32(num, data.Data + pos * data.BYTES_PER_NUM));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui32 LIMIT>
void Read32_DictUtils(benchmark::State& state) {
    ui32 num = 0;
    benchmark::DoNotOptimize(&num);
    const TCodedData32<LIMIT, TDictUtilsCoder32> data;
    size_t i = 0;
    for (auto _ : state) {
        const size_t pos = i % data.COUNT;
        benchmark::DoNotOptimize(UnpackU32(&num, data.Data + pos * data.BYTES_PER_NUM));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui64 LIMIT>
void Write64_Kikimr(benchmark::State& state) {
    char buffer[sizeof(ui64) + 1]; // NOLINT(modernize-avoid-c-arrays)
    benchmark::DoNotOptimize(static_cast<char*>(buffer));
    const TSamples64<LIMIT> data;
    size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(NKikimr::Pack64(data.Data[i % data.COUNT], buffer));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui64 LIMIT>
void Write64_PackedTypes(benchmark::State& state) {
    char buffer[sizeof(ui64) + 1]; // NOLINT(modernize-avoid-c-arrays)
    benchmark::DoNotOptimize(static_cast<char*>(buffer));
    const TSamples64<LIMIT> data;
    size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(Pack64(data.Data[i % data.COUNT], buffer));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui64 LIMIT>
void Write64_DictUtils(benchmark::State& state) {
    char buffer[sizeof(ui64) + 1]; // NOLINT(modernize-avoid-c-arrays)
    benchmark::DoNotOptimize(static_cast<char*>(buffer));
    const TSamples64<LIMIT> data;
    size_t i = 0;
    for (auto _ : state) {
        benchmark::DoNotOptimize(PackU64(data.Data[i % data.COUNT], buffer));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui64 LIMIT>
void Read64_KikimrLong(benchmark::State& state) {
    ui64 num = 0;
    benchmark::DoNotOptimize(&num);
    const TCodedData64<LIMIT, TKikimrCoder64> data;
    size_t i = 0;
    for (auto _ : state) {
        const size_t pos = i % data.COUNT;
        benchmark::DoNotOptimize(NKikimr::Unpack64(data.Data + pos * data.BYTES_PER_NUM, data.BYTES_PER_NUM, num));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui64 LIMIT>
void Read64_KikimrShort(benchmark::State& state) {
    ui64 num = 0;
    benchmark::DoNotOptimize(&num);
    const TCodedData64<LIMIT, TKikimrCoder64> data;
    size_t i = 0;
    for (auto _ : state) {
        const size_t pos = i % data.COUNT;
        benchmark::DoNotOptimize(NKikimr::Unpack64(data.Data + pos * data.BYTES_PER_NUM, data.Length[pos], num));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui64 LIMIT>
void Read64_PackedTypes(benchmark::State& state) {
    ui64 num = 0;
    benchmark::DoNotOptimize(&num);
    const TCodedData64<LIMIT, TPackedTypesCoder64> data;
    size_t i = 0;
    for (auto _ : state) {
        const size_t pos = i % data.COUNT;
        benchmark::DoNotOptimize(Unpack64(num, data.Data + pos * data.BYTES_PER_NUM));
        benchmark::ClobberMemory();
        ++i;
    }
}

template <ui64 LIMIT>
void Read64_DictUtils(benchmark::State& state) {
    ui64 num = 0;
    benchmark::DoNotOptimize(&num);
    const TCodedData64<LIMIT, TDictUtilsCoder64> data;
    size_t i = 0;
    for (auto _ : state) {
        const size_t pos = i % data.COUNT;
        benchmark::DoNotOptimize(UnpackU64(&num, data.Data + pos * data.BYTES_PER_NUM));
        benchmark::ClobberMemory();
        ++i;
    }
}

#define DEF_WRITE_BENCH(base, limit)                                                                  \
    BENCHMARK_TEMPLATE(Write##base##_Kikimr, limit)->Name("Write" #base "_Kikimr_" #limit);           \
    BENCHMARK_TEMPLATE(Write##base##_PackedTypes, limit)->Name("Write" #base "_PackedTypes_" #limit); \
    BENCHMARK_TEMPLATE(Write##base##_DictUtils, limit)->Name("Write" #base "_DictUtils_" #limit);

#define DEF_READ_BENCH(base, limit)                                                                 \
    BENCHMARK_TEMPLATE(Read##base##_KikimrLong, limit)->Name("Read" #base "_KikimrLong_" #limit);   \
    BENCHMARK_TEMPLATE(Read##base##_KikimrShort, limit)->Name("Read" #base "_KikimrShort_" #limit); \
    BENCHMARK_TEMPLATE(Read##base##_PackedTypes, limit)->Name("Read" #base "_PackedTypes_" #limit); \
    BENCHMARK_TEMPLATE(Read##base##_DictUtils, limit)->Name("Read" #base "_DictUtils_" #limit);

DEF_WRITE_BENCH(32, 10)
DEF_WRITE_BENCH(32, 126)
DEF_WRITE_BENCH(32, 127)
DEF_WRITE_BENCH(32, 128)
DEF_WRITE_BENCH(32, 254)
DEF_WRITE_BENCH(32, 255)
DEF_WRITE_BENCH(32, 256)
DEF_WRITE_BENCH(32, 65534)
DEF_WRITE_BENCH(32, 65535)
DEF_WRITE_BENCH(32, 65536)
DEF_WRITE_BENCH(32, 4294967295)

DEF_WRITE_BENCH(64, 10)
DEF_WRITE_BENCH(64, 126)
DEF_WRITE_BENCH(64, 127)
DEF_WRITE_BENCH(64, 128)
DEF_WRITE_BENCH(64, 254)
DEF_WRITE_BENCH(64, 255)
DEF_WRITE_BENCH(64, 256)
DEF_WRITE_BENCH(64, 65534)
DEF_WRITE_BENCH(64, 65535)
DEF_WRITE_BENCH(64, 65536)
DEF_WRITE_BENCH(64, 4294967294ULL)
DEF_WRITE_BENCH(64, 4294967295ULL)
DEF_WRITE_BENCH(64, 4294967296ULL)
DEF_WRITE_BENCH(64, 18446744073709551615ULL)

DEF_READ_BENCH(32, 10)
DEF_READ_BENCH(32, 126)
DEF_READ_BENCH(32, 127)
DEF_READ_BENCH(32, 128)
DEF_READ_BENCH(32, 254)
DEF_READ_BENCH(32, 255)
DEF_READ_BENCH(32, 256)
DEF_READ_BENCH(32, 65534)
DEF_READ_BENCH(32, 65535)
DEF_READ_BENCH(32, 65536)
DEF_READ_BENCH(32, 4294967295)

DEF_READ_BENCH(64, 10)
DEF_READ_BENCH(64, 126)
DEF_READ_BENCH(64, 127)
DEF_READ_BENCH(64, 128)
DEF_READ_BENCH(64, 254)
DEF_READ_BENCH(64, 255)
DEF_READ_BENCH(64, 256)
DEF_READ_BENCH(64, 65534)
DEF_READ_BENCH(64, 65535)
DEF_READ_BENCH(64, 65536)
DEF_READ_BENCH(64, 4294967294ULL)
DEF_READ_BENCH(64, 4294967295ULL)
DEF_READ_BENCH(64, 4294967296ULL)
DEF_READ_BENCH(64, 18446744073709551615ULL)

} // namespace
