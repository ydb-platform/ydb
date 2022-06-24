#include "pack.h"

#include <ydb/library/yql/minikql/pack_num.h>

#include <library/cpp/testing/benchmark/bench.h>
#include <library/cpp/packedtypes/longs.h>

#include <util/generic/xrange.h>
#include <util/generic/singleton.h>
#include <util/random/random.h>

namespace {

template <ui32 UPPER, size_t MAX_BYTE_SIZE = (1 << 15) - (1 << 10)>
struct TSamples32 {
    constexpr static size_t COUNT = MAX_BYTE_SIZE / sizeof(ui32);
    TSamples32() {
        for (size_t i: xrange(COUNT)) {
            Data[i] = RandomNumber<ui32>(UPPER);
        }
    }

    ui32 Data[COUNT];
};

template <ui64 UPPER, size_t MAX_BYTE_SIZE = (1 << 15) - (1 << 10)>
struct TSamples64 {
    constexpr static size_t COUNT = MAX_BYTE_SIZE / sizeof(ui64);
    TSamples64() {
        for (size_t i: xrange(COUNT)) {
            Data[i] = RandomNumber<ui64>(UPPER);
        }
    }

    ui64 Data[COUNT];
};

template <ui32 UPPER, typename TCoder, size_t MAX_BYTE_SIZE = (1 << 15) - (1 << 10)>
struct TCodedData32 {
    constexpr static size_t BYTES_PER_NUM = sizeof(ui32) + 2;
    constexpr static size_t COUNT = MAX_BYTE_SIZE / BYTES_PER_NUM;
    TCodedData32() {
        for (size_t i: xrange(COUNT)) {
            Length[i] = TCoder()(RandomNumber<ui32>(UPPER), Data + i * BYTES_PER_NUM);
        }
    }

    char Data[MAX_BYTE_SIZE + BYTES_PER_NUM];
    size_t Length[COUNT];
};

template <ui64 UPPER, typename TCoder, size_t MAX_BYTE_SIZE = (1 << 15) - (1 << 10)>
struct TCodedData64 {
    constexpr static size_t BYTES_PER_NUM = sizeof(ui64) + 2;
    constexpr static size_t COUNT = MAX_BYTE_SIZE / BYTES_PER_NUM;
    TCodedData64() {
        for (size_t i: xrange(COUNT)) {
            Length[i] = TCoder()(RandomNumber<ui64>(UPPER), Data + i * BYTES_PER_NUM);
        }
    }

    char Data[MAX_BYTE_SIZE + BYTES_PER_NUM];
    size_t Length[COUNT];
};

struct TKikimrCoder32 {
    size_t operator() (ui32 num, char* buf) const {
        return NKikimr::Pack32(num, buf);
    }
};

struct TKikimrCoder64 {
    size_t operator() (ui64 num, char* buf) const {
        return NKikimr::Pack64(num, buf);
    }
};

struct TPackedTypesCoder32 {
    size_t operator() (ui32 num, char* buf) const {
        return Pack32(num, buf) - buf;
    }
};

struct TPackedTypesCoder64 {
    size_t operator() (ui64 num, char* buf) const {
        return Pack64(num, buf) - buf;
    }
};

struct TDictUtilsCoder32 {
    size_t operator() (ui32 num, char* buf) const {
        return PackU32(num, buf);
    }
};

struct TDictUtilsCoder64 {
    size_t operator() (ui64 num, char* buf) const {
        return PackU64(num, buf);
    }
};

} // unnamed

#define DEF_WRITE_BENCH(base, limit) \
    Y_CPU_BENCHMARK(Write##base##_Kikimr_##limit, iface) { \
        char buffer[sizeof(ui##base) + 1]; \
        auto& data = Default<TSamples##base<limit>>(); \
        for (size_t i = 0; i < iface.Iterations(); ++i) { \
            Y_DO_NOT_OPTIMIZE_AWAY(NKikimr::Pack##base(data.Data[i % data.COUNT], buffer)); \
            NBench::Clobber(); \
        } \
    } \
    Y_CPU_BENCHMARK(Write##base##_PackedTypes_##limit, iface) { \
        char buffer[sizeof(ui##base) + 1]; \
        auto& data = Default<TSamples##base<limit>>(); \
        for (size_t i = 0; i < iface.Iterations(); ++i) { \
            Y_DO_NOT_OPTIMIZE_AWAY(Pack##base(data.Data[i % data.COUNT], buffer)); \
            NBench::Clobber(); \
        } \
    } \
    Y_CPU_BENCHMARK(Write##base##_DictUtils_##limit, iface) { \
        char buffer[sizeof(ui##base) + 1]; \
        auto& data = Default<TSamples##base<limit>>(); \
        for (size_t i = 0; i < iface.Iterations(); ++i) { \
            Y_DO_NOT_OPTIMIZE_AWAY(PackU##base(data.Data[i % data.COUNT], buffer)); \
            NBench::Clobber(); \
        } \
    }


#define DEF_READ_BENCH(base, limit) \
    Y_CPU_BENCHMARK(Read##base##_KikimrLong_##limit, iface) { \
        ui##base num = 0; \
        NBench::Escape(&num); \
        const auto& data = *HugeSingleton<TCodedData##base<limit, TKikimrCoder##base>>(); \
        for (size_t i = 0; i < iface.Iterations(); ++i) { \
            const size_t pos = i % data.COUNT; \
            Y_DO_NOT_OPTIMIZE_AWAY(NKikimr::Unpack##base(data.Data + pos * data.BYTES_PER_NUM, data.BYTES_PER_NUM, num)); \
            NBench::Clobber(); \
        } \
    } \
    Y_CPU_BENCHMARK(Read##base##_KikimrShort_##limit, iface) { \
        ui##base num = 0; \
        NBench::Escape(&num); \
        const auto& data = *HugeSingleton<TCodedData##base<limit, TKikimrCoder##base>>(); \
        for (size_t i = 0; i < iface.Iterations(); ++i) { \
            const size_t pos = i % data.COUNT; \
            Y_DO_NOT_OPTIMIZE_AWAY(NKikimr::Unpack##base(data.Data + pos * data.BYTES_PER_NUM, data.Length[pos], num)); \
            NBench::Clobber(); \
        } \
    } \
    Y_CPU_BENCHMARK(Read##base##_PackedTypes_##limit, iface) { \
        ui##base num = 0; \
        NBench::Escape(&num); \
        const auto& data = *HugeSingleton<TCodedData##base<limit, TPackedTypesCoder##base>>(); \
        for (size_t i = 0; i < iface.Iterations(); ++i) { \
            const size_t pos = i % data.COUNT; \
            Y_DO_NOT_OPTIMIZE_AWAY(Unpack##base(num, data.Data + pos * data.BYTES_PER_NUM)); \
            NBench::Clobber(); \
        } \
    } \
    Y_CPU_BENCHMARK(Read##base##_DictUtils_##limit, iface) { \
        ui##base num = 0; \
        NBench::Escape(&num); \
        const auto& data = *HugeSingleton<TCodedData##base<limit, TDictUtilsCoder##base>>(); \
        for (size_t i = 0; i < iface.Iterations(); ++i) { \
            const size_t pos = i % data.COUNT; \
            Y_DO_NOT_OPTIMIZE_AWAY(UnpackU##base(&num, data.Data + pos * data.BYTES_PER_NUM)); \
            NBench::Clobber(); \
        } \
    }


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
DEF_WRITE_BENCH(64, 4294967294ull)
DEF_WRITE_BENCH(64, 4294967295ull)
DEF_WRITE_BENCH(64, 4294967296ull)
DEF_WRITE_BENCH(64, 18446744073709551615ull)

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
DEF_READ_BENCH(64, 4294967294ull)
DEF_READ_BENCH(64, 4294967295ull)
DEF_READ_BENCH(64, 4294967296ull)
DEF_READ_BENCH(64, 18446744073709551615ull)
