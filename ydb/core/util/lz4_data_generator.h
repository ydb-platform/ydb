#include <util/generic/string.h>
#include <util/random/fast.h>

#include <contrib/libs/lz4/lz4.h>

namespace NKikimr {

inline TString GenDataForLZ4(const ui64 size, const ui64 seed = 0) {
    TString data = TString::Uninitialized(size);
    const ui32 long_step = Max<ui32>(2027, size / 20);
    const ui32 short_step = Min<ui32>(53, long_step / 400);
    for (ui32 i = 0; i < data.size(); ++i) {
        const ui32 j = i + seed;
        data[i] = 0xff & (j % short_step + j / long_step);
    }
    return data;
}

inline TString FastGenDataForLZ4(i64 size, ui64 seed) {
    TString data = TString::Uninitialized(size);

    TReallyFastRng32 rng(seed);

    constexpr size_t minRunLen = 32;
    constexpr size_t maxRunLen = 64;
    const size_t runLen = minRunLen + rng() % (maxRunLen - minRunLen + 1);

    char run[maxRunLen];
    std::generate(run, run + runLen, rng);

    for (char *ptr = data.Detach(); size > 0; ptr += runLen, size -= runLen) {
        memcpy(ptr, run, Min<size_t>(size, runLen));
    }

    return data;
}

}
