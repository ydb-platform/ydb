#include "random_provider.h"
#include <util/random/mersenne.h>
#include <util/random/random.h>
#include <util/system/unaligned_mem.h>

namespace {
    void SetV4(TGUID& g) {
        g.dw[1] &= 0x0fffffff;
        g.dw[1] |= 0x40000000;

        g.dw[2] &= 0xffffff3f;
        g.dw[2] |= 0x00000080;
    }
}

class TDefaultRandomProvider: public IRandomProvider {
public:
    ui64 GenRand() noexcept override {
        return RandomNumber<ui64>();
    }

    TGUID GenGuid() noexcept override {
        TGUID ret;
        CreateGuid(&ret);
        return ret;
    }

    TGUID GenUuid4() noexcept override {
        TGUID ret;
        WriteUnaligned<ui64>(ret.dw, RandomNumber<ui64>());
        WriteUnaligned<ui64>(ret.dw + 2, RandomNumber<ui64>());
        SetV4(ret);
        return ret;
    }
};

class TDeterministicRandomProvider: public IRandomProvider {
public:
    TDeterministicRandomProvider(ui64 seed)
        : Gen(seed)
    {
    }

    ui64 GenRand() noexcept override {
        return Gen.GenRand();
    }

    TGUID GenGuid() noexcept override {
        TGUID ret;
        WriteUnaligned<ui64>(ret.dw, Gen.GenRand());
        ret.dw[2] = (ui32)Gen.GenRand();
        ret.dw[3] = ++GuidCount;
        return ret;
    }

    TGUID GenUuid4() noexcept override {
        TGUID ret;
        WriteUnaligned<ui64>(ret.dw, Gen.GenRand());
        WriteUnaligned<ui64>(ret.dw + 2, Gen.GenRand());
        SetV4(ret);
        return ret;
    }

private:
    TMersenne<ui64> Gen;
    ui32 GuidCount = 0;
};

TIntrusivePtr<IRandomProvider> CreateDefaultRandomProvider() {
    return TIntrusivePtr<IRandomProvider>(new TDefaultRandomProvider());
}

TIntrusivePtr<IRandomProvider> CreateDeterministicRandomProvider(ui64 seed) {
    return TIntrusivePtr<IRandomProvider>(new TDeterministicRandomProvider(seed));
}
