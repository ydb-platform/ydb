#pragma once

#include <util/generic/guid.h>
#include <util/random/common_ops.h>

class IRandomProvider: public TThrRefBase, public TCommonRNG<ui64, IRandomProvider> {
public:
    virtual TGUID GenGuid() noexcept = 0;
    virtual TGUID GenUuid4() noexcept = 0;
    virtual ui64 GenRand() noexcept = 0; // for TCommonRNG
};

TIntrusivePtr<IRandomProvider> CreateDefaultRandomProvider();
TIntrusivePtr<IRandomProvider> CreateDeterministicRandomProvider(ui64 seed);
