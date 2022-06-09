#pragma once

#include <util/random/mersenne.h>
#include <util/generic/ptr.h>
#include <library/cpp/deprecated/atomic/atomic.h>

namespace NYql {
class TRandGuid {
public:
    TRandGuid();
    TRandGuid(TRandGuid&&) = default;
    TRandGuid& operator=(TRandGuid&&) = default;

    void ResetSeed();

    TString GenGuid();
    ui64 GenNumber();

private:
    THolder<TMersenne<ui64>> Rnd;
    static TAtomic Counter;
};
}
