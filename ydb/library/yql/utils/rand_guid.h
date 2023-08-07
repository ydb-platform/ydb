#pragma once

#include <library/cpp/deprecated/atomic/atomic.h>
#include <util/random/mersenne.h>

#include <type_traits>

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
    TMersenne<ui64>& GetRnd() {
        return reinterpret_cast<TMersenne<ui64>&>(Rnd_);
    }

private:
    std::aligned_storage<sizeof(TMersenne<ui64>) ,alignof(TMersenne<ui64>)>::type Rnd_;

    static TAtomic Counter;
};
}
