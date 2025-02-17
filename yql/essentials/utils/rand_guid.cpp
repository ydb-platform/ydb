#include "rand_guid.h"

#include <util/system/datetime.h>
#include <util/system/getpid.h>
#include <util/system/unaligned_mem.h>
#include <util/generic/guid.h>

namespace NYql {

TAtomic TRandGuid::Counter = 0;

TRandGuid::TRandGuid() {
    ResetSeed();
}

void TRandGuid::ResetSeed() {
    new (&Rnd_) TMersenne<ui64>(GetCycleCount() + MicroSeconds() + GetPID());
}

TString TRandGuid::GenGuid() {
    TGUID ret = {};
    WriteUnaligned<ui64>(ret.dw, GetRnd().GenRand());
    ret.dw[2] = (ui32)GetRnd().GenRand();
    ret.dw[3] = AtomicIncrement(Counter);

    return GetGuidAsString(ret);
}

ui64 TRandGuid::GenNumber() {
    return GetRnd().GenRand();
}
}
