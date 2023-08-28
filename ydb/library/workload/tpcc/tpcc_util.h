#pragma once

#include <ydb/library/workload/tpcc/tpcc_config.h>

#include <util/random/fast.h>

namespace NYdbWorkload {
namespace NTPCC {

inline ui32 UniformRandom32(ui32 min, ui32 max, TFastRng32& rng) {
    return (rng.GenRand() % (max - min + 1) + min);
}

inline ui32 NonUniformRandom32(ui32 maxOr, ui32 delta, ui32 min, ui32 max, TFastRng32& rng) {
    return ((UniformRandom32(0, maxOr, rng) | UniformRandom32(min, max, rng)) + delta) % (max - min + 1) + min;
}

inline std::string GetLastName(ui64 num) {
    return TPCCNameTokens[num / 100] + TPCCNameTokens[(num / 10) % 10] + TPCCNameTokens[num % 10]; 
}

inline std::string GetNonUniformRandomLastNameForLoad(TFastRng32& rng) {
    return GetLastName(NonUniformRandom32(255, ETPCCWorkloadConstants::TPCC_C_LAST_LOAD_C, 0, 999, rng));
}

inline std::string GetNonUniformRandomLastNameForRun(TFastRng32& rng) {
    return GetLastName(NonUniformRandom32(255, ETPCCWorkloadConstants::TPCC_C_LAST_RUN_C, 0, 999, rng));
}

inline ui32 GetNonUniformCustomerId(ui32 delta, TFastRng32& rng) {
    return NonUniformRandom32(1023, delta, 1, ETPCCWorkloadConstants::TPCC_CUST_PER_DIST, rng);
}

inline ui32 GetNonUniformItemId(ui32 delta, TFastRng32& rng) {
    return NonUniformRandom32(8191, delta, 1, ETPCCWorkloadConstants::TPCC_CUST_PER_DIST, rng);
}

}
}
