#pragma once

#include <ydb/library/workload/tpcc/config.h>

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <util/random/fast.h>

namespace NYdbWorkload {
namespace NTPCC {

inline i32 UniformRandom32(i32 min, i32 max, TFastRng32& rng) {
    Y_ENSURE(min <= max);
    return (rng.GenRand() % (max - min + 1) + min);
}

inline i32 NonUniformRandom32(i32 maxOr, i32 delta, i32 min, i32 max, TFastRng32& rng) {
    return ((UniformRandom32(0, maxOr, rng) | UniformRandom32(min, max, rng)) + delta) % (max - min + 1) + min;
}

inline std::string GetLastName(ui64 num) {
    return NameTokens[num / 100] + NameTokens[(num / 10) % 10] + NameTokens[num % 10]; 
}

inline std::string GetNonUniformRandomLastNameForLoad(TFastRng32& rng) {
    return GetLastName(NonUniformRandom32(255, EWorkloadConstants::TPCC_C_LAST_LOAD_C, 0, 999, rng));
}

inline std::string GetNonUniformRandomLastNameForRun(TFastRng32& rng) {
    return GetLastName(NonUniformRandom32(255, EWorkloadConstants::TPCC_C_LAST_RUN_C, 0, 999, rng));
}

inline i32 GetNonUniformCustomerId(i32 delta, TFastRng32& rng) {
    return NonUniformRandom32(1023, delta, 1, EWorkloadConstants::TPCC_CUST_PER_DIST, rng);
}

inline i32 GetNonUniformItemId(i32 delta, TFastRng32& rng) {
    return NonUniformRandom32(8191, delta, 1, EWorkloadConstants::TPCC_CUST_PER_DIST, rng);
}

}
}
