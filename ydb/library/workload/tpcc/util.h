#pragma once

#include "constants.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/random/random.h>

namespace NYdb::NTPCC {

// [from; to]
inline size_t RandomNumber(size_t from, size_t to) {
    return ::RandomNumber(to - from + 1) + from;
}

// Non-uniform random number generation as per TPC-C spec
inline int NonUniformRandom(int A, int C, int min, int max) {
    int randomNum = RandomNumber(0, A);
    int randomNum2 = RandomNumber(min, max);
    return (((randomNum | randomNum2) + C) % (max - min + 1)) + min;
}

// Get a customer ID according to TPC-C spec
inline int GetRandomCustomerID() {
    return NonUniformRandom(1023, C_ID_C, 1, CUSTOMERS_PER_DISTRICT);
}

// Get an item ID according to TPC-C spec
inline int GetRandomItemID() {
    return NonUniformRandom(8191, OL_I_ID_C, 1, ITEMS_COUNT);
}

constexpr const char* const NameTokens[] = {"BAR", "OUGHT", "ABLE", "PRI",
        "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"};

inline TString GetLastName(int num) {
    TStringStream ss;
    ss << NameTokens[num / 100] << NameTokens[(num / 10) % 10] << NameTokens[num % 10];
    return ss.Str();
}

inline TString GetNonUniformRandomLastNameForRun() {
    return GetLastName(NonUniformRandom(255, C_LAST_RUN_C, 0, 999));
}

inline TString GetNonUniformRandomLastNameForLoad() {
    return GetLastName(NonUniformRandom(255, C_LAST_LOAD_C, 0, 999));
}

// Check if a status should cause program termination
inline bool ShouldExit(const TStatus& status) {
    return status.GetStatus() == EStatus::NOT_FOUND ||
           status.GetStatus() == EStatus::BAD_REQUEST ||
           status.GetStatus() == EStatus::SCHEME_ERROR ||
           status.GetStatus() == EStatus::UNAUTHORIZED;
}

} // namespace NYdb::NTPCC
