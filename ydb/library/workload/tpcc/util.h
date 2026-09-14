#pragma once

#include "constants.h"
#include "runner.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

#include <util/random/fast.h>
#include <util/random/random.h>

#include <string>
#include <stop_token>

namespace NYdb::NTPCC {

//-----------------------------------------------------------------------------

// [from; to]
inline size_t RandomNumber(size_t from, size_t to) {
    return ::RandomNumber(to - from + 1) + from;
}

// [from; to]
inline size_t RandomNumber(TReallyFastRng32& rng, size_t from, size_t to) {
    return rng.Uniform(from, to + 1);
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
    return NonUniformRandom(8191, OL_I_ID_C, 1, ITEM_COUNT);
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

//-----------------------------------------------------------------------------

// Format size in bytes to human-readable format
std::string GetFormattedSize(size_t size);

//-----------------------------------------------------------------------------

// Check if a status should cause program termination
inline bool ShouldExit(const TStatus& status) {
    return status.GetStatus() == EStatus::NOT_FOUND ||
           status.GetStatus() == EStatus::BAD_REQUEST ||
           status.GetStatus() == EStatus::SCHEME_ERROR ||
           status.GetStatus() == EStatus::UNAUTHORIZED;
}

void ExitIfError(const TStatus& status, const TString& what);
void ThrowIfError(const TStatus& status, const TString& what);

//-----------------------------------------------------------------------------

std::stop_source& GetGlobalInterruptSource();
std::atomic<bool>& GetGlobalErrorVariable();

inline void RequestStopWithError() {
    GetGlobalErrorVariable().store(true);
    GetGlobalInterruptSource().request_stop();
}

//-----------------------------------------------------------------------------

size_t NumberOfMyCpus();

size_t NumberOfComputeCpus(TDriver& driver);

} // namespace NYdb::NTPCC
