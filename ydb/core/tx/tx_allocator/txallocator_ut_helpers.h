#pragma once

#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/tx.h>

#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NTxAllocatorUT_Private {

using namespace NKikimr;


// Sets up everything needed for the tests (actually we need only boot txallocator tablet)
class TTestEnv {
public:
    static constexpr ui64 TxAllocatorTablet = TTestTxConfig::TxAllocator;
    static const TDuration SimTimeOut;

public:
    TTestEnv(TTestActorRuntime &runtime)
    {
        Setup(runtime);
        Boot(runtime);
    }

private:
    void Boot(TTestActorRuntime &runtime);
    void SetupLogging(TTestActorRuntime &runtime);
    void Setup(TTestActorRuntime &runtime);

public:
    void Reboot(TTestActorRuntime &runtime);
};

class TIntersectionChecker {
private:
    typedef std::pair<ui64, ui64> TAllocation;
    TVector<TAllocation> Responses;

public:
    void Add(ui64 begin, ui64 end) {
        Responses.emplace_back(begin, end);
    }

    void AssertIntersection(bool continuous = true);
};

    typedef NKikimrTx::TEvTxAllocateResult::EStatus TResultStatus;
    void CheckExpectedStatus(const TVector<TResultStatus> &expected, TResultStatus result);
    void CheckExpectedStatus(TResultStatus expected, TResultStatus result);
    void CheckExpectedCookie(NKikimrTx::TEvTxAllocateResult result, ui64 cockie);

    typedef std::pair<NKikimrTx::TEvTxAllocateResult, ui64> TAnswerWithCookie;
    TAnswerWithCookie GrabAnswer(TTestActorRuntime &runtime);
    void AllocateAndCheck(TTestActorRuntime &runtime, ui64 size, const TVector<TResultStatus> &expected);
    void AllocateAndCheck(TTestActorRuntime &runtime, ui64 size, TResultStatus expected);
    void AsyncAllocate(TTestActorRuntime &runtime, ui64 size);
}
