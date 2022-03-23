#pragma once

#include <ydb/core/tx/tx_allocator_client/actor_client.h>
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

private:
    TTestActorRuntime& Runtime;
    TActorId TxAllocatorClient;

public:
    TTestEnv(TTestActorRuntime &runtime)
        : Runtime(runtime)
    {
        Setup();
        Boot();
        SetupClient();
    }

private:
    void Boot();
    void SetupLogging();
    void Setup();
    void SetupClient();

public:
    void Reboot();

    void AsyncAllocate(ui64 size);
    void AllocateAndCheck(ui64 size);
};

typedef std::pair<TVector<ui64>, ui64> TAnswerWithCookie;
TAnswerWithCookie GrabAnswer(TTestActorRuntime &runtime);
void CheckExpectedCookie(const TEvTxAllocatorClient::TEvAllocateResult& result, ui64 cockie);

struct TMsgCounter {
    TTestActorRuntime& Runtime;
    ui32 Counter;

    TTestActorRuntimeBase::TEventObserver PrevObserver;

    TMsgCounter(TTestActorRuntime& runtime, ui32 msgType);
    ~TMsgCounter();

    void Wait(ui32 count);

    ui32 Get() {
        return Counter;
    }
};

}
