#include "test_with_actor_system.h"

#include <ydb/core/testlib/basics/appdata.h>

namespace NTestUtils {

TTestWithActorSystemFixture::TTestWithActorSystemFixture()
    : TTestWithActorSystemFixture(TSettings{})
{}

TTestWithActorSystemFixture::TTestWithActorSystemFixture(const TSettings& settings)
    : Settings(settings)
    , Runtime(/* nodeCount */ 1, /* useRealThreads */ true)
{
    SetupSignalHandlers();
}

void TTestWithActorSystemFixture::SetUp(NUnitTest::TTestContext& /* ctx */) {
    // Init runtime
    TAutoPtr<NKikimr::TAppPrepare> app = new NKikimr::TAppPrepare();
    Runtime.SetLogBackend(NActors::CreateStderrBackend());
    SetupLogLevel(Runtime, Settings.LogSettings, "TEST");
    Runtime.SetDispatchTimeout(Settings.WaitTimeout);
    Runtime.Initialize(app->Unwrap());

    // Init tls context
    auto* actorSystem = Runtime.GetActorSystem(0);
    Mailbox = std::make_unique<NActors::TMailbox>();
    ExecutorThread = std::make_unique<NActors::TExecutorThread>(0, actorSystem, nullptr, "test thread");
    ActorCtx = std::make_unique<NActors::TActorContext>(*Mailbox, *ExecutorThread, GetCycleCountFast(), NActors::TActorId());
    PrevActorCtx = NActors::TlsActivationContext;
    NActors::TlsActivationContext = ActorCtx.get();
}

void TTestWithActorSystemFixture::TearDown(NUnitTest::TTestContext& /* ctx */) {
    // Release tls context
    NActors::TlsActivationContext = PrevActorCtx;
    PrevActorCtx = nullptr;
}

} // namespace NTestUtils
