#pragma once

#include "test_utils.h"

#include <ydb/core/testlib/actors/test_runtime.h>

#include <library/cpp/testing/unittest/registar.h>

#include <memory>

namespace NTestUtils {

class TTestWithActorSystemFixture : public NUnitTest::TBaseFixture {
public:
    struct TSettings {
        TDuration WaitTimeout = TDuration::Seconds(20);
        TTestLogSettings LogSettings;
    };

    TTestWithActorSystemFixture();

    explicit TTestWithActorSystemFixture(const TSettings& settings);

    void SetUp(NUnitTest::TTestContext& ctx) override;

    void TearDown(NUnitTest::TTestContext& ctx) override;

protected:
    TSettings Settings;
    NActors::TTestActorRuntime Runtime;

private:
    // Like NKikimr::TActorSystemStub but with Runtime as actor system in tls context
    // it enables logging in unit test thread
    // and using NActors::TActivationContext::ActorSystem() method
    std::unique_ptr<NActors::TMailbox> Mailbox;
    std::unique_ptr<NActors::TExecutorThread> ExecutorThread;
    std::unique_ptr<NActors::TActorContext> ActorCtx;
    NActors::TActivationContext* PrevActorCtx;
};

} // namespace NTestUtils
