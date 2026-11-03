#include "ut_helpers.h"
#include <util/folder/tempdir.h>

namespace NKikimr::NIamDelegation::NTests {
namespace {
namespace P = NKikimrIamDelegation;

P::TServiceKey Key(TString service = "ydb") {
    P::TServiceKey key;
    key.SetEndpoint("iam.endpoint:4283");
    key.SetServiceId(service);
    key.SetMicroserviceId("data-plane");
    key.SetSystemTokenName("system-provider");
    key.SetResourceType("resource-manager.cloud");
    return key;
}
P::TRequest Check(ui64 version = 0, P::TServiceKey key = Key()) {
    P::TRequest request;
    auto* check = request.MutableCheckService();
    *check->MutableKey() = std::move(key);
    if (version) check->SetExpectedVersion(version);
    check->SetRequestTimeoutUs(TDuration::Seconds(20).MicroSeconds());
    return request;
}
P::TRequest Report(ui64 version, P::EServiceHealth health, P::TServiceKey key = Key()) {
    P::TRequest request;
    auto* report = request.MutableReportService();
    *report->MutableKey() = std::move(key);
    report->SetVersion(version);
    report->SetHealth(health);
    return request;
}
P::TServiceGateResult Gate(TTestContext& ctx, const P::TRequest& request, P::EServiceGateDisposition disposition) {
    const auto result = ctx.Call(request).GetServiceGate();
    UNIT_ASSERT_VALUES_EQUAL(result.GetDisposition(), disposition);
    return result;
}
P::TServiceGateResult FailOnce(TTestContext& ctx) {
    const auto first = Gate(ctx, Check(), P::SERVICE_OBSERVED);
    const auto admitted = Gate(ctx, Check(first.GetState().GetVersion()), P::SERVICE_ADMITTED);
    return Gate(ctx, Report(admitted.GetState().GetVersion(), P::SERVICE_UNAVAILABLE), P::SERVICE_UPDATED);
}
}

Y_UNIT_TEST_SUITE(IamServiceBackoff) {
    Y_UNIT_TEST(HealthyCallsShareVersionWithoutSerializing) {
        TTestContext ctx;
        const auto observed = Gate(ctx, Check(), P::SERVICE_OBSERVED);
        UNIT_ASSERT_VALUES_EQUAL(observed.GetState().GetVersion(), 1);
        for (size_t i = 0; i < 20; ++i) {
            const auto admitted = Gate(ctx, Check(1), P::SERVICE_ADMITTED);
            UNIT_ASSERT_VALUES_EQUAL(admitted.GetState().GetVersion(), 1);
        }
    }
    Y_UNIT_TEST(CooldownExactBoundaryFencesSimultaneousWakeups) {
        TTestContext ctx;
        const auto failed = FailOnce(ctx);
        const auto state = failed.GetState();
        const auto delay = state.GetNextAllowedAtUs() - failed.GetServerNowUs();
        UNIT_ASSERT(delay >= TDuration::Seconds(1).MicroSeconds());
        UNIT_ASSERT(delay < TDuration::MilliSeconds(1250).MicroSeconds());
        const auto version = state.GetVersion();
        ctx.SetTime(TInstant::MicroSeconds(state.GetNextAllowedAtUs() - 1));
        Gate(ctx, Check(version), P::SERVICE_WAIT);
        ctx.SetTime(TInstant::MicroSeconds(state.GetNextAllowedAtUs()));
        const auto one = Gate(ctx, Check(version), P::SERVICE_ADMITTED);
        UNIT_ASSERT_VALUES_EQUAL(one.GetState().GetVersion(), version + 1);
        Gate(ctx, Check(version), P::SERVICE_STALE);
        Gate(ctx, Check(version + 1), P::SERVICE_WAIT);
    }
    Y_UNIT_TEST(DuplicateAndStaleReportsCannotChangeNewerCooldown) {
        TTestContext ctx;
        const auto failed = FailOnce(ctx);
        for (const auto health : {P::SERVICE_HEALTHY, P::SERVICE_UNAVAILABLE, P::SERVICE_CREDENTIALS_REJECTED}) {
            const auto stale = Gate(ctx, Report(1, health), P::SERVICE_STALE);
            UNIT_ASSERT_VALUES_EQUAL(stale.GetState().SerializeAsString(), failed.GetState().SerializeAsString());
        }
        const auto another = Gate(ctx, Check(0, Key("another-service")), P::SERVICE_OBSERVED);
        UNIT_ASSERT_VALUES_EQUAL(another.GetState().GetIteration(), 0);
    }
    Y_UNIT_TEST(RecoveryTimeoutAllowsAnotherAttemptWithoutAnOwnerLease) {
        TTestContext ctx;
        const auto failed = FailOnce(ctx);
        ctx.SetTime(TInstant::MicroSeconds(failed.GetState().GetNextAllowedAtUs()));
        const auto admitted = Gate(ctx, Check(failed.GetState().GetVersion()), P::SERVICE_ADMITTED);
        ctx.SetTime(TInstant::MicroSeconds(admitted.GetState().GetNextAllowedAtUs()));
        const auto takeover = Gate(ctx, Check(admitted.GetState().GetVersion()), P::SERVICE_ADMITTED);
        UNIT_ASSERT_VALUES_EQUAL(takeover.GetState().GetVersion(), admitted.GetState().GetVersion() + 1);
        Gate(ctx, Report(admitted.GetState().GetVersion(), P::SERVICE_HEALTHY), P::SERVICE_STALE);
        const auto healthy = Gate(ctx, Report(takeover.GetState().GetVersion(), P::SERVICE_HEALTHY), P::SERVICE_UPDATED);
        UNIT_ASSERT_VALUES_EQUAL(healthy.GetState().GetIteration(), 0);
        UNIT_ASSERT_VALUES_EQUAL(healthy.GetState().GetNextAllowedAtUs(), 0);
    }
    Y_UNIT_TEST(CooldownSurvivesTabletReboot) {
        TTestContext ctx;
        const auto failed = FailOnce(ctx);
        ctx.Reboot();
        const auto observed = Gate(ctx, Check(), P::SERVICE_OBSERVED);
        UNIT_ASSERT_VALUES_EQUAL(observed.GetState().SerializeAsString(), failed.GetState().SerializeAsString());
    }
    Y_UNIT_TEST(InvalidInputDoesNotCreateAServiceScope) {
        TTestContext ctx;
        auto request = Check();
        request.MutableCheckService()->SetRequestTimeoutUs(0);
        ctx.Call(request, P::INVALID_ARGUMENT);
        request = Check();
        request.MutableCheckService()->MutableKey()->SetSystemTokenName("");
        ctx.Call(request, P::INVALID_ARGUMENT);
        ctx.Call(Report(0, P::SERVICE_UNAVAILABLE), P::INVALID_ARGUMENT);
    }
}
}
