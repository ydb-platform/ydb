#pragma once

#include <ydb/core/security/iam_delegation/tablet/iam_delegation.h>
#include <ydb/core/security/iam_delegation/tablet/public/events.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NIamDelegation::NTests {

class TTestContext {
public:
    static constexpr ui64 TabletId = MakeTabletID(false, 1);

    explicit TTestContext(NFake::TStorage storage = {}) {
        SetupTabletServices(Runtime, nullptr, false, std::move(storage));
        const auto bootstrapper = CreateTestBootstrapper(
            Runtime,
            CreateTestTabletInfo(TabletId, TTabletTypes::IamDelegation),
            &CreateIamDelegationTablet);
        Runtime.EnableScheduleForActor(bootstrapper);
        WaitTabletBoot();
    }

    void Reboot() {
        ForwardToTablet(Runtime, TabletId, TActorId(), new TEvents::TEvPoison);
        WaitTabletBoot();
        InvalidateTabletResolverCache(Runtime, TabletId);
    }

    TInstant Now() const { return Runtime.GetCurrentTime(); }

    void SetTime(TInstant time) { Runtime.UpdateCurrentTime(time); }

    TTestBasicRuntime& GetRuntime() { return Runtime; }

    void AdvanceTime(TDuration duration) {
        Runtime.AdvanceCurrentTime(duration);
    }

    NKikimrIamDelegation::TResponse Call(
        NKikimrIamDelegation::TRequest request,
        NKikimrIamDelegation::EStatus expectedStatus = NKikimrIamDelegation::SUCCESS)
    {
        auto event = MakeHolder<TEvIamDelegationTablet::TEvRequest>();
        event->Record.Swap(&request);
        auto response = Send(std::move(event));
        UNIT_ASSERT_VALUES_EQUAL_C(response.GetStatus(), expectedStatus, response.GetError());
        return response;
    }

    NKikimrIamDelegation::TResponse Send(THolder<TEvIamDelegationTablet::TEvRequest> request) {
        const auto edge = Runtime.AllocateEdgeActor();
        const auto cookie = ++LastCookie;
        Runtime.SendToPipe(TabletId, edge, request.Release(), 0,
            GetPipeConfigWithRetries(), TActorId(), cookie);
        auto response = Runtime.GrabEdgeEvent<TEvIamDelegationTablet::TEvResponse>(edge);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        return response->Get()->Record;
    }

private:
    void WaitTabletBoot() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back(TEvTablet::EvBoot);
        Runtime.DispatchEvents(options);
    }

    TTestBasicRuntime Runtime;
    ui64 LastCookie = 0;
};

} // namespace NKikimr::NIamDelegation::NTests
