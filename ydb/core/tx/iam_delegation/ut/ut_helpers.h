#pragma once

#include <ydb/core/tx/iam_delegation/iam_delegation.h>
#include <ydb/core/tx/iam_delegation/public/events.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NIamDelegation::NTests {

enum class ECrashPoint {
    BeforeCommit,
    AfterDurableCommitBeforeComplete,
    ResponseLost,
};

class TTestContext {
public:
    static constexpr ui64 TabletId = MakeTabletID(false, 1);

    explicit TTestContext(NFake::TStorage storage = {}) {
        ReadyObserver = Runtime.AddObserver<TEvTablet::TEvReady>([this](auto& event) {
            const auto* ready = event->Get();
            if (ready->TabletID == TabletId) {
                CurrentGeneration = ready->Generation;
                CurrentTabletActor = ready->UserTabletActor;
            }
        });
        SetupTabletServices(Runtime, nullptr, false, std::move(storage));
        const auto bootstrapper = CreateTestBootstrapper(
            Runtime,
            CreateTestTabletInfo(TabletId, TTabletTypes::IamDelegation),
            &CreateIamDelegationTablet);
        Runtime.EnableScheduleForActor(bootstrapper);
        WaitTabletReady(0, TActorId());
    }

    void Reboot() {
        const auto generation = CurrentGeneration;
        const auto actor = CurrentTabletActor;
        Runtime.Send(new IEventHandle(actor, TActorId(), new TEvents::TEvPoison));
        WaitTabletReady(generation, actor);
        InvalidateTabletResolverCache(Runtime, TabletId);
    }

    // Execute one request while a quiescent tablet is interrupted at a proven
    // boundary. There is deliberately no automatic request retry: the caller
    // must inspect durable state before deciding how to recover a lost reply.
    void CrashCall(NKikimrIamDelegation::TRequest request, ECrashPoint point) {
        const auto generation = CurrentGeneration;
        const auto actor = CurrentTabletActor;
        const auto systemActor = ResolveTablet(Runtime, TabletId, 0, true);
        const auto edge = Runtime.AllocateEdgeActor();
        const auto cookie = ++LastCookie;
        bool requestDelivered = false;
        ui32 injectionCount = 0;
        TActorId hiddenCommitRecipient;

        auto inject = [&](const TActorId& target) {
            UNIT_ASSERT_VALUES_EQUAL(injectionCount, 0);
            ++injectionCount;
            Runtime.Send(new IEventHandle(target, TActorId(), new TEvents::TEvPoison));
        };
        auto requests = Runtime.AddObserver<TEvIamDelegationTablet::TEvRequest>([&](auto& event) {
            if (event->Cookie == cookie && event->GetRecipientRewrite() == actor) {
                UNIT_ASSERT(!requestDelivered);
                requestDelivered = true;
            }
        });
        auto commits = Runtime.AddObserver<TEvTablet::TEvCommit>([&](auto& event) {
            if (point != ECrashPoint::BeforeCommit || !requestDelivered
                || event->Get()->TabletID != TabletId || event->Get()->Generation != generation) {
                return;
            }
            // The system tablet never receives this log commit. Destroy both
            // the system tablet and its executor, so an uncommitted write cannot
            // accidentally complete while the user tablet is being restarted.
            event.Reset();
            if (!injectionCount) {
                inject(systemActor);
            }
        });
        auto committed = Runtime.AddObserver<TEvTablet::TEvCommitResult>([&](auto& event) {
            if (point != ECrashPoint::AfterDurableCommitBeforeComplete || !requestDelivered
                || event->Get()->TabletID != TabletId || event->Get()->Generation != generation) {
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(event->Get()->Status, NKikimrProto::OK);
            if (!injectionCount) {
                hiddenCommitRecipient = event->GetRecipientRewrite();
                event.Reset();
                inject(actor);
            } else if (event->GetRecipientRewrite() == hiddenCommitRecipient) {
                // Preserve executor commit ordering: later results must not
                // cause Complete() to run for the hidden successful commit.
                event.Reset();
            }
        });
        auto responses = Runtime.AddObserver<TEvIamDelegationTablet::TEvResponse>([&](auto& event) {
            if (event->Cookie != cookie || event->GetRecipientRewrite() != edge) {
                return;
            }
            UNIT_ASSERT_C(point == ECrashPoint::ResponseLost,
                "Request replied before the selected crash boundary was reached");
            UNIT_ASSERT_VALUES_EQUAL_C(event->Get()->Record.GetStatus(), NKikimrIamDelegation::SUCCESS,
                event->Get()->Record.GetError());
            event.Reset();
            inject(actor);
        });
        auto event = MakeHolder<TEvIamDelegationTablet::TEvRequest>();
        event->Record.Swap(&request);
        Runtime.SendToPipe(TabletId, edge, event.Release(), 0,
            GetPipeConfigWithRetries(), TActorId(), cookie);
        WaitTabletReady(generation, actor);
        UNIT_ASSERT(requestDelivered);
        UNIT_ASSERT_VALUES_EQUAL(injectionCount, 1);
        InvalidateTabletResolverCache(Runtime, TabletId);
    }

    ui32 Generation() const {
        return CurrentGeneration;
    }

    TActorId TabletActor() const {
        return CurrentTabletActor;
    }

    TTestBasicRuntime& GetRuntime() {
        return Runtime;
    }

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
    void WaitTabletReady(ui32 previousGeneration, const TActorId& previousActor) {
        TDispatchOptions options;
        options.CustomFinalCondition = [&] { return CurrentGeneration > previousGeneration; };
        Runtime.DispatchEvents(options, TDuration::Seconds(30));
        UNIT_ASSERT_C(CurrentGeneration > previousGeneration, "The target tablet did not enter a new ready generation");
        UNIT_ASSERT(CurrentTabletActor);
        UNIT_ASSERT_C(CurrentTabletActor != previousActor, "Reboot reused the old tablet actor");
    }

    TTestBasicRuntime Runtime;
    TTestActorRuntime::TEventObserverHolder ReadyObserver;
    ui32 CurrentGeneration = 0;
    TActorId CurrentTabletActor;
    ui64 LastCookie = 0;
};

} // namespace NKikimr::NIamDelegation::NTests
