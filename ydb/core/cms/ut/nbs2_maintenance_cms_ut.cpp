#include "nbs2_maintenance_helpers.h"

#include <ydb/core/cms/cms_ut_common.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <util/generic/algorithm.h>

namespace NKikimr::NCmsTest {
namespace {

using namespace NNbs2Test;
using TEvCms = NCms::TEvCms;

enum class EOutcome {
    Allow,
    Deny,
    Timeout,
};

TStatus::ECode PermissionStatus(EOutcome outcome) {
    switch (outcome) {
    case EOutcome::Allow:
        return TStatus::ALLOW;
    case EOutcome::Deny:
        return TStatus::DISALLOW_TEMP;
    case EOutcome::Timeout:
        return TStatus::ERROR_TEMP;
    }
    Y_ABORT("Unexpected outcome");
}

struct TClient {
    TActorId Actor;
    ui64 Cookie;
};

struct TAttempt {
    ui64 Id;
    TActorId Checker;
    TActorId Pipe;
    TInstant Deadline;
};

// Exercise the real CMS through its events. Only DBSC is substituted, and its
// reply can be held while other client requests and transactions are processed.
class TCmsFixture {
public:
    TCmsFixture() {
        Cms = ResolveTablet(Env, Env.CmsId);
        Controller = Env.Register(new TFakeDbsController(State));
        SetIntegration(true);

        Observer = Env.AddObserver([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTabletResolver::TEvForward::EventType
                && ev->Get<TEvTabletResolver::TEvForward>()->TabletID == MakeDbsControllerID())
            {
                // Keep the real resolver and the environment's whiteboard/BSC
                // mocks intact: override discovery of DBSC only.
                ev->Rewrite(ev->GetTypeRewrite(), Controller);
            }
            if (auto it = Responses.find(ev->GetRecipientRewrite()); it != Responses.end()) {
                it->second.emplace_back(ev.Release());
            }
        });

        PreviousScheduledFilter = Env.SetScheduledEventFilter(
            [this](NActors::TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev,
                   TDuration delay, TInstant& deadline) {
                const bool drop = PreviousScheduledFilter(runtime, ev, delay, deadline);
                if (!drop && ev->GetTypeRewrite() == TEvents::TEvWakeup::EventType) {
                    WakeupDeadlines[ev->GetRecipientRewrite()] = deadline;
                }
                return drop;
            });
    }

    ~TCmsFixture() {
        Env.SetScheduledEventFilter(std::move(PreviousScheduledFilter));
    }

    void SetIntegration(bool enabled) {
        for (ui32 i = 0; i < Env.GetNodeCount(); ++i) {
            Env.GetAppData(i).NbsEnabled = true;
            Env.GetAppData(i).FeatureFlags.SetEnableCmsNbs2MaintenanceChecks(enabled);
        }
    }

    TClient Send(IEventBase* request) {
        const TClient client{Env.AllocateEdgeActor(), ++NextCookie};
        Responses[client.Actor];
        Env.SendAsync(new IEventHandle(Cms, client.Actor, request, 0, client.Cookie));
        return client;
    }

    bool HasResponse(const TClient& client) const {
        return !Responses.at(client.Actor).empty();
    }

    template <typename TPredicate>
    void Await(TPredicate ready, const TString& description) {
        if (!ready()) {
            TDispatchOptions options;
            options.CustomFinalCondition = ready;
            options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
            Env.DispatchEvents(options, TDuration::Seconds(5));
        }
        UNIT_ASSERT_C(ready(), description);
    }

    template <typename TEvent>
    auto Response(const TClient& client, TStatus::ECode status) {
        Await([&] { return HasResponse(client); }, "CMS did not reply to the client");
        const auto& responses = Responses.at(client.Actor);
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        const auto& ev = responses.front();
        UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvent::EventType);
        UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), client.Actor);
        UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, client.Cookie);
        const auto& record = ev->Get<TEvent>()->Record;
        UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus().GetCode(), status, record.ShortDebugString());
        return record;
    }

    TAttempt WaitForCheck(size_t count, const TClient& client) {
        Await([&] { return State.Requests.size() >= count || HasResponse(client); },
            "CMS neither started the NBS2 check nor replied to the client");
        // This is the expected RED assertion until the public handlers call
        // StartNbs2MaintenanceCheck. Do not skip the test or wait indefinitely.
        UNIT_ASSERT_VALUES_EQUAL_C(State.Requests.size(), count,
            "CMS replied without starting the NBS2 check; wire the public handler to DBSController");
        UNIT_ASSERT_C(!HasResponse(client), "CMS must wait for DBSC before replying");
        const auto& request = State.Requests.back();
        UNIT_ASSERT_VALUES_EQUAL(State.Clients.size(), count);
        UNIT_ASSERT_C(WakeupDeadlines.contains(request->Sender), "Checker must have a finite timeout");
        return {request->Cookie, request->Sender, State.Clients.back(), WakeupDeadlines.at(request->Sender)};
    }

    void CheckNodes(size_t index, std::initializer_list<ui32> nodeIndexes) const {
        TVector<ui32> expected;
        for (const auto index : nodeIndexes) {
            expected.push_back(Env.GetNodeId(index));
        }
        Sort(expected);
        const auto& actual = State.Requests.at(index)->Get()->Record.GetNodeIds();
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>(actual.begin(), actual.end()), expected);
    }

    void Drain() {
        Env.SimulateSleep(TDuration::MicroSeconds(1));
    }

    void AdvanceTo(TInstant time) {
        UNIT_ASSERT(Env.GetCurrentTime() < time);
        Env.SimulateSleep(time - Env.GetCurrentTime());
        Drain();
    }

    void Complete(const TAttempt& attempt, EOutcome outcome) {
        if (outcome == EOutcome::Timeout) {
            AdvanceTo(attempt.Deadline + TDuration::MicroSeconds(1));
            return;
        }
        auto response = MakeHolder<TResponse>();
        if (outcome == EOutcome::Deny) {
            response->Record.SetDecision(NDbsc::NProto::DENY);
            response->Record.AddBlockingPartitionIds(101);
        }
        Env.SendAsync(new IEventHandle(attempt.Checker, Controller, response.Release(), 0, attempt.Id));
    }

    void InjectResult(const TActorId& sender, ui64 attemptId) {
        auto result = MakeHolder<TResult>();
        result->AttemptId = attemptId;
        result->Status = TStatus::ALLOW;
        Env.SendAsync(new IEventHandle(Cms, sender, result.Release()));
        Drain();
    }

    NKikimrCms::TAction Shutdown(ui32 nodeIndex) const {
        return MakeAction(NKikimrCms::TAction::SHUTDOWN_HOST, Env.GetNodeId(nodeIndex), Duration.MicroSeconds());
    }

    TClient Request(ui32 nodeIndex) {
        auto request = MakePermissionRequest(TRequestOptions(User), Shutdown(nodeIndex));
        // Isolate the asynchronous NBS2 gate from VDisk availability checks.
        // FORCE must not bypass DBSC, unlike the local failure model.
        request->Record.SetAvailabilityMode(NKikimrCms::MODE_FORCE_RESTART);
        return Send(request.Release());
    }

    TClient Refresh(const TString& requestId) {
        return Send(MakeCheckRequest(User, requestId, false, NKikimrCms::MODE_FORCE_RESTART).Release());
    }

    TClient Approve(const TString& requestId) {
        return Send(MakeManageRequestRequest(User, NKikimrCms::TManageRequestRequest::APPROVE, requestId, false).Release());
    }

    void Reject(const TString& requestId) {
        const auto client = Send(MakeManageRequestRequest(User, NKikimrCms::TManageRequestRequest::REJECT, requestId, false).Release());
        Response<TEvCms::TEvManageRequestResponse>(client, TStatus::OK);
    }

    auto GetRequest(const TString& requestId, TStatus::ECode status = TStatus::OK) {
        const auto client = Send(MakeManageRequestRequest(User, NKikimrCms::TManageRequestRequest::GET, requestId, false).Release());
        return Response<TEvCms::TEvManageRequestResponse>(client, status);
    }

    auto Permissions(size_t count) {
        const auto client = Send(MakeManagePermissionRequest(User, NKikimrCms::TManagePermissionRequest::LIST, false).Release());
        const auto response = Response<TEvCms::TEvManagePermissionResponse>(client, TStatus::OK);
        UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), count);
        return response;
    }

    auto Config() {
        const auto client = Send(new TEvCms::TEvGetConfigRequest());
        return Response<TEvCms::TEvGetConfigResponse>(client, TStatus::OK).GetConfig();
    }

    void DisableMaintenance() {
        auto request = MakeHolder<TEvCms::TEvSetConfigRequest>();
        *request->Record.MutableConfig() = Config();
        request->Record.MutableConfig()->SetDisableMaintenance(true);
        const auto client = Send(request.Release());
        Response<TEvCms::TEvSetConfigResponse>(client, TStatus::OK);
    }

    TString SeedScheduled(ui32 nodeIndex, const TString& taskId) {
        // Store a real task with a waiting action, but no permission. Creating
        // the fixture must not depend on the NBS2 integration under test.
        SetIntegration(false);
        auto request = MakePermissionRequest(TRequestOptions(User, true, false, true), Shutdown(nodeIndex));
        request->Record.SetMaintenanceTaskId(taskId);
        request->Record.SetMaxPermissionCount(0);
        request->Record.SetDuration(Duration.MicroSeconds());
        request->Record.SetAvailabilityMode(NKikimrCms::MODE_MAX_AVAILABILITY);
        const auto response = Response<TEvCms::TEvPermissionResponse>(Send(request.Release()), TStatus::DISALLOW_TEMP);
        UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), 0);
        UNIT_ASSERT(!response.GetRequestId().empty());
        SetIntegration(true);
        return response.GetRequestId();
    }

    void Restart() {
        Env.RestartCms();
        Cms = ResolveTablet(Env, Env.CmsId);
    }

    // State referenced by observers and actors must outlive the runtime.
    TControllerState State;
    THashMap<TActorId, TVector<TAutoPtr<IEventHandle>>> Responses;
    THashMap<TActorId, TInstant> WakeupDeadlines;
    TCmsTestEnv Env{16};
    TActorId Cms;
    TActorId Controller;
    const TString User = "nbs2-test";
    const TDuration Duration = TDuration::Minutes(10);

private:
    ui64 NextCookie = 100;
    NActors::TTestActorRuntimeBase::TEventObserverHolder Observer;
    NActors::TTestActorRuntimeBase::TScheduledEventFilter PreviousScheduledFilter;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TCmsNbs2MaintenanceIntegrationTest) {
    Y_UNIT_TEST(QueueAndCompletion) {
        for (const auto outcome : {EOutcome::Allow, EOutcome::Deny, EOutcome::Timeout}) {
            TCmsFixture fixture;
            const auto first = fixture.Request(0);
            const auto firstAttempt = fixture.WaitForCheck(1, first);
            fixture.CheckNodes(0, {0});
            fixture.Permissions(0);

            const auto second = fixture.Request(1);
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
            UNIT_ASSERT(!fixture.HasResponse(first));
            UNIT_ASSERT(!fixture.HasResponse(second));
            fixture.Config(); // Control requests are not blocked by DBSC.

            fixture.Complete(firstAttempt, outcome);
            const auto firstResponse = fixture.Response<TEvCms::TEvPermissionResponse>(first, PermissionStatus(outcome));
            UNIT_ASSERT_VALUES_EQUAL(firstResponse.PermissionsSize(), outcome == EOutcome::Allow ? 1 : 0);
            const auto secondAttempt = fixture.WaitForCheck(2, second);
            if (outcome == EOutcome::Allow) {
                fixture.CheckNodes(1, {0, 1});
            } else {
                fixture.CheckNodes(1, {1});
            }
            fixture.Permissions(outcome == EOutcome::Allow ? 1 : 0);

            fixture.Complete(secondAttempt, EOutcome::Allow);
            const auto secondResponse = fixture.Response<TEvCms::TEvPermissionResponse>(second, TStatus::ALLOW);
            UNIT_ASSERT_VALUES_EQUAL(secondResponse.PermissionsSize(), 1);
            fixture.Permissions(outcome == EOutcome::Allow ? 2 : 1);
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.Responses.at(first.Actor).size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Responses.at(second.Actor).size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 2);
        }
    }

    Y_UNIT_TEST(RequestFreshness) {
        enum class EChange {
            RemoveOwnRequest,
            NotificationEntersHorizon,
            RemoveUnrelatedRequest,
            RefreshOverrides,
        };
        for (const auto change : {EChange::RemoveOwnRequest, EChange::NotificationEntersHorizon,
                EChange::RemoveUnrelatedRequest, EChange::RefreshOverrides})
        {
            TCmsFixture fixture;
            const auto requestId = fixture.SeedScheduled(0, "watched-task");
            TString unrelatedId;
            if (change == EChange::RemoveUnrelatedRequest) {
                unrelatedId = fixture.SeedScheduled(3, "unrelated-task");
            }
            TInstant entersHorizon;
            if (change == EChange::NotificationEntersHorizon) {
                entersHorizon = fixture.Env.GetCurrentTime() + TDuration::Seconds(1);
                const auto startsAt = entersHorizon + fixture.Duration + fixture.Duration;
                const auto notification = fixture.Send(MakeNotification(fixture.User, startsAt, fixture.Shutdown(9)).Release());
                fixture.Response<TEvCms::TEvNotificationResponse>(notification, TStatus::OK);
            }

            // Stored mode is MAX_AVAILABILITY and MaxPermissionCount is zero;
            // refresh changes both in its effective copy, not in the saved task.
            const auto client = fixture.Refresh(requestId);
            const auto attempt = fixture.WaitForCheck(1, client);
            fixture.CheckNodes(0, {0});
            fixture.Permissions(0);
            const auto stored = fixture.GetRequest(requestId);
            UNIT_ASSERT_VALUES_EQUAL(stored.RequestsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stored.GetRequests(0).ActionsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(stored.GetRequests(0).GetAvailabilityMode(), NKikimrCms::MODE_MAX_AVAILABILITY);
            if (change == EChange::RemoveOwnRequest) {
                fixture.Reject(requestId);
            } else if (change == EChange::RemoveUnrelatedRequest) {
                fixture.Reject(unrelatedId);
            } else if (change == EChange::NotificationEntersHorizon) {
                UNIT_ASSERT(entersHorizon + TDuration::MicroSeconds(1) < attempt.Deadline);
                fixture.AdvanceTo(entersHorizon + TDuration::MicroSeconds(1));
            }

            fixture.Complete(attempt, EOutcome::Allow);
            const bool outdated = change == EChange::RemoveOwnRequest || change == EChange::NotificationEntersHorizon;
            const auto response = fixture.Response<TEvCms::TEvPermissionResponse>(client,
                outdated ? TStatus::ERROR_TEMP : TStatus::ALLOW);
            UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), outdated ? 0 : 1);
            fixture.Permissions(outdated ? 0 : 1);
            if (change == EChange::RemoveOwnRequest) {
                fixture.GetRequest(requestId, TStatus::WRONG_REQUEST); // No resurrection.
            } else if (outdated) {
                const auto pending = fixture.GetRequest(requestId);
                UNIT_ASSERT_VALUES_EQUAL(pending.RequestsSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(pending.GetRequests(0).ActionsSize(), 1);
            }

            // A discarded ALLOW must also release the queue, without a hidden
            // second DBSC check for the outdated attempt.
            const auto next = fixture.Request(2);
            const auto nextAttempt = fixture.WaitForCheck(2, next);
            fixture.Complete(nextAttempt, EOutcome::Allow);
            fixture.Response<TEvCms::TEvPermissionResponse>(next, TStatus::ALLOW);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 2);
        }
    }

    Y_UNIT_TEST(ConfigurationAndManualApproval) {
        {
            TCmsFixture fixture;
            const auto client = fixture.Request(0);
            const auto attempt = fixture.WaitForCheck(1, client);
            fixture.DisableMaintenance();
            fixture.Complete(attempt, EOutcome::Allow);
            const auto response = fixture.Response<TEvCms::TEvPermissionResponse>(client, TStatus::ERROR_TEMP);
            UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), 0);
            fixture.Permissions(0);

            const auto next = fixture.Request(1);
            fixture.Response<TEvCms::TEvPermissionResponse>(next, TStatus::ERROR_TEMP);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
        }
        for (const auto outcome : {EOutcome::Allow, EOutcome::Deny, EOutcome::Timeout}) {
            TCmsFixture fixture;
            const auto requestId = fixture.SeedScheduled(0, "manual-task");
            fixture.DisableMaintenance();
            const auto client = fixture.Approve(requestId);
            const auto attempt = fixture.WaitForCheck(1, client);
            fixture.CheckNodes(0, {0});
            fixture.Permissions(0);
            fixture.Complete(attempt, outcome);
            const auto response = fixture.Response<TEvCms::TEvManageRequestResponse>(client,
                outcome == EOutcome::Allow ? TStatus::OK : PermissionStatus(outcome));
            UNIT_ASSERT_VALUES_EQUAL(response.ManuallyApprovedPermissionsSize(), outcome == EOutcome::Allow ? 1 : 0);
            fixture.Permissions(outcome == EOutcome::Allow ? 1 : 0);
            if (outcome == EOutcome::Allow) {
                UNIT_ASSERT(response.GetManuallyApprovedPermissions(0).GetDeadline() > fixture.Env.GetCurrentTime().MicroSeconds());
            } else {
                const auto pending = fixture.GetRequest(requestId);
                UNIT_ASSERT_VALUES_EQUAL(pending.RequestsSize(), 1);
                UNIT_ASSERT_VALUES_EQUAL(pending.GetRequests(0).ActionsSize(), 1);
            }
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
        }
        {
            TCmsFixture fixture;
            const auto client = fixture.Request(0);
            const auto attempt = fixture.WaitForCheck(1, client);
            fixture.SetIntegration(false);
            fixture.Complete(attempt, EOutcome::Allow);
            const auto response = fixture.Response<TEvCms::TEvPermissionResponse>(client, TStatus::ERROR_TEMP);
            UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), 0);
            fixture.Permissions(0);

            const auto next = fixture.Request(1);
            const auto nextResponse = fixture.Response<TEvCms::TEvPermissionResponse>(next, TStatus::ALLOW);
            UNIT_ASSERT_VALUES_EQUAL(nextResponse.PermissionsSize(), 1);
            fixture.Permissions(1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
        }
    }

    Y_UNIT_TEST(StaleResultsAndRestart) {
        TCmsFixture fixture;
        const auto first = fixture.Request(0);
        const auto firstAttempt = fixture.WaitForCheck(1, first);
        fixture.Complete(firstAttempt, EOutcome::Allow);
        const auto firstResponse = fixture.Response<TEvCms::TEvPermissionResponse>(first, TStatus::ALLOW);
        UNIT_ASSERT_VALUES_EQUAL(firstResponse.PermissionsSize(), 1);
        const auto permissionId = firstResponse.GetPermissions(0).GetId();

        const auto second = fixture.Request(1);
        const auto secondAttempt = fixture.WaitForCheck(2, second);
        fixture.CheckNodes(1, {0, 1});
        const auto queued = fixture.Request(2);
        fixture.Drain();

        // Validate CMS guards, not just the checker's cookie filter. Correct
        // attempt from a wrong sender and vice versa must both be ignored.
        fixture.InjectResult(fixture.Controller, secondAttempt.Id);
        fixture.InjectResult(secondAttempt.Checker, secondAttempt.Id + 1);
        fixture.InjectResult(firstAttempt.Checker, firstAttempt.Id);
        UNIT_ASSERT(!fixture.HasResponse(second));
        UNIT_ASSERT(!fixture.HasResponse(queued));
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 2);
        fixture.Permissions(1);

        const auto oldCms = fixture.Cms;
        fixture.Restart();
        UNIT_ASSERT(fixture.Cms != oldCms);
        fixture.Await([&] {
            return !fixture.Env.FindActor(secondAttempt.Checker)
                && !fixture.Env.FindActor(secondAttempt.Pipe)
                && fixture.State.DisconnectedServers.contains(fixture.State.Servers.at(secondAttempt.Pipe));
        }, "Stopping CMS must cancel the pending checker and close its pipe");
        fixture.Complete(secondAttempt, EOutcome::Allow); // Late reply to the cancelled checker.
        fixture.Drain();
        const auto persisted = fixture.Permissions(1);
        UNIT_ASSERT_VALUES_EQUAL(persisted.GetPermissions(0).GetId(), permissionId);

        const auto next = fixture.Request(3);
        const auto nextAttempt = fixture.WaitForCheck(3, next);
        fixture.CheckNodes(2, {0, 3}); // Neither pending nor queued requests acquired locks.
        // Attempt counters may restart along with CMS. Even a matching id must
        // not let a result from the previous checker complete this request.
        fixture.InjectResult(secondAttempt.Checker, nextAttempt.Id);
        UNIT_ASSERT(!fixture.HasResponse(next));
        fixture.Permissions(1);
        fixture.Complete(nextAttempt, EOutcome::Allow);
        const auto nextResponse = fixture.Response<TEvCms::TEvPermissionResponse>(next, TStatus::ALLOW);
        UNIT_ASSERT_VALUES_EQUAL(nextResponse.PermissionsSize(), 1);
        fixture.Permissions(2);
        fixture.InjectResult(nextAttempt.Checker, nextAttempt.Id); // Duplicate after completion.
        UNIT_ASSERT_VALUES_EQUAL(fixture.Responses.at(next.Actor).size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 3);
        fixture.Permissions(2);
    }
}

} // namespace NKikimr::NCmsTest
