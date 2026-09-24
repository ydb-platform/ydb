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

// Exercise the real CMS through its events. Only DBSC is substituted, and its
// reply can be held while other client requests and transactions are processed.
class TCmsFixture {
public:
    explicit TCmsFixture(ui32 vdisks = 1)
        : Env(16, vdisks)
    {
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
        return Send(request, ++NextCookie);
    }

    TClient Send(IEventBase* request, ui64 cookie) {
        const TClient client{Env.AllocateEdgeActor(), cookie};
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
    auto ResponseRecord(const TClient& client) {
        Await([&] { return HasResponse(client); }, "CMS did not reply to the client");
        const auto& responses = Responses.at(client.Actor);
        UNIT_ASSERT_VALUES_EQUAL(responses.size(), 1);
        const auto& ev = responses.front();
        UNIT_ASSERT_VALUES_EQUAL(ev->GetTypeRewrite(), TEvent::EventType);
        UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite(), client.Actor);
        UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, client.Cookie);
        return ev->Get<TEvent>()->Record;
    }

    template <typename TEvent>
    auto Response(const TClient& client, TStatus::ECode status) {
        auto record = ResponseRecord<TEvent>(client);
        UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus().GetCode(), status, record.ShortDebugString());
        return record;
    }

    template <typename TEvent>
    auto Response(const TClient& client, Ydb::StatusIds::StatusCode status) {
        auto record = ResponseRecord<TEvent>(client);
        UNIT_ASSERT_VALUES_EQUAL_C(record.GetStatus(), status, record.ShortDebugString());
        return record;
    }

    TAttempt WaitForCheck(size_t count, const TClient& client) {
        Await([&] { return State.Requests.size() >= count || HasResponse(client); },
            "CMS neither started the NBS2 check nor replied to the client");
        // Manual approval remains RED until its handler calls
        // StartNbs2MaintenanceCheck. Fail explicitly instead of waiting forever.
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
    TCmsTestEnv Env;
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

    Y_UNIT_TEST(PermissionBatchAndLegacyPaths) {
        for (const auto outcome : {EOutcome::Allow, EOutcome::Deny, EOutcome::Timeout}) {
            TCmsFixture fixture;
            auto request = MakePermissionRequest(TRequestOptions(fixture.User, true, false, true),
                fixture.Shutdown(2), fixture.Shutdown(0), fixture.Shutdown(1));
            request->Record.SetAvailabilityMode(NKikimrCms::MODE_FORCE_RESTART);
            request->Record.SetMaxPermissionCount(1);
            const auto client = fixture.Send(request.Release());
            const auto attempt = fixture.WaitForCheck(1, client);
            // DBSC sees the whole request before the local limit selects one action.
            fixture.CheckNodes(0, {0, 1, 2});
            fixture.Permissions(0);
            fixture.Complete(attempt, outcome);

            const auto response = fixture.Response<TEvCms::TEvPermissionResponse>(client,
                outcome == EOutcome::Allow ? TStatus::ALLOW_PARTIAL : PermissionStatus(outcome));
            const size_t granted = outcome == EOutcome::Allow ? 1 : 0;
            UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), granted);
            UNIT_ASSERT(!response.GetRequestId().empty());
            fixture.Permissions(granted);

            const auto pending = fixture.GetRequest(response.GetRequestId());
            UNIT_ASSERT_VALUES_EQUAL(pending.RequestsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(pending.GetRequests(0).ActionsSize(), 3 - granted);
            TVector<TString> targets;
            for (const auto& permission : response.GetPermissions()) {
                targets.push_back(permission.GetAction().GetHost());
            }
            for (const auto& action : pending.GetRequests(0).GetActions()) {
                targets.push_back(action.GetHost());
                if (outcome == EOutcome::Deny) {
                    UNIT_ASSERT_C(action.GetIssue().GetMessage().Contains("101"), action.ShortDebugString());
                }
            }
            Sort(targets);
            TVector<TString> expected = {
                ToString(fixture.Env.GetNodeId(0)), ToString(fixture.Env.GetNodeId(1)),
                ToString(fixture.Env.GetNodeId(2)),
            };
            Sort(expected);
            UNIT_ASSERT_VALUES_EQUAL(targets, expected);
            if (outcome == EOutcome::Deny) {
                UNIT_ASSERT_C(response.GetStatus().GetReason().Contains("101"), response.ShortDebugString());
            }
            // Neither DENY nor a transport error triggers checks of subsets.
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
        }

        {
            TCmsFixture fixture(0);
            // Establish that both nodes pass the ordinary local checks when
            // cluster limits are disabled. Dry-run must leave no locks behind.
            fixture.SetIntegration(false);
            auto baseline = MakePermissionRequest(TRequestOptions(fixture.User, true, true, true),
                fixture.Shutdown(10), fixture.Shutdown(11));
            baseline->Record.SetAvailabilityMode(NKikimrCms::MODE_KEEP_AVAILABLE);
            const auto allowed = fixture.Response<TEvCms::TEvPermissionResponse>(
                fixture.Send(baseline.Release()), TStatus::ALLOW);
            UNIT_ASSERT_VALUES_EQUAL(allowed.PermissionsSize(), 2);
            fixture.Permissions(0);
            fixture.SetIntegration(true);

            auto request = MakePermissionRequest(TRequestOptions(fixture.User, true, false, true),
                fixture.Shutdown(10), fixture.Shutdown(11));
            request->Record.SetAvailabilityMode(NKikimrCms::MODE_KEEP_AVAILABLE);
            const auto client = fixture.Send(request.Release());
            const auto attempt = fixture.WaitForCheck(1, client);
            fixture.CheckNodes(0, {10, 11});

            auto config = MakeHolder<TEvCms::TEvSetConfigRequest>();
            *config->Record.MutableConfig() = fixture.Config();
            auto* limits = config->Record.MutableConfig()->MutableClusterLimits();
            UNIT_ASSERT_VALUES_EQUAL(limits->GetDisabledNodesLimit(), 0);
            UNIT_ASSERT_VALUES_EQUAL(limits->GetDisabledNodesRatioLimit(), 0);
            limits->SetDisabledNodesLimit(1);
            fixture.Response<TEvCms::TEvSetConfigResponse>(fixture.Send(config.Release()), TStatus::OK);
            UNIT_ASSERT(!fixture.HasResponse(client));
            fixture.Permissions(0);

            // The cached cluster snapshot must use the updated limits after
            // DBSC replies, without another DBSC check or a subset request.
            fixture.Complete(attempt, EOutcome::Allow);
            const auto response = fixture.Response<TEvCms::TEvPermissionResponse>(client, TStatus::ALLOW_PARTIAL);
            UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), 1);
            UNIT_ASSERT(!response.GetRequestId().empty());
            fixture.Permissions(1);
            const auto pending = fixture.GetRequest(response.GetRequestId());
            UNIT_ASSERT_VALUES_EQUAL(pending.RequestsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(pending.GetRequests(0).ActionsSize(), 1);
            UNIT_ASSERT_VALUES_EQUAL(pending.GetRequests(0).GetActions(0).GetIssue().GetType(),
                NKikimrCms::TAction::TIssue::DISABLED_NODES_LIMIT_REACHED);
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
        }

        TCmsFixture fixture;
        fixture.SetIntegration(false);
        auto response = fixture.Response<TEvCms::TEvPermissionResponse>(fixture.Request(0), TStatus::ALLOW);
        UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), 1);
        fixture.SetIntegration(true);
        for (ui32 i = 0; i < fixture.Env.GetNodeCount(); ++i) {
            fixture.Env.GetAppData(i).NbsEnabled = false;
        }
        response = fixture.Response<TEvCms::TEvPermissionResponse>(fixture.Request(1), TStatus::ALLOW);
        UNIT_ASSERT_VALUES_EQUAL(response.PermissionsSize(), 1);
        fixture.Permissions(2);
        UNIT_ASSERT(fixture.State.Requests.empty());

        fixture.SetIntegration(true);
        auto invalid = MakePermissionRequest(TRequestOptions(fixture.User),
            MakeAction(NKikimrCms::TAction::SHUTDOWN_HOST, "unknown-nbs2-host", fixture.Duration.MicroSeconds()));
        const auto rejected = fixture.Response<TEvCms::TEvPermissionResponse>(
            fixture.Send(invalid.Release()), TStatus::NO_SUCH_HOST);
        UNIT_ASSERT_VALUES_EQUAL(rejected.PermissionsSize(), 0);
        // No resolved targets: preserve normal validation even when other nodes
        // have locks; do not send a batch containing only those existing locks.
        UNIT_ASSERT(fixture.State.Requests.empty());
        fixture.Permissions(2);
    }

    Y_UNIT_TEST(RefreshMaintenanceTaskLifecycle) {
        TCmsFixture fixture(0);
        const TString taskId = "nbs2-refresh-task";
        auto refresh = [&] {
            auto request = MakeHolder<TEvCms::TEvRefreshMaintenanceTaskRequest>();
            request->Record.MutableRequest()->set_task_uid(taskId);
            return fixture.Send(request.Release(), 0);
        };
        auto getTask = [&] {
            auto request = MakeHolder<TEvCms::TEvGetMaintenanceTaskRequest>();
            request->Record.MutableRequest()->set_task_uid(taskId);
            auto result = fixture.Response<TEvCms::TEvGetMaintenanceTaskResponse>(
                fixture.Send(request.Release(), 0), Ydb::StatusIds::SUCCESS).GetResult();
            UNIT_ASSERT_VALUES_EQUAL(result.task_options().task_uid(), taskId);
            return result;
        };
        auto checkActions = [&](const auto& result, size_t performed, size_t pending, bool denied = false) {
            UNIT_ASSERT_VALUES_EQUAL(result.action_group_states_size(), performed + pending);
            TVector<Ydb::Maintenance::ActionUid> actions;
            size_t waiting = 0;
            for (const auto& group : result.action_group_states()) {
                UNIT_ASSERT_VALUES_EQUAL(group.action_states_size(), 1);
                const auto& action = group.action_states(0);
                if (action.status() == Ydb::Maintenance::ActionState::ACTION_STATUS_PERFORMED) {
                    actions.push_back(action.action_uid());
                } else {
                    UNIT_ASSERT_VALUES_EQUAL(action.status(), Ydb::Maintenance::ActionState::ACTION_STATUS_PENDING);
                    ++waiting;
                    if (denied) {
                        UNIT_ASSERT_C(TString(action.details()).Contains("101"), action.ShortDebugString());
                    }
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(actions.size(), performed);
            UNIT_ASSERT_VALUES_EQUAL(waiting, pending);
            return actions;
        };
        auto completeAction = [&](const Ydb::Maintenance::ActionUid& action) {
            auto request = MakeHolder<TEvCms::TEvCompleteActionRequest>();
            *request->Record.MutableRequest()->add_action_uids() = action;
            const auto response = fixture.Response<TEvCms::TEvManageActionResponse>(
                fixture.Send(request.Release(), 0), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(response.GetResult().action_statuses_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response.GetResult().action_statuses(0).status(), Ydb::StatusIds::SUCCESS);
        };

        auto request = MakePermissionRequest(TRequestOptions(fixture.User, true, false, true),
            fixture.Shutdown(12), fixture.Shutdown(10), fixture.Shutdown(11));
        request->Record.SetMaintenanceTaskId(taskId);
        request->Record.SetAvailabilityMode(NKikimrCms::MODE_FORCE_RESTART);
        request->Record.SetMaxPermissionCount(1);
        auto client = fixture.Send(request.Release());
        auto attempt = fixture.WaitForCheck(1, client);
        fixture.CheckNodes(0, {10, 11, 12});
        fixture.Complete(attempt, EOutcome::Deny);
        const auto created = fixture.Response<TEvCms::TEvPermissionResponse>(client, TStatus::DISALLOW_TEMP);
        const auto requestId = created.GetRequestId();
        UNIT_ASSERT(!requestId.empty());
        fixture.Permissions(0);
        checkActions(getTask(), 0, 3, true);

        client = refresh();
        attempt = fixture.WaitForCheck(2, client);
        fixture.CheckNodes(1, {10, 11, 12});
        fixture.Permissions(0);
        fixture.Complete(attempt, EOutcome::Allow);
        auto response = fixture.Response<TEvCms::TEvMaintenanceTaskResponse>(client, Ydb::StatusIds::SUCCESS);
        auto performed = checkActions(response.GetResult(), 1, 2);
        fixture.Permissions(1);

        // An exhausted quota does not shrink the DBSC batch. Its ALLOW still
        // cannot grant more actions until an existing permission is completed.
        client = refresh();
        attempt = fixture.WaitForCheck(3, client);
        fixture.CheckNodes(2, {10, 11, 12});
        fixture.Complete(attempt, EOutcome::Allow);
        response = fixture.Response<TEvCms::TEvMaintenanceTaskResponse>(client, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(checkActions(response.GetResult(), 1, 2).front().action_id(), performed.front().action_id());
        fixture.Permissions(1);

        const auto beforeTimeout = getTask();
        const auto pendingBeforeTimeout = fixture.GetRequest(requestId);
        client = refresh();
        attempt = fixture.WaitForCheck(4, client);
        fixture.CheckNodes(3, {10, 11, 12});
        fixture.Complete(attempt, EOutcome::Timeout);
        fixture.Response<TEvCms::TEvMaintenanceTaskResponse>(client, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(getTask().SerializeAsString(), beforeTimeout.SerializeAsString());
        UNIT_ASSERT_VALUES_EQUAL(fixture.GetRequest(requestId).SerializeAsString(), pendingBeforeTimeout.SerializeAsString());
        fixture.Permissions(1);

        // CompleteAction is processed while refresh waits for DBSC. The
        // continuation must use the freed slot, not the previously full quota.
        client = refresh();
        attempt = fixture.WaitForCheck(5, client);
        fixture.CheckNodes(4, {10, 11, 12});
        const auto completedId = performed.front().action_id();
        completeAction(performed.front());
        fixture.Permissions(0);
        UNIT_ASSERT(!fixture.HasResponse(client));
        fixture.Complete(attempt, EOutcome::Allow);
        response = fixture.Response<TEvCms::TEvMaintenanceTaskResponse>(client, Ydb::StatusIds::SUCCESS);
        performed = checkActions(response.GetResult(), 1, 1);
        UNIT_ASSERT(performed.front().action_id() != completedId);
        const auto persisted = fixture.Permissions(1);
        const auto pending = fixture.GetRequest(requestId);
        UNIT_ASSERT_VALUES_EQUAL(pending.GetRequests(0).ActionsSize(), 1);
        TVector<ui32> remainingNodes = {
            FromString<ui32>(persisted.GetPermissions(0).GetAction().GetHost()),
            FromString<ui32>(pending.GetRequests(0).GetActions(0).GetHost()),
        };
        Sort(remainingNodes);

        // Restart in the middle of a refresh must preserve the task, its
        // pending action and its old permission, but not the unfinished check.
        client = fixture.Refresh(requestId);
        attempt = fixture.WaitForCheck(6, client);
        fixture.Restart();
        fixture.Await([&] { return !fixture.Env.FindActor(attempt.Checker); }, "Refresh checker survived CMS restart");
        fixture.Complete(attempt, EOutcome::Allow);
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Permissions(1).GetPermissions(0).GetId(), performed.front().action_id());
        checkActions(getTask(), 1, 1);

        client = refresh();
        attempt = fixture.WaitForCheck(7, client);
        const auto& nodes = fixture.State.Requests.back()->Get()->Record.GetNodeIds();
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>(nodes.begin(), nodes.end()), remainingNodes);
        fixture.Complete(attempt, EOutcome::Deny);
        response = fixture.Response<TEvCms::TEvMaintenanceTaskResponse>(client, Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(checkActions(response.GetResult(), 1, 1, true).front().action_id(), performed.front().action_id());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Permissions(1).GetPermissions(0).GetId(), performed.front().action_id());

        completeAction(performed.front());
        fixture.Permissions(0);
        const auto beforeDryRun = fixture.GetRequest(requestId);
        const ui32 lastNode = FromString<ui32>(beforeDryRun.GetRequests(0).GetActions(0).GetHost());
        client = fixture.Send(MakeCheckRequest(fixture.User, requestId, true, NKikimrCms::MODE_KEEP_AVAILABLE).Release());
        attempt = fixture.WaitForCheck(8, client);
        const auto& dryRunNodes = fixture.State.Requests.back()->Get()->Record.GetNodeIds();
        UNIT_ASSERT_VALUES_EQUAL(dryRunNodes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(dryRunNodes.Get(0), lastNode);
        fixture.Complete(attempt, EOutcome::Allow);
        const auto dryRun = fixture.Response<TEvCms::TEvPermissionResponse>(client, TStatus::ALLOW);
        UNIT_ASSERT_VALUES_EQUAL(dryRun.PermissionsSize(), 1);
        fixture.Permissions(0);
        // Dry-run must not overwrite the saved availability mode or actions.
        UNIT_ASSERT_VALUES_EQUAL(fixture.GetRequest(requestId).SerializeAsString(), beforeDryRun.SerializeAsString());
        checkActions(getTask(), 0, 1, true);

        client = refresh();
        attempt = fixture.WaitForCheck(9, client);
        fixture.Complete(attempt, EOutcome::Allow);
        response = fixture.Response<TEvCms::TEvMaintenanceTaskResponse>(client, Ydb::StatusIds::SUCCESS);
        performed = checkActions(response.GetResult(), 1, 0);
        fixture.Permissions(1);
        fixture.GetRequest(requestId, TStatus::WRONG_REQUEST);

        // No pending actions means no new locks and no DBSC check on refresh.
        response = fixture.Response<TEvCms::TEvMaintenanceTaskResponse>(refresh(), Ydb::StatusIds::SUCCESS);
        UNIT_ASSERT_VALUES_EQUAL(checkActions(response.GetResult(), 1, 0).front().action_id(), performed.front().action_id());
        fixture.Drain();
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 9);

        const auto legacyId = fixture.SeedScheduled(3, "legacy-refresh-task");
        fixture.SetIntegration(false);
        const auto legacy = fixture.Response<TEvCms::TEvPermissionResponse>(fixture.Refresh(legacyId), TStatus::ALLOW);
        UNIT_ASSERT_VALUES_EQUAL(legacy.PermissionsSize(), 1);
        fixture.Permissions(2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 9);
    }

    Y_UNIT_TEST(CreateMaintenanceTaskBatchAndDryRun) {
        struct TCase {
            EOutcome Outcome;
            bool DryRun;
        };
        const TVector<TCase> cases = {
            {EOutcome::Allow, false},
            {EOutcome::Deny, false},
            {EOutcome::Allow, true},
            {EOutcome::Deny, true},
        };
        for (const auto& test : cases) {
            TCmsFixture fixture;
            const TString taskId = "nbs2-create-task";
            auto request = MakeHolder<TEvCms::TEvCreateMaintenanceTaskRequest>();
            request->Record.SetUserSID(fixture.User);
            auto& apiRequest = *request->Record.MutableRequest();
            auto& options = *apiRequest.mutable_task_options();
            options.set_task_uid(taskId);
            options.set_availability_mode(Ydb::Maintenance::AVAILABILITY_MODE_FORCE);
            options.set_dry_run(test.DryRun);
            if (!test.DryRun) {
                options.set_max_inflight_actions(1);
            }
            AddActionGroups(apiRequest,
                MakeActionGroup(MakeLockAction(fixture.Env.GetNodeId(2), fixture.Duration)),
                MakeActionGroup(MakeLockAction(fixture.Env.GetNodeId(0), fixture.Duration)),
                MakeActionGroup(MakeLockAction(fixture.Env.GetNodeId(1), fixture.Duration)));

            // Public API adapters reply with cookie 0; unlike direct CMS
            // requests, they do not propagate the incoming event's cookie.
            const auto client = fixture.Send(request.Release(), 0);
            const auto attempt = fixture.WaitForCheck(1, client);
            fixture.CheckNodes(0, {0, 1, 2});
            fixture.Permissions(0);
            fixture.Complete(attempt, test.Outcome);
            // Without dry-run, a denied task is created with pending actions;
            // DISALLOW_TEMP is not a top-level API error in either mode.
            const auto response = fixture.Response<TEvCms::TEvMaintenanceTaskResponse>(
                client, Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(response.GetResult().task_uid(), taskId);

            const size_t granted = test.Outcome == EOutcome::Allow ? (test.DryRun ? 3 : 1) : 0;
            auto checkActions = [&](const auto& result) {
                UNIT_ASSERT_VALUES_EQUAL(result.action_group_states_size(), 3);
                size_t performed = 0;
                TVector<ui32> targets;
                for (const auto& group : result.action_group_states()) {
                    UNIT_ASSERT_VALUES_EQUAL(group.action_states_size(), 1);
                    const auto& action = group.action_states(0);
                    targets.push_back(action.action().lock_action().scope().node_id());
                    if (action.status() == Ydb::Maintenance::ActionState::ACTION_STATUS_PERFORMED) {
                        ++performed;
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(action.status(), Ydb::Maintenance::ActionState::ACTION_STATUS_PENDING);
                        if (test.Outcome == EOutcome::Deny) {
                            UNIT_ASSERT_C(TString(action.details()).Contains("101"), action.ShortDebugString());
                        }
                    }
                }
                UNIT_ASSERT_VALUES_EQUAL(performed, granted);
                Sort(targets);
                TVector<ui32> expected = {
                    fixture.Env.GetNodeId(0), fixture.Env.GetNodeId(1), fixture.Env.GetNodeId(2),
                };
                Sort(expected);
                UNIT_ASSERT_VALUES_EQUAL(targets, expected);
            };
            if (test.DryRun && test.Outcome == EOutcome::Deny) {
                UNIT_ASSERT_VALUES_EQUAL(response.GetResult().action_group_states_size(), 0);
                UNIT_ASSERT_VALUES_EQUAL(response.IssuesSize(), 1);
                UNIT_ASSERT_C(TString(response.GetIssues(0).message()).Contains("101"), response.ShortDebugString());
            } else {
                checkActions(response.GetResult());
            }
            fixture.Permissions(test.DryRun ? 0 : granted);

            auto get = MakeHolder<TEvCms::TEvGetMaintenanceTaskRequest>();
            get->Record.MutableRequest()->set_task_uid(taskId);
            const auto stored = fixture.Response<TEvCms::TEvGetMaintenanceTaskResponse>(
                fixture.Send(get.Release(), 0), test.DryRun ? Ydb::StatusIds::BAD_REQUEST : Ydb::StatusIds::SUCCESS);
            if (test.DryRun) {
                const auto list = fixture.Send(MakeManageRequestRequest(
                    fixture.User, NKikimrCms::TManageRequestRequest::LIST, false).Release());
                UNIT_ASSERT_VALUES_EQUAL(fixture.Response<TEvCms::TEvManageRequestResponse>(list, TStatus::OK).RequestsSize(), 0);
            } else {
                checkActions(stored.GetResult()); // Waiting actions survive the response.
            }
            fixture.Drain();
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
        }
    }
}

} // namespace NKikimr::NCmsTest
