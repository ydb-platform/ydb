#include <ydb/core/cms/cms_impl.h>
#include <ydb/core/cms/nbs2_maintenance.h>

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/dbs_controller/dbs_controller_events_private.h>
#include <ydb/core/nbs/cloud/storage/core/libs/common/error.h>
#include <ydb/core/testlib/basics/appdata.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NCmsTest {
namespace {

namespace NDbsc = NYdb::NBS::NBlockStore::NStorage::NDbsController;
using TRequest = NDbsc::TEvDbsControllerPrivate::TEvNodeMaintenancePermissionRequest;
using TResponse = NDbsc::TEvDbsControllerPrivate::TEvNodeMaintenancePermissionResponse;
using TResponseRecord = NDbsc::NProto::TNodeMaintenancePermissionResponse;
using TResult = NCms::TCms::TEvPrivate::TEvNbs2MaintenanceResult;
using TStatus = NKikimrCms::TStatus;

enum class EConnectMode {
    Accept,
    Reject,
    Stall,
};

struct TControllerState {
    EConnectMode ConnectMode = EConnectMode::Accept;
    TVector<TActorId> Clients;
    THashMap<TActorId, TActorId> Servers;
    THashSet<TActorId> DisconnectedServers;
    TVector<TRequest::TPtr> Requests;
};

// Mock only tablet discovery and DBSC itself. Both ends of tablet pipe are real.
class TFakeDbsController : public TActor<TFakeDbsController> {
public:
    explicit TFakeDbsController(TControllerState& state)
        : TActor(&TThis::StateWork)
        , State(state)
        , Acceptor(NTabletPipe::CreateConnectAcceptor(MakeDbsControllerID()))
    {
    }

private:
    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTabletResolver::TEvForward, Handle);
            IgnoreFunc(TEvTabletResolver::TEvTabletProblem);
            hFunc(TEvTabletPipe::TEvConnect, Handle);
            IgnoreFunc(TEvTabletPipe::TEvServerConnected);
            hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);
            hFunc(TEvTabletPipe::TEvServerDestroyed, Handle);
            hFunc(TRequest, Handle);
            sFunc(TEvents::TEvPoisonPill, PassAway);
        }
    }

    void Handle(TEvTabletResolver::TEvForward::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->TabletID, MakeDbsControllerID());
        Send(ev->Sender, new TEvTabletResolver::TEvForwardResult(
            MakeDbsControllerID(), SelfId(), SelfId(), 0), 0, ev->Cookie);
    }

    void Handle(TEvTabletPipe::TEvConnect::TPtr& ev) {
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Record.GetTabletId(), MakeDbsControllerID());
        const auto client = ActorIdFromProto(ev->Get()->Record.GetClientId());
        State.Clients.push_back(client);
        switch (State.ConnectMode) {
        case EConnectMode::Accept:
            State.Servers[client] = Acceptor->Accept(ev, SelfId(), SelfId());
            break;
        case EConnectMode::Reject:
            Acceptor->Reject(ev, SelfId(), NKikimrProto::ERROR);
            break;
        case EConnectMode::Stall:
            // Leave the real pipe client waiting for the handshake reply.
            break;
        }
    }

    void Handle(TEvTabletPipe::TEvServerDisconnected::TPtr& ev) {
        State.DisconnectedServers.insert(ev->Get()->ServerId);
    }

    void Handle(TEvTabletPipe::TEvServerDestroyed::TPtr& ev) {
        Acceptor->Erase(ev);
    }

    void Handle(TRequest::TPtr& ev) {
        State.Requests.emplace_back(ev.Release());
    }

    void PassAway() override {
        Acceptor->Detach(SelfId());
        TActor::PassAway();
    }

private:
    TControllerState& State;
    THolder<NTabletPipe::IConnectAcceptor> Acceptor;
};

struct TAttempt {
    ui64 Id;
    TActorId Checker;
    TActorId Pipe;
    TInstant Deadline;
};

class TCheckerFixture {
public:
    TCheckerFixture() {
        Runtime.Initialize(TAppPrepare().Unwrap());
        Client = Runtime.AllocateEdgeActor();
        Controller = Runtime.Register(new TFakeDbsController(State));
        Runtime.RegisterService(MakeTabletResolverID(), Controller);
        ResultObserver = Runtime.AddObserver<TResult>([this](TResult::TPtr& ev) {
            UNIT_ASSERT_VALUES_EQUAL(ev->Recipient, Client);
            Results.emplace_back(ev.Release());
        });
    }

    TAttempt Start(ui64 attemptId, const TVector<ui32>& nodeIds) {
        const auto deadline = Runtime.GetCurrentTime() + Timeout;
        const auto connectionsBefore = State.Clients.size();
        const auto checker = Runtime.Register(NCms::CreateNbs2MaintenanceChecker(
            Client, attemptId, nodeIds, Timeout));
        Runtime.EnableScheduleForActor(checker);
        Dispatch();
        UNIT_ASSERT_VALUES_EQUAL(State.Clients.size(), connectionsBefore + 1);
        return {attemptId, checker, State.Clients.back(), deadline};
    }

    void Reply(const TAttempt& attempt, const TResponseRecord& record, ui64 cookie) {
        auto response = MakeHolder<TResponse>();
        response->Record = record;
        // Produce results during dispatch: observers do not see edge events
        // that were already queued before dispatch started.
        Runtime.SendAsync(new IEventHandle(attempt.Checker, Controller, response.Release(), 0, cookie));
        Dispatch();
    }

    void Dispatch() {
        // Drain ready messages using a virtual-time marker, not a wall-clock sleep.
        Runtime.SimulateSleep(TDuration::MicroSeconds(1));
    }

    void AdvanceTo(TInstant time) {
        UNIT_ASSERT(Runtime.GetCurrentTime() < time);
        Runtime.SimulateSleep(time - Runtime.GetCurrentTime());
        Dispatch();
    }

    void CheckRequest(size_t index, const TAttempt& attempt, const TVector<ui32>& nodeIds) const {
        const auto& ev = State.Requests.at(index);
        UNIT_ASSERT_VALUES_EQUAL(ev->Sender, attempt.Checker);
        UNIT_ASSERT_VALUES_EQUAL(ev->Cookie, attempt.Id);
        const auto& actual = ev->Get()->Record.GetNodeIds();
        UNIT_ASSERT_VALUES_EQUAL(TVector<ui32>(actual.begin(), actual.end()), nodeIds);
    }

    void CheckResult(const TAttempt& attempt, TStatus::ECode status,
            const TVector<ui64>& blockingPartitions = {}, const TString& reason = "") const
    {
        UNIT_ASSERT_VALUES_EQUAL(Results.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(Results.front()->Sender, attempt.Checker);
        const auto& result = *Results.front()->Get();
        UNIT_ASSERT_VALUES_EQUAL(result.AttemptId, attempt.Id);
        UNIT_ASSERT_VALUES_EQUAL(result.Status, status);
        UNIT_ASSERT_VALUES_EQUAL(result.BlockingPartitionIds, blockingPartitions);
        if (status == TStatus::ALLOW) {
            UNIT_ASSERT(result.Reason.empty());
        } else {
            UNIT_ASSERT(!result.Reason.empty());
            UNIT_ASSERT_C(result.Reason.Contains(reason), result.Reason);
        }
    }

    void CheckStopped(const TAttempt& attempt) {
        UNIT_ASSERT(!Runtime.FindActor(attempt.Checker));
        UNIT_ASSERT(!Runtime.FindActor(attempt.Pipe));
        if (const auto it = State.Servers.find(attempt.Pipe); it != State.Servers.end()) {
            UNIT_ASSERT(State.DisconnectedServers.contains(it->second));
            UNIT_ASSERT(!Runtime.FindActor(it->second));
        }
    }

    // Keep the state alive until the runtime has destroyed its actors.
    TControllerState State;
    TVector<TResult::TPtr> Results;
    NActors::TTestActorRuntime Runtime;
    TActorId Client;
    TActorId Controller;
    const TDuration Timeout = TDuration::Seconds(10);
    NActors::TTestActorRuntime::TEventObserverHolder ResultObserver;
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(TCmsNbs2MaintenanceCheckerTest) {
    Y_UNIT_TEST(RequestAndResponse) {
        TResponseRecord deny;
        deny.SetDecision(NDbsc::NProto::DENY);
        deny.AddBlockingPartitionIds(101);
        deny.AddBlockingPartitionIds(202);

        TResponseRecord error;
        *error.MutableError() = NYdb::NBS::MakeError(NYdb::NBS::E_REJECTED, "controller unavailable");
        error.SetDecision(NDbsc::NProto::ALLOW);

        TResponseRecord unknown;
        unknown.SetDecision(static_cast<NDbsc::NProto::EDecision>(100));

        struct TCase {
            TResponseRecord Response;
            TStatus::ECode Status;
            TVector<ui64> BlockingPartitions;
            TString Reason;
        };
        const TVector<TCase> cases = {
            // A default proto3 response omits both Error and Decision.
            {{}, TStatus::ALLOW, {}, ""},
            {deny, TStatus::DISALLOW_TEMP, {101, 202}, "denied"},
            {error, TStatus::ERROR_TEMP, {}, "controller unavailable"},
            {unknown, TStatus::ERROR_TEMP, {}, "Unknown DBSController maintenance decision"},
        };
        for (const auto& test : cases) {
            TCheckerFixture fixture;
            const TVector<ui32> nodes = {8, 3, 5};
            const auto attempt = fixture.Start(42, nodes);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
            fixture.CheckRequest(0, attempt, nodes);
            UNIT_ASSERT(fixture.Results.empty());

            fixture.Reply(attempt, test.Response, attempt.Id);
            fixture.CheckResult(attempt, test.Status, test.BlockingPartitions, test.Reason);
            fixture.CheckStopped(attempt);

            // A duplicate reply and the originally scheduled timeout must not
            // produce a second completion or retry the request.
            fixture.Reply(attempt, test.Response, attempt.Id);
            fixture.AdvanceTo(attempt.Deadline + TDuration::Seconds(1));
            fixture.CheckResult(attempt, test.Status, test.BlockingPartitions, test.Reason);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Clients.size(), 1);
        }
    }

    Y_UNIT_TEST(TransportFailuresAndTimeout) {
        enum class EFailure {
            ConnectError,
            Disconnect,
            ConnectTimeout,
            ResponseTimeout,
        };
        for (const auto failure : {EFailure::ConnectError, EFailure::Disconnect,
                EFailure::ConnectTimeout, EFailure::ResponseTimeout})
        {
            TCheckerFixture fixture;
            if (failure == EFailure::ConnectError) {
                fixture.State.ConnectMode = EConnectMode::Reject;
            } else if (failure == EFailure::ConnectTimeout) {
                fixture.State.ConnectMode = EConnectMode::Stall;
            }

            const auto attempt = fixture.Start(123, {1, 2});
            const bool connected = failure == EFailure::Disconnect || failure == EFailure::ResponseTimeout;
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), connected ? 1 : 0);
            if (connected) {
                fixture.CheckRequest(0, attempt, {1, 2});
            }

            TString reason;
            if (failure == EFailure::ConnectError) {
                reason = "Cannot connect to DBSController";
            } else if (failure == EFailure::Disconnect) {
                UNIT_ASSERT(fixture.Results.empty());
                // Break the real server side; the pipe must notify the helper.
                const auto server = fixture.State.Servers.at(attempt.Pipe);
                fixture.Runtime.SendAsync(new IEventHandle(server, fixture.Client, new TEvents::TEvPoisonPill()));
                fixture.Dispatch();
                reason = "connection lost";
            } else {
                UNIT_ASSERT(fixture.Results.empty());
                fixture.AdvanceTo(attempt.Deadline - TDuration::MilliSeconds(1));
                UNIT_ASSERT(fixture.Results.empty());
                UNIT_ASSERT(fixture.Runtime.FindActor(attempt.Checker));
                fixture.AdvanceTo(attempt.Deadline + TDuration::MicroSeconds(1));
                reason = "timed out";
            }

            fixture.CheckResult(attempt, TStatus::ERROR_TEMP, {}, reason);
            fixture.CheckStopped(attempt);
            fixture.Reply(attempt, {}, attempt.Id);
            fixture.AdvanceTo(attempt.Deadline + TDuration::Seconds(1));
            fixture.CheckResult(attempt, TStatus::ERROR_TEMP, {}, reason);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), connected ? 1 : 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.State.Clients.size(), 1);
        }
    }

    Y_UNIT_TEST(AttemptIsolationAndCancellation) {
        TCheckerFixture fixture;
        const auto first = fixture.Start(11, {1});
        const auto second = fixture.Start(22, {2, 3});
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 2);
        fixture.CheckRequest(0, first, {1});
        fixture.CheckRequest(1, second, {2, 3});

        // A response addressed to the first actor but carrying another attempt's
        // cookie must not complete either check.
        fixture.Reply(first, {}, second.Id);
        UNIT_ASSERT(fixture.Results.empty());
        UNIT_ASSERT(fixture.Runtime.FindActor(first.Checker));
        UNIT_ASSERT(fixture.Runtime.FindActor(second.Checker));
        UNIT_ASSERT(fixture.State.DisconnectedServers.empty());

        fixture.Runtime.SendAsync(new IEventHandle(first.Checker, fixture.Client, new TEvents::TEvPoisonPill()));
        fixture.Dispatch();
        fixture.CheckStopped(first);
        UNIT_ASSERT(fixture.Results.empty());
        UNIT_ASSERT(fixture.Runtime.FindActor(second.Checker));
        UNIT_ASSERT(fixture.Runtime.FindActor(second.Pipe));

        fixture.Reply(first, {}, first.Id);
        UNIT_ASSERT(fixture.Results.empty());
        fixture.Reply(second, {}, second.Id);
        fixture.CheckResult(second, TStatus::ALLOW);
        fixture.CheckStopped(second);
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.DisconnectedServers.size(), 2);

        fixture.Reply(first, {}, first.Id);
        fixture.Reply(second, {}, second.Id);
        fixture.AdvanceTo(second.Deadline + TDuration::Seconds(1));
        fixture.CheckResult(second, TStatus::ALLOW);
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.Requests.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.State.Clients.size(), 2);
    }
}

} // namespace NKikimr::NCmsTest
