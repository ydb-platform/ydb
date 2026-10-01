#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/aclib/aclib.h>

#include <util/generic/scope.h>

namespace NKikimr::NKqp {
namespace {

class TKillSessionFixture {
public:
    explicit TKillSessionFixture(ui32 nodes = 1) {
        auto settings = Tests::TServerSettings(Ports.GetPort(2134))
            .SetDomainName("Root")
            .SetNodeCount(nodes)
            .SetUseRealThreads(false);
        Server = new Tests::TServer(settings);
        Runtime = Server->GetRuntime();
        Sender = Runtime->AllocateEdgeActor();
        InitRoot(Server, Sender);
        for (ui32 index = 0; index < nodes; ++index) {
            Runtime->EnableScheduleForActor(Runtime->GetLocalServiceId(Proxy(index), index));
        }
    }

    TActorId Proxy(ui32 nodeIndex = 0) const {
        return MakeKqpProxyID(Runtime->GetNodeId(nodeIndex));
    }

    TString CreateSession(const TString& sid = "owner@builtin", ui32 nodeIndex = 0) {
        auto request = MakeHolder<TEvKqp::TEvCreateSessionRequest>();
        request->Record.MutableRequest()->SetDatabase("/Root");
        request->Record.SetUserSID(sid);
        Runtime->Send(new IEventHandle(Proxy(nodeIndex), Sender, request.Release()));
        auto response = Runtime->GrabEdgeEventRethrow<TEvKqp::TEvCreateSessionResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::SUCCESS);
        return response->Get()->Record.GetResponse().GetSessionId();
    }

    void Kill(const TString& sessionId, ui64 cookie, TDuration timeout = TDuration::Seconds(30),
        const TString& sid = "owner@builtin", bool canKillAnySession = false, const TString& database = "/Root")
    {
        auto request = MakeHolder<TEvKqp::TEvKillSessionRequest>();
        auto& record = request->Record;
        record.SetSessionId(sessionId);
        record.SetDatabase(database);
        record.SetCanKillAnySession(canKillAnySession);
        record.SetDeadlineUs((Runtime->GetCurrentTime() + timeout).MicroSeconds());
        NACLibProto::TUserToken token;
        token.SetUserSID(sid);
        record.SetUserToken(token.SerializeAsString());
        Runtime->Send(new IEventHandle(Proxy(), Sender, request.Release(), 0, cookie));
    }

    NKikimrKqp::TEvKillSessionResponse ExpectKill(ui64 cookie, Ydb::StatusIds::StatusCode status) {
        auto response = Runtime->GrabEdgeEventRethrow<TEvKqp::TEvKillSessionResponse>(Sender, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, cookie);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), status, response->Get()->Record.ShortDebugString());
        return response->Get()->Record;
    }

    void Barrier(ui32 nodeIndex = 0) {
        const auto sender = Runtime->AllocateEdgeActor(nodeIndex);
        Runtime->Send(new IEventHandle(Proxy(nodeIndex), sender, new TEvKqp::TEvProxyPingRequest()), nodeIndex);
        Runtime->GrabEdgeEventRethrow<TEvKqp::TEvProxyPingResponse>(sender);
    }

    void ExpectClosingQueryCancelled(const TString& sessionId) {
        auto request = MakeHolder<TEvKqp::TEvQueryRequest>();
        auto* query = request->Record.MutableRequest();
        query->SetSessionId(sessionId);
        query->SetDatabase("/Root");
        query->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        query->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        query->SetQuery("SELECT 1;");
        query->SetKeepSession(true);
        Runtime->Send(new IEventHandle(Proxy(), Sender, request.Release()));
        auto response = Runtime->GrabEdgeEventRethrow<TEvKqp::TEvQueryResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetYdbStatus(), Ydb::StatusIds::CANCELLED);
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.ShortDebugString(), "KILL SESSION");
        UNIT_ASSERT_STRING_CONTAINS(response->Get()->Record.ShortDebugString(), "owner@builtin");
    }

    TPortManager Ports;
    Tests::TServer::TPtr Server;
    TTestActorRuntime* Runtime = nullptr;
    TActorId Sender;
};

} // namespace

Y_UNIT_TEST_SUITE(KqpKillSessionProxy) {
    Y_UNIT_TEST(MultipleWaitersAndClosingRequests) {
        TKillSessionFixture fixture;
        const auto sessionId = fixture.CreateSession();
        TVector<TAutoPtr<IEventHandle>> closes;
        auto closeObserver = fixture.Runtime->AddObserver<TEvKqp::TEvCloseSessionRequest>(
            [&](TEvKqp::TEvCloseSessionRequest::TPtr& ev) {
                if (ev->Get()->Record.GetRequest().GetSessionId() == sessionId) {
                    UNIT_ASSERT(ev->Get()->Record.GetRequest().GetAdministrative());
                    closes.emplace_back(ev.Release());
                }
            });
        ui32 responses = 0;
        auto responseObserver = fixture.Runtime->AddObserver<TEvKqp::TEvKillSessionResponse>(
            [&](TEvKqp::TEvKillSessionResponse::TPtr&) { ++responses; });

        fixture.Kill(sessionId, 1);
        fixture.Runtime->WaitFor("administrative close", [&] { return !closes.empty(); }, TDuration::Seconds(5));
        fixture.Kill(sessionId, 2);
        fixture.Barrier();
        fixture.ExpectClosingQueryCancelled(sessionId);
        UNIT_ASSERT_VALUES_EQUAL(closes.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(responses, 0);

        closeObserver.Remove();
        fixture.Runtime->Send(closes.front().Release());
        THashSet<ui64> cookies;
        for (ui32 i = 0; i < 2; ++i) {
            auto response = fixture.Runtime->GrabEdgeEventRethrow<TEvKqp::TEvKillSessionResponse>(fixture.Sender);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Record.GetStatus(), Ydb::StatusIds::SUCCESS);
            cookies.insert(response->Cookie);
        }
        UNIT_ASSERT_VALUES_EQUAL(cookies.size(), 2);
        UNIT_ASSERT(cookies.contains(1));
        UNIT_ASSERT(cookies.contains(2));
        fixture.Kill(sessionId, 3, TDuration::Seconds(30), "root@builtin", true);
        fixture.ExpectKill(3, Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST(TimeoutDoesNotCancelTerminationOrOtherWaiters) {
        TKillSessionFixture fixture;
        const auto sessionId = fixture.CreateSession();
        TVector<TAutoPtr<IEventHandle>> closes;
        auto closeObserver = fixture.Runtime->AddObserver<TEvKqp::TEvCloseSessionRequest>(
            [&](TEvKqp::TEvCloseSessionRequest::TPtr& ev) {
                if (ev->Get()->Record.GetRequest().GetSessionId() == sessionId) {
                    closes.emplace_back(ev.Release());
                }
            });
        TVector<TEvKqp::TEvKillSessionResponse::TPtr> timedOutCallerResponses;
        auto responseObserver = fixture.Runtime->AddObserver<TEvKqp::TEvKillSessionResponse>(
            [&](TEvKqp::TEvKillSessionResponse::TPtr& ev) {
                if (ev->Recipient == fixture.Sender && ev->Cookie == 1) {
                    // Consume replies: an event left in an edge mailbox can be observed repeatedly.
                    timedOutCallerResponses.emplace_back(ev.Release());
                }
            });

        fixture.Kill(sessionId, 1, TDuration::MilliSeconds(100));
        fixture.Runtime->WaitFor("administrative close", [&] { return !closes.empty(); }, TDuration::Seconds(5));
        fixture.Kill(sessionId, 2);
        fixture.Runtime->WaitFor("termination timeout", [&] { return !timedOutCallerResponses.empty(); }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(timedOutCallerResponses.front()->Get()->Record.GetStatus(), Ydb::StatusIds::TIMEOUT);
        fixture.ExpectClosingQueryCancelled(sessionId);
        UNIT_ASSERT_VALUES_EQUAL(closes.size(), 1);

        closeObserver.Remove();
        fixture.Runtime->Send(closes.front().Release());
        fixture.ExpectKill(2, Ydb::StatusIds::SUCCESS);
        fixture.Kill(sessionId, 3, TDuration::Seconds(30), "root@builtin", true);
        fixture.ExpectKill(3, Ydb::StatusIds::PRECONDITION_FAILED);
        UNIT_ASSERT_VALUES_EQUAL(timedOutCallerResponses.size(), 1);
    }

    Y_UNIT_TEST(OwnershipDatabaseAndMalformedToken) {
        TKillSessionFixture fixture;
        const auto sessionId = fixture.CreateSession();
        fixture.Kill(sessionId, 1, TDuration::Seconds(30), "other@builtin");
        const auto denied = fixture.ExpectKill(1, Ydb::StatusIds::UNAUTHORIZED);
        fixture.Kill(sessionId, 2, TDuration::Seconds(30), "root@builtin", true, "/OtherDatabase");
        fixture.ExpectKill(2, Ydb::StatusIds::UNAUTHORIZED);

        auto malformed = MakeHolder<TEvKqp::TEvKillSessionRequest>();
        malformed->Record.SetSessionId(sessionId);
        malformed->Record.SetDatabase("/Root");
        malformed->Record.SetUserToken("not a serialized user token");
        malformed->Record.SetCanKillAnySession(true);
        fixture.Runtime->Send(new IEventHandle(fixture.Proxy(), fixture.Sender, malformed.Release(), 0, 3));
        fixture.ExpectKill(3, Ydb::StatusIds::UNAUTHORIZED);

        fixture.Kill(sessionId, 4);
        fixture.ExpectKill(4, Ydb::StatusIds::SUCCESS);
        fixture.Kill(sessionId, 5, TDuration::Seconds(30), "other@builtin");
        const auto missing = fixture.ExpectKill(5, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(denied.GetIssues(0).message(), missing.GetIssues(0).message());
        fixture.Kill(sessionId, 6, TDuration::Seconds(30), "root@builtin", true);
        fixture.ExpectKill(6, Ydb::StatusIds::PRECONDITION_FAILED);

        const auto anonymousSession = fixture.CreateSession("");
        fixture.Kill(anonymousSession, 7, TDuration::Seconds(30), "");
        fixture.ExpectKill(7, Ydb::StatusIds::UNAUTHORIZED);
        fixture.Kill(anonymousSession, 8, TDuration::Seconds(30), "root@builtin", true);
        fixture.ExpectKill(8, Ydb::StatusIds::SUCCESS);
    }

    Y_UNIT_TEST_TWIN(RemoteOwnerWaitsForRemoval, canKillAnySession) {
        TKillSessionFixture fixture(2);
        const auto sessionId = fixture.CreateSession("owner@builtin", 1);
        TVector<TAutoPtr<IEventHandle>> closes;
        auto closeObserver = fixture.Runtime->AddObserver<TEvKqp::TEvCloseSessionRequest>(
            [&](TEvKqp::TEvCloseSessionRequest::TPtr& ev) {
                if (ev->Get()->Record.GetRequest().GetSessionId() == sessionId) {
                    UNIT_ASSERT_VALUES_EQUAL(ev->GetRecipientRewrite().NodeId(), fixture.Runtime->GetNodeId(1));
                    closes.emplace_back(ev.Release());
                }
            });
        ui32 callerResponses = 0;
        auto responseObserver = fixture.Runtime->AddObserver<TEvKqp::TEvKillSessionResponse>(
            [&](TEvKqp::TEvKillSessionResponse::TPtr& ev) {
                if (ev->Recipient == fixture.Sender) {
                    ++callerResponses;
                }
            });

        fixture.Kill(sessionId, 42, TDuration::Seconds(30),
            canKillAnySession ? "operator@builtin" : "owner@builtin", canKillAnySession);
        fixture.Runtime->WaitFor("remote administrative close", [&] { return !closes.empty(); }, TDuration::Seconds(5));
        fixture.Barrier(1);
        UNIT_ASSERT_VALUES_EQUAL(callerResponses, 0);
        closeObserver.Remove();
        fixture.Runtime->Send(closes.front().Release(), 1);
        fixture.ExpectKill(42, Ydb::StatusIds::SUCCESS);
        fixture.Kill(sessionId, 43, TDuration::Seconds(30), "root@builtin", true);
        fixture.ExpectKill(43, Ydb::StatusIds::PRECONDITION_FAILED);
    }

    Y_UNIT_TEST_TWIN(NoDeadlineDoesNotScheduleTimeout, remote) {
        TKillSessionFixture fixture(remote ? 2 : 1);
        const ui32 ownerNodeIndex = remote ? 1 : 0;
        const auto sessionId = fixture.CreateSession("owner@builtin", ownerNodeIndex);
        TVector<TAutoPtr<IEventHandle>> closes;
        auto closeObserver = fixture.Runtime->AddObserver<TEvKqp::TEvCloseSessionRequest>(
            [&](TEvKqp::TEvCloseSessionRequest::TPtr& ev) {
                if (ev->Get()->Record.GetRequest().GetSessionId() == sessionId) {
                    closes.emplace_back(ev.Release());
                }
            });
        ui32 infiniteTimeouts = 0;
        TTestActorRuntimeBase::TScheduledEventFilter previousScheduledFilter;
        previousScheduledFilter = fixture.Runtime->SetScheduledEventFilter(
            [&](TTestActorRuntimeBase& runtime, TAutoPtr<IEventHandle>& ev, TDuration delay, TInstant& deadline) {
                if (deadline == TInstant::Max()) {
                    ++infiniteTimeouts;
                }
                return previousScheduledFilter(runtime, ev, delay, deadline);
            });
        Y_DEFER { fixture.Runtime->SetScheduledEventFilter(previousScheduledFilter); };

        fixture.Kill(sessionId, 42, TDuration::Max());
        fixture.Runtime->WaitFor("administrative close", [&] { return !closes.empty(); }, TDuration::Seconds(5));
        fixture.Barrier(ownerNodeIndex);
        UNIT_ASSERT_VALUES_EQUAL(infiniteTimeouts, 0);

        closeObserver.Remove();
        fixture.Runtime->Send(closes.front().Release(), ownerNodeIndex);
        fixture.ExpectKill(42, Ydb::StatusIds::SUCCESS);
        fixture.Kill(sessionId, 43, TDuration::Seconds(30), "root@builtin", true);
        fixture.ExpectKill(43, Ydb::StatusIds::PRECONDITION_FAILED);
    }
}

} // namespace NKikimr::NKqp
