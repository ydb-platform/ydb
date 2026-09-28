#include <ydb/core/security/iam_delegation/events.h>
#include <ydb/core/security/iam_delegation/iam_actor_base.h>
#include <ydb/core/security/iam_delegation/iam_delegation_service.h>
#include <ydb/core/security/iam_delegation/services.h>
#include <ydb/core/security/iam_delegation/settings.h>
#include <ydb/core/security/iam_delegation/system_token_service.h>

#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/testlib/service_mocks/operation_service_mock.h>
#include <ydb/library/testlib/service_mocks/service_control_service_mock.h>

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/network/sock.h>

#include <grpcpp/server_builder.h>

namespace NKikimr::NIamDelegation {

using namespace NActors;
using namespace Tests;

namespace {

// Hang guard: polls until the condition holds; the condition must be made inevitable by the test itself.
template <class TCondition>
void WaitUntil(TCondition condition, TStringBuf what, TDuration timeout = TDuration::Seconds(120)) {
    const TInstant deadline = TInstant::Now() + timeout;
    while (!condition()) {
        UNIT_ASSERT_C(TInstant::Now() < deadline, "timed out waiting for " << what);
        Sleep(TDuration::MilliSeconds(20));
    }
}

// An operation the mock never reports as done.
constexpr ui32 NEVER_DONE_OPERATION = 1000000;

// A stand-in for the system token service. Answers TEvGetSystemToken at once with the token (or Error when it
// is set) while Deliver is true; otherwise only records the request so that the test can answer it late.
// The state is read by the test thread while the actor runs on the runtime's threads, hence the lock.
struct TSystemTokenFake : TThrRefBase {
    TMutex Mutex;
    TActorId Service; // the fake actor
    TActorId Recipient;
    ui64 Cookie = 0;
    ui32 Requests = 0;
    bool Deliver = true;
    TString Error;

    ui32 RequestCount() {
        with_lock (Mutex) {
            return Requests;
        }
    }

    // Delivers the system token for the last request; returns its recipient.
    TActorId DeliverLast(TTestActorRuntime& runtime) {
        with_lock (Mutex) {
            runtime.Send(new IEventHandle(Recipient, Service, new TEvIamDelegation::TEvSystemTokenReady("ssa-token", {}), 0, Cookie));
            return Recipient;
        }
    }
};

class TFakeSystemTokenService : public TActor<TFakeSystemTokenService> {
public:
    explicit TFakeSystemTokenService(TIntrusivePtr<TSystemTokenFake> state)
        : TActor(&TThis::StateWork)
        , State(std::move(state))
    {}

    STRICT_STFUNC(StateWork,
        hFunc(TEvIamDelegation::TEvGetSystemToken, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

private:
    void Handle(TEvIamDelegation::TEvGetSystemToken::TPtr& ev) {
        with_lock (State->Mutex) {
            State->Recipient = ev->Sender;
            State->Cookie = ev->Cookie;
            ++State->Requests;
            if (State->Deliver) {
                Send(ev->Sender, new TEvIamDelegation::TEvSystemTokenReady(State->Error ? TString() : TString("ssa-token"), State->Error), 0, ev->Cookie);
            }
        }
    }

    const TIntrusivePtr<TSystemTokenFake> State;
};

struct TFixture {
    TPortManager PortManager;
    THolder<TServer> Server;
    TTestActorRuntime* Runtime = nullptr;
    TActorId Sender;

    ui16 IamPort = 0;
    TServiceControlServiceMock ServiceControlMock;
    TOperationServiceMock OperationMock;
    std::unique_ptr<grpc::Server> IamServer; // hosts the IAM control plane services on one port

    TIamDelegationSettings Settings;

    TFixture() {
        const ui16 kikimrPort = PortManager.GetPort(2134);
        NKikimrProto::TAuthConfig authConfig;
        auto settings = TServerSettings(kikimrPort, authConfig);
        settings.SetDomainName("Root");
        Server = MakeHolder<TServer>(settings);
        Runtime = Server->GetRuntime();
        Runtime->SetLogPriority(NKikimrServices::IAM_DELEGATION, NLog::PRI_DEBUG);
        Sender = Runtime->AllocateEdgeActor();

        IamPort = PortManager.GetPort(8443);
        {
            grpc::ServerBuilder builder;
            builder.AddListeningPort("[::]:" + ToString(IamPort), grpc::InsecureServerCredentials());
            builder.RegisterService(&ServiceControlMock);
            builder.RegisterService(&OperationMock);
            IamServer = builder.BuildAndStart();
        }

        Settings.TokenServiceEndpoint = "localhost:" + ToString(IamPort);
        Settings.ServiceControlEndpoint = "localhost:" + ToString(IamPort);
        Settings.EnableSsl = false;
        Settings.ServiceId = "ydb";
        Settings.MicroserviceId = "data-plane";
        Settings.ResourceType = "resource-manager.cloud";
        Settings.RequestTimeout = TDuration::Seconds(5);
        Settings.OperationPollInterval = TDuration::MilliSeconds(100);
        Settings.OperationPollTimeout = TDuration::Seconds(3);
        Settings.MaxRetries = 3;

        ServiceControlMock.ExpectedAuthorization = "Bearer ssa-token";
    }

    ~TFixture() {
        IamServer->Shutdown();
    }

    // Registers a fake system token service answering every request at once: with the token, or with the error.
    TIntrusivePtr<TSystemTokenFake> SystemToken(const TString& error = {}) {
        auto fake = MakeIntrusive<TSystemTokenFake>();
        fake->Error = error;
        fake->Service = Runtime->Register(new TFakeSystemTokenService(fake));
        return fake;
    }

    // Registers a fake system token service that records the requests and answers none until told to.
    TIntrusivePtr<TSystemTokenFake> SilentSystemToken() {
        auto fake = SystemToken();
        fake->Deliver = false;
        return fake;
    }

    TActorId StartDelegationService(TActorId systemTokenService = {}) {
        if (!systemTokenService) {
            systemTokenService = SystemToken()->Service;
        }
        const TActorId id = Runtime->Register(CreateIamDelegationService(Settings, systemTokenService));
        Runtime->RegisterService(MakeIamDelegationServiceId(), id);
        return id;
    }

    // Registers the real system token service over the metadata service at host:port under its service id.
    TActorId StartSystemTokenService(const TString& host, ui16 port) {
        const TActorId id = Runtime->Register(CreateIamSystemTokenService(host, port));
        Runtime->RegisterService(MakeIamSystemTokenServiceId(), id);
        return id;
    }

    // Asks the system token service at the id for a token on behalf of the edge actor.
    void RequestSystemToken(const TActorId& service, ui64 cookie) {
        Runtime->Send(new IEventHandle(service, Sender, new TEvIamDelegation::TEvGetSystemToken(), 0, cookie));
    }

    static TDelegationSpec Spec(const TString& sa = "sa-1", const TString& referrer = "ref-1") {
        TDelegationSpec spec;
        spec.ServiceAccountId = sa;
        spec.CloudId = "cloud-1";
        spec.ReferrerId = referrer;
        return spec;
    }

    // Sends the request to the service and returns the result it answers with.
    template <class TResultEv>
    TDelegationResult Call(const TActorId& service, IEventBase* request) {
        Runtime->Send(new IEventHandle(service, Sender, request, 0, 42));
        TAutoPtr<IEventHandle> handle;
        auto* result = Runtime->GrabEdgeEvent<TResultEv>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 42u);
        return result->Result;
    }

    TDelegationResult Setup(const TActorId& service, const TDelegationSpec& spec) {
        return Call<TEvIamDelegation::TEvSetupDelegationResult>(service, new TEvIamDelegation::TEvSetupDelegation(spec, "user-1"));
    }

    TDelegationResult Revoke(const TActorId& service, const TDelegationSpec& spec) {
        return Call<TEvIamDelegation::TEvRevokeDelegationResult>(service, new TEvIamDelegation::TEvRevokeDelegation(spec));
    }

    // The same on the default service, started with the current Settings on first use.
    TDelegationResult Setup(const TDelegationSpec& spec = Spec()) {
        return Setup(DefaultService(), spec);
    }

    TDelegationResult Revoke(const TDelegationSpec& spec = Spec()) {
        return Revoke(DefaultService(), spec);
    }

    TActorId DefaultService() {
        if (!Service) {
            Service = StartDelegationService();
        }
        return Service;
    }

    TActorId Service;

    // Sends a tracked event to a (dead) actor and waits for the undelivered notification: the deterministic
    // proof that the actor is gone and that the actor system still runs.
    void ExpectUndelivered(const TActorId& actor, IEventBase* event) {
        Runtime->Send(new IEventHandle(actor, Sender, event, IEventHandle::FlagTrackDelivery));
        TAutoPtr<IEventHandle> handle;
        auto* undelivered = Runtime->GrabEdgeEvent<TEvents::TEvUndelivered>(handle);
        UNIT_ASSERT(undelivered);
    }
};

void ExpectSuccess(const TDelegationResult& result) {
    UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
}

void ExpectError(const TDelegationResult& result, Ydb::StatusIds::StatusCode status, TStringBuf text = {}) {
    UNIT_ASSERT_VALUES_EQUAL_C(result.Status, status, result.Issues.ToOneLineString());
    UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), text);
}

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationService) {
    Y_UNIT_TEST(SetupDone) {
        TFixture f;
        ExpectSuccess(f.Setup());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);
        const auto call = f.ServiceControlMock.LastCall().Setup;
        UNIT_ASSERT_VALUES_EQUAL(call.service_id(), "ydb");
        UNIT_ASSERT_VALUES_EQUAL(call.microservice_id(), "data-plane");
        UNIT_ASSERT_VALUES_EQUAL(call.resource().id(), "cloud-1");
        UNIT_ASSERT_VALUES_EQUAL(call.resource().type(), "resource-manager.cloud");
        UNIT_ASSERT_VALUES_EQUAL(call.target_service_account_id(), "sa-1");
        UNIT_ASSERT_VALUES_EQUAL(call.referrer().id(), "ref-1");
        UNIT_ASSERT_VALUES_EQUAL(call.referrer().type(), "ydb.secret");
        UNIT_ASSERT_VALUES_EQUAL(call.on_behalf_of_subject_id(), "user-1");
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 0u);
    }

    Y_UNIT_TEST(RevokeAndNotFound) {
        TFixture f;
        ExpectSuccess(f.Revoke());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.RevokeCalls(), 1u);
        const auto call = f.ServiceControlMock.LastCall();
        UNIT_ASSERT_VALUES_EQUAL(call.Method, "RevokeDelegation");
        UNIT_ASSERT_VALUES_EQUAL(call.Revoke.referrer().id(), "ref-1");
        UNIT_ASSERT_VALUES_EQUAL(call.Revoke.target_service_account_id(), "sa-1");

        // an already revoked delegation is not an error
        f.ServiceControlMock.FailCount = 1;
        f.ServiceControlMock.FailStatus = grpc::StatusCode::NOT_FOUND;
        ExpectSuccess(f.Revoke());
    }

    // An operation that is not done at once is polled until it is, for Setup and Revoke alike.
    Y_UNIT_TEST(OperationIsPolled) {
        TFixture f;
        f.ServiceControlMock.NotDoneCount = 2;
        f.OperationMock.GetsUntilDone = 3;
        ExpectSuccess(f.Setup());
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 3u);
        ExpectSuccess(f.Revoke());
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 6u);
    }

    // The error of an operation is mapped whether the operation is done at once or after polling.
    Y_UNIT_TEST(OperationErrorIsMapped) {
        TFixture f;
        f.ServiceControlMock.OperationErrorCount = 1;
        ExpectError(f.Setup(), Ydb::StatusIds::UNAUTHORIZED, "injected operation error");
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 0u);

        f.ServiceControlMock.NotDoneCount = 1;
        f.OperationMock.OperationError = "user is not allowed to delegate";
        ExpectError(f.Setup(), Ydb::StatusIds::UNAUTHORIZED, "user is not allowed to delegate");
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 1u);
    }

    Y_UNIT_TEST(OperationPollTimeout) {
        TFixture f;
        f.ServiceControlMock.NotDoneCount = 1;
        f.OperationMock.GetsUntilDone = NEVER_DONE_OPERATION;
        ExpectError(f.Setup(), Ydb::StatusIds::TIMEOUT);
    }

    // The IAM failure types the user can act on come with a hint, in a direct answer and in an operation.
    Y_UNIT_TEST(IamFailuresAreExplained) {
        TFixture f;
        f.ServiceControlMock.FailCount = 1;
        f.ServiceControlMock.FailStatus = grpc::StatusCode::FAILED_PRECONDITION;
        f.ServiceControlMock.FailureType = "BAD_SERVICE_ACCOUNT_CLOUD";
        auto result = f.Setup();
        ExpectError(result, Ydb::StatusIds::BAD_REQUEST, "BAD_SERVICE_ACCOUNT_CLOUD (injected BAD_SERVICE_ACCOUNT_CLOUD)");
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "set RESOURCE to the cloud of the service account");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);

        f.ServiceControlMock.OperationErrorCount = 1;
        f.ServiceControlMock.FailureType = "SERVICE_NOT_ENABLED";
        result = f.Setup();
        ExpectError(result, Ydb::StatusIds::UNAUTHORIZED, "injected operation error; SERVICE_NOT_ENABLED");
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "YDB service is not enabled");
    }

    Y_UNIT_TEST(RetryableStatusesAreRetried) {
        TFixture f;
        ui32 expectedCalls = 0;
        for (const auto status : {
            grpc::StatusCode::DEADLINE_EXCEEDED,
            grpc::StatusCode::RESOURCE_EXHAUSTED,
            grpc::StatusCode::UNKNOWN,
            grpc::StatusCode::INTERNAL,
            grpc::StatusCode::ABORTED,
        }) {
            f.ServiceControlMock.FailStatus = status;
            f.ServiceControlMock.FailCount = 1;
            ExpectSuccess(f.Setup());
            expectedCalls += 2;
            UNIT_ASSERT_VALUES_EQUAL_C(f.ServiceControlMock.SetupCalls(), expectedCalls, "status " << status);
        }

        for (const auto& [grpcStatus, expected] : {
            std::pair{grpc::StatusCode::INVALID_ARGUMENT, Ydb::StatusIds::BAD_REQUEST},
            std::pair{grpc::StatusCode::NOT_FOUND, Ydb::StatusIds::NOT_FOUND},
            std::pair{grpc::StatusCode::UNAUTHENTICATED, Ydb::StatusIds::UNAUTHORIZED},
            std::pair{grpc::StatusCode::PERMISSION_DENIED, Ydb::StatusIds::UNAUTHORIZED},
            std::pair{grpc::StatusCode::FAILED_PRECONDITION, Ydb::StatusIds::BAD_REQUEST},
        }) {
            f.ServiceControlMock.FailStatus = grpcStatus;
            f.ServiceControlMock.FailCount = 1;
            ExpectError(f.Setup(), expected, "injected failure");
            expectedCalls += 1;
            UNIT_ASSERT_VALUES_EQUAL_C(f.ServiceControlMock.SetupCalls(), expectedCalls, "status " << grpcStatus);
        }
    }

    Y_UNIT_TEST(RetriesExhaustedMapsLastStatus) {
        TFixture f;
        f.ServiceControlMock.FailStatus = grpc::StatusCode::RESOURCE_EXHAUSTED;
        f.ServiceControlMock.FailCount = 100;
        ExpectError(f.Setup(), Ydb::StatusIds::OVERLOADED, "failed after 3 attempts");

        f.ServiceControlMock.FailStatus = grpc::StatusCode::DEADLINE_EXCEEDED;
        f.ServiceControlMock.FailCount = 100;
        ExpectError(f.Setup(), Ydb::StatusIds::TIMEOUT, "failed after 3 attempts");
    }

    // MaxRetries bounds the number of attempts of one call, and MaxRetries = 0 still makes the one attempt.
    Y_UNIT_TEST(RetryCountBounds) {
        TFixture f;
        f.ServiceControlMock.FailCount = 2;
        ExpectSuccess(f.Setup());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 3u);

        f.ServiceControlMock.FailCount = 100;
        ui32 expectedCalls = 3;
        for (const ui32 maxRetries : {1u, 0u}) {
            f.Settings.MaxRetries = maxRetries;
            ExpectError(f.Setup(f.StartDelegationService(), f.Spec()), Ydb::StatusIds::UNAVAILABLE, "failed after 1 attempts");
            UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), ++expectedCalls);
        }
    }

    Y_UNIT_TEST(SystemTokenFailure) {
        TFixture f;
        ExpectError(f.Setup(f.StartDelegationService(f.SystemToken("metadata is down")->Service), f.Spec()),
            Ydb::StatusIds::UNAVAILABLE, "metadata is down");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 0u);
    }

    // A system token that does not come in time is a retryable failure of the call.
    Y_UNIT_TEST(SystemTokenTimeoutIsRetried) {
        TFixture f;
        f.Settings.RequestTimeout = TDuration::Seconds(1);
        f.Settings.MaxRetries = 2;
        auto source = f.SilentSystemToken();
        ExpectError(f.Setup(f.StartDelegationService(source->Service), f.Spec()), Ydb::StatusIds::UNAVAILABLE,
            "failed after 2 attempts: timeout while obtaining the system service account token");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(source->RequestCount(), 2u);
    }

    Y_UNIT_TEST(LateSystemTokenIsIgnored) {
        TFixture f;
        f.Settings.RequestTimeout = TDuration::Seconds(1);
        f.Settings.MaxRetries = 1;
        auto source = f.SilentSystemToken();
        const auto service = f.StartDelegationService(source->Service);
        ExpectError(f.Setup(service, f.Spec()), Ydb::StatusIds::UNAVAILABLE);

        // the token arrives after the timeout: it reaches the state function and is ignored. The mailbox is
        // FIFO, so the Setup below is handled after the late token; it is the one and only ServiceControl call.
        UNIT_ASSERT_VALUES_EQUAL(source->DeliverLast(*f.Runtime), service);

        with_lock (source->Mutex) {
            source->Deliver = true;
        }
        ExpectSuccess(f.Setup(service, f.Spec()));
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(source->RequestCount(), 2u);
    }

    Y_UNIT_TEST(PoisonWhileWaiting) {
        TFixture f;
        f.ServiceControlMock.NotDoneCount = 1;
        f.OperationMock.GetsUntilDone = NEVER_DONE_OPERATION;
        const auto service = f.StartDelegationService();

        // the request polls the never-done operation; the actor is poisoned in the middle of it
        f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvIamDelegation::TEvSetupDelegation(f.Spec(), "user-1")));
        WaitUntil([&]() { return f.OperationMock.GetCalls.load() > 0; }, "the first poll");
        f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvents::TEvPoison()));
        // the actor is gone without a crash: a tracked event to it comes back undelivered, and a fresh
        // service on the same runtime works
        f.ExpectUndelivered(service, new TEvIamDelegation::TEvSetupDelegation(f.Spec(), "user-1"));
        f.OperationMock.GetsUntilDone = 1;
        ExpectSuccess(f.Setup(f.StartDelegationService(), f.Spec("sa-2", "ref-2")));
    }
}

Y_UNIT_TEST_SUITE(IamSystemTokenService) {
    // A TCP listener that accepts connections and never answers: the metadata service is "up" but silent.
    struct TSilentListener {
        TPortManager PortManager;
        ui16 Port = 0;
        TInetStreamSocket Socket;

        TSilentListener() {
            Port = PortManager.GetPort();
            TSockAddrInet addr("127.0.0.1", Port);
            UNIT_ASSERT_VALUES_EQUAL(Socket.Bind(&addr), 0);
            UNIT_ASSERT_VALUES_EQUAL(Socket.Listen(16), 0);
        }
    };

    // The system token service asks the SDK provider asynchronously: the metadata request runs on the provider's
    // own thread, so the service keeps handling requests when the endpoint never answers, and an actor waiting
    // for the token fails with the configured timeout instead of hanging.
    Y_UNIT_TEST(ServiceKeepsHandlingRequestsWhenMetadataIsUnreachable) {
        TSilentListener metadata;
        TFixture f;
        const auto service = f.StartSystemTokenService("127.0.0.1", metadata.Port);

        f.RequestSystemToken(service, 7);
        // (the SDK keeps retrying the silent endpoint in the background and no TEvSystemTokenReady is ever
        // produced; the service handles the requests below meanwhile)

        f.Settings.RequestTimeout = TDuration::Seconds(1); // makes the failure below inevitable
        f.Settings.MaxRetries = 1;
        ExpectError(f.Setup(f.StartDelegationService(service), f.Spec()), Ydb::StatusIds::UNAVAILABLE,
            "timeout while obtaining the system service account token");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 0u);
    }

    // While one actor waits for a system token that never comes, the actor system keeps processing other work:
    // a second delegation service on the same runtime, on a system token answered at once, completes a request,
    // and when the token is finally delivered to the first one its request completes too.
    Y_UNIT_TEST(PendingSystemTokenDoesNotBlockOtherWork) {
        TFixture f;
        f.Settings.RequestTimeout = TDuration::Minutes(5); // the wait is ended by the test, not by a timeout
        auto source = f.SilentSystemToken();
        const auto parked = f.StartDelegationService(source->Service);
        const auto other = f.StartDelegationService();

        f.Runtime->Send(new IEventHandle(parked, f.Sender, new TEvIamDelegation::TEvSetupDelegation(f.Spec(), "user-1"), 0, 41));
        WaitUntil([&]() { return source->RequestCount() >= 1; }, "the system token request"); // the first service is parked on it

        ExpectSuccess(f.Setup(other, f.Spec("sa-2", "ref-2")));
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u); // the first service is still parked
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.LastCall().Setup.target_service_account_id(), "sa-2");

        UNIT_ASSERT_VALUES_EQUAL(source->DeliverLast(*f.Runtime), parked);
        TAutoPtr<IEventHandle> handle;
        auto* setup = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvSetupDelegationResult>(handle);
        UNIT_ASSERT(setup);
        ExpectSuccess(setup->Result);
        UNIT_ASSERT_VALUES_EQUAL(handle->Sender, parked);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 41u);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.LastCall().Setup.target_service_account_id(), "sa-1");
        UNIT_ASSERT_VALUES_EQUAL(source->RequestCount(), 1u);
    }

    // A VM metadata service answering the token request the way the real one does.
    class TMetadataServer : public THttpServer::ICallBack {
        class TRequest : public TRequestReplier {
        public:
            explicit TRequest(TMetadataServer* parent) : Parent(parent) {}

            bool DoReply(const TReplyParams& params) override {
                const TParsedHttpFull parsed(params.Input.FirstLine());
                with_lock (Parent->Mutex) {
                    ++Parent->Requests;
                    Parent->LastPath = TString(parsed.Path);
                    const auto* flavor = params.Input.Headers().FindHeader("Metadata-Flavor");
                    Parent->LastFlavor = flavor ? flavor->Value() : TString();
                }
                const auto code = static_cast<HttpCodes>(Parent->StatusCode.load());
                if (code != HTTP_OK) {
                    THttpResponse(code).SetContent("no token here").OutTo(params.Output);
                    return true;
                }
                THttpResponse(HTTP_OK).SetContentType("application/json")
                    .SetContent(R"({"access_token":"ssa-token","expires_in":3600})").OutTo(params.Output);
                return true;
            }

        private:
            TMetadataServer* const Parent;
        };

    public:
        explicit TMetadataServer(ui16 port)
            : Port(port)
            , Server(this, THttpServer::TOptions(port))
        {
            UNIT_ASSERT_C(Server.Start(), "cannot start the metadata server on port " << port);
        }

        ~TMetadataServer() {
            Server.Stop();
        }

        TClientRequest* CreateClient() override {
            return new TRequest(this);
        }

        const ui16 Port;
        THttpServer Server;
        std::atomic<int> StatusCode = HTTP_OK; // what the next requests are answered with
        TMutex Mutex;
        ui32 Requests = 0;
        TString LastPath;
        TString LastFlavor;
    };

    // The production path end to end: the token source asks the metadata service through the SDK provider
    // (GET .../service-accounts/default/token with the Metadata-Flavor header), delivers the token to the
    // delegation service, and ServiceControl accepts the call authorized with it.
    // The metadata service answers 404 (no service account is attached to the VM yet): the SDK provider gives
    // up for good on such an answer, so the source drops it and asks with a fresh one on the next request.
    Y_UNIT_TEST(ProviderIsRecreatedAfterANonRetryableMetadataError) {
        TPortManager portManager;
        TMetadataServer metadata(portManager.GetPort());
        metadata.StatusCode = HTTP_NOT_FOUND;
        TFixture f;
        const auto service = f.StartSystemTokenService("127.0.0.1", metadata.Port);

        const auto request = [&](ui64 cookie) {
            f.RequestSystemToken(service, cookie);
            TAutoPtr<IEventHandle> handle;
            auto* ready = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvSystemTokenReady>(handle);
            UNIT_ASSERT(ready);
            UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, cookie);
            return std::make_pair(ready->Token, ready->Error);
        };

        const auto first = request(1);
        UNIT_ASSERT_VALUES_EQUAL(first.first, "");
        UNIT_ASSERT_STRING_CONTAINS(first.second, "404");
        ui32 requestsAfterFailure = 0;
        with_lock (metadata.Mutex) {
            requestsAfterFailure = metadata.Requests;
        }
        UNIT_ASSERT(requestsAfterFailure >= 1);

        // the account is attached: the next request goes to the metadata service again and gets the token
        metadata.StatusCode = HTTP_OK;
        const auto second = request(2);
        UNIT_ASSERT_VALUES_EQUAL_C(second.second, "", second.second);
        UNIT_ASSERT_VALUES_EQUAL(second.first, "ssa-token");
        with_lock (metadata.Mutex) {
            UNIT_ASSERT(metadata.Requests > requestsAfterFailure);
        }
    }

    Y_UNIT_TEST(TokenFromMetadataServiceAuthorizesTheCall) {
        TPortManager portManager;
        TMetadataServer metadata(portManager.GetPort());
        TFixture f;
        const auto service = f.StartSystemTokenService("127.0.0.1", metadata.Port);

        ExpectSuccess(f.Setup(f.StartDelegationService(service), f.Spec()));
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u); // the mock requires "Bearer ssa-token"
        with_lock (metadata.Mutex) {
            UNIT_ASSERT(metadata.Requests >= 1);
            UNIT_ASSERT_VALUES_EQUAL(metadata.LastPath, "/computeMetadata/v1/instance/service-accounts/default/token");
            UNIT_ASSERT_VALUES_EQUAL(metadata.LastFlavor, "Google");
        }
    }
}

// The KQP proxy starts the system token service and the delegation service at its bootstrap according to the
// feature flag and the configuration: both or neither, with a warning when the configuration is incomplete. The
// flag is followed at runtime in one direction: a config notification turning it on starts the services.
Y_UNIT_TEST_SUITE(IamDelegationProxyRegistration) {
    struct TProxyNode {
        TPortManager PortManager;
        THolder<TServer> Server;
        TTestActorRuntime* Runtime = nullptr;
        TActorId Proxy;
        TActorId Sender;

        TProxyNode(const NKikimrConfig::TAppConfig& appConfig, bool flag) {
            NKikimrConfig::TFeatureFlags featureFlags;
            featureFlags.SetEnableIamDelegationSecrets(flag);
            auto settings = TServerSettings(PortManager.GetPort(2134));
            settings.SetDomainName("Root").SetAppConfig(appConfig).SetFeatureFlags(featureFlags);
            Server = MakeHolder<TServer>(settings);
            Runtime = Server->GetRuntime();
            Proxy = NKqp::MakeKqpProxyID(Runtime->GetNodeId(0));
            Sender = Runtime->AllocateEdgeActor();
            Ping();
        }

        // A request the proxy has answered proves that it has handled everything sent to it before (FIFO).
        void Ping() {
            Runtime->Send(new IEventHandle(Proxy, Sender, new NKqp::TEvKqp::TEvCreateSessionRequest()));
            UNIT_ASSERT(Runtime->GrabEdgeEvent<NKqp::TEvKqp::TEvCreateSessionResponse>(Sender, TDuration::Seconds(120)));
        }

        // The proxy acknowledges a notification and then re-initializes the services in the same handler.
        void Notify(bool flag) {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            request->Record.MutableConfig()->MutableFeatureFlags()->SetEnableIamDelegationSecrets(flag);
            Runtime->Send(new IEventHandle(Proxy, Sender, request.Release()));
            UNIT_ASSERT(Runtime->GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(Sender, TDuration::Seconds(120)));
            Ping();
        }

        // The ids of the running system token service and delegation service, both empty or both set.
        std::pair<TActorId, TActorId> Services() {
            const auto result = std::pair{Runtime->GetLocalServiceId(MakeIamSystemTokenServiceId(), 0),
                Runtime->GetLocalServiceId(MakeIamDelegationServiceId(), 0)};
            UNIT_ASSERT_VALUES_EQUAL(bool(result.first), bool(result.second));
            return result;
        }

        bool Started() {
            return bool(Services().first);
        }
    };

    NKikimrConfig::TAppConfig FullConfig() {
        NKikimrConfig::TAppConfig appConfig;
        auto& iam = *appConfig.MutableIamConfig();
        iam.SetTokenServiceEndpoint("localhost:1"); // never called: only the registration is checked
        iam.SetServiceControlEndpoint("localhost:1");
        iam.SetServiceId("ydb");
        iam.SetMicroserviceId("data-plane");
        iam.SetResourceType("resource-manager.cloud");
        return appConfig;
    }

    Y_UNIT_TEST(NothingWithoutTheFlag) {
        UNIT_ASSERT(!TProxyNode(FullConfig(), false).Started());
    }

    Y_UNIT_TEST(NothingWithoutTheIdentity) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableIamConfig()->SetServiceControlEndpoint("localhost:1");
        UNIT_ASSERT(!TProxyNode(appConfig, true).Started());
    }

    Y_UNIT_TEST(NothingWithoutTheControlPlane) {
        auto appConfig = FullConfig();
        appConfig.MutableIamConfig()->ClearServiceControlEndpoint();
        UNIT_ASSERT(!TProxyNode(appConfig, true).Started());
    }

    Y_UNIT_TEST(BothWithFullConfig) {
        UNIT_ASSERT(TProxyNode(FullConfig(), true).Started());
    }

    Y_UNIT_TEST(BothWithIdentityFromTheReplicationSection) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableIamConfig()->SetServiceControlEndpoint("localhost:1");
        auto& replication = *appConfig.MutableReplicationConfig()->MutableIamServiceControl();
        replication.SetEndpoint("localhost:1");
        replication.SetServiceId("ydb");
        replication.SetMicroserviceId("data-plane");
        replication.SetResourceType("resource-manager.cloud");
        UNIT_ASSERT(TProxyNode(appConfig, true).Started());
    }

    // A config notification turning the flag on starts the services, a second one changes nothing, and
    // turning the flag off is not followed.
    Y_UNIT_TEST(ConfigNotificationWithTheFlagStartsTheServices) {
        TProxyNode node(FullConfig(), false);
        UNIT_ASSERT(!node.Started());

        node.Notify(true);
        const auto services = node.Services();
        UNIT_ASSERT(node.Started());

        node.Notify(true);
        UNIT_ASSERT(node.Services() == services);

        node.Notify(false);
        UNIT_ASSERT(node.Services() == services);
    }
}

} // namespace NKikimr::NIamDelegation
