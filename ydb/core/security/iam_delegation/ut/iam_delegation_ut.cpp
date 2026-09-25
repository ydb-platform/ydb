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

// A stand-in for the system token service. Answers TEvGetSystemToken at once with Token (or Error when it
// is set) while Deliver is true; otherwise only records the request so that the test can answer it late.
// The state is read by the test thread while the actor runs on the runtime's threads, hence the lock.
struct TSystemTokenFake : TThrRefBase {
    TMutex Mutex;
    TActorId Service; // the fake actor
    TActorId Recipient;
    ui64 Cookie = 0;
    ui32 Requests = 0;
    bool Deliver = true;
    TString Token = "ssa-token";
    TString Error;

    std::pair<TActorId, ui64> LastRequest() {
        with_lock (Mutex) {
            return {Recipient, Cookie};
        }
    }

    ui32 RequestCount() {
        with_lock (Mutex) {
            return Requests;
        }
    }

    // Delivers the system token for the last request.
    void DeliverLast(TTestActorRuntime& runtime) {
        const auto [recipient, cookie] = LastRequest();
        runtime.Send(new IEventHandle(recipient, Service, new TEvIamDelegation::TEvSystemTokenReady(Token, {}), 0, cookie));
    }

    // hang guard: the requests are made inevitable by the test
    void WaitRequests(ui32 count) {
        WaitUntil([&]() { return RequestCount() >= count; }, "the system token requests");
        UNIT_ASSERT_VALUES_EQUAL(RequestCount(), count);
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
        bool deliver = false;
        TString token;
        TString error;
        with_lock (State->Mutex) {
            State->Recipient = ev->Sender;
            State->Cookie = ev->Cookie;
            ++State->Requests;
            deliver = State->Deliver;
            token = State->Token;
            error = State->Error;
        }
        if (deliver) {
            Send(ev->Sender, new TEvIamDelegation::TEvSystemTokenReady(error ? TString() : token, error), 0, ev->Cookie);
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
        Settings.ResourceManagerEndpoint = "localhost:" + ToString(IamPort);
        Settings.EnableSsl = false;
        Settings.ServiceId = "ydb";
        Settings.MicroserviceId = "data-plane";
        Settings.ResourceType = "resource-manager.cloud";
        Settings.RequestTimeout = TDuration::Seconds(5);
        Settings.OperationPollInterval = TDuration::MilliSeconds(100);
        Settings.OperationPollTimeout = TDuration::Seconds(3);
        Settings.MaxRetries = 3;
        UNIT_ASSERT_VALUES_EQUAL(Settings.ValidateForDelegation(), "");

        ServiceControlMock.ExpectedAuthorization = "Bearer ssa-token";
    }

    ~TFixture() {
        IamServer->Shutdown();
    }

    template <class TCondition>
    void WaitUntil(TCondition condition, TStringBuf what, TDuration timeout = TDuration::Seconds(120)) {
        ::NKikimr::NIamDelegation::WaitUntil(std::move(condition), what, timeout);
    }

    // Registers a fake system token service answering every request with the token at once.
    TIntrusivePtr<TSystemTokenFake> StaticSystemToken(const TString& token = "ssa-token") {
        auto fake = MakeIntrusive<TSystemTokenFake>();
        fake->Token = token;
        fake->Service = Runtime->Register(new TFakeSystemTokenService(fake));
        return fake;
    }

    // Registers a fake system token service failing every request with the error (the metadata service is down).
    TIntrusivePtr<TSystemTokenFake> FailingSystemToken(const TString& error) {
        auto fake = StaticSystemToken();
        fake->Error = error;
        return fake;
    }

    // Registers a fake system token service that records the requests and answers none until told to.
    TIntrusivePtr<TSystemTokenFake> SilentSystemToken() {
        auto fake = StaticSystemToken();
        fake->Deliver = false;
        return fake;
    }

    TActorId StartDelegationService(TActorId systemTokenService = {}) {
        if (!systemTokenService) {
            systemTokenService = StaticSystemToken()->Service;
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

    TDelegationResult Setup(const TActorId& service, const TDelegationSpec& spec, const TString& subject = "user-1") {
        Runtime->Send(new IEventHandle(service, Sender, new TEvIamDelegation::TEvSetupDelegation(spec, subject), 0, 42));
        TAutoPtr<IEventHandle> handle;
        auto* result = Runtime->GrabEdgeEvent<TEvIamDelegation::TEvSetupDelegationResult>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 42u);
        return result->Result;
    }

    TDelegationResult Revoke(const TActorId& service, const TDelegationSpec& spec) {
        Runtime->Send(new IEventHandle(service, Sender, new TEvIamDelegation::TEvRevokeDelegation(spec), 0, 43));
        TAutoPtr<IEventHandle> handle;
        auto* result = Runtime->GrabEdgeEvent<TEvIamDelegation::TEvRevokeDelegationResult>(handle);
        UNIT_ASSERT(result);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 43u);
        return result->Result;
    }

    // Sends a tracked event to a (dead) actor and waits for the undelivered notification: the deterministic
    // proof that the actor is gone and that the actor system still runs.
    void ExpectUndelivered(const TActorId& actor, IEventBase* event) {
        Runtime->Send(new IEventHandle(actor, Sender, event, IEventHandle::FlagTrackDelivery));
        TAutoPtr<IEventHandle> handle;
        auto* undelivered = Runtime->GrabEdgeEvent<TEvents::TEvUndelivered>(handle);
        UNIT_ASSERT(undelivered);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(IamDelegationService) {
    Y_UNIT_TEST(SetupDone) {
        TFixture f;
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);
        const auto call = f.ServiceControlMock.LastCall();
        UNIT_ASSERT_VALUES_EQUAL(call.Setup.service_id(), "ydb");
        UNIT_ASSERT_VALUES_EQUAL(call.Setup.microservice_id(), "data-plane");
        UNIT_ASSERT_VALUES_EQUAL(call.Setup.resource().id(), "cloud-1");
        UNIT_ASSERT_VALUES_EQUAL(call.Setup.resource().type(), "resource-manager.cloud");
        UNIT_ASSERT_VALUES_EQUAL(call.Setup.target_service_account_id(), "sa-1");
        UNIT_ASSERT_VALUES_EQUAL(call.Setup.referrer().id(), "ref-1");
        UNIT_ASSERT_VALUES_EQUAL(call.Setup.referrer().type(), "ydb.secret");
        UNIT_ASSERT_VALUES_EQUAL(call.Setup.on_behalf_of_subject_id(), "user-1");
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 0u);
    }

    Y_UNIT_TEST(SetupPollsOperation) {
        TFixture f;
        f.ServiceControlMock.NotDoneCount = 1;
        f.OperationMock.GetsUntilDone = 3;
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 3u);
    }

    Y_UNIT_TEST(OperationFailure) {
        TFixture f;
        f.ServiceControlMock.NotDoneCount = 1;
        f.OperationMock.OperationError = "user is not allowed to delegate";
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "user is not allowed to delegate");
    }

    Y_UNIT_TEST(OperationPollTimeout) {
        TFixture f;
        f.ServiceControlMock.NotDoneCount = 1;
        f.OperationMock.GetsUntilDone = NEVER_DONE_OPERATION;
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::TIMEOUT);
    }

    Y_UNIT_TEST(CloudMismatchIsExplained) {
        TFixture f;
        f.ServiceControlMock.FailCount = 1;
        f.ServiceControlMock.FailStatus = grpc::StatusCode::FAILED_PRECONDITION;
        f.ServiceControlMock.FailureType = "BAD_SERVICE_ACCOUNT_CLOUD";
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::BAD_REQUEST);
        const TString issues = result.Issues.ToOneLineString();
        UNIT_ASSERT_STRING_CONTAINS(issues, "BAD_SERVICE_ACCOUNT_CLOUD (injected BAD_SERVICE_ACCOUNT_CLOUD)");
        UNIT_ASSERT_STRING_CONTAINS(issues, "set RESOURCE to the cloud of the service account");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);
    }

    Y_UNIT_TEST(ServiceNotEnabledInOperationIsExplained) {
        TFixture f;
        f.ServiceControlMock.OperationErrorCount = 1;
        f.ServiceControlMock.FailureType = "SERVICE_NOT_ENABLED";
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT(!result.IsSuccess());
        const TString issues = result.Issues.ToOneLineString();
        UNIT_ASSERT_STRING_CONTAINS(issues, "injected operation error; SERVICE_NOT_ENABLED");
        UNIT_ASSERT_STRING_CONTAINS(issues, "YDB service is not enabled");
    }

    Y_UNIT_TEST(SystemTokenFailure) {
        TFixture f;
        const auto service = f.StartDelegationService(f.FailingSystemToken("metadata is down")->Service);

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "metadata is down");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 0u);
    }

    Y_UNIT_TEST(RevokeAndNotFound) {
        TFixture f;
        const auto service = f.StartDelegationService();

        auto result = f.Revoke(service, f.Spec());
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.RevokeCalls(), 1u);
        const auto call = f.ServiceControlMock.LastCall();
        UNIT_ASSERT_VALUES_EQUAL(call.Method, "RevokeDelegation");
        UNIT_ASSERT_VALUES_EQUAL(call.Revoke.referrer().id(), "ref-1");
        UNIT_ASSERT_VALUES_EQUAL(call.Revoke.target_service_account_id(), "sa-1");

        // an already revoked delegation is not an error
        f.ServiceControlMock.FailCount = 1;
        f.ServiceControlMock.FailStatus = grpc::StatusCode::NOT_FOUND;
        result = f.Revoke(service, f.Spec());
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
    }

    Y_UNIT_TEST(PoisonWhileWaiting) {
        TFixture f;
        f.ServiceControlMock.NotDoneCount = 1;
        f.OperationMock.GetsUntilDone = NEVER_DONE_OPERATION;
        const auto service = f.StartDelegationService();

        // the request polls the never-done operation; the actor is poisoned in the middle of it
        f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvIamDelegation::TEvSetupDelegation(f.Spec(), "user-1")));
        f.WaitUntil([&]() { return f.OperationMock.GetCalls.load() > 0; }, "the first poll");
        f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvents::TEvPoison()));
        // the actor is gone without a crash: a tracked event to it comes back undelivered, and a fresh
        // service on the same runtime works
        f.ExpectUndelivered(service, new TEvIamDelegation::TEvSetupDelegation(f.Spec(), "user-1"));
        f.OperationMock.GetsUntilDone = 1;
        const auto fresh = f.StartDelegationService();
        UNIT_ASSERT_C(f.Setup(fresh, f.Spec("sa-2", "ref-2")).IsSuccess(), "a fresh service must work after the poison");
    }

    Y_UNIT_TEST(SetupDoneOperationCarriesError) {
        TFixture f;
        f.ServiceControlMock.OperationErrorCount = 1;
        const auto service = f.StartDelegationService();

        // the operation is done at once but carries an error: no polling, the error is mapped
        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "injected operation error");
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);
    }

    Y_UNIT_TEST(SystemTokenFailureIsRetried) {
        // a failure to obtain the system token counts as a retryable failure of the call
        TFixture f;
        f.Settings.RequestTimeout = TDuration::Seconds(1);
        f.Settings.MaxRetries = 2;
        auto source = f.SilentSystemToken();
        const auto service = f.StartDelegationService(source->Service);

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "failed after 2 attempts: timeout while obtaining the system service account token");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(source->RequestCount(), 2u);
    }

    Y_UNIT_TEST(LateSystemTokenIsIgnored) {
        TFixture f;
        f.Settings.RequestTimeout = TDuration::Seconds(1);
        f.Settings.MaxRetries = 1;
        auto source = f.SilentSystemToken();
        const auto service = f.StartDelegationService(source->Service);

        auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);

        // the token arrives after the timeout: it reaches the state function and is ignored. The mailbox is
        // FIFO, so the Setup below is handled after the late token; it is the one and only ServiceControl call.
        const auto [recipient, cookie] = source->LastRequest();
        UNIT_ASSERT_VALUES_EQUAL(recipient, service);
        f.Runtime->Send(new IEventHandle(recipient, f.Sender, new TEvIamDelegation::TEvSystemTokenReady("ssa-token", {}), 0, cookie));

        // the source recovers: a normal Setup works
        with_lock (source->Mutex) {
            source->Deliver = true;
        }
        result = f.Setup(service, f.Spec());
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(source->RequestCount(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.LastCall().Setup.on_behalf_of_subject_id(), "user-1");
    }

    Y_UNIT_TEST(RetryableStatusesAreRetried) {
        TFixture f;
        const auto service = f.StartDelegationService();

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
            const auto result = f.Setup(service, f.Spec());
            UNIT_ASSERT_C(result.IsSuccess(), "status " << status << ": " << result.Issues.ToOneLineString());
            expectedCalls += 2;
            UNIT_ASSERT_VALUES_EQUAL_C(f.ServiceControlMock.SetupCalls(), expectedCalls, "status " << status);
        }

        struct TNonRetryable {
            grpc::StatusCode Grpc;
            Ydb::StatusIds::StatusCode Expected;
        };
        for (const auto& [grpcStatus, expected] : {
            TNonRetryable{grpc::StatusCode::INVALID_ARGUMENT, Ydb::StatusIds::BAD_REQUEST},
            TNonRetryable{grpc::StatusCode::NOT_FOUND, Ydb::StatusIds::NOT_FOUND},
            TNonRetryable{grpc::StatusCode::UNAUTHENTICATED, Ydb::StatusIds::UNAUTHORIZED},
            TNonRetryable{grpc::StatusCode::PERMISSION_DENIED, Ydb::StatusIds::UNAUTHORIZED},
            TNonRetryable{grpc::StatusCode::FAILED_PRECONDITION, Ydb::StatusIds::BAD_REQUEST},
        }) {
            f.ServiceControlMock.FailStatus = grpcStatus;
            f.ServiceControlMock.FailCount = 1;
            const auto result = f.Setup(service, f.Spec());
            UNIT_ASSERT_VALUES_EQUAL_C(result.Status, expected, "status " << grpcStatus << ": " << result.Issues.ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "injected failure");
            expectedCalls += 1;
            UNIT_ASSERT_VALUES_EQUAL_C(f.ServiceControlMock.SetupCalls(), expectedCalls, "status " << grpcStatus);
        }
    }

    Y_UNIT_TEST(RetriesExhaustedMapsLastStatus) {
        TFixture f;
        const auto service = f.StartDelegationService();

        f.ServiceControlMock.FailStatus = grpc::StatusCode::RESOURCE_EXHAUSTED;
        f.ServiceControlMock.FailCount = 100;
        auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::OVERLOADED);
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "failed after 3 attempts");

        f.ServiceControlMock.FailStatus = grpc::StatusCode::DEADLINE_EXCEEDED;
        f.ServiceControlMock.FailCount = 100;
        result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::TIMEOUT);
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "failed after 3 attempts");
    }

    // MaxRetries bounds the number of attempts of one call: every retry is one more ServiceControl call with
    // the same request id, and with MaxRetries = 1 the first failure is final
    Y_UNIT_TEST(RetryCountBounds) {
        TFixture f;
        f.ServiceControlMock.FailCount = 2;
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 3u);

        f.Settings.MaxRetries = 1;
        f.ServiceControlMock.FailCount = 100;
        const auto single = f.Runtime->Register(CreateIamDelegationService(f.Settings, f.StaticSystemToken()->Service));
        const auto failed = f.Setup(single, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(failed.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(failed.Issues.ToOneLineString(), "failed after 1 attempts");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 4u);

        // MaxRetries = 0 still makes the one attempt, and the error says so
        f.Settings.MaxRetries = 0;
        const auto none = f.Runtime->Register(CreateIamDelegationService(f.Settings, f.StaticSystemToken()->Service));
        const auto failedToo = f.Setup(none, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(failedToo.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(failedToo.Issues.ToOneLineString(), "failed after 1 attempts");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 5u);
    }

    Y_UNIT_TEST(RevokePollsOperation) {
        TFixture f;
        f.ServiceControlMock.NotDoneCount = 1;
        f.OperationMock.GetsUntilDone = 2;
        const auto service = f.StartDelegationService();

        const auto result = f.Revoke(service, f.Spec());
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.OperationMock.GetCalls.load(), 2u);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.RevokeCalls(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.LastCall().Method, "RevokeDelegation");
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
        const auto delegation = f.StartDelegationService(service);
        const auto setup = f.Setup(delegation, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(setup.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(setup.Issues.ToOneLineString(), "timeout while obtaining the system service account token");
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
        source->WaitRequests(1); // the first service is now parked on the system token

        const auto result = f.Setup(other, f.Spec("sa-2", "ref-2"));
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u); // the first service is still parked
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.LastCall().Setup.target_service_account_id(), "sa-2");

        source->DeliverLast(*f.Runtime);
        TAutoPtr<IEventHandle> handle;
        auto* setup = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvSetupDelegationResult>(handle);
        UNIT_ASSERT(setup);
        UNIT_ASSERT_C(setup->Result.IsSuccess(), setup->Result.Issues.ToOneLineString());
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

        const auto delegation = f.StartDelegationService(service);
        const auto setup = f.Setup(delegation, f.Spec());
        UNIT_ASSERT_C(setup.IsSuccess(), setup.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u); // the mock requires "Bearer ssa-token"
        with_lock (metadata.Mutex) {
            UNIT_ASSERT(metadata.Requests >= 1);
            UNIT_ASSERT_VALUES_EQUAL(metadata.LastPath, "/computeMetadata/v1/instance/service-accounts/default/token");
            UNIT_ASSERT_VALUES_EQUAL(metadata.LastFlavor, "Google");
        }
    }
}

// The KQP proxy starts the system token service and the delegation service at its bootstrap according to the
// feature flag and the configuration: no flag - nothing; incomplete identity or no control plane endpoint -
// nothing (with a warning); both when everything is there, including when the identity comes from the
// replication section. The flag is followed at runtime in one direction: a config notification turning it on
// starts the services.
Y_UNIT_TEST_SUITE(IamDelegationProxyRegistration) {
    struct TCase {
        bool Flag = true;
        NKikimrConfig::TIamConfig Iam;
        NKikimrReplication::TReplicationDefaults::TIamServiceControl Replication;
        bool Started = false; // the system token service and the delegation service, or neither
    };

    void Check(const TCase& c, TStringBuf what) {
        TPortManager portManager;
        NKikimrConfig::TAppConfig appConfig;
        *appConfig.MutableIamConfig() = c.Iam;
        *appConfig.MutableReplicationConfig()->MutableIamServiceControl() = c.Replication;
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableIamDelegationSecrets(c.Flag);
        auto settings = TServerSettings(portManager.GetPort(2134));
        settings.SetDomainName("Root").SetAppConfig(appConfig).SetFeatureFlags(featureFlags);
        TServer server(settings);
        auto* runtime = server.GetRuntime();

        // the proxy registers the services in its Bootstrap; a request it has answered proves Bootstrap ran (FIFO)
        const auto sender = runtime->AllocateEdgeActor();
        runtime->Send(new IEventHandle(NKqp::MakeKqpProxyID(runtime->GetNodeId(0)), sender, new NKqp::TEvKqp::TEvCreateSessionRequest()));
        UNIT_ASSERT_C(runtime->GrabEdgeEvent<NKqp::TEvKqp::TEvCreateSessionResponse>(sender, TDuration::Seconds(120)), what);

        UNIT_ASSERT_VALUES_EQUAL_C(bool(runtime->GetLocalServiceId(MakeIamDelegationServiceId(), 0)), c.Started, what);
        UNIT_ASSERT_VALUES_EQUAL_C(bool(runtime->GetLocalServiceId(MakeIamSystemTokenServiceId(), 0)), c.Started, what);
    }

    NKikimrConfig::TIamConfig FullIamConfig() {
        NKikimrConfig::TIamConfig iam;
        iam.SetTokenServiceEndpoint("localhost:1"); // never called: only the registration is checked
        iam.SetServiceControlEndpoint("localhost:1");
        iam.SetServiceId("ydb");
        iam.SetMicroserviceId("data-plane");
        iam.SetResourceType("resource-manager.cloud");
        return iam;
    }

    Y_UNIT_TEST(NothingWithoutTheFlag) {
        Check({.Flag = false, .Iam = FullIamConfig()}, "flag off");
    }

    Y_UNIT_TEST(NothingWithoutTheIdentity) {
        NKikimrConfig::TIamConfig iam;
        iam.SetServiceControlEndpoint("localhost:1");
        Check({.Iam = iam}, "no token service endpoint anywhere");
    }

    Y_UNIT_TEST(NothingWithoutTheControlPlane) {
        auto iam = FullIamConfig();
        iam.ClearServiceControlEndpoint();
        Check({.Iam = iam}, "no control plane endpoint");
    }

    Y_UNIT_TEST(BothWithFullConfig) {
        Check({.Iam = FullIamConfig(), .Started = true}, "full IamConfig");
    }

    // Turning the feature flag on at runtime starts the services: a config notification with the flag delivers
    // them, a second one changes nothing (each registered once). Turning the flag off is not followed.
    Y_UNIT_TEST(ConfigNotificationWithTheFlagStartsTheServices) {
        TPortManager portManager;
        NKikimrConfig::TAppConfig appConfig;
        *appConfig.MutableIamConfig() = FullIamConfig();
        NKikimrConfig::TFeatureFlags featureFlags; // the flag is off at bootstrap
        auto settings = TServerSettings(portManager.GetPort(2134));
        settings.SetDomainName("Root").SetAppConfig(appConfig).SetFeatureFlags(featureFlags);
        TServer server(settings);
        auto* runtime = server.GetRuntime();
        const auto proxy = NKqp::MakeKqpProxyID(runtime->GetNodeId(0));
        const auto sender = runtime->AllocateEdgeActor();

        runtime->Send(new IEventHandle(proxy, sender, new NKqp::TEvKqp::TEvCreateSessionRequest()));
        UNIT_ASSERT(runtime->GrabEdgeEvent<NKqp::TEvKqp::TEvCreateSessionResponse>(sender, TDuration::Seconds(120)));
        UNIT_ASSERT(!runtime->GetLocalServiceId(MakeIamDelegationServiceId(), 0));
        UNIT_ASSERT(!runtime->GetLocalServiceId(MakeIamSystemTokenServiceId(), 0));

        // The proxy acknowledges a notification before it re-initializes the services, in the same handler;
        // a request it has answered afterwards proves the re-initialization ran (FIFO).
        const auto notify = [&](const NKikimrConfig::TFeatureFlags& flags) {
            auto request = MakeHolder<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            *request->Record.MutableConfig()->MutableFeatureFlags() = flags;
            runtime->Send(new IEventHandle(proxy, sender, request.Release()));
            UNIT_ASSERT(runtime->GrabEdgeEvent<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender, TDuration::Seconds(120)));
            runtime->Send(new IEventHandle(proxy, sender, new NKqp::TEvKqp::TEvCreateSessionRequest()));
            UNIT_ASSERT(runtime->GrabEdgeEvent<NKqp::TEvKqp::TEvCreateSessionResponse>(sender, TDuration::Seconds(120)));
        };

        featureFlags.SetEnableIamDelegationSecrets(true);
        notify(featureFlags);
        const auto systemTokenService = runtime->GetLocalServiceId(MakeIamSystemTokenServiceId(), 0);
        const auto delegationService = runtime->GetLocalServiceId(MakeIamDelegationServiceId(), 0);
        UNIT_ASSERT(systemTokenService && delegationService);

        notify(featureFlags);
        UNIT_ASSERT_VALUES_EQUAL(runtime->GetLocalServiceId(MakeIamSystemTokenServiceId(), 0), systemTokenService);
        UNIT_ASSERT_VALUES_EQUAL(runtime->GetLocalServiceId(MakeIamDelegationServiceId(), 0), delegationService);

        featureFlags.SetEnableIamDelegationSecrets(false);
        notify(featureFlags);
        UNIT_ASSERT_VALUES_EQUAL(runtime->GetLocalServiceId(MakeIamSystemTokenServiceId(), 0), systemTokenService);
        UNIT_ASSERT_VALUES_EQUAL(runtime->GetLocalServiceId(MakeIamDelegationServiceId(), 0), delegationService);
    }

    Y_UNIT_TEST(BothWithIdentityFromTheReplicationSection) {
        NKikimrConfig::TIamConfig iam;
        iam.SetServiceControlEndpoint("localhost:1");
        NKikimrReplication::TReplicationDefaults::TIamServiceControl replication;
        replication.SetEndpoint("localhost:1");
        replication.SetServiceId("ydb");
        replication.SetMicroserviceId("data-plane");
        replication.SetResourceType("resource-manager.cloud");
        Check({.Iam = iam, .Replication = replication, .Started = true}, "identity from ReplicationConfig");
    }
}

} // namespace NKikimr::NIamDelegation
