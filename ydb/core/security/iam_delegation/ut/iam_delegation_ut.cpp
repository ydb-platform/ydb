#include <ydb/core/base/iam_delegation.h>
#include <ydb/core/security/iam_delegation/iam_actor_base.h>
#include <ydb/core/security/iam_delegation/iam_delegation_service.h>
#include <ydb/core/security/iam_delegation/settings.h>

#include <ydb/core/cms/console/console.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/testlib/service_mocks/operation_service_mock.h>
#include <ydb/library/testlib/service_mocks/service_control_service_mock.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>


#include <grpcpp/server_builder.h>

namespace NKikimr::NIamDelegation {

using namespace NActors;
using namespace Tests;

namespace {

// Subscription stand-in for the gRPC integration tests. Deterministic update, timeout and
// shutdown coverage lives in iam_delegation_actor_ut.cpp.
class TFakeTokenManager : public TActor<TFakeTokenManager> {
public:
    explicit TFakeTokenManager(TString error = {})
        : TActor(&TThis::StateWork)
        , Error(std::move(error))
    {}

    STRICT_STFUNC(StateWork,
        hFunc(TEvTokenManager::TEvSubscribeUpdateToken, Handle);
        cFunc(TEvents::TEvPoison::EventType, PassAway);
    )

private:
    void Handle(TEvTokenManager::TEvSubscribeUpdateToken::TPtr& ev) {
        const TEvTokenManager::TStatus status{Error.empty() ? TEvTokenManager::TStatus::ECode::SUCCESS : TEvTokenManager::TStatus::ECode::ERROR, Error};
        Send(ev->Sender, new TEvTokenManager::TEvUpdateToken(ev->Get()->Id, "ssa-token", status));
    }

    const TString Error;
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

        Settings.Config.SetTokenServiceEndpoint("localhost:" + ToString(IamPort));
        Settings.Config.SetServiceControlEndpoint("localhost:" + ToString(IamPort));
        Settings.Config.SetEnableSsl(false);
        Settings.Config.SetServiceId("ydb");
        Settings.Config.SetMicroserviceId("data-plane");
        Settings.Config.SetResourceType("resource-manager.cloud");
        Settings.Config.SetSystemTokenName("delegation-system-account");

        ServiceControlMock.ExpectedAuthorization = "Bearer ssa-token";
    }

    ~TFixture() {
        IamServer->Shutdown();
        IamServer->Wait();
    }

    TActorId StartDelegationService(const TString& tokenError = {}) {
        Runtime->RegisterService(MakeTokenManagerID(), Runtime->Register(new TFakeTokenManager(tokenError)));
        const TActorId id = Runtime->Register(CreateIamDelegationService(Settings));
        Runtime->RegisterService(MakeIamDelegationServiceId(), id);
        return id;
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
        auto* result = Runtime->GrabEdgeEvent<TResultEv>(handle, TDuration::Seconds(120));
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
        ExpectError(f.Setup(), Ydb::StatusIds::OVERLOADED, "failed after 5 attempts");

        f.ServiceControlMock.FailStatus = grpc::StatusCode::DEADLINE_EXCEEDED;
        f.ServiceControlMock.FailCount = 100;
        ExpectError(f.Setup(), Ydb::StatusIds::TIMEOUT, "failed after 5 attempts");
    }

    Y_UNIT_TEST(RetryCountBounds) {
        TFixture f;
        f.ServiceControlMock.FailCount = TIamDelegationSettings::MaxRetries - 1;
        ExpectSuccess(f.Setup());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), TIamDelegationSettings::MaxRetries);
        f.ServiceControlMock.FailCount = 100;
        ExpectError(f.Setup(), Ydb::StatusIds::UNAVAILABLE, "failed after 5 attempts");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 2 * TIamDelegationSettings::MaxRetries);
    }

    Y_UNIT_TEST(SystemTokenFailure) {
        TFixture f;
        ExpectError(f.Setup(f.StartDelegationService("metadata is down"), f.Spec()),
            Ydb::StatusIds::UNAVAILABLE, "metadata is down");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 0u);
    }

}

// The proxy starts the delegation service at bootstrap or on dynamic feature enable, using the
// independently initialized token manager. Repeated notifications must preserve the same actor.
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
            auto settings = TServerSettings(PortManager.GetPort(2134), appConfig.GetAuthConfig());
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

        TActorId Service() {
            return Runtime->GetLocalServiceId(MakeIamDelegationServiceId(), 0);
        }

        bool Started() {
            return bool(Service());
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
        iam.SetSystemTokenName("delegation-system-account");
        appConfig.MutableAuthConfig()->MutableTokenManager()->SetEnable(true);
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

    Y_UNIT_TEST(NothingWithoutSystemTokenName) {
        auto appConfig = FullConfig();
        appConfig.MutableIamConfig()->ClearSystemTokenName();
        // An AccessService identity is not an implicit delegation identity.
        appConfig.MutableAuthConfig()->SetAccessServiceTokenName("access-service-account");
        UNIT_ASSERT(!TProxyNode(appConfig, true).Started());
    }

    Y_UNIT_TEST(NothingWithDisabledTokenManager) {
        auto appConfig = FullConfig();
        appConfig.MutableAuthConfig()->MutableTokenManager()->SetEnable(false);
        UNIT_ASSERT(!TProxyNode(appConfig, true).Started());
    }

    Y_UNIT_TEST(StartsWithFullConfig) {
        UNIT_ASSERT(TProxyNode(FullConfig(), true).Started());
    }

    Y_UNIT_TEST(StartsWithIdentityFromTheReplicationSection) {
        NKikimrConfig::TAppConfig appConfig;
        appConfig.MutableIamConfig()->SetServiceControlEndpoint("localhost:1");
        appConfig.MutableIamConfig()->SetSystemTokenName("delegation-system-account");
        appConfig.MutableAuthConfig()->MutableTokenManager()->SetEnable(true);
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
        const auto services = node.Service();
        UNIT_ASSERT(node.Started());

        node.Notify(true);
        UNIT_ASSERT(node.Service() == services);

        node.Notify(false);
        UNIT_ASSERT(node.Service() == services);
    }
}

} // namespace NKikimr::NIamDelegation
