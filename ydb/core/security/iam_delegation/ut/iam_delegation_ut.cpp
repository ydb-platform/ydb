#include <ydb/core/security/iam_delegation/cloud_resolver.h>
#include <ydb/core/security/iam_delegation/events.h>
#include <ydb/core/security/iam_delegation/iam_actor_base.h>
#include <ydb/core/security/iam_delegation/iam_delegated_token_service.h>
#include <ydb/core/security/iam_delegation/iam_delegation_service.h>
#include <ydb/core/security/iam_delegation/services.h>
#include <ydb/core/security/iam_delegation/settings.h>
#include <ydb/core/security/iam_delegation/system_token_service.h>

#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/events/events.h>
#include <ydb/core/kqp/common/simple/services.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/testlib/service_mocks/folder_service_mock.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>
#include <ydb/library/testlib/service_mocks/operation_service_mock.h>
#include <ydb/library/testlib/service_mocks/service_account_service_mock.h>
#include <ydb/library/testlib/service_mocks/service_control_service_mock.h>
#include <ydb/library/ycloud/api/folder_service.h>
#include <ydb/library/ycloud/api/service_account_service.h>
#include <ydb/library/ycloud/impl/folder_service.h>
#include <ydb/library/ycloud/impl/service_account_service.h>

#include <library/cpp/http/misc/parsed_request.h>
#include <library/cpp/http/server/http.h>
#include <library/cpp/http/server/response.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <util/generic/guid.h>
#include <util/network/sock.h>
#include <util/stream/file.h>

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

// Runs ResolveCloud inside an actor and reports the outcome to the edge actor.
class TResolveCloudProbe : public TActorBootstrapped<TResolveCloudProbe> {
public:
    struct TEvResult : TEventLocal<TEvResult, EventSpaceBegin(TEvents::ES_PRIVATE)> {
        TDelegationResult Result;
        TResolvedCloud Resolved;
    };

    TResolveCloudProbe(TIamDelegationSettings settings, TString userToken, TString serviceAccountId, const TActorId& replyTo)
        : Settings(std::move(settings))
        , UserToken(std::move(userToken))
        , ServiceAccountId(std::move(serviceAccountId))
        , ReplyTo(replyTo)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        auto result = std::make_unique<TEvResult>();
        try {
            result->Resolved = co_await ResolveCloud(Settings, UserToken, ServiceAccountId);
        } catch (const TIamCallError& e) {
            result->Result = TDelegationResult::Error(e.Status, e.what());
        }
        Send(ReplyTo, result.release());
        Become(&TThis::StateDying);
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        // late replies of the client actors
        IgnoreFunc(NCloud::TEvServiceAccountService::TEvGetServiceAccountResponse);
        IgnoreFunc(NCloud::TEvFolderService::TEvResolveFoldersResponse);
        IgnoreFunc(TEvents::TEvUndelivered);
    )

    STFUNC(StateDying) {
        Y_UNUSED(ev); // PassAway unregisters the actor only when its coroutine tasks have unwound
    }

private:
    const TIamDelegationSettings Settings;
    const TString UserToken;
    const TString ServiceAccountId;
    const TActorId ReplyTo;
};

struct TFixture {
    TPortManager PortManager;
    THolder<TServer> Server;
    TTestActorRuntime* Runtime = nullptr;
    TActorId Sender;

    ui16 IamPort = 0;
    TServiceControlServiceMock ServiceControlMock;
    TOperationServiceMock OperationMock;
    TIamTokenServiceMock TokenMock;
    TServiceAccountServiceMock ServiceAccountMock;
    TFolderServiceMock FolderMock;
    std::unique_ptr<grpc::Server> IamServer; // hosts all the IAM services on one port; the private API has three

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
            builder.RegisterService(&TokenMock);
            builder.RegisterService(&ServiceAccountMock);
            builder.RegisterService(&FolderMock);
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
        Settings.TokenRefreshMargin = TDuration::Seconds(1);
        Settings.MaxTokenCacheLifetime = TDuration::Hours(1);
        Settings.IdleKeyTtl = TDuration::Seconds(2);
        UNIT_ASSERT_VALUES_EQUAL(Settings.ValidateForDelegation(), "");

        ServiceControlMock.ExpectedAuthorization = "Bearer ssa-token";
        TokenMock.Identity = "Bearer ssa-token";
        // these tests tell a refreshed token from the one it replaced by its value
        TokenMock.UniqueServiceTokens = true;
        // service accounts are read with the user's token; the system token is refused like IAM does
        ServiceAccountMock.Identity = "Bearer user-token";
        ServiceAccountMock.ServiceAccountData["sa-1"].set_id("sa-1");
        ServiceAccountMock.ServiceAccountData["sa-1"].set_folder_id("folder-1");
        FolderMock.Folders["folder-1"].set_id("folder-1");
        FolderMock.Folders["folder-1"].set_cloud_id("cloud-1");
    }

    ~TFixture() {
        // a call held by the token mock must be released before the gRPC server shuts down
        TokenMock.ReleaseHeldServiceTokenCalls();
        IamServer->Shutdown();
    }

    // Sensors of the delegated token service of the node (ydb/core/security/iam_delegation/iam_delegated_token_service.cpp)
    i64 TokenSensor(const char* name, bool derivative = true) {
        return GetServiceCounters(Runtime->GetAppData().Counters, "iam_delegation")
            ->GetSubgroup("component", "token_service")->GetCounter(name, derivative)->Val();
    }

    template <class TCondition>
    void WaitUntil(TCondition condition, TStringBuf what, TDuration timeout = TDuration::Seconds(120)) {
        ::NKikimr::NIamDelegation::WaitUntil(std::move(condition), what, timeout);
    }

    // Resolves the cloud of the service account with the given user token from a probe actor and returns the outcome.
    TResolveCloudProbe::TEvResult::TPtr ResolveCloud(const TString& sa, const TString& userToken = "user-token") {
        Runtime->Register(new TResolveCloudProbe(Settings, userToken, sa, Sender));
        auto ev = Runtime->GrabEdgeEvent<TResolveCloudProbe::TEvResult>(Sender, TDuration::Seconds(30));
        UNIT_ASSERT(ev);
        return ev;
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

    TActorId StartTokenService(TActorId systemTokenService = {}) {
        if (!systemTokenService) {
            systemTokenService = StaticSystemToken()->Service;
        }
        const TActorId id = Runtime->Register(CreateIamDelegatedTokenService(Settings, systemTokenService));
        Runtime->RegisterService(MakeIamDelegatedTokenServiceId(), id);
        return id;
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

    THolder<TEvIamDelegation::TEvGetTokenResult> GetToken(const TActorId& service, const TTokenKey& key, const TActorId& sender = {}) {
        Runtime->Send(new IEventHandle(service, sender ? sender : Sender, new TEvIamDelegation::TEvGetToken(key)));
        TAutoPtr<IEventHandle> handle;
        auto* result = Runtime->GrabEdgeEvent<TEvIamDelegation::TEvGetTokenResult>(handle);
        UNIT_ASSERT(result);
        return THolder<TEvIamDelegation::TEvGetTokenResult>(static_cast<TEvIamDelegation::TEvGetTokenResult*>(handle->ReleaseBase().Release()));
    }
};

const TTokenKey KEY{"sa-1", "cloud-1"};

} // namespace

#ifdef IAM_LIVE_TESTS
// Probes of the real services against a live IAM installation instead of the mocks. Compiled only with
// -DIAM_LIVE_TESTS (see ya.make), so they are neither run nor counted in CI. Each probe needs its own
// environment and skips when it is missing:
//   IAM_LIVE_TOKEN_FILE  a file with the token the probe authenticates with (never printed)
//   IAM_LIVE_SA          the service account to resolve or delegate
//   IAM_LIVE_CLOUD       the cloud of the delegation (SetupAndRevoke, CreateForService)
//   IAM_LIVE_SUBJECT     the subject the delegation is made on behalf of (SetupAndRevoke)
//   IAM_LIVE_SC_ENDPOINT, IAM_LIVE_RM_ENDPOINT   the IAM control plane and Resource Manager (ResolveCloud)
// The services verify TLS against the endpoint host, so the probes need direct network access to the IAM
// endpoints (or a tunnel plus a hosts entry for the real names).
Y_UNIT_TEST_SUITE(IamDelegationLive) {
    static TString EnvOr(const char* name, const TString& fallback = {}) {
        const char* v = getenv(name);
        return v ? TString(v) : fallback;
    }

    // The cloud resolver against real IAM: GetServiceAccount on the control plane, then ResolveFolders on
    // Resource Manager, both with the user's token. Prints what it learns; a refusal from IAM is a valid
    // outcome, only a transport failure is a defect.
    Y_UNIT_TEST(ResolveCloudFromServiceAccountAgainstRealIam) {
        const TString tokenFile = EnvOr("IAM_LIVE_TOKEN_FILE");
        const TString sa = EnvOr("IAM_LIVE_SA");
        const TString iamEp = EnvOr("IAM_LIVE_SC_ENDPOINT");
        const TString rmEp = EnvOr("IAM_LIVE_RM_ENDPOINT");
        if (!tokenFile || !sa || !iamEp || !rmEp) {
            Cerr << "skipped: set IAM_LIVE_TOKEN_FILE, IAM_LIVE_SA, IAM_LIVE_SC_ENDPOINT, IAM_LIVE_RM_ENDPOINT" << Endl;
            return;
        }
        TString token = TFileInput(tokenFile).ReadAll();
        StripInPlace(token);

        TFixture f;
        f.Settings.ServiceControlEndpoint = iamEp;
        f.Settings.ResourceManagerEndpoint = rmEp;
        f.Settings.EnableSsl = true;
        const auto result = f.ResolveCloud(sa, token);
        Cerr << "ResolveCloud: " << result->Get()->Result.Status << " " << result->Get()->Result.Issues.ToOneLineString()
             << " folder " << result->Get()->FolderId << " cloud " << result->Get()->CloudId << Endl;
        UNIT_ASSERT_C(result->Get()->Result.Status != Ydb::StatusIds::UNAVAILABLE,
            "transport failure talking to " << iamEp << " / " << rmEp << ": " << result->Get()->Result.Issues.ToOneLineString());
    }

    // Full lifecycle against real IAM: SetupDelegation, then CreateForService for the delegated
    // account (which must now succeed where it was denied before), then RevokeDelegation.
    // Needs IAM_LIVE_SUBJECT too: the cloud subject the delegation is made on behalf of.
    // ServiceControl lives on the IAM control plane (iam.private-api.<env>:4283), a different
    // endpoint from the token service, and access to it must be opened separately.
    Y_UNIT_TEST(SetupAndRevokeDelegationAgainstRealIam) {
        const TString tokenFile = EnvOr("IAM_LIVE_TOKEN_FILE");
        const TString cloud = EnvOr("IAM_LIVE_CLOUD");
        const TString sa = EnvOr("IAM_LIVE_SA");
        const TString subject = EnvOr("IAM_LIVE_SUBJECT");
        if (!tokenFile || !cloud || !sa || !subject) {
            Cerr << "skipped: set IAM_LIVE_TOKEN_FILE, IAM_LIVE_CLOUD, IAM_LIVE_SA and IAM_LIVE_SUBJECT" << Endl;
            return;
        }
        TString systemToken = TFileInput(tokenFile).ReadAll();
        StripInPlace(systemToken);

        TFixture f;
        f.Settings.TokenServiceEndpoint = EnvOr("IAM_LIVE_ENDPOINT", "ts.private-api.cloud-preprod.yandex.net:4282");
        f.Settings.ServiceControlEndpoint = EnvOr("IAM_LIVE_SC_ENDPOINT", "iam.private-api.cloud-preprod.yandex.net:4283");
        f.Settings.EnableSsl = true;
        f.Settings.RequestTimeout = TDuration::Seconds(20);
        f.Settings.OperationPollTimeout = TDuration::Seconds(60);
        f.Settings.MaxRetries = 2;

        TDelegationSpec spec;
        spec.ServiceAccountId = sa;
        spec.CloudId = cloud;
        spec.ReferrerId = "ydb-live-probe-" + CreateGuidAsString();

        Cerr << "service control: " << f.Settings.ServiceControlEndpoint << ", cloud: " << cloud
             << ", target sa: " << sa << ", subject: " << subject
             << ", referrer: " << spec.ReferrerId << Endl;

        const auto delegation = f.StartDelegationService(f.StaticSystemToken(systemToken)->Service);
        const auto setup = f.Setup(delegation, spec, subject);
        Cerr << "SetupDelegation: " << Ydb::StatusIds::StatusCode_Name(setup.Status)
             << " " << setup.Issues.ToOneLineString() << Endl;

        if (setup.IsSuccess()) {
            // the delegation now exists, so minting a token for the target must work
            const auto tokens = f.StartTokenService(f.StaticSystemToken(systemToken)->Service);
            const auto got = f.GetToken(tokens, TTokenKey{.ServiceAccountId = sa, .CloudId = cloud});
            Cerr << "CreateForService after delegation: "
                 << Ydb::StatusIds::StatusCode_Name(got->Status)
                 << (got->Token ? TString(", token length ") + ToString(got->Token.size()) : TString())
                 << " " << got->Issues.ToOneLineString() << Endl;

            // always clean up: a live delegation must not be left behind
            const auto revoke = f.Revoke(delegation, spec);
            Cerr << "RevokeDelegation: " << Ydb::StatusIds::StatusCode_Name(revoke.Status)
                 << " " << revoke.Issues.ToOneLineString() << Endl;
            UNIT_ASSERT_C(revoke.IsSuccess(), "FAILED TO REVOKE, delegation left in IAM: "
                << revoke.Issues.ToOneLineString());
        }

        UNIT_ASSERT_C(setup.Status != Ydb::StatusIds::UNAVAILABLE,
            "transport failure talking to " << f.Settings.ServiceControlEndpoint
            << " (reachable from this host?): " << setup.Issues.ToOneLineString());
    }

    Y_UNIT_TEST(CreateForServiceAgainstRealIam) {
        const TString tokenFile = EnvOr("IAM_LIVE_TOKEN_FILE");
        const TString cloud = EnvOr("IAM_LIVE_CLOUD");
        const TString sa = EnvOr("IAM_LIVE_SA");
        if (!tokenFile || !cloud || !sa) {
            Cerr << "skipped: set IAM_LIVE_TOKEN_FILE, IAM_LIVE_CLOUD and IAM_LIVE_SA to run" << Endl;
            return;
        }
        TString systemToken = TFileInput(tokenFile).ReadAll();
        StripInPlace(systemToken);
        UNIT_ASSERT_C(systemToken, "token file is empty: " << tokenFile);

        TFixture f;
        // point the real client at the live endpoint instead of the local mock
        f.Settings.TokenServiceEndpoint = EnvOr("IAM_LIVE_ENDPOINT", "ts.private-api.cloud-preprod.yandex.net:4282");
        f.Settings.EnableSsl = true;
        f.Settings.RequestTimeout = TDuration::Seconds(20);
        f.Settings.MaxRetries = 2;
        Cerr << "endpoint: " << f.Settings.TokenServiceEndpoint
             << ", service: " << f.Settings.ServiceId << "/" << f.Settings.MicroserviceId
             << ", cloud: " << cloud << ", target sa: " << sa
             << ", system token length: " << systemToken.size() << Endl;

        const auto service = f.StartTokenService(f.StaticSystemToken(systemToken)->Service);
        const TTokenKey key{.ServiceAccountId = sa, .CloudId = cloud};
        const auto result = f.GetToken(service, key);

        Cerr << "status: " << Ydb::StatusIds::StatusCode_Name(result->Status) << Endl;
        if (result->Issues) {
            Cerr << "issues: " << result->Issues.ToOneLineString() << Endl;
        }
        if (result->Token) {
            Cerr << "GOT A TOKEN: length " << result->Token.size()
                 << ", expires at " << result->ExpiresAt << Endl;
        }
        // The point is the observed behaviour, not a pass/fail: a denial from real IAM is a valid
        // outcome and tells us which identity IAM sees. Only a transport failure is a defect here.
        UNIT_ASSERT_C(result->Status != Ydb::StatusIds::UNAVAILABLE,
            "transport failure talking to " << f.Settings.TokenServiceEndpoint
            << ": " << result->Issues.ToOneLineString());
    }
}
#endif // IAM_LIVE_TESTS

Y_UNIT_TEST_SUITE(IamDelegationSettings) {
    // The identity of YDB and the token service come from replication_config.iam_service_control when
    // IamConfig leaves them empty; IamConfig wins when it sets them; the delegation endpoints never fall back.
    Y_UNIT_TEST(FromConfigFallsBackToReplicationSection) {
        NKikimrReplication::TReplicationDefaults replication;
        auto& shared = *replication.MutableIamServiceControl();
        shared.SetEndpoint("ts.example.net:4282");
        shared.SetServiceId("ydb");
        shared.SetMicroserviceId("data-plane");
        shared.SetResourceType("resource-manager.cloud");
        shared.SetEnableSsl(false);

        NKikimrConfig::TIamConfig config;
        config.SetServiceControlEndpoint("iam.example.net:4283");
        {
            const auto settings = TIamDelegationSettings::FromConfig(config, replication);
            UNIT_ASSERT_VALUES_EQUAL(settings.TokenServiceEndpoint, "ts.example.net:4282");
            UNIT_ASSERT_VALUES_EQUAL(settings.ServiceId, "ydb");
            UNIT_ASSERT_VALUES_EQUAL(settings.MicroserviceId, "data-plane");
            UNIT_ASSERT_VALUES_EQUAL(settings.ResourceType, "resource-manager.cloud");
            UNIT_ASSERT_VALUES_EQUAL(settings.EnableSsl, false);
            UNIT_ASSERT_VALUES_EQUAL(settings.ServiceControlEndpoint, "iam.example.net:4283");
            UNIT_ASSERT_VALUES_EQUAL(settings.ResourceManagerEndpoint, "");
            UNIT_ASSERT_VALUES_EQUAL(settings.Validate(), "");
            UNIT_ASSERT_VALUES_EQUAL(settings.ValidateForDelegation(), "");
        }
        {
            config.SetTokenServiceEndpoint("ts.other.net:4282");
            config.SetServiceId("ydb-other");
            config.SetEnableSsl(true);
            const auto settings = TIamDelegationSettings::FromConfig(config, replication);
            UNIT_ASSERT_VALUES_EQUAL(settings.TokenServiceEndpoint, "ts.other.net:4282");
            UNIT_ASSERT_VALUES_EQUAL(settings.ServiceId, "ydb-other");
            UNIT_ASSERT_VALUES_EQUAL(settings.MicroserviceId, "data-plane");
            UNIT_ASSERT_VALUES_EQUAL(settings.EnableSsl, true);
        }
        {
            // nothing anywhere: the identity is missing, as without the fallback
            const auto settings = TIamDelegationSettings::FromConfig(NKikimrConfig::TIamConfig(), NKikimrReplication::TReplicationDefaults());
            UNIT_ASSERT_STRING_CONTAINS(settings.Validate(), "TokenServiceEndpoint");
        }
    }

    Y_UNIT_TEST(FromConfigCopiesIamConfig) {
        NKikimrConfig::TIamConfig config;
        config.SetTokenServiceEndpoint("ts.example.net:4282");
        config.SetServiceControlEndpoint("iam.example.net:4283");
        config.SetResourceManagerEndpoint("rm.example.net:4284");
        config.SetEnableSsl(false);
        config.SetServiceId("ydb");
        config.SetMicroserviceId("data-plane");
        config.SetResourceType("resource-manager.cloud");

        const auto settings = TIamDelegationSettings::FromConfig(config);
        // the token service, the IAM control plane and Resource Manager are different endpoints
        UNIT_ASSERT_VALUES_EQUAL(settings.TokenServiceEndpoint, "ts.example.net:4282");
        UNIT_ASSERT_VALUES_EQUAL(settings.ServiceControlEndpoint, "iam.example.net:4283");
        UNIT_ASSERT_VALUES_EQUAL(settings.ResourceManagerEndpoint, "rm.example.net:4284");
        UNIT_ASSERT_VALUES_EQUAL(settings.CanResolveCloud(), true);
        UNIT_ASSERT_VALUES_EQUAL(settings.EnableSsl, false);
        UNIT_ASSERT_VALUES_EQUAL(settings.ServiceId, "ydb");
        UNIT_ASSERT_VALUES_EQUAL(settings.MicroserviceId, "data-plane");
        UNIT_ASSERT_VALUES_EQUAL(settings.ResourceType, "resource-manager.cloud");
        UNIT_ASSERT_VALUES_EQUAL(settings.Validate(), "");
        UNIT_ASSERT_VALUES_EQUAL(settings.ValidateForDelegation(), "");

        // the constants are not affected by the config
        UNIT_ASSERT_VALUES_EQUAL(settings.ReferrerType, "ydb.secret");
        UNIT_ASSERT_VALUES_EQUAL(settings.RequestTimeout, TDuration::Seconds(10));
        UNIT_ASSERT_VALUES_EQUAL(settings.OperationPollInterval, TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(settings.OperationPollTimeout, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(settings.MaxRetries, 5u);
        UNIT_ASSERT_VALUES_EQUAL(settings.TokenRefreshMargin, TDuration::Minutes(5));
        UNIT_ASSERT_VALUES_EQUAL(settings.MaxTokenCacheLifetime, TDuration::Hours(1));
        UNIT_ASSERT_VALUES_EQUAL(settings.IdleKeyTtl, TDuration::Minutes(10));

        // SSL is on by default
        const auto defaults = TIamDelegationSettings::FromConfig(NKikimrConfig::TIamConfig());
        UNIT_ASSERT_VALUES_EQUAL(defaults.EnableSsl, true);
        UNIT_ASSERT_VALUES_EQUAL(defaults.TokenServiceEndpoint, "");
        UNIT_ASSERT_VALUES_EQUAL(defaults.ServiceControlEndpoint, "");
        UNIT_ASSERT_VALUES_EQUAL(defaults.ResourceManagerEndpoint, "");
    }

    // Resource Manager is optional: without it delegations still work, only the cloud of a
    // service account cannot be looked up and the cloud of the database is used instead.
    Y_UNIT_TEST(ResourceManagerEndpointIsOptional) {
        NKikimrConfig::TIamConfig config;
        config.SetTokenServiceEndpoint("ts.example.net:4282");
        config.SetServiceControlEndpoint("iam.example.net:4283");
        config.SetServiceId("ydb");
        config.SetMicroserviceId("data-plane");
        config.SetResourceType("resource-manager.cloud");

        const auto withoutRm = TIamDelegationSettings::FromConfig(config);
        UNIT_ASSERT_VALUES_EQUAL(withoutRm.ValidateForDelegation(), "");
        UNIT_ASSERT_VALUES_EQUAL(withoutRm.CanResolveCloud(), false);

        config.SetResourceManagerEndpoint("rm.example.net:4284");
        UNIT_ASSERT_VALUES_EQUAL(TIamDelegationSettings::FromConfig(config).CanResolveCloud(), true);

        // the lookup starts on the control plane, Resource Manager alone is not enough
        config.ClearServiceControlEndpoint();
        UNIT_ASSERT_VALUES_EQUAL(TIamDelegationSettings::FromConfig(config).CanResolveCloud(), false);
    }

    Y_UNIT_TEST(ValidateListsMissingFields) {
        const TString prefix = "IAM delegation is not configured, missing IamConfig fields:";

        TIamDelegationSettings empty;
        const TString all = empty.Validate();
        UNIT_ASSERT_C(all.StartsWith(prefix), all);
        UNIT_ASSERT_STRING_CONTAINS(all, " TokenServiceEndpoint");
        UNIT_ASSERT_STRING_CONTAINS(all, " ServiceId");
        UNIT_ASSERT_STRING_CONTAINS(all, " MicroserviceId");
        UNIT_ASSERT_STRING_CONTAINS(all, " ResourceType");

        TIamDelegationSettings settings;
        settings.TokenServiceEndpoint = "ts.example.net:4282";
        settings.ServiceId = "ydb";
        settings.ResourceType = "resource-manager.cloud";
        // MicroserviceId is part of the identity IAM checks (the agent service account is named after it)
        const TString onlyMicroservice = settings.Validate();
        UNIT_ASSERT_C(onlyMicroservice.StartsWith(prefix), onlyMicroservice);
        UNIT_ASSERT_STRING_CONTAINS(onlyMicroservice, " MicroserviceId");
        UNIT_ASSERT_C(!onlyMicroservice.Contains("Endpoint"), onlyMicroservice);
        UNIT_ASSERT_C(!onlyMicroservice.Contains("ServiceId"), onlyMicroservice);
        UNIT_ASSERT_C(!onlyMicroservice.Contains("ResourceType"), onlyMicroservice);

        settings.MicroserviceId = "data-plane";
        UNIT_ASSERT_VALUES_EQUAL(settings.Validate(), "");
    }

    // Minting a token needs only the token service, setting a delegation up also needs the IAM
    // control plane. A cluster configured only for the IAM auth of external data sources has the
    // token service endpoint and nothing else, and must keep reading the secrets it already has.
    Y_UNIT_TEST(ValidateForDelegationRequiresServiceControlEndpoint) {
        NKikimrConfig::TIamConfig config;
        config.SetTokenServiceEndpoint("ts.example.net:4282");
        config.SetServiceId("ydb");
        config.SetMicroserviceId("data-plane");
        config.SetResourceType("resource-manager.cloud");

        const auto tokenServiceOnly = TIamDelegationSettings::FromConfig(config);
        UNIT_ASSERT_VALUES_EQUAL(tokenServiceOnly.Validate(), "");

        const TString error = tokenServiceOnly.ValidateForDelegation();
        UNIT_ASSERT_C(error.StartsWith("Setting up and revoking IAM delegations is not configured,"
            " missing IamConfig fields:"), error);
        UNIT_ASSERT_STRING_CONTAINS(error, " ServiceControlEndpoint");
        UNIT_ASSERT_C(!error.Contains(" TokenServiceEndpoint"), error);
        UNIT_ASSERT_C(!error.Contains(" ServiceId"), error);
        UNIT_ASSERT_C(!error.Contains(" ResourceType"), error);

        config.SetServiceControlEndpoint("iam.example.net:4283");
        UNIT_ASSERT_VALUES_EQUAL(TIamDelegationSettings::FromConfig(config).ValidateForDelegation(), "");

        // the delegation check also reports what the token path is missing
        const TString all = TIamDelegationSettings().ValidateForDelegation();
        UNIT_ASSERT_STRING_CONTAINS(all, " TokenServiceEndpoint");
        UNIT_ASSERT_STRING_CONTAINS(all, " ServiceControlEndpoint");
        UNIT_ASSERT_STRING_CONTAINS(all, " ServiceId");
        UNIT_ASSERT_STRING_CONTAINS(all, " ResourceType");
    }
}

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

    Y_UNIT_TEST(RetryableErrorThenSuccess) {
        TFixture f;
        f.ServiceControlMock.FailCount = 2;
        f.ServiceControlMock.FailStatus = grpc::StatusCode::UNAVAILABLE;
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_C(result.IsSuccess(), result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 3u);
    }

    Y_UNIT_TEST(RetriesExhausted) {
        TFixture f;
        f.ServiceControlMock.FailCount = 100;
        f.ServiceControlMock.FailStatus = grpc::StatusCode::UNAVAILABLE;
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 3u);
    }

    Y_UNIT_TEST(PermissionDeniedIsNotRetried) {
        TFixture f;
        f.ServiceControlMock.FailCount = 1;
        f.ServiceControlMock.FailStatus = grpc::StatusCode::PERMISSION_DENIED;
        const auto service = f.StartDelegationService();

        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);
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

    Y_UNIT_TEST(SystemTokenTimeout) {
        TFixture f;
        f.Settings.RequestTimeout = TDuration::Seconds(1);
        f.Settings.MaxRetries = 1;
        auto source = f.SilentSystemToken();
        const auto service = f.StartDelegationService(source->Service);

        // the service never answers: the request fails with the timeout (RequestTimeout is what makes it inevitable)
        const auto result = f.Setup(service, f.Spec());
        UNIT_ASSERT_VALUES_EQUAL(result.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result.Issues.ToOneLineString(), "timeout while obtaining the system service account token");
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(source->RequestCount(), 1u);
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

Y_UNIT_TEST_SUITE(IamDelegatedTokenService) {
    Y_UNIT_TEST(GetTokenAndCache) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        const auto service = f.StartTokenService();

        auto result = f.GetToken(service, KEY);
        UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");
        UNIT_ASSERT(result->ExpiresAt > TInstant::Now()); // the mock issues 12 h tokens
        {
            with_lock (f.TokenMock.ServiceMutex) {
                UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceRequests.size(), 1u);
                const auto& request = f.TokenMock.CreateForServiceRequests.back();
                UNIT_ASSERT_VALUES_EQUAL(request.service_id(), "ydb");
                UNIT_ASSERT_VALUES_EQUAL(request.microservice_id(), "data-plane");
                UNIT_ASSERT_VALUES_EQUAL(request.resource_id(), "cloud-1");
                UNIT_ASSERT_VALUES_EQUAL(request.resource_type(), "resource-manager.cloud");
                UNIT_ASSERT_VALUES_EQUAL(request.target_service_account_id(), "sa-1");
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("Mints"), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("CachedKeys", false), 1);

        // cached: the same token without a new IAM call, the cookie of the request is echoed
        const TActorId sender = f.Runtime->AllocateEdgeActor();
        f.Runtime->Send(new IEventHandle(service, sender, new TEvIamDelegation::TEvGetToken(KEY), 0, 777));
        TAutoPtr<IEventHandle> handle;
        auto* reply = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvGetTokenResult>(handle);
        UNIT_ASSERT(reply);
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 777u);
        UNIT_ASSERT_VALUES_EQUAL(reply->Token, "delegated-1");
        UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceCalls.load(), 1u);
    }

    Y_UNIT_TEST(ConcurrentRequestsMintOnce) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        const auto service = f.StartTokenService();

        for (ui32 i = 0; i < 5; ++i) {
            f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvIamDelegation::TEvGetToken(KEY)));
        }
        for (ui32 i = 0; i < 5; ++i) {
            TAutoPtr<IEventHandle> handle;
            auto* result = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvGetTokenResult>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");
        }
        UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceCalls.load(), 1u);
    }

    // Five readers of one key, no cached token, IAM down: the refresh loop makes one attempt (MaxRetries calls)
    // and every waiter gets the same failure as soon as that attempt is over. The mock holds the fourth call
    // (the first call of the next attempt of the loop), so exactly three calls can have been answered.
    Y_UNIT_TEST(ConcurrentRequestsShareOneFailedAttempt) {
        TFixture f;
        f.TokenMock.ServiceTokenFailCount = 100;
        f.TokenMock.HoldServiceTokenCallsFrom = 4;
        f.Settings.MaxRetries = 3;
        f.Settings.RequestTimeout = TDuration::Minutes(5); // a held call must not time out
        const auto service = f.StartTokenService();

        for (ui32 i = 0; i < 5; ++i) {
            f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvIamDelegation::TEvGetToken(KEY)));
        }
        for (ui32 i = 0; i < 5; ++i) {
            TAutoPtr<IEventHandle> handle;
            auto* result = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvGetTokenResult>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::UNAVAILABLE, result->Issues.ToOneLineString());
            UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToOneLineString(), "failed after 3 attempts");
        }
        UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceAnswered.load(), 3u);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("MintErrors"), 1);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("Mints"), 0);
    }

    // Polls the service until it serves a token other than `previous` (hang guard: the test makes the new token inevitable).
    THolder<TEvIamDelegation::TEvGetTokenResult> WaitForNewToken(TFixture& f, const TActorId& service, const TString& previous) {
        THolder<TEvIamDelegation::TEvGetTokenResult> result;
        WaitUntil([&]() {
            result = f.GetToken(service, KEY);
            return !result->IsSuccess() || result->Token != previous;
        }, TStringBuilder() << "a token other than " << previous);
        return result;
    }

    // A token whose expiry is already within the refresh margin is refreshed while it is still valid.
    Y_UNIT_TEST(RefreshAheadOfExpiry) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.TokenMock.ServiceTokenLifetime = TDuration::Minutes(20);
        f.Settings.TokenRefreshMargin = TDuration::Minutes(30); // every token is within the margin at once
        const auto service = f.StartTokenService();

        auto first = f.GetToken(service, KEY);
        UNIT_ASSERT_VALUES_EQUAL(first->Token, "delegated-1");

        // the second mint happens on its own (after the tight-loop guard of the loop), long before the first expires
        auto second = WaitForNewToken(f, service, "delegated-1");
        UNIT_ASSERT_C(second->IsSuccess(), second->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(second->Token, "delegated-2");
        UNIT_ASSERT(first->ExpiresAt > TInstant::Now());
        UNIT_ASSERT(f.TokenSensor("Mints") >= 2);
    }

    Y_UNIT_TEST(FailureAndRecovery) {
        TFixture f;
        f.TokenMock.HoldServiceTokenCallsFrom = 2; // the second attempt waits for the test, whatever the backoff
        const auto service = f.StartTokenService();

        // no delegation: PERMISSION_DENIED
        auto result = f.GetToken(service, KEY);
        UNIT_ASSERT(!result->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("MintErrors"), 1);

        // the delegation appears: the background loop's next attempt recovers (hang guard only)
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.TokenMock.ReleaseHeldServiceTokenCalls();
        f.WaitUntil([&]() { result = f.GetToken(service, KEY); return result->IsSuccess(); }, "recovery");
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("Mints"), 1);
    }

    // The refresh fails with a retryable error while the cached token is valid: the token is served on.
    Y_UNIT_TEST(RetryableFailureKeepsCachedToken) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.Settings.MaxTokenCacheLifetime = TDuration::Seconds(1); // the loop refreshes at its tight-loop guard (5 s)
        f.Settings.IdleKeyTtl = TDuration::Minutes(10); // eviction is not what this test is about
        const auto service = f.StartTokenService();

        auto result = f.GetToken(service, KEY);
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");

        f.TokenMock.ServiceTokenFailCount = 100;
        // the reader keeps asking (so the key is in use and not evicted) and always gets the cached token
        f.WaitUntil([&]() {
            result = f.GetToken(service, KEY);
            UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1"); // the 12 h token of the mock is still valid
            return f.TokenSensor("MintErrors") >= 1;
        }, "the failed refresh");
        result = f.GetToken(service, KEY);
        UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");
        UNIT_ASSERT(f.TokenMock.CreateForServiceCalls.load() >= 2u);
    }

    // IAM refuses new tokens for the key (the delegation was revoked): once the refresh is refused the cached
    // token is dropped and requests fail.
    Y_UNIT_TEST(RevokedDelegationDropsCachedToken) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.Settings.MaxTokenCacheLifetime = TDuration::Seconds(1); // the loop refreshes at its tight-loop guard (5 s)
        const auto service = f.StartTokenService();

        auto result = f.GetToken(service, KEY);
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");

        f.TokenMock.EraseServiceToken("cloud-1", "sa-1");
        f.WaitUntil([&]() { result = f.GetToken(service, KEY); return !result->IsSuccess(); }, "the refused refresh");
        UNIT_ASSERT_VALUES_EQUAL_C(result->Status, Ydb::StatusIds::UNAUTHORIZED, result->Issues.ToOneLineString());
        UNIT_ASSERT(result->Token.empty());
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("Mints"), 1);
        UNIT_ASSERT(f.TokenSensor("MintErrors") >= 1);
    }

    // Nobody asks for the key: at the next wake-up of its loop (5 s, later than IdleKeyTtl = 2 s) the entry is
    // dropped without a mint, and a new request starts a new loop.
    // The system token is rejected at refresh time (UNAUTHENTICATED): a problem of this node, not a revocation.
    // The valid cached token is served on while the loop retries.
    Y_UNIT_TEST(UnauthenticatedRefreshKeepsCachedToken) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.Settings.MaxTokenCacheLifetime = TDuration::Seconds(1);
        f.Settings.IdleKeyTtl = TDuration::Minutes(10);
        const auto service = f.StartTokenService();

        auto result = f.GetToken(service, KEY);
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");

        f.TokenMock.ServiceTokenFailStatus = grpc::StatusCode::UNAUTHENTICATED;
        f.TokenMock.ServiceTokenFailCount = 100;
        f.WaitUntil([&]() {
            result = f.GetToken(service, KEY);
            UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
            UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");
            return f.TokenSensor("MintErrors") >= 1;
        }, "the refused refresh");
        result = f.GetToken(service, KEY);
        UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");
    }

    // A token service that answers without expires_at: the token is not served, the request fails.
    Y_UNIT_TEST(MintWithoutExpiresAtIsAnError) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.TokenMock.OmitServiceTokenExpiry = true;
        const auto service = f.StartTokenService();

        const auto result = f.GetToken(service, KEY);
        UNIT_ASSERT(!result->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToOneLineString(), "without expires_at");
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("MintErrors"), 1);
    }

    // A token service that answers with an empty token: the same.
    Y_UNIT_TEST(MintOfEmptyTokenIsAnError) {
        TFixture f;
        f.TokenMock.UniqueServiceTokens = false;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "");
        const auto service = f.StartTokenService();

        const auto result = f.GetToken(service, KEY);
        UNIT_ASSERT(!result->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::INTERNAL_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToOneLineString(), "empty token");
    }

    Y_UNIT_TEST(IdleEviction) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.Settings.MaxTokenCacheLifetime = TDuration::Seconds(1); // the loop wakes up at its tight-loop guard (5 s)
        const auto service = f.StartTokenService();

        auto result = f.GetToken(service, KEY);
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("CachedKeys", false), 1);

        f.WaitUntil([&]() { return f.TokenSensor("CachedKeys", false) == 0; }, "the idle eviction");
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("Mints"), 1); // evicted at the wake-up, not refreshed

        result = f.GetToken(service, KEY);
        UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-2");
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("Mints"), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("CachedKeys", false), 1);
    }

    // A reader that keeps asking keeps the key alive and always gets a valid token while the loop refreshes it.
    Y_UNIT_TEST(KeyInUseIsKeptFresh) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.Settings.MaxTokenCacheLifetime = TDuration::Seconds(1); // refresh at the tight-loop guard (5 s), IdleKeyTtl is 2 s
        const auto service = f.StartTokenService();

        TString last;
        f.WaitUntil([&]() {
            const auto result = f.GetToken(service, KEY);
            UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
            UNIT_ASSERT(result->ExpiresAt > TInstant::Now());
            last = result->Token;
            return f.TokenSensor("Mints") >= 3;
        }, "three mints");
        UNIT_ASSERT_VALUES_UNEQUAL(last, "delegated-1");
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("CachedKeys", false), 1);
    }

    Y_UNIT_TEST(PoisonWhileRefreshLoopParked) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        const auto service = f.StartTokenService();

        auto result = f.GetToken(service, KEY);
        UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");

        // the refresh loop is parked in its sleep (the 12 h token needs no refresh): poison cancels it
        f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvents::TEvPoison()));
        f.WaitUntil([&]() { return f.TokenSensor("CachedKeys", false) == 0; }, "the shutdown");
        // the service is gone: a tracked request comes back undelivered, nothing was minted
        f.ExpectUndelivered(service, new TEvIamDelegation::TEvGetToken(KEY));
        UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceCalls.load(), 1u);
    }

    Y_UNIT_TEST(PoisonWhileWaitersParkedOnUpdated) {
        TFixture f;
        f.TokenMock.ServiceTokenFailCount = 100;
        f.TokenMock.HoldServiceTokenCallsFrom = 1; // the mint attempt stays in flight
        f.Settings.RequestTimeout = TDuration::Minutes(5);
        const auto service = f.StartTokenService();

        // the requests wait for the mint attempt that the mock holds
        for (ui32 i = 0; i < 3; ++i) {
            f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvIamDelegation::TEvGetToken(KEY)));
        }
        f.WaitUntil([&]() { return f.TokenMock.CreateForServiceCalls.load() >= 1; }, "the held mint");
        f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvents::TEvPoison()));

        // the parked waiters are cancelled with the actor: a tracked request comes back undelivered
        f.ExpectUndelivered(service, new TEvIamDelegation::TEvGetToken(KEY));
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("CachedKeys", false), 0);
        f.TokenMock.ReleaseHeldServiceTokenCalls();
    }

    Y_UNIT_TEST(MaxTokenCacheLifetimeCapsRefresh) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.Settings.MaxTokenCacheLifetime = TDuration::Seconds(3); // the token itself lives 12h
        const auto service = f.StartTokenService();

        auto result = f.GetToken(service, KEY);
        UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-1");
        UNIT_ASSERT(result->ExpiresAt > TInstant::Now() + TDuration::Hours(1));

        // the cache lifetime triggers a refresh although the token is nowhere near its expiry
        result = WaitForNewToken(f, service, "delegated-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Token, "delegated-2");
        UNIT_ASSERT(f.TokenMock.CreateForServiceCalls.load() >= 2u);
    }

    // A flood of requests for one key while IAM is slow: one CreateForService, nobody is answered before it
    // returns, everybody is answered with its token right after. The mock holds the call until the test
    // releases it; a request for another key sent after the flood is answered in the meantime, which proves
    // (by the FIFO order of the mailbox) that the held mint stalls neither the actor nor the other keys.
    Y_UNIT_TEST(FloodOfRequestsForOneKeyMintsOnce) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        f.TokenMock.SetServiceToken("cloud-1", "sa-2", "other");
        f.TokenMock.HoldServiceTokenCallsFrom = 1;
        f.TokenMock.SetServiceTokenHoldTarget("sa-1");
        f.Settings.RequestTimeout = TDuration::Minutes(5);
        const auto service = f.StartTokenService();

        constexpr ui32 requests = 1000;
        for (ui32 i = 0; i < requests; ++i) {
            f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvIamDelegation::TEvGetToken(KEY)));
        }
        // the sentinel: queued after the flood, answered at once by the mock
        const TTokenKey other{"sa-2", "cloud-1"};
        f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvIamDelegation::TEvGetToken(other)));
        {
            TAutoPtr<IEventHandle> handle;
            auto* sentinel = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvGetTokenResult>(handle);
            UNIT_ASSERT(sentinel);
            // the first reply of all is the sentinel's: none of the 1000 requests was answered while the mint is held
            UNIT_ASSERT_VALUES_EQUAL(sentinel->Key.ServiceAccountId, "sa-2");
            UNIT_ASSERT_C(sentinel->IsSuccess(), sentinel->Issues.ToOneLineString());
            UNIT_ASSERT_C(sentinel->Token.StartsWith("other-"), sentinel->Token);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceCalls.load(), 2u); // one held for sa-1, one answered for sa-2
        UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceAnswered.load(), 1u);

        f.TokenMock.ReleaseHeldServiceTokenCalls();
        TString token;
        for (ui32 i = 0; i < requests; ++i) {
            TAutoPtr<IEventHandle> handle;
            auto* result = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvGetTokenResult>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_VALUES_EQUAL(result->Key.ServiceAccountId, "sa-1");
            UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
            if (token.empty()) {
                token = result->Token;
                UNIT_ASSERT_C(token.StartsWith("delegated-"), token);
            }
            UNIT_ASSERT_VALUES_EQUAL(result->Token, token);
        }
        ui32 mintsOfHeldKey = 0;
        with_lock (f.TokenMock.ServiceMutex) {
            for (const auto& request : f.TokenMock.CreateForServiceRequests) {
                mintsOfHeldKey += request.target_service_account_id() == "sa-1";
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(mintsOfHeldKey, 1u);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("Mints"), 2);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("CachedKeys", false), 2);
    }

    // A flood spread over many keys: one CreateForService per key, every reply carries the token of its key.
    Y_UNIT_TEST(FloodAcrossManyKeysMintsOncePerKey) {
        TFixture f;
        constexpr ui32 keys = 50;
        constexpr ui32 requestsPerKey = 20;
        for (ui32 k = 0; k < keys; ++k) {
            f.TokenMock.SetServiceToken("cloud-1", TStringBuilder() << "sa-" << k, TStringBuilder() << "token-of-sa-" << k);
        }
        const auto service = f.StartTokenService();

        for (ui32 i = 0; i < requestsPerKey; ++i) {
            for (ui32 k = 0; k < keys; ++k) {
                f.Runtime->Send(new IEventHandle(service, f.Sender, new TEvIamDelegation::TEvGetToken(TTokenKey{TStringBuilder() << "sa-" << k, "cloud-1"})));
            }
        }
        THashMap<TString, ui32> repliesByKey;
        for (ui32 i = 0; i < keys * requestsPerKey; ++i) {
            TAutoPtr<IEventHandle> handle;
            auto* result = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvGetTokenResult>(handle);
            UNIT_ASSERT(result);
            UNIT_ASSERT_C(result->IsSuccess(), result->Issues.ToOneLineString());
            UNIT_ASSERT_C(result->Token.StartsWith(TStringBuilder() << "token-of-" << result->Key.ServiceAccountId << "-"), result->Token);
            ++repliesByKey[result->Key.ServiceAccountId];
        }
        UNIT_ASSERT_VALUES_EQUAL(repliesByKey.size(), keys);
        for (const auto& [key, replies] : repliesByKey) {
            UNIT_ASSERT_VALUES_EQUAL_C(replies, requestsPerKey, key);
        }
        UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceCalls.load(), keys);
        THashSet<TString> minted;
        with_lock (f.TokenMock.ServiceMutex) {
            for (const auto& request : f.TokenMock.CreateForServiceRequests) {
                UNIT_ASSERT_C(minted.insert(request.target_service_account_id()).second, "minted twice: " << request.target_service_account_id());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(minted.size(), keys);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("Mints"), keys);
        UNIT_ASSERT_VALUES_EQUAL(f.TokenSensor("CachedKeys", false), keys);
    }

    Y_UNIT_TEST(TokenServiceSystemTokenFailure) {
        TFixture f;
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");
        const auto service = f.StartTokenService(f.FailingSystemToken("metadata is down")->Service);

        const auto result = f.GetToken(service, KEY);
        UNIT_ASSERT(!result->IsSuccess());
        UNIT_ASSERT_VALUES_EQUAL(result->Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result->Issues.ToOneLineString(), "metadata is down");
        UNIT_ASSERT_VALUES_EQUAL(f.TokenMock.CreateForServiceCalls.load(), 0u);
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
    // a token service on the same runtime answers, and when the token is finally delivered the request completes.
    Y_UNIT_TEST(PendingSystemTokenDoesNotBlockOtherWork) {
        TFixture f;
        f.Settings.RequestTimeout = TDuration::Minutes(5); // the wait is ended by the test, not by a timeout
        auto source = f.SilentSystemToken();
        const auto delegation = f.StartDelegationService(source->Service);
        const auto tokens = f.StartTokenService();
        f.TokenMock.SetServiceToken("cloud-1", "sa-1", "delegated");

        f.Runtime->Send(new IEventHandle(delegation, f.Sender, new TEvIamDelegation::TEvSetupDelegation(f.Spec(), "user-1"), 0, 42));
        source->WaitRequests(1); // the delegation service is now parked on the system token

        const auto token = f.GetToken(tokens, KEY);
        UNIT_ASSERT_C(token->IsSuccess(), token->Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 0u); // still parked

        source->DeliverLast(*f.Runtime);
        TAutoPtr<IEventHandle> handle;
        auto* setup = f.Runtime->GrabEdgeEvent<TEvIamDelegation::TEvSetupDelegationResult>(handle);
        UNIT_ASSERT(setup);
        UNIT_ASSERT_C(setup->Result.IsSuccess(), setup->Result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(handle->Cookie, 42u);
        UNIT_ASSERT_VALUES_EQUAL(f.ServiceControlMock.SetupCalls(), 1u);
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

// The KQP proxy starts the two services at its bootstrap according to the feature flag and the configuration:
// no flag - nothing; incomplete identity - nothing (with a warning); token service only when the control plane
// endpoint is missing; both when everything is there, including when the identity comes from the replication section.
Y_UNIT_TEST_SUITE(IamDelegationProxyRegistration) {
    struct TCase {
        bool Flag = true;
        NKikimrConfig::TIamConfig Iam;
        NKikimrReplication::TReplicationDefaults::TIamServiceControl Replication;
        bool TokenService = false;
        bool DelegationService = false;
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

        UNIT_ASSERT_VALUES_EQUAL_C(bool(runtime->GetLocalServiceId(MakeIamDelegatedTokenServiceId(), 0)), c.TokenService, what);
        UNIT_ASSERT_VALUES_EQUAL_C(bool(runtime->GetLocalServiceId(MakeIamDelegationServiceId(), 0)), c.DelegationService, what);
        // the system token service comes with the first of the two
        UNIT_ASSERT_VALUES_EQUAL_C(bool(runtime->GetLocalServiceId(MakeIamSystemTokenServiceId(), 0)), c.TokenService, what);
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

    Y_UNIT_TEST(TokenServiceOnlyWithoutTheControlPlane) {
        auto iam = FullIamConfig();
        iam.ClearServiceControlEndpoint();
        Check({.Iam = iam, .TokenService = true}, "no control plane endpoint");
    }

    Y_UNIT_TEST(BothWithFullConfig) {
        Check({.Iam = FullIamConfig(), .TokenService = true, .DelegationService = true}, "full IamConfig");
    }

    Y_UNIT_TEST(BothWithIdentityFromTheReplicationSection) {
        NKikimrConfig::TIamConfig iam;
        iam.SetServiceControlEndpoint("localhost:1");
        NKikimrReplication::TReplicationDefaults::TIamServiceControl replication;
        replication.SetEndpoint("localhost:1");
        replication.SetServiceId("ydb");
        replication.SetMicroserviceId("data-plane");
        replication.SetResourceType("resource-manager.cloud");
        Check({.Iam = iam, .Replication = replication, .TokenService = true, .DelegationService = true}, "identity from ReplicationConfig");
    }
}

Y_UNIT_TEST_SUITE(IamCloudResolver) {
    Y_UNIT_TEST(ResolvesFolderThenCloud) {
        TFixture f;
        const auto result = f.ResolveCloud("sa-1");
        UNIT_ASSERT_C(result->Get()->Result.IsSuccess(), result->Get()->Result.Issues.ToOneLineString());
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Resolved.FolderId, "folder-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Resolved.CloudId, "cloud-1");
    }

    // The lookup is done with the user's token: with the system token IAM refuses to read the
    // service account, which is reported instead of retried
    Y_UNIT_TEST(SystemTokenIsRefused) {
        TFixture f;
        const auto result = f.ResolveCloud("sa-1", "ssa-token");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "GetServiceAccount failed");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Resolved.CloudId, "");
    }

    Y_UNIT_TEST(UnknownServiceAccount) {
        TFixture f;
        const auto result = f.ResolveCloud("sa-9");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "GetServiceAccount failed");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Resolved.FolderId, "");
    }

    Y_UNIT_TEST(FolderWithoutCloud) {
        TFixture f;
        f.FolderMock.NoAnswerFolders.insert("folder-1");
        const auto result = f.ResolveCloud("sa-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "did not resolve folder folder-1");
    }

    // The account is known but IAM returned it without a folder: an error naming the account, not a lookup of "".
    Y_UNIT_TEST(ServiceAccountWithoutFolder) {
        TFixture f;
        f.ServiceAccountMock.ServiceAccountData["sa-1"].clear_folder_id();
        const auto result = f.ResolveCloud("sa-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::INTERNAL_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "returned no folder for sa-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Resolved.FolderId, "");
    }

    // Resource Manager resolved the folder but reported no cloud for it.
    Y_UNIT_TEST(ResolvedFolderWithoutCloudId) {
        TFixture f;
        f.FolderMock.Folders["folder-1"].clear_cloud_id();
        const auto result = f.ResolveCloud("sa-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "did not resolve folder folder-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Resolved.CloudId, "");
    }

    // No user token to authorize the lookups with: refused at once, nothing is sent to IAM.
    Y_UNIT_TEST(EmptyUserTokenIsRefused) {
        TFixture f;
        const auto result = f.ResolveCloud("sa-1", "");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::UNAUTHORIZED);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "no user token to look service account sa-1 up with");
        with_lock (f.ServiceAccountMock.MetadataMutex) {
            UNIT_ASSERT_VALUES_EQUAL(f.ServiceAccountMock.CapturedUserAgent, "");
        }
    }

    Y_UNIT_TEST(UnknownFolder) {
        TFixture f;
        f.ServiceAccountMock.ServiceAccountData["sa-1"].set_folder_id("folder-9");
        const auto result = f.ResolveCloud("sa-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::NOT_FOUND);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "ResolveFolders failed");
    }

    // Resource Manager unreachable: the transport error is retried, then reported as UNAVAILABLE
    Y_UNIT_TEST(ResourceManagerDown) {
        TFixture f;
        f.Settings.ResourceManagerEndpoint = "localhost:" + ToString(f.PortManager.GetPort());
        f.Settings.RequestTimeout = TDuration::Seconds(1);
        const auto result = f.ResolveCloud("sa-1");
        UNIT_ASSERT_VALUES_EQUAL(result->Get()->Result.Status, Ydb::StatusIds::UNAVAILABLE);
        UNIT_ASSERT_STRING_CONTAINS(result->Get()->Result.Issues.ToOneLineString(), "ResolveFolders failed after 3 attempts");
    }
}

} // namespace NKikimr::NIamDelegation
