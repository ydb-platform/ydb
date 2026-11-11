#include <ydb/core/base/path.h>
#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/common/proxy/mirror_credentials.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/services/scheme_secret/service.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;
using namespace NKikimr;
using namespace NKikimr::NPQ;

namespace {

using TCredentials = NKikimrPQ::TMirrorPartitionConfig::TCredentials;
using TEvResolveSecret = NSecret::TDescribeSchemaSecretsService::TEvResolveSecret;

class TCredentialsFactory : public TPersQueueMirrorReaderFactory {
public:
    mutable TMaybe<TCredentials> Received;
    mutable ui32 Calls = 0;

    NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProviderImpl(
        const TCredentials& credentials
    ) const override {
        ++Calls;
        Received = credentials;
        return NThreading::MakeFuture(NYdb::CreateInsecureCredentialsProviderFactory());
    }
};

struct TCredentialsEnv {
    TTestBasicRuntime Runtime{1, false};
    TCredentialsFactory Factory;
    TActorId Edge;
    TActorId NavigationSender;
    THolder<TEvTxProxySchemeCache::TEvNavigateKeySet> Navigation;
    THolder<TEvResolveSecret> SecretRequest;
    ui32 NavigationCount = 0;
    ui32 SecretRequestCount = 0;

    explicit TCredentialsEnv(const TCredentials& credentials) {
        TAppPrepare prepare;
        Runtime.Initialize(prepare.Unwrap());
        Runtime.GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        Edge = Runtime.AllocateEdgeActor();
        Runtime.RegisterService(MakeSchemeCacheID(), Runtime.AllocateEdgeActor());
        Runtime.RegisterService(NSecret::MakeDescribeSchemaSecretServiceId(Runtime.GetNodeId(0)), Runtime.AllocateEdgeActor());
        Runtime.SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::TEvNavigateKeySet::EventType) {
                ++NavigationCount;
                NavigationSender = ev->Sender;
                Navigation = ev->Release<TEvTxProxySchemeCache::TEvNavigateKeySet>();
                return TTestActorRuntime::EEventAction::DROP;
            }
            if (ev->GetTypeRewrite() == TEvResolveSecret::EventType) {
                ++SecretRequestCount;
                SecretRequest = ev->Get<TEvResolveSecret>()->MakeCopy();
                return TTestActorRuntime::EEventAction::DROP;
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        });
        Runtime.Register(CreateMirrorCredentialsInitializer(Edge, 42, credentials, "/Root", "/Root/dst-topic", &Factory));
        Runtime.SimulateSleep(TDuration::MilliSeconds(20));
    }

    void ResolveOwner() {
        UNIT_ASSERT(Navigation);
        auto response = MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(Navigation->Request.Release());
        auto& entry = response->Request->ResultSet.at(0);
        entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        entry.SecurityObject = MakeIntrusive<TSecurityObject>("topic-owner@builtin", TString(), false);
        Runtime.Send(new IEventHandle(NavigationSender, MakeSchemeCacheID(), response.Release()), 0, true);
        Runtime.SimulateSleep(TDuration::MilliSeconds(20));
        UNIT_ASSERT(SecretRequest);
    }

    void ResolveSecret(const TString& value) {
        SecretRequest->Promise.SetValue(NKqp::TEvDescribeSecretsResponse::TDescription({value}));
        auto response = Runtime.GrabEdgeEvent<TEvPQ::TEvCredentialsCreated>(Edge);
        UNIT_ASSERT_VALUES_EQUAL(response->Cookie, 42u);
        UNIT_ASSERT(!response->Get()->Error);
        UNIT_ASSERT(response->Get()->Credentials);
        UNIT_ASSERT(Factory.Received);
        UNIT_ASSERT_VALUES_EQUAL(Factory.Calls, 1u);
        UNIT_ASSERT_VALUES_EQUAL(NavigationCount, 1u);
        UNIT_ASSERT_VALUES_EQUAL(SecretRequestCount, 1u);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TMirrorerCredentials) {

Y_UNIT_TEST(UnresolvedSecretReferencesCannotCreateProviders) {
    TPersQueueMirrorReaderFactory factory;
    auto check = [&factory](const NKikimrPQ::TMirrorPartitionConfig::TCredentials& credentials) {
        UNIT_ASSERT(MirrorCredentialsNeedResolution(credentials));
        UNIT_ASSERT(factory.GetCredentialsProvider(credentials).HasException());
    };

    NKikimrPQ::TMirrorPartitionConfig::TCredentials oauth;
    oauth.SetOauthTokenSecretName("/Root/oauth-secret");
    check(oauth);

    NKikimrPQ::TMirrorPartitionConfig::TCredentials jwt;
    jwt.SetJwtParamsSecretName("/Root/jwt-secret");
    check(jwt);

    NKikimrPQ::TMirrorPartitionConfig::TCredentials iam;
    iam.MutableIam()->SetServiceAccountKeySecretName("/Root/iam-secret");
    check(iam);
}

Y_UNIT_TEST(OAuthSecretResolvedWithTopicOwnerWithoutChangingInput) {
    TCredentials credentials;
    credentials.SetOauthTokenSecretName("/Root/oauth-secret");
    TCredentialsEnv env(credentials);
    UNIT_ASSERT(env.Navigation);
    UNIT_ASSERT_VALUES_EQUAL(env.Navigation->Request->DatabaseName, "/Root");
    UNIT_ASSERT_VALUES_EQUAL(CanonizePath(env.Navigation->Request->ResultSet.front().Path), "/Root/dst-topic");
    UNIT_ASSERT(!env.Factory.Received);
    env.ResolveOwner();
    UNIT_ASSERT_VALUES_EQUAL(env.SecretRequest->Database, "/Root");
    UNIT_ASSERT_VALUES_EQUAL(env.SecretRequest->SecretNames.size(), 1u);
    UNIT_ASSERT_VALUES_EQUAL(env.SecretRequest->SecretNames.front(), "/Root/oauth-secret");
    UNIT_ASSERT(env.SecretRequest->UserToken);
    UNIT_ASSERT_VALUES_EQUAL(env.SecretRequest->UserToken->GetUserSID(), "topic-owner@builtin");
    env.ResolveSecret("resolved-oauth");
    UNIT_ASSERT_VALUES_EQUAL(env.Factory.Received->GetOauthToken(), "resolved-oauth");
    UNIT_ASSERT(!env.Factory.Received->HasOauthTokenSecretName());
    UNIT_ASSERT_VALUES_EQUAL(credentials.GetOauthTokenSecretName(), "/Root/oauth-secret");
    UNIT_ASSERT(!credentials.HasOauthToken());
}

Y_UNIT_TEST(JwtAndIamSecretsUseExistingCredentialFactories) {
    {
        TCredentials jwt;
        jwt.SetJwtParamsSecretName("/Root/jwt-secret");
        TCredentialsEnv jwtEnv(jwt);
        jwtEnv.ResolveOwner();
        jwtEnv.ResolveSecret("resolved-jwt");
        UNIT_ASSERT_VALUES_EQUAL(jwtEnv.Factory.Received->GetJwtParams(), "resolved-jwt");
        UNIT_ASSERT_VALUES_EQUAL(jwt.GetJwtParamsSecretName(), "/Root/jwt-secret");

    }
    {
        TCredentials iam;
        iam.MutableIam()->SetEndpoint("iam-endpoint");
        iam.MutableIam()->SetServiceAccountKeySecretName("/Root/iam-secret");
        TCredentialsEnv iamEnv(iam);
        iamEnv.ResolveOwner();
        iamEnv.ResolveSecret("resolved-key");
        const auto& resolved = iamEnv.Factory.Received->GetIam();
        UNIT_ASSERT_VALUES_EQUAL(resolved.GetEndpoint(), "iam-endpoint");
        UNIT_ASSERT_VALUES_EQUAL(resolved.GetServiceAccountKey(), "resolved-key");
        UNIT_ASSERT(!resolved.HasServiceAccountKeySecretName());
        UNIT_ASSERT_VALUES_EQUAL(iam.GetIam().GetServiceAccountKeySecretName(), "/Root/iam-secret");
        UNIT_ASSERT(!iam.GetIam().HasServiceAccountKey());
    }
}

} // Y_UNIT_TEST_SUITE(TMirrorerCredentials)
