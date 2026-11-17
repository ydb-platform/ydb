#include "pqrb_ut_common.h"

#include <library/cpp/threading/future/future.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/persqueue/common/proxy/actor_persqueue_client_iface.h>
#include <ydb/core/persqueue/pqrb/mirror_describer_factory.h>
#include <ydb/core/persqueue/pqrb/mirror_describer.h>
#include <ydb/core/persqueue/pqrb/partition_scale_request.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/services/scheme_secret/service.h>
#include <ydb/library/actors/core/events.h>
#include <util/generic/yexception.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/credentials/credentials.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

namespace NKikimr::NPQ {

class TControllableMirrorFactory : public IPersQueueMirrorReaderFactory {
public:
    mutable ui32 DescribeCalls = 0;
    mutable ui32 CredCalls = 0;
    bool ThrowCredentials = false;
    bool ImmediateCredentials = true;
    mutable std::vector<NThreading::TPromise<NYdb::NTopic::TDescribeTopicResult>> DescribePromises;
    mutable std::vector<NThreading::TPromise<NYdb::TCredentialsProviderFactoryPtr>> CredPromises;
    mutable std::vector<TString> CredTokens;
    mutable NYdb::TCredentialsProviderFactoryPtr LastDescribeCredentials;
    mutable TString LastDescribeTopic;
    mutable NKikimrPQ::TMirrorPartitionConfig LastDescribeConfig;

    NThreading::TFuture<NYdb::TCredentialsProviderFactoryPtr> GetCredentialsProviderImpl(
        const NKikimrPQ::TMirrorPartitionConfig::TCredentials& cred
    ) const override {
        CredTokens.push_back(TString{cred.GetOauthToken()});
        ++CredCalls;
        if (ThrowCredentials) {
            ythrow yexception() << "cred-fail";
        }
        if (ImmediateCredentials) {
            return NThreading::MakeFuture(NYdb::CreateInsecureCredentialsProviderFactory());
        }
        auto promise = NThreading::NewPromise<NYdb::TCredentialsProviderFactoryPtr>();
        CredPromises.push_back(promise);
        return promise.GetFuture();
    }

    std::shared_ptr<NYdb::NTopic::IReadSession> GetReadSession(
        const NKikimrPQ::TMirrorPartitionConfig&,
        ui32,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>,
        ui64,
        TMaybe<TLog>
    ) const override {
        return nullptr;
    }

    NThreading::TFuture<NYdb::NTopic::TDescribeTopicResult> GetTopicDescription(
        const NKikimrPQ::TMirrorPartitionConfig& config,
        std::shared_ptr<NYdb::ICredentialsProviderFactory> credentialsProviderFactory
    ) const override {
        LastDescribeCredentials = credentialsProviderFactory;
        LastDescribeTopic = config.GetTopic();
        LastDescribeConfig = config;
        ++DescribeCalls;
        auto promise = NThreading::NewPromise<NYdb::NTopic::TDescribeTopicResult>();
        DescribePromises.push_back(promise);
        return promise.GetFuture();
    }

    NThreading::TFuture<NYdb::TStatus> CommitOffset(
        const NKikimrPQ::TMirrorPartitionConfig&,
        std::shared_ptr<NYdb::ICredentialsProviderFactory>,
        ui32,
        ui64
    ) const override {
        return NThreading::MakeFuture(NYdb::TStatus(NYdb::EStatus::SUCCESS, NYdb::NIssue::TIssues()));
    }
};

NKikimrPQ::TMirrorPartitionConfig MakeMirrorConfig(const TString& topic = "src-topic") {
    NKikimrPQ::TMirrorPartitionConfig config;
    config.SetEndpoint("src");
    config.SetEndpointPort(2135);
    config.SetTopic(topic);
    return config;
}

struct TSecretDescriberEnv {
    using TEvResolveSecret = NSecret::TDescribeSchemaSecretsService::TEvResolveSecret;

    TControllableMirrorFactory Factory;
    THolder<TEvTxProxySchemeCache::TEvNavigateKeySet> Navigation;
    TActorId NavigationSender;
    size_t NavigationCount = 0;
    TActorId SecretService;
    THolder<TEvResolveSecret> SecretRequest;
    NKikimrPQ::TMirrorPartitionConfig Config = MakeMirrorConfig();
    TTestContext Context;

    TSecretDescriberEnv() {
        Context.Prepare();
        Context.Runtime->SetScheduledLimit(10000);
        PQTabletPrepare({}, {}, Context);
        Context.Runtime->GetAppData(0).PersQueueMirrorReaderFactory = &Factory;
        Context.Runtime->GetAppData(0).FeatureFlags.SetEnableMirroredTopicSplitMerge(true);
        Context.Runtime->GetAppData(0).FeatureFlags.SetEnableSchemaSecrets(true);
        Context.Runtime->RegisterService(MakeSchemeCacheID(), Context.Runtime->AllocateEdgeActor());
        SecretService = Context.Runtime->AllocateEdgeActor();
        Context.Runtime->RegisterService(
            NSecret::MakeDescribeSchemaSecretServiceId(Context.Runtime->GetNodeId(0)), SecretService);
        Context.Runtime->SetObserverFunc([this](TAutoPtr<IEventHandle>& ev) {
            if (ev->GetTypeRewrite() == TEvPQ::TEvInitCredentials::EventType) {
                Context.Runtime->EnableScheduleForActor(ev->Recipient);
            }
            if (ev->GetTypeRewrite() == TEvTxProxySchemeCache::TEvNavigateKeySet::EventType) {
                ++NavigationCount;
                NavigationSender = ev->Sender;
                Navigation = ev->Release<TEvTxProxySchemeCache::TEvNavigateKeySet>();
                return TTestActorRuntimeBase::EEventAction::DROP;
            }
            return TTestActorRuntimeBase::EEventAction::PROCESS;
        });
        Config.MutableCredentials()->SetOauthTokenSecretName("/Root/describe-secret");
        UpdateTopicPath("dst-topic");
    }

    void UpdateTopicPath(const TString& topic) {
        const auto expectedNavigations = NavigationCount + 1;
        Navigation = nullptr;
        SendBalancerUpdate(Context, {
            .Partitions = {{0, {Context.TabletId, 1}}},
            .Strategy = NKikimrPQ::TPQTabletConfig::CAN_SPLIT,
            .Path = "/Root/" + topic,
            .Database = "/Root",
            .MirrorFrom = Config,
        });
        Context.Runtime->WaitFor("mirror topic owner lookup", [&] { return bool(Navigation); }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(NavigationCount, expectedNavigations);
        UNIT_ASSERT_VALUES_EQUAL(Navigation->Request->ResultSet.at(0).Path, (TVector<TString>{"Root", topic}));
    }

    void ResolveOwner() {
        UNIT_ASSERT(Navigation);
        UNIT_ASSERT_VALUES_EQUAL(Navigation->Request->DatabaseName, "/Root");
        auto response = MakeHolder<TEvTxProxySchemeCache::TEvNavigateKeySetResult>(Navigation->Request.Release());
        auto& entry = response->Request->ResultSet.at(0);
        entry.Status = NSchemeCache::TSchemeCacheNavigate::EStatus::Ok;
        entry.SecurityObject = MakeIntrusive<TSecurityObject>("topic-owner@builtin", TString(), false);
        Context.Runtime->Send(new IEventHandle(NavigationSender, MakeSchemeCacheID(), response.Release()));
        auto request = Context.Runtime->GrabEdgeEvent<TEvResolveSecret>(SecretService, TDuration::Seconds(5));
        UNIT_ASSERT(request);
        SecretRequest = request->Get()->MakeCopy();
        UNIT_ASSERT_VALUES_EQUAL(SecretRequest->Database, "/Root");
        UNIT_ASSERT_VALUES_EQUAL(SecretRequest->SecretNames.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(SecretRequest->SecretNames.front(), "/Root/describe-secret");
        UNIT_ASSERT(SecretRequest->UserToken);
        UNIT_ASSERT_VALUES_EQUAL(SecretRequest->UserToken->GetUserSID(), "topic-owner@builtin");
    }

    void ResolveSecretAndExpectDescription(const TString& value) {
        SecretRequest->Promise.SetValue(NKqp::TEvDescribeSecretsResponse::TDescription({value}));
        for (size_t i = 0; i < 100 && Factory.CredCalls == 0; ++i) {
            DispatchFor(Context);
        }
        UNIT_ASSERT_VALUES_EQUAL(Factory.CredCalls, 1u);
        UNIT_ASSERT_VALUES_EQUAL(Factory.CredTokens.size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(Factory.CredTokens.front(), value);
        Context.Runtime->WaitFor("mirror description", [&] { return Factory.DescribeCalls != 0; }, TDuration::Seconds(5));
        UNIT_ASSERT_VALUES_EQUAL(Factory.DescribeCalls, 1u);
        UNIT_ASSERT_VALUES_EQUAL(Factory.LastDescribeConfig.GetCredentials().SerializeAsString(),
            Config.GetCredentials().SerializeAsString());
    }
};

Y_UNIT_TEST_SUITE(TPqrbMirrorDescriber) {

Y_UNIT_TEST(SecretCredentialsAreResolvedForDescribeWithoutPersistingValue) {
    TSecretDescriberEnv env;
    env.UpdateTopicPath("updated-topic");
    env.ResolveOwner();
    env.ResolveSecretAndExpectDescription("resolved-oauth");
}

}

} // namespace NKikimr::NPQ
