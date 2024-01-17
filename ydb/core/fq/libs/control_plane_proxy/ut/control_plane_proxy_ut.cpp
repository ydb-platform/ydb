#include <ydb/core/fq/libs/actors/logging/log.h>
#include <ydb/core/fq/libs/control_plane_config/control_plane_config.h>
#include <ydb/core/fq/libs/control_plane_proxy/control_plane_proxy.h>
#include <ydb/core/fq/libs/control_plane_storage/control_plane_storage.h>
#include <ydb/core/fq/libs/control_plane_storage/events/events.h>
#include <ydb/core/fq/libs/test_connection/events/events.h>
#include <ydb/core/fq/libs/test_connection/test_connection.h>
#include <ydb/core/fq/libs/quota_manager/events/events.h>
#include <ydb/core/fq/libs/quota_manager/quota_manager.h>
#include <ydb/core/fq/libs/quota_manager/ut_helpers/fake_quota_manager.h>
#include <ydb/core/fq/libs/rate_limiter/control_plane_service/rate_limiter_control_plane_service.h>
#include <ydb/core/fq/libs/rate_limiter/events/control_plane_events.h>
#include <ydb/core/fq/libs/ydb/util.h>
#include <ydb/core/fq/libs/ydb/ydb.h>

#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <library/cpp/testing/unittest/registar.h>

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/base/path.h>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/mock/mock_folder_service_adapter.h>

#include <util/system/env.h>

namespace NFq {

using namespace NActors;
using namespace NKikimr;

namespace {

//////////////////////////////////////////////////////

using TRuntimePtr = std::shared_ptr<TTestActorRuntime>;

class TGrabActor: public TActor<TGrabActor> {
    std::deque<NThreading::TPromise<TAutoPtr<IEventHandle>>> Futures;
    std::deque<TAutoPtr<IEventHandle>> Inputs;
    TMutex Mutex;

public:
    TRuntimePtr Runtime;

    TGrabActor(TRuntimePtr runtime)
        : TActor(&TGrabActor::StateFunc)
        , Runtime(runtime)
    { }

    STFUNC(StateFunc)
    {
        TGuard<TMutex> lock(Mutex);
        if (!Futures.empty()) {
            auto front = Futures.front();
            Futures.pop_front();
            front.SetValue(ev);
            return;
        }
        Inputs.push_back(ev);
    }

    NThreading::TFuture<TAutoPtr<IEventHandle>> WaitRequest()
    {
        TGuard<TMutex> lock(Mutex);
        if (!Inputs.empty()) {
            auto front = Inputs.front();
            Inputs.pop_front();
            return NThreading::MakeFuture(front);
        }
        Futures.push_back(NThreading::NewPromise<TAutoPtr<IEventHandle>>());
        return Futures.back();
    }

    TAutoPtr<IEventHandle> GetRequest()
    {
        auto future = WaitRequest();
        while (!future.HasValue()) {
            Runtime->DispatchEvents({}, TDuration::MilliSeconds(1));
        }
        return future.GetValue();
    }
};

struct TTestBootstrap {
    const TDuration RequestTimeout = TDuration::Seconds(10);
    NConfig::TControlPlaneProxyConfig Config;
    NConfig::TControlPlaneStorageConfig StorageConfig;
    NConfig::TComputeConfig ComputeConfig;
    NConfig::TCommonConfig CommonConfig;
    NYql::TS3GatewayConfig S3Config;

    TRuntimePtr Runtime;
    TGrabActor* MetaStorageGrab;
    TGrabActor* TestConnectionGrab;
    TGrabActor* RateLimiterGrab;

    TTestBootstrap(const NConfig::TControlPlaneProxyConfig& config = {})
        : Config(config)
        , Runtime(PrepareTestActorRuntime())
    {
    }

    ~TTestBootstrap()
    {
        MetaStorageGrab->Runtime.reset();
        TestConnectionGrab->Runtime.reset();
        RateLimiterGrab->Runtime.reset();
    }

    void SendCreateQueryRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff", bool processCreateRateLimiterResource = true)
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::CreateQueryRequest proto;
        proto.mutable_content()->set_name("my_query_name");

        auto request = std::make_unique<TEvControlPlaneProxy::TEvCreateQueryRequest>("", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
        if (processCreateRateLimiterResource) {
            auto req = RateLimiterGrab->GetRequest();
            SendCreateRateLimiterResourceSuccess(req->Sender);
        }
    }

    void SendCreateQueryRequestDontWaitForRateLimiter(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        SendCreateQueryRequest(permissions, user, false);
    }

    void SendCreateRateLimiterResourceSuccess(const TActorId& id, const TString& rateLimiter = "rate_limiter") {
        Runtime->Send(new IEventHandle(id, id, new TEvRateLimiter::TEvCreateResourceResponse(rateLimiter, NYql::TIssues())));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendCreateRateLimiterResourceError(const TActorId& id) {
        NYql::TIssues issues;
        issues.AddIssue("Trololo");
        Runtime->Send(new IEventHandle(id, id, new TEvRateLimiter::TEvCreateResourceResponse(issues)));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendListQueriesRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::ListQueriesRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvListQueriesRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendDescribeQueryRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::DescribeQueryRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvDescribeQueryRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendGetQueryStatusRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::GetQueryStatusRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvGetQueryStatusRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendModifyQueryRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::ModifyQueryRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvModifyQueryRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendDeleteQueryRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::DeleteQueryRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvDeleteQueryRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendControlQueryRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::ControlQueryRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvControlQueryRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendGetResultDataRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::GetResultDataRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvGetResultDataRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendListJobsRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::ListJobsRequest proto;
        proto.set_query_id("my_query_id");

        auto request = std::make_unique<TEvControlPlaneProxy::TEvListJobsRequest>("", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendDescribeJobRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::DescribeJobRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvDescribeJobRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendCreateConnectionRequest(const TVector<TString>& permissions = {}, const TString& serviceAccountId = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::CreateConnectionRequest proto;
        if (serviceAccountId) {
            proto.mutable_content()
                ->mutable_setting()
                ->mutable_ydb_database()
                ->mutable_auth()
                ->mutable_service_account()
                ->set_id(serviceAccountId);
        }

        auto request = std::make_unique<TEvControlPlaneProxy::TEvCreateConnectionRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendListConnectionsRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::ListConnectionsRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvListConnectionsRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendDescribeConnectionRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::DescribeConnectionRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvDescribeConnectionRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendModifyConnectionRequest(const TVector<TString>& permissions = {}, const TString& serviceAccountId = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::ModifyConnectionRequest proto;
        if (serviceAccountId) {
            proto.mutable_content()
                ->mutable_setting()
                ->mutable_ydb_database()
                ->mutable_auth()
                ->mutable_service_account()
                ->set_id(serviceAccountId);
        }

        auto request = std::make_unique<TEvControlPlaneProxy::TEvModifyConnectionRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendDeleteConnectionRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::DeleteConnectionRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvDeleteConnectionRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendTestConnectionRequest(const TVector<TString>& permissions = {}, const TString& serviceAccountId = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::TestConnectionRequest proto;
        if (serviceAccountId) {
            proto.mutable_setting()
                ->mutable_ydb_database()
                ->mutable_auth()
                ->mutable_service_account()
                ->set_id(serviceAccountId);
        }

        auto request = std::make_unique<TEvControlPlaneProxy::TEvTestConnectionRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendCreateBindingRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::CreateBindingRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvCreateBindingRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendListBindingsRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::ListBindingsRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvListBindingsRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendDescribeBindingRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::DescribeBindingRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvDescribeBindingRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendModifyBindingRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::ModifyBindingRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvModifyBindingRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    void SendDeleteBindingRequest(const TVector<TString>& permissions = {}, const TString& user = "test_user@staff")
    {
        TActorId sender = Runtime->AllocateEdgeActor();
        FederatedQuery::DeleteBindingRequest proto;

        auto request = std::make_unique<TEvControlPlaneProxy::TEvDeleteBindingRequest>("yandexcloud://my_folder", proto, user, "", permissions);
        Runtime->Send(new IEventHandle(ControlPlaneProxyActorId(), sender, request.release()));
        Runtime->DispatchEvents({}, TDuration::Zero());
    }

    template<typename T>
    std::pair<TAutoPtr<IEventHandle>, T*> Grab()
    {
        TAutoPtr<IEventHandle> handle;
        T* event = Runtime->GrabEdgeEvent<T>(handle);
        return {handle, event};
    }


private:
    TRuntimePtr PrepareTestActorRuntime()
    {
        TRuntimePtr runtime(new TTestBasicRuntime());
        runtime->SetLogPriority(NKikimrServices::STREAMS_CONTROL_PLANE_SERVICE, NLog::PRI_DEBUG);
        auto controlPlaneProxy = CreateControlPlaneProxyActor(
            Config,
            StorageConfig,
            ComputeConfig,
            CommonConfig,
            S3Config,
            nullptr,
            NFq::TYqSharedResources::TPtr{},
            NKikimr::TYdbCredentialsProviderFactory(nullptr),
            MakeIntrusive<::NMonitoring::TDynamicCounters>(),
            true);
        runtime->AddLocalService(
            ControlPlaneProxyActorId(),
            TActorSetupCmd(controlPlaneProxy, TMailboxType::Simple, 0));

        auto folderService = NKikimr::NFolderService::CreateMockFolderServiceAdapterActor(NKikimrProto::NFolderService::TFolderServiceConfig{});
        runtime->AddLocalService(
            NKikimr::NFolderService::FolderServiceActorId(),
            TActorSetupCmd(folderService, TMailboxType::Simple, 0),
            0
        );

        auto configService = CreateControlPlaneConfigActor(NFq::TYqSharedResources::TPtr{}, NKikimr::TYdbCredentialsProviderFactory(nullptr),
            NConfig::TControlPlaneStorageConfig{}, NConfig::TComputeConfig{}, MakeIntrusive<::NMonitoring::TDynamicCounters>());
        runtime->AddLocalService(
            NFq::ControlPlaneConfigActorId(),
            TActorSetupCmd(configService, TMailboxType::Simple, 0),
            0
        );

        MetaStorageGrab = new TGrabActor(runtime);
        runtime->AddLocalService(
            ControlPlaneStorageServiceActorId(),
            TActorSetupCmd(MetaStorageGrab, TMailboxType::Simple, 0),
            0
        );

        TestConnectionGrab = new TGrabActor(runtime);
        runtime->AddLocalService(
            TestConnectionActorId(),
            TActorSetupCmd(TestConnectionGrab, TMailboxType::Simple, 0),
            0
        );

        RateLimiterGrab = new TGrabActor(runtime);
        runtime->AddLocalService(
            RateLimiterControlPlaneServiceId(),
            TActorSetupCmd(RateLimiterGrab, TMailboxType::Simple, 0),
            0
        );

        runtime->AddLocalService(
            NFq::MakeQuotaServiceActorId(runtime->GetNodeId(0)),
            TActorSetupCmd(new TQuotaServiceFakeActor(), TMailboxType::Simple, 0),
            0
        );

        SetupTabletServices(*runtime);

        return runtime;
    }
};

TVector<TString> AllPermissions() {
    return {
        "yq.queries.create@as",
        "yq.queries.invoke@as",
        "yq.queries.get@as",
        "yq.queries.viewAst@as",
        "yq.queries.getStatus@as",
        "yq.queries.update@as",
        "yq.queries.invoke@as",
        "yq.queries.delete@as",
        "yq.queries.control@as",
        "yq.queries.getData@as",
        "yq.jobs.get@as",
        "yq.connections.create@as",
        "yq.connections.get@as",
        "yq.connections.update@as",
        "yq.connections.delete@as",
        "yq.bindings.create@as",
        "yq.bindings.get@as",
        "yq.bindings.update@as",
        "yq.bindings.delete@as",
        "yq.resources.viewPublic@as",
        "yq.resources.viewPrivate@as",
        "yq.resources.managePublic@as",
        "yq.resources.managePrivate@as",
        "iam.serviceAccounts.use@as",
        "yq.queries.viewQueryText@as"
    };
}

TVector<TString> AllPermissionsExcept(const TVector<TString>& exceptItems) {
    auto permissions = AllPermissions();
    for (const auto& item: exceptItems) {
        auto it = std::remove_if(
                            permissions.begin(),
                            permissions.end(),
                            [item](const TString& permission) { return permission == item; });
        permissions.erase(it, permissions.end());
    }
    return permissions;
}

} // namespace

//////////////////////////////////////////////////////

Y_UNIT_TEST_SUITE(TControlPlaneProxyTest) {
    Y_UNIT_TEST(ShouldSendCreateQuery)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendCreateQueryRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateQueryRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Request.content().name(), "my_query_name");
    }

    Y_UNIT_TEST(FailsOnCreateQueryWhenRateLimiterResourceNotCreated)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendCreateQueryRequestDontWaitForRateLimiter();

        auto req = bootstrap.RateLimiterGrab->GetRequest();
        bootstrap.SendCreateRateLimiterResourceError(req->Sender);

        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Trololo");
    }

    Y_UNIT_TEST(ShouldSendListQueries)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendListQueriesRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListQueriesRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendDescribeQuery)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendDescribeQueryRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeQueryRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendGetQueryStatus)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendGetQueryStatusRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvGetQueryStatusRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendModifyQuery)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendModifyQueryRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyQueryRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendDeleteQuery)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendDeleteQueryRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteQueryRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendControlQuery)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendControlQueryRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvControlQueryRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendGetResultData)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendGetResultDataRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvGetResultDataRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendListJobs)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendListJobsRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListJobsRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Request.query_id(), "my_query_id");
    }

    Y_UNIT_TEST(ShouldSendDescribeJob)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendDescribeJobRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeJobRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendCreateConnection)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendCreateConnectionRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendListConnections)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendListConnectionsRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListConnectionsRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendDescribeConnection)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendDescribeConnectionRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeConnectionRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendModifyConnection)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendModifyConnectionRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendDeleteConnection)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendDeleteConnectionRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteConnectionRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendTestConnection)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendTestConnectionRequest();
        auto request = bootstrap.TestConnectionGrab->GetRequest();
        auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendCreateBinding)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendCreateBindingRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateBindingRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendListBindings)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendListBindingsRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListBindingsRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendDescribeBinding)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendDescribeBindingRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeBindingRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendModifyBinding)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendModifyBindingRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyBindingRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }

    Y_UNIT_TEST(ShouldSendDeleteBinding)
    {
        TTestBootstrap bootstrap;
        bootstrap.SendDeleteBindingRequest();
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteBindingRequest>();
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
    }
};

Y_UNIT_TEST_SUITE(TControlPlaneProxyCheckPermissionsFailed) {
    Y_UNIT_TEST(ShouldSendCreateQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateQueryRequestDontWaitForRateLimiter();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendListQueries)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListQueriesRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListQueriesResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDescribeQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeQueryRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendGetQueryStatus)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendGetQueryStatusRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvGetQueryStatusResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendModifyQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyQueryRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDeleteQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteQueryRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendControlQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendControlQueryRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvControlQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendGetResultData)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendGetResultDataRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvGetResultDataResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendListJobs)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListJobsRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListJobsResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDescribeJob)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeJobRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeJobResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendCreateConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateConnectionRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendCreateConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateConnectionRequest({"yq.connections.create@as"}, "my_sa_id");
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendListConnections)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListConnectionsRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListConnectionsResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDescribeConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeConnectionRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendModifyConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyConnectionRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendModifyConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyConnectionRequest({"yq.connections.update@as"}, "my_sa_id");
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDeleteConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteConnectionRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendTestConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendTestConnectionRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvTestConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendTestConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendTestConnectionRequest({"yq.connections.create@as"}, "my_sa_id");
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvTestConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendCreateBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateBindingRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateBindingResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendListBindings)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListBindingsRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListBindingsResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDescribeBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeBindingRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeBindingResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendModifyBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyBindingRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyBindingResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDeleteBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteBindingRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteBindingResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }
};

Y_UNIT_TEST_SUITE(TControlPlaneProxyCheckPermissionsSuccess) {
    Y_UNIT_TEST(ShouldSendCreateQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateQueryRequest({"yq.queries.create@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Request.content().name(), "my_query_name");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListQueries)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListQueriesRequest({"yq.queries.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListQueriesRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeQueryRequest({"yq.queries.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendGetQueryStatus)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendGetQueryStatusRequest({"yq.queries.getStatus@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvGetQueryStatusRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyQueryRequest({"yq.queries.update@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteQueryRequest({"yq.queries.delete@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendControlQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendControlQueryRequest({"yq.queries.control@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvControlQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendGetResultData)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendGetResultDataRequest({"yq.queries.getData@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvGetResultDataRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListJobs)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListJobsRequest({"yq.jobs.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListJobsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Request.query_id(), "my_query_id");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeJob)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeJobRequest({"yq.jobs.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeJobRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateConnectionRequest({"yq.connections.create@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateConnectionRequest({"yq.connections.create@as", "iam.serviceAccounts.use@as"}, "my_sa_id");
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListConnections)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListConnectionsRequest({"yq.connections.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListConnectionsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeConnectionRequest({"yq.connections.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyConnectionRequest({"yq.connections.update@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyConnectionRequest({"yq.connections.update@as", "iam.serviceAccounts.use@as"}, "my_sa_id");
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteConnectionRequest({"yq.connections.delete@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendTestConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendTestConnectionRequest({"yq.connections.create@as"});
        auto request = bootstrap.TestConnectionGrab->GetRequest();
        auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendTestConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendTestConnectionRequest({"yq.connections.create@as", "iam.serviceAccounts.use@as"}, "my_sa_id");
        auto request = bootstrap.TestConnectionGrab->GetRequest();
        auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateBindingRequest({"yq.bindings.create@as", "yq.connections.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListBindings)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListBindingsRequest({"yq.bindings.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListBindingsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeBindingRequest({"yq.bindings.get@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyBindingRequest({"yq.bindings.update@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteBindingRequest({"yq.bindings.delete@as"});
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }
};

Y_UNIT_TEST_SUITE(TControlPlaneProxyCheckPermissionsControlPlaneStorageSuccess) {
    Y_UNIT_TEST(ShouldSendCreateQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateQueryRequest({
            "yq.queries.create@as",
            "yq.queries.invoke@as",
            "yq.resources.managePublic@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Request.content().name(), "my_query_name");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListQueries)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListQueriesRequest({
            "yq.queries.get@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListQueriesRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeQueryRequest({
            "yq.queries.get@as",
            "yq.queries.viewAst@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as",
            "yq.queries.viewQueryText@as",
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyQueryRequest({
            "yq.queries.update@as",
            "yq.queries.invoke@as",
            "yq.resources.managePublic@as",
            "yq.resources.managePrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteQueryRequest({
            "yq.queries.delete@as",
            "yq.resources.managePublic@as",
            "yq.resources.managePrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendControlQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendControlQueryRequest({
            "yq.queries.control@as",
            "yq.resources.managePublic@as",
            "yq.resources.managePrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvControlQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendGetResultData)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendGetResultDataRequest({
            "yq.queries.getData@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvGetResultDataRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListJobs)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListJobsRequest({
            "yq.jobs.get@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListJobsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Request.query_id(), "my_query_id");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeJob)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeJobRequest({
            "yq.jobs.get@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeJobRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateConnectionRequest({
            "yq.connections.create@as",
            "yq.resources.managePublic@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateConnectionRequest({
            "yq.connections.create@as",
            "iam.serviceAccounts.use@as",
            "yq.resources.managePublic@as"
        }, "my_sa_id");
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListConnections)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListConnectionsRequest({
            "yq.connections.get@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListConnectionsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeConnectionRequest({
            "yq.connections.get@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyConnectionRequest({
            "yq.connections.update@as",
            "yq.resources.managePublic@as",
            "yq.resources.managePrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyConnectionRequest({
            "yq.connections.update@as",
            "iam.serviceAccounts.use@as",
            "yq.resources.managePublic@as",
            "yq.resources.managePrivate@as"
        }, "my_sa_id");
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteConnectionRequest({
            "yq.connections.delete@as",
            "yq.resources.managePublic@as",
            "yq.resources.managePrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendTestConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendTestConnectionRequest({
            "yq.connections.create@as"
        });

        auto request = bootstrap.TestConnectionGrab->GetRequest();
        auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendTestConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendTestConnectionRequest({
            "yq.connections.create@as",
            "iam.serviceAccounts.use@as"
        }, "my_sa_id");

        auto request = bootstrap.TestConnectionGrab->GetRequest();
        auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateBindingRequest({
            "yq.connections.get@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as",
            "yq.bindings.create@as",
            "yq.resources.managePublic@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListBindings)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListBindingsRequest({
            "yq.bindings.get@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListBindingsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeBindingRequest({
            "yq.bindings.get@as",
            "yq.resources.viewPublic@as",
            "yq.resources.viewPrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyBindingRequest({
            "yq.bindings.update@as",
            "yq.resources.managePublic@as",
            "yq.resources.managePrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteBindingRequest({
            "yq.bindings.delete@as",
            "yq.resources.managePublic@as",
            "yq.resources.managePrivate@as"
        });
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }
};

Y_UNIT_TEST_SUITE(TControlPlaneProxyCheckNegativePermissionsFailed) {
    Y_UNIT_TEST(ShouldSendCreateQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.queries.create@as"});
        bootstrap.SendCreateQueryRequestDontWaitForRateLimiter(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendListQueries)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.queries.get@as"});
        bootstrap.SendListQueriesRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListQueriesResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDescribeQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.queries.get@as"});
        bootstrap.SendDescribeQueryRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendGetQueryStatus)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.queries.getStatus@as"});
        bootstrap.SendGetQueryStatusRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvGetQueryStatusResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendModifyQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.queries.update@as"});
        bootstrap.SendModifyQueryRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDeleteQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.queries.delete@as"});
        bootstrap.SendDeleteQueryRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendControlQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.queries.control@as"});
        bootstrap.SendControlQueryRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvControlQueryResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendGetResultData)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.queries.getData@as"});
        bootstrap.SendGetResultDataRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvGetResultDataResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendListJobs)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.jobs.get@as"});
        bootstrap.SendListJobsRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListJobsResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDescribeJob)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.jobs.get@as"});
        bootstrap.SendDescribeJobRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeJobResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendCreateConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.connections.create@as"});
        bootstrap.SendCreateConnectionRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendCreateConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"iam.serviceAccounts.use@as"});
        bootstrap.SendCreateConnectionRequest(permissions, "my_sa_id");
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendListConnections)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.connections.get@as"});
        bootstrap.SendListConnectionsRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListConnectionsResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDescribeConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.connections.get@as"});
        bootstrap.SendDescribeConnectionRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendModifyConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.connections.update@as"});
        bootstrap.SendModifyConnectionRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendModifyConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"iam.serviceAccounts.use@as"});
        bootstrap.SendModifyConnectionRequest(permissions, "my_sa_id");
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDeleteConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.connections.delete@as"});
        bootstrap.SendDeleteConnectionRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendTestConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.connections.create@as"});
        bootstrap.SendTestConnectionRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvTestConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendTestConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"iam.serviceAccounts.use@as"});
        bootstrap.SendTestConnectionRequest(permissions, "my_sa_id");
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvTestConnectionResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendCreateBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.bindings.create@as"});
        bootstrap.SendCreateBindingRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateBindingResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendListBindings)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.bindings.get@as"});
        bootstrap.SendListBindingsRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListBindingsResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDescribeBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.bindings.get@as"});
        bootstrap.SendDescribeBindingRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeBindingResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendModifyBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.bindings.update@as"});
        bootstrap.SendModifyBindingRequest(permissions);
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyBindingResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }

    Y_UNIT_TEST(ShouldSendDeleteBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        auto permissions = AllPermissionsExcept({"yq.bindings.delete@as"});
        bootstrap.SendDeleteBindingRequest();
        const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteBindingResponse>();
        UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
    }
};

Y_UNIT_TEST_SUITE(TControlPlaneProxyCheckNegativePermissionsSuccess) {
    Y_UNIT_TEST(ShouldSendCreateQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateQueryRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Request.content().name(), "my_query_name");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListQueries)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListQueriesRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListQueriesRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeQueryRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendGetQueryStatus)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendGetQueryStatusRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvGetQueryStatusRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyQueryRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteQueryRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendControlQuery)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendControlQueryRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvControlQueryRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendGetResultData)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendGetResultDataRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvGetResultDataRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListJobs)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListJobsRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListJobsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Request.query_id(), "my_query_id");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeJob)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeJobRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeJobRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateConnectionRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateConnectionRequest(AllPermissions(), "my_sa_id");
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListConnections)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListConnectionsRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListConnectionsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeConnectionRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyConnectionRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyConnectionRequest(AllPermissions(), "my_sa_id");
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteConnectionRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendTestConnection)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendTestConnectionRequest(AllPermissions());
        auto request = bootstrap.TestConnectionGrab->GetRequest();
        auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendTestConnectionWithServiceAccount)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendTestConnectionRequest(AllPermissions(), "my_sa_id");
        auto request = bootstrap.TestConnectionGrab->GetRequest();
        auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendCreateBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendCreateBindingRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvCreateBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendListBindings)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendListBindingsRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvListBindingsRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDescribeBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDescribeBindingRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDescribeBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendModifyBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendModifyBindingRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvModifyBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }

    Y_UNIT_TEST(ShouldSendDeleteBinding)
    {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);
        bootstrap.SendDeleteBindingRequest(AllPermissions());
        auto request = bootstrap.MetaStorageGrab->GetRequest();
        auto event = request->Get<TEvControlPlaneStorage::TEvDeleteBindingRequest>();
        auto permissions = event->Permissions;
        UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
        UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
        UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
        UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
    }
};

Y_UNIT_TEST_SUITE(TControlPlaneProxyShouldPassHids) {
    Y_UNIT_TEST(ShouldCheckScenario) {
        NConfig::TControlPlaneProxyConfig config;
        config.SetEnablePermissions(true);
        TTestBootstrap bootstrap(config);

        const TVector<TString> testUser1Permissions {
            "yq.resources.viewPrivate@as",
            "yq.resources.managePrivate@as",

            // yq.connections.*
            "yq.connections.create@as",
            "yq.connections.update@as",
            "yq.connections.delete@as",

            // yq.bindings.*
            "yq.bindings.create@as",
            "yq.bindings.update@as",
            "yq.bindings.delete@as",

            // yq.queries.*
            "yq.queries.create@as",
            "yq.queries.update@as",
            "yq.queries.delete@as",
            "yq.queries.control@as",

            // yq.jobs.*

            "yq.resources.managePublic@as",
            "yq.queries.invoke@as",
            "yq.queries.getData@as",
            "yq.queries.getStatus@as",
            "yq.connections.get@as",
            "yq.bindings.get@as",
            "yq.queries.get@as",
            "yq.jobs.get@as",
            "yq.resources.viewPublic@as",
        };

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateQueryRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvCreateQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Request.content().name(), "my_query_name");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListQueriesRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListQueriesRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeQueryRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendGetQueryStatusRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvGetQueryStatusRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyQueryRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvModifyQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteQueryRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDeleteQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendControlQueryRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvControlQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendGetResultDataRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvGetResultDataRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListJobsRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListJobsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Request.query_id(), "my_query_id");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeJobRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeJobRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateConnectionRequest(testUser1Permissions, {}, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListConnectionsRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListConnectionsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeConnectionRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyConnectionRequest(testUser1Permissions, {}, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteConnectionRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDeleteConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendTestConnectionRequest(testUser1Permissions, {}, "test_user_1@staff");
            auto request = bootstrap.TestConnectionGrab->GetRequest();
            auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateBindingRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvCreateBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListBindingsRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListBindingsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeBindingRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyBindingRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvModifyBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteBindingRequest(testUser1Permissions, "test_user_1@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDeleteBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        const TVector<TString> testUser2Permissions {
            // yq.connections.*
            "yq.connections.create@as",
            "yq.connections.update@as",
            "yq.connections.delete@as",

            // yq.bindings.*
            "yq.bindings.create@as",
            "yq.bindings.update@as",
            "yq.bindings.delete@as",

            // yq.queries.*
            "yq.queries.create@as",
            "yq.queries.update@as",
            "yq.queries.delete@as",
            "yq.queries.control@as",

            // yq.jobs.*

            "yq.resources.managePublic@as",
            "yq.queries.invoke@as",
            "yq.queries.getData@as",
            "yq.queries.getStatus@as",
            "yq.connections.get@as",
            "yq.bindings.get@as",
            "yq.queries.get@as",
            "yq.jobs.get@as",
            "yq.resources.viewPublic@as",
        };

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateQueryRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvCreateQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Request.content().name(), "my_query_name");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListQueriesRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListQueriesRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeQueryRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendGetQueryStatusRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvGetQueryStatusRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyQueryRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvModifyQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteQueryRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDeleteQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendControlQueryRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvControlQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendGetResultDataRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvGetResultDataRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListJobsRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListJobsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Request.query_id(), "my_query_id");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeJobRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeJobRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateConnectionRequest(testUser2Permissions, {}, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvCreateConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListConnectionsRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListConnectionsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeConnectionRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyConnectionRequest(testUser2Permissions, {}, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvModifyConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteConnectionRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDeleteConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendTestConnectionRequest(testUser2Permissions, {}, "test_user_2@staff");
            auto request = bootstrap.TestConnectionGrab->GetRequest();
            auto event = request->Get<TEvTestConnection::TEvTestConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateBindingRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvCreateBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListBindingsRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListBindingsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeBindingRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyBindingRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvModifyBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteBindingRequest(testUser2Permissions, "test_user_2@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDeleteBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        const TVector<TString> testUser3Permissions {
            "yq.connections.get@as",
            "yq.bindings.get@as",
            "yq.queries.get@as",
            "yq.jobs.get@as",
            "yq.resources.viewPublic@as",
        };

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateQueryRequestDontWaitForRateLimiter(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateQueryResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListQueriesRequest(testUser3Permissions, "test_user_3@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListQueriesRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeQueryRequest(testUser3Permissions, "test_user_3@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendGetQueryStatusRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvGetQueryStatusResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyQueryRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyQueryResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteQueryRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteQueryResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendControlQueryRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvControlQueryResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendGetResultDataRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvGetResultDataResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListJobsRequest(testUser3Permissions, "test_user_3@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListJobsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Request.query_id(), "my_query_id");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeJobRequest(testUser3Permissions, "test_user_3@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeJobRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateConnectionRequest(testUser3Permissions, {}, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListConnectionsRequest(testUser3Permissions, "test_user_3@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListConnectionsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeConnectionRequest(testUser3Permissions, "test_user_3@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeConnectionRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyConnectionRequest(testUser3Permissions, {}, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteConnectionRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendTestConnectionRequest(testUser3Permissions, {}, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvTestConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateBindingRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateBindingResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListBindingsRequest(testUser3Permissions, "test_user_3@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvListBindingsRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeBindingRequest(testUser3Permissions, "test_user_3@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvDescribeBindingRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyBindingRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyBindingResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteBindingRequest(testUser3Permissions, "test_user_3@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteBindingResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        const TVector<TString> testUser4Permissions {
            "yq.queries.invoke@as",
            "yq.queries.create@as",
            "yq.queries.getData@as",
            "yq.queries.getStatus@as",
            "yq.resources.viewPublic@as",
        };

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateQueryRequest(testUser4Permissions, "test_user_4@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvCreateQueryRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Request.content().name(), "my_query_name");
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListQueriesRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListQueriesResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeQueryRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeQueryResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendGetQueryStatusRequest(testUser4Permissions, "test_user_4@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvGetQueryStatusRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyQueryRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyQueryResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteQueryRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteQueryResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendControlQueryRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvControlQueryResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendGetResultDataRequest(testUser4Permissions, "test_user_4@staff");
            auto request = bootstrap.MetaStorageGrab->GetRequest();
            auto event = request->Get<TEvControlPlaneStorage::TEvGetResultDataRequest>();
            auto permissions = event->Permissions;
            UNIT_ASSERT_VALUES_EQUAL(event->Scope, "yandexcloud://my_folder");
            UNIT_ASSERT(permissions.Check(TPermissions::VIEW_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_AST));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PUBLIC));
            UNIT_ASSERT(!permissions.Check(TPermissions::MANAGE_PRIVATE));
            UNIT_ASSERT(!permissions.Check(TPermissions::QUERY_INVOKE));
            UNIT_ASSERT(!permissions.Check(TPermissions::VIEW_QUERY_TEXT));
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListJobsRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListJobsResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeJobRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeJobResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateConnectionRequest(testUser4Permissions, {}, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListConnectionsRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListConnectionsResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeConnectionRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyConnectionRequest(testUser4Permissions, {}, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteConnectionRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendTestConnectionRequest(testUser4Permissions, {}, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvTestConnectionResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendCreateBindingRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvCreateBindingResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendListBindingsRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvListBindingsResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDescribeBindingRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDescribeBindingResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendModifyBindingRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvModifyBindingResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }

        {
            NConfig::TControlPlaneProxyConfig config;
            config.SetEnablePermissions(true);
            TTestBootstrap bootstrap(config);
            bootstrap.SendDeleteBindingRequest(testUser4Permissions, "test_user_4@staff");
            const auto [_, response] = bootstrap.Grab<TEvControlPlaneProxy::TEvDeleteBindingResponse>();
            UNIT_ASSERT_STRING_CONTAINS(response->Issues.ToString(), "Error: No permission");
        }
    }
};



} // namespace NFq
