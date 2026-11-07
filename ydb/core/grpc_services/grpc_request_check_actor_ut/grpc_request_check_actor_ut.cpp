#include <ydb/core/testlib/test_client.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_request_check_actor.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/cloud_permissions/cloud_permissions.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

using namespace NKikimr;
using namespace Tests;

namespace {

struct TTestSetup {
    TPortManager PortManager;
    const ui16 KikimrPort;
    const TString AccessServiceEndpoint;
    TTicketParserAccessServiceMockV2 AccessServiceMock;
    std::unique_ptr<grpc::Server> AccessServiceServer;
    std::unique_ptr<TServer> Server;
    TActorId FakeMonActor;
    TSchemeBoardEvents::TDescribeSchemeResult DescribeSchemeResult;
    TVector<std::pair<TString, TString>> RootAttributes;
    const TString UserSid;
    const TString DbPath;

    static const TString PeerName;
    static const TString CloudId;
    static const TString FolderId;
    static const TString DatabaseId;
    static const TString ClusterCloudId;
    static const TString ClusterFolderId;
    static const TString GizmoId;

    static const THashSet<TString> GizmoPermissions;
    static const THashSet<TString> ClusterPermissions;

    TTestSetup(const TString& userSid, const TString& dbPath, const std::vector<std::pair<TString, TString>>& userAttributes,
        bool useRealThreads = true)
        : KikimrPort(PortManager.GetPort(2134))
        , AccessServiceEndpoint("localhost:" + ToString(PortManager.GetPort(4284)))
        , Server(std::make_unique<TServer>(GetSettings(useRealThreads)))
        , UserSid(userSid)
        , DbPath(dbPath)
    {
        StartAccessServiceMock();
        StartYdb();
        InitializeDb(userAttributes);
    }

    TTestActorRuntime* GetRuntime() {
        return Server->GetRuntime();
    }

    TServerSettings GetSettings(bool useRealThreads) {
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetAccessServiceType("Yandex_v2");
        authConfig.SetUseAccessServiceApiKey(false);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(AccessServiceEndpoint);
        authConfig.SetUseStaff(false);

        auto settings = TServerSettings(KikimrPort, authConfig);
        settings.SetUseRealThreads(useRealThreads);
        settings.SetEnableAccessServiceBulkAuthorization(true);
        settings.SetDomainName("Root");
        settings.CreateTicketParser = NKikimr::CreateTicketParser;
        return settings;
    }

    void StartAccessServiceMock() {
        InitAccessServiceMock();
        grpc::ServerBuilder accessServiceServerBuilder;
        accessServiceServerBuilder.AddListeningPort(AccessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&AccessServiceMock);
        AccessServiceServer = accessServiceServerBuilder.BuildAndStart();
    }

    void InitAccessServiceMock() {
        AccessServiceMock.AllowedResourceIds = {
            CloudId,
            FolderId,
            DatabaseId,
            ClusterCloudId,
            ClusterFolderId,
            GizmoId
        };

        AccessServiceMock.AllowedUserPermissions.clear();
        for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
            AccessServiceMock.AllowedUserPermissions.insert(UserSid + "-" + permission);
        }
        for (const auto& permission : GizmoPermissions) {
            AccessServiceMock.AllowedUserPermissions.insert(UserSid + "-" + permission);
        }
        for (const auto& permission : ClusterPermissions) {
            AccessServiceMock.AllowedUserPermissions.insert(UserSid + "-" + permission);
        }
    }

    void StartYdb() {
        TTestActorRuntime* runtime = Server->GetRuntime();
        runtime->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
        runtime->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
        runtime->SetLogPriority(NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::GRPC_SERVER, NLog::PRI_DEBUG);
        runtime->GetAppData().EnforceUserTokenRequirement = true;
        FakeMonActor = runtime->AllocateEdgeActor();
    }

    void InitializeDb(const std::vector<std::pair<TString, TString>>& userAttributes) {
        DescribeSchemeResult.SetPath(DbPath);
        auto pathDescription = DescribeSchemeResult.MutablePathDescription();
        for (const auto& [key, value] : userAttributes) {
            auto userAttribute = pathDescription->AddUserAttributes();
            userAttribute->SetKey(key);
            userAttribute->SetValue(value);
        }

        RootAttributes =  {
            {"cloud_id", ClusterCloudId},
            {"folder_id", ClusterFolderId}
        };
    }

    template <typename TEvType>
    void RequestCheckActor(std::unique_ptr<TEvType>&& ev) {
        NACLib::TSecurityObject object("owner", false);
        object.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::ConnectDatabase, UserSid);
        TIntrusivePtr<TSecurityObject> securityObject = MakeIntrusive<TSecurityObject>(object.GetOwnerSID(), object.GetACL().SerializeAsString(), false);

        std::unique_ptr<IEventHandle> ieh = std::make_unique<IEventHandle>(NGRpcService::CreateGRpcRequestProxyId(),
            FakeMonActor,
            ev.release(),
            IEventHandle::FlagTrackDelivery);

        TAutoPtr<TEventHandle<TEvType>> request = reinterpret_cast<TEventHandle<TEvType>*>(ieh.release());

        TTestActorRuntime* runtime = Server->GetRuntime();
        TActorId fakeGrpcRequestProxy = runtime->AllocateEdgeActor();
        runtime->Register(CreateGrpcRequestCheckActor<TEvType>(fakeGrpcRequestProxy,
            DescribeSchemeResult,
            securityObject,
            request,
            NGRpcService::CreateGRpcProxyCounters(runtime->GetAppData().Counters), // Counters
            false,
            RootAttributes,
            nullptr, // FacilityProvider
            {
                .UseAccessService = true,
                .NeedClusterAccessResourceCheck = true,
                .AccessServiceType = runtime->GetAppData().AuthConfig.GetAccessServiceType()
            }));
    }
};

const TString TTestSetup::PeerName = "ipv4:192.168.0.101:2135";
const TString TTestSetup::CloudId = "cloud12345";
const TString TTestSetup::FolderId = "folder12345";
const TString TTestSetup::DatabaseId = "database12345";
const TString TTestSetup::ClusterCloudId = "cluster.cloud98765";
const TString TTestSetup::ClusterFolderId = "cluster.folder98765";
const TString TTestSetup::GizmoId = "gizmo";

const THashSet<TString> TTestSetup::GizmoPermissions {
    "ydb.developerApi.get",
    "ydb.developerApi.update",
};

const THashSet<TString> TTestSetup::ClusterPermissions {
    "ydb.clusters.get",
    "ydb.clusters.monitor",
    "ydb.clusters.manage",
};

void CheckResourceDatabaseResolution(
    NSchemeCache::TSchemeCacheNavigate::EStatus status,
    Ydb::StatusIds::StatusCode expectedStatus)
{
    TTestSetup setup("user1", "/Root/serverless", {}, false);
    const TPathId domainKey(1, 100);
    const TPathId resourceDomainKey(1, 200);
    auto* domainDescription = setup.DescribeSchemeResult.MutablePathDescription()->MutableDomainDescription();
    domainDescription->MutableDomainKey()->SetSchemeShard(domainKey.OwnerId);
    domainDescription->MutableDomainKey()->SetPathId(domainKey.LocalPathId);
    domainDescription->MutableResourcesDomainKey()->SetSchemeShard(resourceDomainKey.OwnerId);
    domainDescription->MutableResourcesDomainKey()->SetPathId(resourceDomainKey.LocalPathId);

    auto* runtime = setup.GetRuntime();
    size_t resolveRequests = 0;
    const auto observer = runtime->AddObserver<TEvTxProxySchemeCache::TEvNavigateKeySet>(
        [&](TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
            auto& navigate = ev->Get()->Request;
            if (navigate->ResultSet.size() != 1 || navigate->ResultSet.front().TableId.PathId != resourceDomainKey) {
                return;
            }
            ++resolveRequests;
            UNIT_ASSERT(!navigate->UserToken);
            auto& entry = navigate->ResultSet.front();
            entry.Status = status;
            entry.Path = SplitPath("/Root/shared");
            navigate->ErrorCount = status == NSchemeCache::TSchemeCacheNavigate::EStatus::Ok ? 0 : 1;
            runtime->Send(new IEventHandle(ev->Sender, ev->Recipient,
                new TEvTxProxySchemeCache::TEvNavigateKeySetResult(navigate), 0, ev->Cookie));
            ev.Reset();
        });

    const TString userToken = "Bearer " + setup.UserSid;
    auto ev = std::make_unique<NGRpcService::TEvRequestAuthAndCheck>(
        setup.DbPath,
        TMaybe<TString>(userToken),
        setup.FakeMonActor,
        NGRpcService::TAuditMode::Modifying(NGRpcService::TAuditMode::TLogClassConfig::ClusterAdmin),
        TTestSetup::PeerName);
    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    auto* response = runtime->GrabEdgeEvent<NGRpcService::TEvRequestAuthAndCheckResult>(handle);
    UNIT_ASSERT_VALUES_EQUAL(resolveRequests, 1);
    UNIT_ASSERT_VALUES_EQUAL_C(response->Status, expectedStatus, status);

    if (expectedStatus == Ydb::StatusIds::SUCCESS) {
        UNIT_ASSERT(response->UserToken);
        UNIT_ASSERT(response->Issues.Empty());
    } else {
        UNIT_ASSERT_VALUES_EQUAL(response->Issues.Size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(response->Issues.begin()->GetMessage(), "Unknown resource database");
        if (expectedStatus == Ydb::StatusIds::UNAVAILABLE) {
            UNIT_ASSERT_VALUES_EQUAL(response->Issues.begin()->GetCode(), ui32(NKikimrIssues::TIssuesIds::GENERIC_RESOLVE_ERROR));
        }
    }
}

} // namespace

Y_UNIT_TEST_SUITE(TestSetCloudPermissions) {

Y_UNIT_TEST(CanSetAllPermissions) {
    TTestSetup setup("user1", "/Root/db", {
        {"cloud_id", TTestSetup::CloudId},
        {"folder_id", TTestSetup::FolderId},
        {"database_id", TTestSetup::DatabaseId}
    });
    const TString userToken = "Bearer " + setup.UserSid;
    // Use TEvRequestAuthAndCheck to check permissions for gizmo resource
    std::unique_ptr<NGRpcService::TEvRequestAuthAndCheck> ev = std::make_unique<NGRpcService::TEvRequestAuthAndCheck>(
        setup.DbPath,
        TMaybe<TString>(userToken),
        setup.FakeMonActor,
        NGRpcService::TAuditMode::Modifying(NGRpcService::TAuditMode::TLogClassConfig::ClusterAdmin),
        "192.168.0.101");

    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    NGRpcService::TEvRequestAuthAndCheckResult* requestAuthAndCheckResultEv = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TEvRequestAuthAndCheckResult>(handle);
    UNIT_ASSERT_EQUAL(requestAuthAndCheckResultEv->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT(requestAuthAndCheckResultEv->UserToken);
    UNIT_ASSERT_EQUAL_C(requestAuthAndCheckResultEv->UserToken->GetUserSID(), "user1@as", requestAuthAndCheckResultEv->UserToken->GetUserSID());
    UNIT_ASSERT_EQUAL_C(requestAuthAndCheckResultEv->UserToken->GetGroupSIDs().size(), 23, requestAuthAndCheckResultEv->UserToken->GetGroupSIDs().size());
    THashSet<TString> groups;
    for (const auto& p : requestAuthAndCheckResultEv->UserToken->GetGroupSIDs()) {
        groups.insert(p);
    }
    // Check that user has all permissions, check_actor sends all permissions to exam them
    for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(groups.contains(permission + "-" + TTestSetup::DatabaseId + "@as"), permission + "-" + TTestSetup::DatabaseId + "@as");
    }
    for (const auto& permission : TTestSetup::GizmoPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(groups.contains(permission + "-" + TTestSetup::GizmoId + "@as"), permission + "-" + TTestSetup::GizmoId + "@as");
    }
    for (const auto& permission : TTestSetup::ClusterPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
    }
}

Y_UNIT_TEST(CanSetPermissionsWithoutGizmoResourse) {
    TTestSetup setup("user1", "/Root/db", {
        {"cloud_id", TTestSetup::CloudId},
        {"folder_id", TTestSetup::FolderId},
        {"database_id", TTestSetup::DatabaseId}
    });
    const TString userToken = "Bearer " + setup.UserSid;
    // Use TRefreshTokenGenericRequest to simple initialize it
    std::unique_ptr<NGRpcService::TRefreshTokenGenericRequest> ev = std::make_unique<NGRpcService::TRefreshTokenGenericRequest>(
        userToken,
        setup.DbPath,
        TTestSetup::PeerName,
        setup.FakeMonActor);
    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    NGRpcService::TRefreshTokenGenericRequest* refreshTokenGenericRequestEv = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TRefreshTokenGenericRequest>(handle);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetPeerName(), TTestSetup::PeerName);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetAuthState().State, NYdbGrpc::TAuthState::AS_OK);
    UNIT_ASSERT(refreshTokenGenericRequestEv->GetInternalToken());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID(), "user1@as", refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size(), 19, refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size());
    THashSet<TString> groups;
    for (const auto& p : refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs()) {
        groups.insert(p);
    }
    // Check that user has all permissions, check_actor sends all permissions to exam them except for ydb.developerApi.get and ydb.developerApi.update
    for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(groups.contains(permission + "-" + TTestSetup::DatabaseId + "@as"), permission + "-" + TTestSetup::DatabaseId + "@as");
    }
    for (const auto& permission : TTestSetup::ClusterPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
    }
    // Check that permissions for gizmo resourse are absent
    for (const auto& permission : TTestSetup::GizmoPermissions) {
        UNIT_ASSERT_C(!groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::GizmoId + "@as"), permission + "-" + TTestSetup::GizmoId + "@as");
    }
}

Y_UNIT_TEST(CanSetPermissionsForRootDb) {
    TTestSetup setup("user1", "/Root", {
        {"cloud_id", TTestSetup::ClusterCloudId},
        {"folder_id", TTestSetup::ClusterFolderId},
    });
    const TString userToken = "Bearer " + setup.UserSid;
    // Use TRefreshTokenGenericRequest to simple initialize it
    std::unique_ptr<NGRpcService::TRefreshTokenGenericRequest> ev = std::make_unique<NGRpcService::TRefreshTokenGenericRequest>(
        userToken,
        setup.DbPath,
        TTestSetup::PeerName,
        setup.FakeMonActor);
    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    NGRpcService::TRefreshTokenGenericRequest* refreshTokenGenericRequestEv = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TRefreshTokenGenericRequest>(handle);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetPeerName(), TTestSetup::PeerName);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetAuthState().State, NYdbGrpc::TAuthState::AS_OK);
    UNIT_ASSERT(refreshTokenGenericRequestEv->GetInternalToken());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID(), "user1@as", refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size(), 12, refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size());
    THashSet<TString> groups;
    for (const auto& p : refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs()) {
        groups.insert(p);
    }
    // Check that user has all permissions, check_actor sends all permissions to exam them
    for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
        // Attribute database_id is not set for root db, user has not virtual group in format <permission>-<database_id>@as
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::DatabaseId + "@as"), permission + "-" + TTestSetup::DatabaseId + "@as");
    }
    for (const auto& permission : TTestSetup::ClusterPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
    }
    // Check that permissions for gizmo resourse are absent
    for (const auto& permission : TTestSetup::GizmoPermissions) {
        UNIT_ASSERT_C(!groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::GizmoId + "@as"), permission + "-" + TTestSetup::GizmoId + "@as");
    }
}

Y_UNIT_TEST(CanSetPermissionsForDbWithoutCloudUserAttributes) {
    TTestSetup setup("user1", "/Root/db", {
        {"test_attr_1", "111"},
        {"test_attr_2", "222"},
        {"test_attr_1", "333"}
    });
    const TString userToken = "Bearer " + setup.UserSid;
    // Use TRefreshTokenGenericRequest to simple initialize it
    std::unique_ptr<NGRpcService::TRefreshTokenGenericRequest> ev = std::make_unique<NGRpcService::TRefreshTokenGenericRequest>(
        userToken,
        setup.DbPath,
        TTestSetup::PeerName,
        setup.FakeMonActor);
    setup.RequestCheckActor(std::move(ev));

    TAutoPtr<IEventHandle> handle;
    NGRpcService::TRefreshTokenGenericRequest* refreshTokenGenericRequestEv = setup.GetRuntime()->GrabEdgeEvent<NGRpcService::TRefreshTokenGenericRequest>(handle);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetPeerName(), TTestSetup::PeerName);
    UNIT_ASSERT_EQUAL(refreshTokenGenericRequestEv->GetAuthState().State, NYdbGrpc::TAuthState::AS_OK);
    UNIT_ASSERT(refreshTokenGenericRequestEv->GetInternalToken());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID(), "user1@as", refreshTokenGenericRequestEv->GetInternalToken()->GetUserSID());
    UNIT_ASSERT_EQUAL_C(refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size(), 5, refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs().size());
    THashSet<TString> groups;
    for (const auto& p : refreshTokenGenericRequestEv->GetInternalToken()->GetGroupSIDs()) {
        groups.insert(p);
    }
    // /Root/db has not user attributes associated with cloud resources cloud_id, folder_id, database_id
    for (const auto& permission : NCloudPermissions::TCloudPermissions<NCloudPermissions::EType::DEFAULT>::Get()) {
        UNIT_ASSERT_C(!groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::DatabaseId + "@as"), permission + "-" + TTestSetup::DatabaseId + "@as");
    }
    for (const auto& permission : TTestSetup::ClusterPermissions) {
        UNIT_ASSERT_C(groups.contains(permission + "@as"), permission + "@as");
    }
    // Check that permissions for gizmo resourse are absent
    for (const auto& permission : TTestSetup::GizmoPermissions) {
        UNIT_ASSERT_C(!groups.contains(permission + "@as"), permission + "@as");
        UNIT_ASSERT_C(!groups.contains(permission + "-" + TTestSetup::GizmoId + "@as"), permission + "-" + TTestSetup::GizmoId + "@as");
    }
}

} // CheckCloudPermissions

Y_UNIT_TEST_SUITE(ResourceDatabaseResolution) {

Y_UNIT_TEST(LookupErrorIsRetryable) {
    CheckResourceDatabaseResolution(NSchemeCache::TSchemeCacheNavigate::EStatus::LookupError,
        Ydb::StatusIds::UNAVAILABLE);
}

Y_UNIT_TEST(RedirectLookupErrorIsRetryable) {
    CheckResourceDatabaseResolution(NSchemeCache::TSchemeCacheNavigate::EStatus::RedirectLookupError,
        Ydb::StatusIds::UNAVAILABLE);
}

Y_UNIT_TEST(AccessDenied) {
    CheckResourceDatabaseResolution(NSchemeCache::TSchemeCacheNavigate::EStatus::AccessDenied,
        Ydb::StatusIds::UNAUTHORIZED);
}

Y_UNIT_TEST(PermanentErrors) {
    using EStatus = NSchemeCache::TSchemeCacheNavigate::EStatus;
    for (const auto status : {EStatus::Unknown, EStatus::RootUnknown, EStatus::PathErrorUnknown,
        EStatus::PathNotTable, EStatus::PathNotPath, EStatus::TableCreationNotComplete})
    {
        CheckResourceDatabaseResolution(status, Ydb::StatusIds::UNAUTHORIZED);
    }
}

Y_UNIT_TEST(Success) {
    CheckResourceDatabaseResolution(NSchemeCache::TSchemeCacheNavigate::EStatus::Ok,
        Ydb::StatusIds::SUCCESS);
}

}
