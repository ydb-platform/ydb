#include <ydb/core/testlib/test_client.h>
#include <ydb/core/base/auth.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/grpc_request_check_actor.h>
#include <ydb/core/grpc_services/base/http_database_access_verdict.h>
#include <ydb/core/grpc_services/counters/proxy_counters.h>
#include <ydb/core/tx/scheme_board/events.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
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

    TTestSetup(const TString& userSid, const TString& dbPath, const std::vector<std::pair<TString, TString>>& userAttributes)
        : KikimrPort(PortManager.GetPort(2134))
        , AccessServiceEndpoint("localhost:" + ToString(PortManager.GetPort(4284)))
        , Server(std::make_unique<TServer>(GetSettings()))
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

    TServerSettings GetSettings() {
        NKikimrProto::TAuthConfig authConfig;
        authConfig.SetUseBlackBox(false);
        authConfig.SetUseAccessService(true);
        authConfig.SetAccessServiceType("Yandex_v2");
        authConfig.SetUseAccessServiceApiKey(false);
        authConfig.SetUseAccessServiceTLS(false);
        authConfig.SetAccessServiceEndpoint(AccessServiceEndpoint);
        authConfig.SetUseStaff(false);

        auto settings = TServerSettings(KikimrPort, authConfig);
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

namespace {

constexpr TStringBuf DatabaseOnlySid = "database-only@as";
constexpr TStringBuf ViewerOnlySid = "viewer-only@as";
constexpr TStringBuf MonitoringOnlySid = "monitoring-only@as";
constexpr TStringBuf AdminOnlySid = "admin-only@as";

void ConfigureSecurityConfig(TTestActorRuntime* runtime) {
    auto& securityConfig = *runtime->GetAppData().DomainsConfig.MutableSecurityConfig();
    securityConfig.AddDatabaseAllowedSIDs(TString{DatabaseOnlySid});
    securityConfig.AddViewerAllowedSIDs(TString{ViewerOnlySid});
    securityConfig.AddMonitoringAllowedSIDs(TString{MonitoringOnlySid});
    securityConfig.AddAdministrationAllowedSIDs(TString{AdminOnlySid});
    runtime->GetAppData().AdministrationAllowedSIDs = {TString{AdminOnlySid}};
}

void SetupDedicatedSubDomain(
    TSchemeBoardEvents::TDescribeSchemeResult& describeSchemeResult,
    const TString& path,
    const ui64 pathId = 100,
    const ui64 schemeShard = 1)
{
    describeSchemeResult.SetPath(CanonizePath(path));
    auto* pathDescription = describeSchemeResult.MutablePathDescription();
    auto* self = pathDescription->MutableSelf();
    const TVector<TString> parts = SplitPath(path);
    self->SetName(parts.back());
    self->SetPathType(NKikimrSchemeOp::EPathTypeSubDomain);
    self->SetPathId(pathId);
    self->SetSchemeshardId(schemeShard);

    auto* domainDescription = pathDescription->MutableDomainDescription();
    domainDescription->MutableDomainKey()->SetSchemeShard(schemeShard);
    domainDescription->MutableDomainKey()->SetPathId(pathId);
    domainDescription->MutableResourcesDomainKey()->SetSchemeShard(schemeShard);
    domainDescription->MutableResourcesDomainKey()->SetPathId(pathId);
}

void SetupTopicInDedicatedSubDomain(
    TSchemeBoardEvents::TDescribeSchemeResult& describeSchemeResult,
    const TString& databasePath,
    const TString& topicName,
    const ui64 databasePathId = 100,
    const ui64 topicPathId = 101,
    const ui64 schemeShard = 1)
{
    const TString path = databasePath + "/" + topicName;
    describeSchemeResult.SetPath(CanonizePath(path));
    auto* pathDescription = describeSchemeResult.MutablePathDescription();
    auto* self = pathDescription->MutableSelf();
    self->SetName(topicName);
    self->SetPathType(NKikimrSchemeOp::EPathTypePersQueueGroup);
    self->SetPathId(topicPathId);
    self->SetSchemeshardId(schemeShard);

    auto* domainDescription = pathDescription->MutableDomainDescription();
    domainDescription->MutableDomainKey()->SetSchemeShard(schemeShard);
    domainDescription->MutableDomainKey()->SetPathId(databasePathId);
    domainDescription->MutableResourcesDomainKey()->SetSchemeShard(schemeShard);
    domainDescription->MutableResourcesDomainKey()->SetPathId(databasePathId);
}

TIntrusivePtr<TSecurityObject> MakeSecurityObjectWithConnect(const TString& userSid) {
    NACLib::TSecurityObject object("owner", false);
    object.AddAccess(NACLib::EAccessType::Allow, NACLib::EAccessRights::ConnectDatabase, userSid);
    return MakeIntrusive<TSecurityObject>(object.GetOwnerSID(), object.GetACL().SerializeAsString(), false);
}

TIntrusivePtr<TSecurityObject> MakeSecurityObjectWithoutConnect() {
    NACLib::TSecurityObject object("owner", false);
    return MakeIntrusive<TSecurityObject>(object.GetOwnerSID(), object.GetACL().SerializeAsString(), false);
}

struct THttpAuthCheckResponse {
    TAutoPtr<IEventHandle> Handle;
    const NGRpcService::TEvRequestAuthAndCheckResult* Result = nullptr;
};

THttpAuthCheckResponse RunHttpAuthCheck(
    TTestSetup& setup,
    const TString& requestDatabase,
    TSchemeBoardEvents::TDescribeSchemeResult& describeSchemeResult,
    TIntrusivePtr<TSecurityObject> securityObject)
{
    TTestActorRuntime* runtime = setup.GetRuntime();
    runtime->GetAppData().FeatureFlags.SetCheckDatabaseAccessPermission(false);

    const TString userToken = "Bearer " + setup.UserSid;
    auto ev = std::make_unique<NGRpcService::TEvRequestAuthAndCheck>(
        requestDatabase,
        TMaybe<TString>(userToken),
        setup.FakeMonActor,
        NGRpcService::TAuditMode::Modifying(NGRpcService::TAuditMode::TLogClassConfig::ClusterAdmin),
        "192.168.0.101");

    std::unique_ptr<IEventHandle> ieh = std::make_unique<IEventHandle>(
        NGRpcService::CreateGRpcRequestProxyId(),
        setup.FakeMonActor,
        ev.release(),
        IEventHandle::FlagTrackDelivery);

    TAutoPtr<TEventHandle<NGRpcService::TEvRequestAuthAndCheck>> request =
        reinterpret_cast<TEventHandle<NGRpcService::TEvRequestAuthAndCheck>*>(ieh.release());

    TActorId fakeGrpcRequestProxy = runtime->AllocateEdgeActor();
    runtime->Register(CreateGrpcRequestCheckActor<NGRpcService::TEvRequestAuthAndCheck>(
        fakeGrpcRequestProxy,
        describeSchemeResult,
        std::move(securityObject),
        request,
        NGRpcService::CreateGRpcProxyCounters(runtime->GetAppData().Counters),
        false,
        setup.RootAttributes,
        nullptr,
        {
            .UseAccessService = true,
            .NeedClusterAccessResourceCheck = true,
            .AccessServiceType = runtime->GetAppData().AuthConfig.GetAccessServiceType(),
        }));

    THttpAuthCheckResponse response;
    response.Result = runtime->GrabEdgeEvent<NGRpcService::TEvRequestAuthAndCheckResult>(response.Handle);
    UNIT_ASSERT_C(response.Result, "Expected TEvRequestAuthAndCheckResult");
    return response;
}

} // namespace

Y_UNIT_TEST_SUITE(HttpDatabaseAccessObserveMode) {

Y_UNIT_TEST(DedicatedOwnDbOk) {
    TTestSetup setup("database-only", "/Root/db", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupDedicatedSubDomain(describeSchemeResult, "/Root/db");
    const auto response = RunHttpAuthCheck(
        setup, "/Root/db", describeSchemeResult, MakeSecurityObjectWithConnect(TString{DatabaseOnlySid}));
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::Ok);
    UNIT_ASSERT_EQUAL(response.Result->Database, "/Root/db");
}

Y_UNIT_TEST(DedicatedNoConnectRightButSuccess) {
    TTestSetup setup("database-only", "/Root/db", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupDedicatedSubDomain(describeSchemeResult, "/Root/db");
    const auto response = RunHttpAuthCheck(setup, "/Root/db", describeSchemeResult, MakeSecurityObjectWithoutConnect());
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::NoConnectRight);
}

Y_UNIT_TEST(NoSecurityObject) {
    TTestSetup setup("database-only", "/Root/db", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupDedicatedSubDomain(describeSchemeResult, "/Root/db");
    const auto response = RunHttpAuthCheck(setup, "/Root/db", describeSchemeResult, nullptr);
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::NoSecurityObject);
}

Y_UNIT_TEST(ServerlessOwnDbOk) {
    TTestSetup setup("database-only", "/Root/serverless", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupDedicatedSubDomain(describeSchemeResult, "/Root/serverless", 2);
    describeSchemeResult.MutablePathDescription()->MutableSelf()->SetPathType(NKikimrSchemeOp::EPathTypeExtSubDomain);
    const auto response = RunHttpAuthCheck(
        setup, "/Root/serverless", describeSchemeResult, MakeSecurityObjectWithConnect(TString{DatabaseOnlySid}));
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::Ok);
}

Y_UNIT_TEST(ServerlessForeignNoConnectRight) {
    TTestSetup setup("database-only", "/Root/foreign", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupDedicatedSubDomain(describeSchemeResult, "/Root/foreign", 3);
    describeSchemeResult.MutablePathDescription()->MutableSelf()->SetPathType(NKikimrSchemeOp::EPathTypeExtSubDomain);
    const auto response = RunHttpAuthCheck(setup, "/Root/foreign", describeSchemeResult, MakeSecurityObjectWithoutConnect());
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::NoConnectRight);
}

Y_UNIT_TEST(TopicPathNotADatabaseDespiteConnectRight) {
    TTestSetup setup("database-only", "/Root/db", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupTopicInDedicatedSubDomain(describeSchemeResult, "/Root/db", "mytopic");
    const auto response = RunHttpAuthCheck(
        setup, "/Root/db/mytopic", describeSchemeResult, MakeSecurityObjectWithConnect(TString{DatabaseOnlySid}));
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::NotADatabase);
}

Y_UNIT_TEST(EmptyDatabase) {
    TTestSetup setup("database-only", "/Root/db", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupDedicatedSubDomain(describeSchemeResult, "/Root/db");
    const auto response = RunHttpAuthCheck(
        setup, "", describeSchemeResult, MakeSecurityObjectWithConnect(TString{DatabaseOnlySid}));
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::EmptyDatabase);
}

Y_UNIT_TEST(RootDatabase) {
    TTestSetup setup("database-only", "/Root", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupDedicatedSubDomain(describeSchemeResult, "/Root", 1);
    const auto response = RunHttpAuthCheck(
        setup, "/Root", describeSchemeResult, MakeSecurityObjectWithConnect(TString{DatabaseOnlySid}));
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::Ok);
}

Y_UNIT_TEST(ViewerTokenSkipsVerdict) {
    TTestSetup setup("viewer-only", "/Root/db", {});
    ConfigureSecurityConfig(setup.GetRuntime());
    TSchemeBoardEvents::TDescribeSchemeResult describeSchemeResult;
    SetupDedicatedSubDomain(describeSchemeResult, "/Root/db");
    const auto response = RunHttpAuthCheck(setup, "/Root/db", describeSchemeResult, MakeSecurityObjectWithoutConnect());
    UNIT_ASSERT_EQUAL(response.Result->Status, Ydb::StatusIds::SUCCESS);
    UNIT_ASSERT_EQUAL(response.Result->DatabaseAccessVerdict, NGRpcService::EHttpDatabaseAccessVerdict::Ok);
}

} // HttpDatabaseAccessObserveMode

