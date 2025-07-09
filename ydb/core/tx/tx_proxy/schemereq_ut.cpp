#include <library/cpp/testing/unittest/registar.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/discovery/discovery.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/storage_pools.h>

#include <ydb/core/testlib/test_client.h>

#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/protos/console_tenant.pb.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>


namespace NKikimr::NTxProxyUT {

using namespace NYdb;

// TTestEnv from proxy_ut_helpers.h does not fit for the tuning we need here.
class TTestEnv {
public:
    TString RootToken;  // auth token of the superuser
    TString RootPath;  // root database path

    TPortManager PortManager;

    Tests::TServerSettings::TPtr ServerSettings;
    Tests::TServer::TPtr Server;
    THolder<Tests::TClient> Client;
    THolder<Tests::TTenants> Tenants;

    TString Endpoint;
    TDriverConfig DriverConfig;
    THolder<TDriver> Driver;

    Tests::TServer& GetTestServer() const {
        return *Server;
    }

    Tests::TClient& GetTestClient() const {
        return *Client;
    }

    Tests::TTenants& GetTestTenants() const {
        return *Tenants;
    }

    TDriver& GetDriver() const {
        return *Driver;
    }

    const TString& GetEndpoint() const {
        return Endpoint;
    }

    const Tests::TServerSettings& GetSettings() const {
        return *ServerSettings;
    }

    TTestEnv(const Tests::TServerSettings& settings, const TString rootToken) {
        RootToken = rootToken;

        auto mbusPort = PortManager.GetPort();
        auto grpcPort = PortManager.GetPort();

        Cerr << "Starting YDB, grpc: " << grpcPort << ", msgbus: " << mbusPort << Endl;

        ServerSettings = new Tests::TServerSettings;
        ServerSettings->Port = mbusPort;

        // default settings
        ServerSettings->AppConfig = std::make_shared<NKikimrConfig::TAppConfig>();
        ServerSettings->AppConfig->MutableDomainsConfig()->MutableSecurityConfig()->AddAdministrationAllowedSIDs(RootToken);
        ServerSettings->AuthConfig = settings.AuthConfig;
        ServerSettings->AuthConfig.SetUseBuiltinDomain(true);
        ServerSettings->SetEnableMockOnSingleNode(false);

        // settings possible override
        // it's imperative that DomainName was without leading '/' -- is a name
        ServerSettings->SetDomainName(ToString(ExtractDomain(settings.DomainName)));  // also creates storage pool for the root db
        ServerSettings->SetNodeCount(settings.NodeCount);
        ServerSettings->SetDynamicNodeCount(settings.DynamicNodeCount);
        ServerSettings->SetUseRealThreads(settings.UseRealThreads);
        ServerSettings->SetLoggerInitializer(settings.LoggerInitializer);

        // feature flags
        ServerSettings->SetFeatureFlags(settings.FeatureFlags);
        ServerSettings->SetEnableAlterDatabaseCreateHiveFirst(true);

        // additional storage pool type for use by tenant
        ServerSettings->AddStoragePoolType("tenant-db");

        // test server with default logging settings
        Server = new Tests::TServer(ServerSettings);
        Server->EnableGRpc(grpcPort);
        {
            auto& runtime = *Server->GetRuntime();
            runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_REPLICA, NActors::NLog::PRI_ERROR);
            runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_POPULATOR, NActors::NLog::PRI_ERROR);
            runtime.SetLogPriority(NKikimrServices::SCHEME_BOARD_SUBSCRIBER, NActors::NLog::PRI_ERROR);
            runtime.SetLogPriority(NKikimrServices::TX_PROXY_SCHEME_CACHE, NActors::NLog::PRI_ERROR);

            runtime.SetLogPriority(NKikimrServices::TX_PROXY, NActors::NLog::PRI_DEBUG);
        }

        // test tenant control
        Tenants = MakeHolder<Tests::TTenants>(Server);

        // root database path
        // it's imperative that RootPath has leading '/' -- is a path
        RootPath = CanonizePath(ServerSettings->DomainName);

        // test client
        Client = MakeHolder<Tests::TClient>(*ServerSettings);
        Client->SetSecurityToken(RootToken);
        Client->InitRootScheme();
        Client->TestGrant("/", ServerSettings->DomainName, RootToken, NACLib::EAccessRights::GenericFull );

        // driver for actual grpc clients
        Endpoint = "localhost:" + ToString(grpcPort);
        DriverConfig = TDriverConfig()
            .SetEndpoint(Endpoint)
            .SetDatabase(RootPath)
            .SetDiscoveryMode(EDiscoveryMode::Async)
            .SetAuthToken(RootToken)
        ;
        Driver = MakeHolder<TDriver>(DriverConfig);
    }

    ~TTestEnv() {
        Driver->Stop(true);
    }

    TStoragePools CreatePools(const TString& databaseName) {
        TStoragePools result;
        for (const auto& [kind, _]: ServerSettings->StoragePoolTypes) {
            result.emplace_back(Client->CreateStoragePool(kind, databaseName), kind);
        }
        return result;
    }
};

void CreateDatabase(TTestEnv& env, const TString& databaseName) {
    {
        NKikimrSubDomains::TSubDomainSettings subdomain;
        subdomain.SetName(databaseName);
        auto status = env.GetTestClient().CreateExtSubdomain(env.RootPath, subdomain);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
    }
    env.GetTestTenants().Run(JoinPath({env.RootPath, databaseName}), 1);
    {
        NKikimrSubDomains::TSubDomainSettings subdomain;
        subdomain.SetName(databaseName);
        subdomain.SetPlanResolution(50);
        subdomain.SetCoordinators(1);
        subdomain.SetMediators(1);
        subdomain.SetTimeCastBucketsPerMediator(2);
        subdomain.SetExternalSchemeShard(true);
        subdomain.SetExternalHive(true);
        for (auto& pool : env.CreatePools(databaseName)) {
            *subdomain.AddStoragePools() = pool;
        }
        auto status = env.GetTestClient().AlterExtSubdomain(env.RootPath, subdomain);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
    }
}

TString LoginUser(TTestEnv& env, const TString& database, const TString& user, const TString& password) {
    Ydb::Auth::LoginRequest request;
    request.set_user(user);
    request.set_password(password);

    using TEvLoginRequest = NGRpcService::TGRpcRequestWrapperNoAuth<NGRpcService::TRpcServices::EvLogin, Ydb::Auth::LoginRequest, Ydb::Auth::LoginResponse>;

    auto result = NRpcService::DoLocalRpc<TEvLoginRequest>(
        std::move(request), database, {}, env.GetTestServer().GetRuntime()->GetActorSystem(0)
    ).ExtractValueSync();

    const auto& operation = result.operation();
    UNIT_ASSERT_VALUES_EQUAL_C(operation.status(), Ydb::StatusIds::SUCCESS, operation.issues(0).message());
    Ydb::Auth::LoginResult loginResult;
    operation.result().UnpackTo(&loginResult);

    return loginResult.token();
}

TString LoginUser2(TTestEnv& env, const TString& database, const TString& user, const TString& password) {
    ui64 schemeshardId = 0;
    auto runtime = env.GetTestServer().GetRuntime();
    TActorId sender = runtime->AllocateEdgeActor();
    {
        TAutoPtr<NSchemeShard::TEvSchemeShard::TEvDescribeScheme> request(new NSchemeShard::TEvSchemeShard::TEvDescribeScheme());
        request->Record.SetPath(database);
        const ui64 rootSchemeshardId = Tests::ChangeStateStorage(Tests::SchemeRoot, env.GetSettings().Domain);
        ForwardToTablet(*runtime, rootSchemeshardId, sender, request.Release(), 0);

        TAutoPtr<IEventHandle> handle;
        runtime->GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>(handle);
        const auto& record = handle->Get<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult>()->GetRecord();

        schemeshardId = record.GetPathDescription().GetDomainDescription().GetProcessingParams().GetSchemeShard();
    }
    // schemeshardId could be equal to rootSchemeshardId if database is a root
    {
        auto evLogin = new NSchemeShard::TEvSchemeShard::TEvLogin();
        evLogin->Record.SetUser(user);
        evLogin->Record.SetPassword(password);

        ForwardToTablet(*runtime, schemeshardId, sender, evLogin);

        TAutoPtr<IEventHandle> handle;
        auto event = runtime->GrabEdgeEvent<NSchemeShard::TEvSchemeShard::TEvLoginResult>(handle);

        UNIT_ASSERT_C(event->Record.GetError().empty(), event->Record.GetError());
        return event->Record.GetToken();
    }
}

NYdb::NQuery::TQueryClient CreateQueryClient(const TTestEnv& env, const TString& token, const TString& database) {
    NYdb::NQuery::TClientSettings settings;
    settings.Database(database);
    settings.AuthToken(token);
    return NYdb::NQuery::TQueryClient(env.GetDriver(), settings);
}

NYdb::NScheme::TSchemeClient CreateSchemeClient(const TTestEnv& env, const TString& token) {
    NYdb::TCommonClientSettings settings;
    settings.AuthToken(token);
    return NYdb::NScheme::TSchemeClient(env.GetDriver(), settings);
}

NYdb::NDiscovery::TDiscoveryClient CreateDiscoveryClient(const TTestEnv& env, const TString& database, const TString& token) {
    NYdb::TCommonClientSettings settings;
    settings.Database(database);
    settings.AuthToken(token);
    return NYdb::NDiscovery::TDiscoveryClient(env.GetDriver(), settings);
}

void CreateLocalUser(const TTestEnv& env, const TString& database, const TString& name, const TString& token) {
    auto query = Sprintf(
        R"(
            CREATE USER %s PASSWORD 'passwd'
        )",
        name.c_str()
    );
    auto sessionResult = CreateQueryClient(env, token, database).GetSession().ExtractValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto result = sessionResult.GetSession().ExecuteQuery(query,  NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}
void CreateLocalUser(const TTestEnv& env, const TString& database, const TString& user) {
    CreateLocalUser(env, database, user, env.RootToken);
}
void CreateLocalGroup(const TTestEnv& env, const TString& database, const TString& name, const TString& token) {
    auto query = Sprintf(
        R"(
            CREATE GROUP `%s`
        )",
        name.c_str()
    );
    auto sessionResult = CreateQueryClient(env, token, database).GetSession().ExtractValueSync();
    UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
    auto result = sessionResult.GetSession().ExecuteQuery(query,  NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}
void CreateLocalGroup(const TTestEnv& env, const TString& database, const TString& name) {
    CreateLocalGroup(env, database, name, env.RootToken);
}
void CreateLocalUser2(TTestEnv& env, const TString& database, const TString& name, const TString& token) {
    auto runtime = env.GetTestServer().GetRuntime();
    const auto edge = runtime->AllocateEdgeActor(0);
    TString userToken;
    {
        runtime->Send(new IEventHandle(MakeTicketParserID(), edge, new TEvTicketParser::TEvAuthorizeTicket({
            .Database = database,
            .Ticket = token,
            .PeerName = "test",
        })), 0);

        Cerr << __FUNCTION__ << " call ticket_parser" << Endl;

        TAutoPtr<IEventHandle> handle;
        auto event = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        Cerr << __FUNCTION__ << " grab ticket_parser result" << Endl;

        UNIT_ASSERT_C(event->Error.empty(), event->Error);
        UNIT_ASSERT(event->Token != nullptr);
        userToken = event->Token->SerializeAsString();
    }
    {
        TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> ev(new TEvTxUserProxy::TEvProposeTransaction());
        auto& record = ev->Record;
        record.SetDatabaseName(database);
        record.SetUserToken(userToken);

        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetWorkingDir(database);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);

        auto& createUser = *modifyScheme.MutableAlterLogin()->MutableCreateUser();

        createUser.SetUser(name);
        createUser.SetPassword("passwd");

        runtime->Send(new IEventHandle(MakeTxProxyID(), edge, ev.Release()), 0);
        Cerr << __FUNCTION__ << " call tx-proxy" << Endl;

        TAutoPtr<IEventHandle> handle;
        auto event = runtime->GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransactionStatus>(handle);
        Cerr << __FUNCTION__ << " grab tx-proxy result" << Endl;

        UNIT_ASSERT_C(event->Status(), TEvTxUserProxy::TResultStatus::ExecComplete);
        UNIT_ASSERT_VALUES_EQUAL(NKikimrScheme::EStatus(event->Record.GetSchemeShardStatus()), NKikimrScheme::EStatus::StatusSuccess);
    }
}
void CreateLocalGroup2(TTestEnv& env, const TString& database, const TString& name, const TString& token) {
    auto runtime = env.GetTestServer().GetRuntime();
    const auto edge = runtime->AllocateEdgeActor(0);
    TString userToken;
    {
        runtime->Send(new IEventHandle(MakeTicketParserID(), edge, new TEvTicketParser::TEvAuthorizeTicket({
            .Database = database,
            .Ticket = token,
            .PeerName = "test",
        })), 0);

        Cerr << __FUNCTION__ << " call ticket_parser" << Endl;

        TAutoPtr<IEventHandle> handle;
        auto event = runtime->GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicketResult>(handle);
        Cerr << __FUNCTION__ << " grab ticket_parser result" << Endl;

        UNIT_ASSERT_C(event->Error.empty(), event->Error);
        UNIT_ASSERT(event->Token != nullptr);
        userToken = event->Token->SerializeAsString();
    }
    {
        TAutoPtr<TEvTxUserProxy::TEvProposeTransaction> ev(new TEvTxUserProxy::TEvProposeTransaction());
        auto& record = ev->Record;
        record.SetDatabaseName(database);
        record.SetUserToken(userToken);

        auto& modifyScheme = *record.MutableTransaction()->MutableModifyScheme();
        modifyScheme.SetWorkingDir(database);
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterLogin);

        auto& createGroup = *modifyScheme.MutableAlterLogin()->MutableCreateGroup();

        createGroup.SetGroup(name);

        runtime->Send(new IEventHandle(MakeTxProxyID(), edge, ev.Release()), 0);
        Cerr << __FUNCTION__ << " call tx-proxy" << Endl;

        TAutoPtr<IEventHandle> handle;
        auto event = runtime->GrabEdgeEvent<TEvTxUserProxy::TEvProposeTransactionStatus>(handle);
        Cerr << __FUNCTION__ << " grab tx-proxy result" << Endl;

        UNIT_ASSERT_C(event->Status(), TEvTxUserProxy::TResultStatus::ExecComplete);
        UNIT_ASSERT_VALUES_EQUAL(NKikimrScheme::EStatus(event->Record.GetSchemeShardStatus()), NKikimrScheme::EStatus::StatusSuccess);
    }
}

void SetPermissions(const TTestEnv& env, const TString& path, const TString& targetSid, const std::vector<std::string>& permissions) {
    auto client = CreateSchemeClient(env, env.RootToken);
    auto modify = NYdb::NScheme::TModifyPermissionsSettings();
    auto status = client.ModifyPermissions(path, modify.AddSetPermissions({targetSid, permissions}))
        .ExtractValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
}

void ChangeOwner(const TTestEnv& env, const TString& path, const TString& targetSid, const TString& token) {
    auto client = CreateSchemeClient(env, token);
    auto modify = NYdb::NScheme::TModifyPermissionsSettings();
    auto status = client.ModifyPermissions(path, modify.AddChangeOwner(targetSid))
        .ExtractValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
}
void ChangeOwner(const TTestEnv& env, const TString& path, const TString& targetSid) {
    ChangeOwner(env, path, targetSid, env.RootToken);
}

NKikimrSchemeOp::TPathDescription DescribePath(const TTestEnv& env, const TString& path, const TString& token) {
    auto client = Tests::TClient(env.GetSettings());
    client.SetSecurityToken(token);
    auto result = client.Ls(path);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetStatus(), NMsgBusProxy::MSTATUS_OK);
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetSchemeStatus(), NKikimrScheme::StatusSuccess);
    return result->Record.GetPathDescription();
}

NYdb::NScheme::TSchemeEntry DescribePath2(const TTestEnv& env, const TString& database, const TString& path, const TString& token) {
    NYdb::TCommonClientSettings settings;
    settings.Database(database);
    settings.AuthToken(token);
    auto client = NYdb::NScheme::TSchemeClient(env.GetDriver(), settings);

    // auto client = CreateSchemeClient(env, token);
    auto result = client.DescribePath(path).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetEntry();
}

NYdb::NDiscovery::TWhoAmIResult WhoAmI(const TTestEnv& env, const TString& database, const TString& token) {
    auto client = CreateDiscoveryClient(env, database, token);
    auto whoami = NYdb::NDiscovery::TWhoAmISettings().WithGroups(true);
    auto result = client.WhoAmI(whoami).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result;
}


Y_UNIT_TEST_SUITE(SchemeReqAccess) {

    enum class EAccessLevel {
        User,
        DatabaseAdmin,
        ClusterAdmin,
    };

    const TString UserName = "ordinaryuser@builtin";
    const TString DatabaseAdminName = "db_admin@builtin";
    const TString ClusterAdminName = "cluster_admin@builtin";

    TString BuiltinSubjectSid(EAccessLevel level) {
        switch (level) {
            case EAccessLevel::User:
                return UserName;
            case EAccessLevel::DatabaseAdmin:
                return DatabaseAdminName;
            case EAccessLevel::ClusterAdmin:
                return ClusterAdminName;
        }
    }

    const TString LocalUserName = "ordinaryuser";
    const TString LocalDatabaseAdminName = "dbadmin";
    const TString LocalClusterAdminName = "clusteradmin";

    TString LocalSubjectSid(EAccessLevel level) {
        switch (level) {
            case EAccessLevel::User:
                return LocalUserName;
            case EAccessLevel::DatabaseAdmin:
                return LocalDatabaseAdminName;
            case EAccessLevel::ClusterAdmin:
                return LocalClusterAdminName;
        }
    }

    TString ToString(bool arg) {
        return arg ? "true" : "false";
    }

    // dimensions:
    // + EnforceUserTokenRequirement: true or false
    // + EnableStrictUserManagement: true or false
    // + EnableDatabaseAdmin: true or false
    // - database: root or tenant
    // + subject: user, db_admin, cluster_admin
    // + subject: local or non-local
    // - subject is admin directly or by group membership
    // + subject permissions on database
    // + protected operation: create|modify|drop user
    // - protected operation: create|modify|drop group
    // - protected operation: modify ACL - change owner
    // - protected operation: modify ACL - change permissions

    struct TAlterLoginTestCase {
        TString Tag;
        bool PrecreateTarget = false;
        TString SqlStatement;
        bool EnforceUserTokenRequirement = false;
        EAccessLevel SubjectLevel;
        std::vector<std::string> SubjectPermissions;
        bool LocalSid = false;
        bool EnableStrictUserManagement = false;
        bool EnableDatabaseAdmin = false;
        bool ExpectedResult;
    };
//     void AlterLoginProtect_TenantDB(NUnitTest::TTestContext&, const TAlterLoginTestCase params) {
//         auto settings = Tests::TServerSettings()
//             .SetNodeCount(1)
//             .SetDynamicNodeCount(1)
//             .SetEnableStrictUserManagement(params.EnableStrictUserManagement)
//             .SetEnableDatabaseAdmin(params.EnableDatabaseAdmin)
//             // .SetLoggerInitializer([](auto& runtime) {
//             //     runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_INFO);
//             // })
//         ;
//         TTestEnv env(settings, /*rootToken*/ "root@builtin");
//
//         // Turn on mandatory authentication if requested
//         env.GetTestServer().GetRuntime()->GetAppData().EnforceUserTokenRequirement = params.EnforceUserTokenRequirement;
//
//
//
//         env.GetTestServer()->GetRuntime().GetAppData().SetDomainLoginOnly(true);
//
//         // Create tenant database
//         CreateDatabase(env, "tenant-db");
//
//
//     }
    void AlterLoginProtect_RootDB(NUnitTest::TTestContext&, const TAlterLoginTestCase params) {
        auto settings = Tests::TServerSettings()
            .SetNodeCount(1)
            .SetDynamicNodeCount(1)
            .SetEnableStrictUserManagement(params.EnableStrictUserManagement)
            .SetEnableDatabaseAdmin(params.EnableDatabaseAdmin)
            // .SetLoggerInitializer([](auto& runtime) {
            //     runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_INFO);
            // })
        ;
        TTestEnv env(settings, /*rootToken*/ "root@builtin");

        // Test context preparations

        // Turn on mandatory authentication, if requested
        env.GetTestServer().GetRuntime()->GetAppData().EnforceUserTokenRequirement = params.EnforceUserTokenRequirement;

        // Create local user for the subject and obtain auth token, if requested
        TString subjectSid;
        TString subjectToken;
        if (params.LocalSid) {
            subjectSid = LocalSubjectSid(params.SubjectLevel);
            CreateLocalUser(env, env.RootPath, subjectSid);
            subjectToken = LoginUser(env, env.RootPath, subjectSid, "passwd");
        } else {
            subjectSid = subjectToken = BuiltinSubjectSid(params.SubjectLevel);
        }

        // Make subject a proper cluster admin, if requested
        if (params.SubjectLevel == EAccessLevel::ClusterAdmin) {
            env.GetTestServer().GetRuntime()->GetAppData().AdministrationAllowedSIDs.push_back(subjectSid);
        }

        // Give subject requested schema permissions
        SetPermissions(env, env.RootPath, subjectSid, params.SubjectPermissions);

        // Precreate target user, if requested
        if (params.PrecreateTarget) {
            CreateLocalUser(env, env.RootPath, "targetuser");
        }

        // Make subject a proper database admin (by transfer the database ownership to them), if requested
        // This should be the last preparation step (as right after the transfer root@builtin will loose it privileges)
        if (params.SubjectLevel == EAccessLevel::DatabaseAdmin) {
            ChangeOwner(env, env.RootPath, subjectSid);
        }

        // Test body
        {
            auto client = CreateQueryClient(env, subjectToken, env.RootPath);
            auto sessionResult = client.GetSession().ExtractValueSync();
            UNIT_ASSERT_C(sessionResult.IsSuccess(), sessionResult.GetIssues().ToString());
            auto session = sessionResult.GetSession();

            // test body
            auto result = session.ExecuteQuery(params.SqlStatement, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), params.ExpectedResult,
                "query '" << params.SqlStatement << "'"
                << ", subject " << subjectSid
                << ", permissions '" << JoinSeq("|", params.SubjectPermissions) << "'"
                << ", EnforceUserTokenRequirement " << ToString(params.EnforceUserTokenRequirement)
                << ", EnableStrictUserManagement " << ToString(params.EnableStrictUserManagement)
                << ", EnableDatabaseAdmin " << ToString(params.EnableDatabaseAdmin)
                << ", expected result " << ToString(params.ExpectedResult)
                << ", actual result " << ToString(result.IsSuccess()) << " '" << (result.IsSuccess() ? "" : result.GetIssues().ToString()) << "'"
            );
        }
    }
    static const std::vector<TAlterLoginTestCase> AlterLoginProtect_Tests = {
        // CreateUser
        // Cluster admin can always administer users, but require the same schema permissions as ordinary user (but why?).
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        // Database admin can administer users if EnableStrictUserManagement and EnableDatabaseAdmin are true.
        // If not, database admin still can administer users as the owner of the database.
        // In both cases it require no schema permissions except ydb.database.connect (why?).
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        // Ordinary user can create users only if EnableStrictUserManagement is false
        // and ydb.granular.alter_schema is granted (besides ydb.database.connect).
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "CreateUser", .SqlStatement = "CREATE USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },

        // ModifyUser
        // Cluster admin can always administer users, but require the same schema permissions as ordinary user (but why?).
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        // Database admin can administer users if EnableStrictUserManagement and EnableDatabaseAdmin are true.
        // If not, database admin still can administer users as the owner of the database.
        // In both cases it require no schema permissions except ydb.database.connect (why?).
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        // Ordinary user can create users only if EnableStrictUserManagement is false
        // and ydb.granular.alter_schema is granted (besides ydb.database.connect).
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "ModifyUser", .PrecreateTarget = true, .SqlStatement = "ALTER USER targetuser PASSWORD 'passwd'",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },

        // DropUser
        // Cluster admin can always administer users, but require the same schema permissions as ordinary user (but why?).
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::ClusterAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        // Database admin can administer users if EnableStrictUserManagement and EnableDatabaseAdmin are true.
        // If not, database admin still can administer users as the owner of the database.
        // In both cases it require no schema permissions except ydb.database.connect (why?).
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::DatabaseAdmin, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        // Ordinary user can create users only if EnableStrictUserManagement is false
        // and ydb.granular.alter_schema is granted (besides ydb.database.connect).
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = true
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect", "ydb.granular.alter_schema"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = false, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = false, .ExpectedResult = false
        },
        { .Tag = "DropUser", .PrecreateTarget = true, .SqlStatement = "DROP USER targetuser",
            .SubjectLevel = EAccessLevel::User, .SubjectPermissions = {"ydb.database.connect"},
            .EnableStrictUserManagement = true, .EnableDatabaseAdmin = true, .ExpectedResult = false
        },
    };
    struct TTestRegistration_AlterLoginProtect_RootDB {
        TTestRegistration_AlterLoginProtect_RootDB() {
            static std::vector<TString> TestNames;

            for (size_t testId = 0; const auto& entry : AlterLoginProtect_Tests) {
                TestNames.emplace_back(TStringBuilder() << "AlterLoginProtect-RootDB-NoAuth-BuiltinUser-" << entry.Tag << "-" << ++testId);
                TCurrentTest::AddTest(TestNames.back().c_str(), std::bind(AlterLoginProtect_RootDB, std::placeholders::_1, entry), /*forceFork*/ false);
            }

            static auto testsWithAuth = AlterLoginProtect_Tests;
            for (auto& entry : testsWithAuth) {
                entry.EnforceUserTokenRequirement = true;
            }
            for (size_t testId = 0; const auto& entry : testsWithAuth) {
                TestNames.emplace_back(TStringBuilder() << "AlterLoginProtect-RootDB-Auth-BuiltinUser-" << entry.Tag << "-" << ++testId);
                TCurrentTest::AddTest(TestNames.back().c_str(), std::bind(AlterLoginProtect_RootDB, std::placeholders::_1, entry), /*forceFork*/ false);
            }

            static auto testsWithLocalSubject = AlterLoginProtect_Tests;
            for (auto& entry : testsWithLocalSubject) {
                entry.LocalSid = true;
            }
            for (size_t testId = 0; const auto& entry : testsWithLocalSubject) {
                TestNames.emplace_back(TStringBuilder() << "AlterLoginProtect-RootDB-NoAuth-LocalUser-" << entry.Tag << "-" << ++testId);
                TCurrentTest::AddTest(TestNames.back().c_str(), std::bind(AlterLoginProtect_RootDB, std::placeholders::_1, entry), /*forceFork*/ false);
            }

            static auto testsWithAuthAndLocalSubject = AlterLoginProtect_Tests;
            for (auto& entry : testsWithAuthAndLocalSubject) {
                entry.EnforceUserTokenRequirement = true;
                entry.LocalSid = true;
            }
            for (size_t testId = 0; const auto& entry : testsWithAuthAndLocalSubject) {
                TestNames.emplace_back(TStringBuilder() << "AlterLoginProtect-RootDB-Auth-LocalUser-" << entry.Tag << "-" << ++testId);
                TCurrentTest::AddTest(TestNames.back().c_str(), std::bind(AlterLoginProtect_RootDB, std::placeholders::_1, entry), /*forceFork*/ false);
            }
        }
    };
    static TTestRegistration_AlterLoginProtect_RootDB testRegistration_AlterLoginProtect_RootDB;

}


#define Y_UNIT_TEST_FLAGS(N, OPT1, OPT2)                                                                           \
    template<bool OPT1, bool OPT2> void N(NUnitTest::TTestContext&);                                               \
    struct TTestRegistration##N {                                                                                  \
        TTestRegistration##N() {                                                                                   \
            TCurrentTest::AddTest(#N, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, false>), false);                   \
            TCurrentTest::AddTest(#N "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<false, true>), false);          \
            TCurrentTest::AddTest(#N "-" #OPT1, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, false>), false);          \
            TCurrentTest::AddTest(#N "-" #OPT1 "-" #OPT2, static_cast<void (*)(NUnitTest::TTestContext&)>(&N<true, true>), false); \
        }                                                                                                          \
    };                                                                                                             \
    static TTestRegistration##N testRegistration##N;                                                               \
    template<bool OPT1, bool OPT2>                                                                                 \
    void N(NUnitTest::TTestContext&)

Y_UNIT_TEST_SUITE(SchemeReqAdminAccessInTenant) {

    Y_UNIT_TEST_FLAGS(ClusterAdminCanAdministerTenant, DomainLoginOnly, StrictAclCheck) {
        auto settings = Tests::TServerSettings()
            .SetNodeCount(1)
            .SetDynamicNodeCount(1)
            .SetEnableMetadataProvider(false)
            .SetEnableStrictUserManagement(true)
            // .SetEnableDatabaseAdmin(params.EnableDatabaseAdmin)
            .SetLoggerInitializer([](auto& runtime) {
                runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
                runtime.SetLogPriority(NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, NActors::NLog::PRI_DEBUG);
                // runtime.SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
            })
        ;
        settings.AuthConfig.SetDomainLoginOnly(DomainLoginOnly);
        settings.FeatureFlags.SetEnableStrictAclCheck(StrictAclCheck);
        // settings.FeatureFlags.SetEnableGrpcAudit(true);
        TTestEnv env(settings, /*rootToken*/ "root@builtin");

        // Test context preparations

        // Create tenant database
        Cerr << "TEST create tenant" << Endl;
        CreateDatabase(env, "tenant-db");
        const TString tenantPath = JoinPath({env.RootPath, "tenant-db"});

        // Create cluster user, make them cluster admin and give them connect rights on both databases
        Cerr << "TEST create admin clusteradmin" << Endl;
        CreateLocalUser(env, env.RootPath, "clusteradmin");
        env.GetTestServer().GetRuntime()->GetAppData(0).AdministrationAllowedSIDs.push_back("clusteradmin");
        env.GetTestServer().GetRuntime()->GetAppData(1).AdministrationAllowedSIDs.push_back("clusteradmin");

        Cerr << "TEST login clusteradmin" << Endl;
        auto subjectToken = LoginUser(env, env.RootPath, "clusteradmin", "passwd");
        Cerr << "TEST sleep" << Endl;
        // give system time to propagate keys for the logged users tokens
        Sleep(TDuration::Seconds(1));

        // Test body
        Cerr << "TEST body start" << Endl;

        Cerr << "TEST clusteradmin creates user dbadmin" << Endl;
        CreateLocalUser2(env, tenantPath, "dbadmin", subjectToken);

        Cerr << "TEST clusteradmin gives ownership to user dbadmin" << Endl;
        ChangeOwner(env, tenantPath, "dbadmin", subjectToken);
        UNIT_ASSERT_STRINGS_EQUAL(DescribePath(env, tenantPath, env.RootToken).GetSelf().GetOwner(), "dbadmin");

        Cerr << "TEST clusteradmin creates group dbadmins" << Endl;
        CreateLocalGroup2(env, tenantPath, "dbadmins", subjectToken);

        Cerr << "TEST clusteradmin gives ownership to group dbadmins" << Endl;
        ChangeOwner(env, tenantPath, "dbadmins", subjectToken);
        UNIT_ASSERT_STRINGS_EQUAL(DescribePath(env, tenantPath, env.RootToken).GetSelf().GetOwner(), "dbadmins");
    }

    Y_UNIT_TEST_FLAGS(ClusterAdminCanAuthOnEmptyTenant, DomainLoginOnly, StrictAclCheck) {
        auto settings = Tests::TServerSettings()
            .SetNodeCount(1)
            .SetDynamicNodeCount(1)
            .SetEnableMetadataProvider(false)
            .SetEnableStrictUserManagement(true)
            // .SetEnableDatabaseAdmin(params.EnableDatabaseAdmin)
            .SetLoggerInitializer([](auto& runtime) {
                runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
                runtime.SetLogPriority(NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, NActors::NLog::PRI_DEBUG);
                // runtime.SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
            })
        ;
        settings.AuthConfig.SetDomainLoginOnly(DomainLoginOnly);
        settings.FeatureFlags.SetEnableStrictAclCheck(StrictAclCheck);
        // settings.FeatureFlags.SetEnableGrpcAudit(true);
        TTestEnv env(settings, /*rootToken*/ "root@builtin");

        // Test context preparations

        // Create tenant database
        Cerr << "TEST create tenant" << Endl;
        CreateDatabase(env, "tenant-db");
        const TString tenantPath = JoinPath({env.RootPath, "tenant-db"});

        // Create cluster user, make them cluster admin and give them connect rights on both databases
        Cerr << "TEST create admin clusteradmin" << Endl;
        CreateLocalUser(env, env.RootPath, "clusteradmin");
        env.GetTestServer().GetRuntime()->GetAppData(0).AdministrationAllowedSIDs.push_back("clusteradmin");
        env.GetTestServer().GetRuntime()->GetAppData(1).AdministrationAllowedSIDs.push_back("clusteradmin");

        Cerr << "TEST login clusteradmin" << Endl;
        auto subjectToken = LoginUser(env, env.RootPath, "clusteradmin", "passwd");
        Cerr << "TEST sleep" << Endl;
        // give system time to propagate keys for the logged users tokens
        Sleep(TDuration::Seconds(1));

        // Test body
        Cerr << "TEST body start" << Endl;

        // auto result = WhoAmI(env, tenantPath, subjectToken);
        // UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "clusteradmin");

        SetPermissions(env, tenantPath, "clusteradmin", {"ydb.granular.describe_schema"});

        // Cerr << "TEST clusteradmin triggers auth on tenant" << Endl;
        // auto result = DescribePath2(env, tenantPath, tenantPath, subjectToken);
        // UNIT_ASSERT_STRINGS_EQUAL(result.Owner, env.RootToken);

        Cerr << "TEST clusteradmin triggers auth on tenant" << Endl;
        auto result = DescribePath(env, tenantPath, subjectToken);
        UNIT_ASSERT_STRINGS_EQUAL(result.GetSelf().GetOwner(), env.RootToken);
    }

    Y_UNIT_TEST_FLAGS(ClusterAdminCanAuthOnNonEmptyTenant, DomainLoginOnly, StrictAclCheck) {
        auto settings = Tests::TServerSettings()
            .SetNodeCount(1)
            .SetDynamicNodeCount(1)
            .SetEnableMetadataProvider(false)
            .SetEnableStrictUserManagement(true)
            // .SetEnableDatabaseAdmin(params.EnableDatabaseAdmin)
            .SetLoggerInitializer([](auto& runtime) {
                runtime.SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_DEBUG);
                runtime.SetLogPriority(NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, NActors::NLog::PRI_DEBUG);
                // runtime.SetLogPriority(NKikimrServices::GRPC_SERVER, NActors::NLog::PRI_DEBUG);
            })
        ;
        settings.AuthConfig.SetDomainLoginOnly(DomainLoginOnly);
        settings.FeatureFlags.SetEnableStrictAclCheck(StrictAclCheck);
        // settings.FeatureFlags.SetEnableGrpcAudit(true);
        TTestEnv env(settings, /*rootToken*/ "root@builtin");

        // Test context preparations

        // Create tenant database
        Cerr << "TEST create tenant" << Endl;
        CreateDatabase(env, "tenant-db");
        const TString tenantPath = JoinPath({env.RootPath, "tenant-db"});

        // Create cluster user, make them cluster admin and give them connect rights on both databases
        Cerr << "TEST create admin clusteradmin" << Endl;
        CreateLocalUser(env, env.RootPath, "clusteradmin");
        env.GetTestServer().GetRuntime()->GetAppData(0).AdministrationAllowedSIDs.push_back("clusteradmin");
        env.GetTestServer().GetRuntime()->GetAppData(1).AdministrationAllowedSIDs.push_back("clusteradmin");

        Cerr << "TEST login clusteradmin" << Endl;
        auto subjectToken = LoginUser(env, env.RootPath, "clusteradmin", "passwd");
        Cerr << "TEST sleep" << Endl;
        // give system time to propagate keys for the logged users tokens
        Sleep(TDuration::Seconds(1));

        // Test body
        Cerr << "TEST body start" << Endl;

        Cerr << "TEST clusteradmin creates user in tenant -- make tenant's login provider non empty" << Endl;
        CreateLocalUser2(env, tenantPath, "tenantuser", subjectToken);

        // auto result = WhoAmI(env, tenantPath, subjectToken);
        // UNIT_ASSERT_STRINGS_EQUAL(result.GetUserName(), "clusteradmin");

        SetPermissions(env, tenantPath, "clusteradmin", {"ydb.granular.describe_schema"});

        // Cerr << "TEST clusteradmin triggers auth on tenant" << Endl;
        // auto result = DescribePath2(env, tenantPath, tenantPath, subjectToken);
        // UNIT_ASSERT_STRINGS_EQUAL(result.Owner, env.RootToken);

        Cerr << "TEST clusteradmin triggers auth on tenant" << Endl;
        auto result = DescribePath(env, tenantPath, subjectToken);
        UNIT_ASSERT_STRINGS_EQUAL(result.GetSelf().GetOwner(), env.RootToken);
    }

}

}  // namespace NKikimr::NTxProxyUT
