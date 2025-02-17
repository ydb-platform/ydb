#include <library/cpp/testing/unittest/registar.h>

#include <ydb-cpp-sdk/client/query/client.h>
#include <ydb-cpp-sdk/client/scheme/scheme.h>
#include <ydb-cpp-sdk/client/driver/driver.h>

#include <ydb/core/base/path.h>
#include <ydb/core/base/storage_pools.h>

#include <ydb/core/testlib/test_client.h>

#include <ydb/core/protos/subdomains.pb.h>
#include <ydb/core/protos/console_tenant.pb.h>
#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/grpc/ydb_auth_v1.grpc.pb.h>


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
        // it's imperative that RootPath was with leading '/' -- is a path
        RootPath = CanonizePath(ServerSettings->DomainName);

        // test client
        Client = MakeHolder<Tests::TClient>(*ServerSettings);
        Client->SetSecurityToken(RootToken);
        Client->InitRootScheme();
        Client->GrantConnect(RootToken);

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
    NKikimrSubDomains::TSubDomainSettings subdomain;
    subdomain.SetName(databaseName);
    {
        auto status = env.GetTestClient().CreateExtSubdomain(env.RootPath, subdomain);
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);
    }
    env.GetTestTenants().Run(JoinPath({env.RootPath, databaseName}), 1);
    subdomain.SetExternalSchemeShard(true);
    subdomain.SetPlanResolution(50);
    subdomain.SetCoordinators(1);
    subdomain.SetMediators(1);
    subdomain.SetTimeCastBucketsPerMediator(2);
    for (auto& pool : env.CreatePools(databaseName)) {
        *subdomain.AddStoragePools() = pool;
    }
    {
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

void CreateLocalUser(const TTestEnv& env, const TString& database, const TString& user) {
    auto query = Sprintf(
        R"(
            CREATE USER %s PASSWORD 'passwd'
        )",
        user.c_str()
    );
    auto result = CreateQueryClient(env, env.RootToken, database).GetSession().GetValueSync().GetSession()
        .ExecuteQuery(query,  NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void SetPermissions(const TTestEnv& env, const TString& path, const TString& targetSid, const std::vector<std::string>& permissions) {
    auto client = CreateSchemeClient(env, env.RootToken);
    auto modify = NYdb::NScheme::TModifyPermissionsSettings();
    auto status = client.ModifyPermissions(path, modify.AddSetPermissions({targetSid, permissions}))
        .ExtractValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
}

void ChangeOwner(const TTestEnv& env, const TString& path, const TString& targetSid) {
    auto client = CreateSchemeClient(env, env.RootToken);
    auto modify = NYdb::NScheme::TModifyPermissionsSettings();
    auto status = client.ModifyPermissions(path, modify.AddChangeOwner(targetSid))
        .ExtractValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
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
        TTestEnv env(settings, /* rootToken*/ "root@builtin");

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

}  // namespace NKikimr::NTxProxyUT
