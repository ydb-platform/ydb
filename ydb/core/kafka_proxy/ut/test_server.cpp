#include "test_server.h"

template <class TKikimr, bool secure>
TTestServer<TKikimr, secure>::TTestServer(const TTestServerSettings& settings) {
    TPortManager portManager;
    Port = portManager.GetTcpPort();

    ui16 accessServicePort = portManager.GetPort(4284);
    TString accessServiceEndpoint = "localhost:" + ToString(accessServicePort);

    NKikimrConfig::TAppConfig appConfig;
    appConfig.MutableAuthConfig()->SetUseLoginProvider(true);
    appConfig.MutableAuthConfig()->SetUseBlackBox(false);
    appConfig.MutableAuthConfig()->SetUseBlackBox(false);
    appConfig.MutableAuthConfig()->SetUseAccessService(true);
    appConfig.MutableAuthConfig()->SetUseAccessServiceApiKey(true);
    appConfig.MutableAuthConfig()->SetUseAccessServiceTLS(false);
    appConfig.MutableAuthConfig()->SetAccessServiceEndpoint(accessServiceEndpoint);

    appConfig.MutablePQConfig()->SetTopicsAreFirstClassCitizen(true);
    appConfig.MutablePQConfig()->SetEnabled(true);
    // NOTE(shmel1k@): KIKIMR-14221
    if (!settings.CheckACL) {
        appConfig.MutablePQConfig()->SetCheckACL(false);
    }
    appConfig.MutablePQConfig()->SetRequireCredentialsInNewProtocol(false);

    auto cst = appConfig.MutablePQConfig()->AddClientServiceType();
    cst->SetName("data-transfer");
    cst = appConfig.MutablePQConfig()->AddClientServiceType();
    cst->SetName("data-transfer2");

    appConfig.MutableKafkaProxyConfig()->SetEnableKafkaProxy(true);
    appConfig.MutableKafkaProxyConfig()->SetListeningPort(Port);
    appConfig.MutableKafkaProxyConfig()->SetMaxMessageSize(1024);
    appConfig.MutableKafkaProxyConfig()->SetMaxInflightSize(2048);
    if (settings.EnableAutoTopicCreation) {
        appConfig.MutableKafkaProxyConfig()->SetAutoCreateTopicsEnable(true);
    }
    appConfig.MutableKafkaProxyConfig()->SetTopicCreationDefaultPartitions(2);
    if (!settings.EnableAutoConsumerCreation) {
        appConfig.MutableKafkaProxyConfig()->SetAutoCreateConsumersEnable(false);
    }
    if (settings.Serverless) {
        appConfig.MutableKafkaProxyConfig()->MutableProxy()->SetHostname("localhost");
        appConfig.MutableKafkaProxyConfig()->MutableProxy()->SetPort(FAKE_SERVERLESS_KAFKA_PROXY_PORT);
    }

    appConfig.MutablePQConfig()->MutableQuotingConfig()->SetEnableQuoting(settings.EnableQuoting);
    if (!settings.EnableQuoting)
        appConfig.MutablePQConfig()->MutableQuotingConfig()->SetEnableReadQuoting(false);

    appConfig.MutablePQConfig()->MutableQuotingConfig()->SetQuotaWaitDurationMs(300);
    appConfig.MutablePQConfig()->MutableQuotingConfig()->SetPartitionReadQuotaIsTwiceWriteQuota(settings.EnableQuoting);
    appConfig.MutablePQConfig()->MutableBillingMeteringConfig()->SetEnabled(true);
    appConfig.MutablePQConfig()->MutableBillingMeteringConfig()->SetFlushIntervalSec(1);
    appConfig.MutablePQConfig()->AddClientServiceType()->SetName("data-streams");
    appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER);
    appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER_X);
    appConfig.MutablePQConfig()->AddNonChargeableUser(NON_CHARGEABLE_USER_Y);

    appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(128);
    appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(512);
    appConfig.MutablePQConfig()->AddValidWriteSpeedLimitsKbPerSec(1_KB);

    appConfig.MutableGRpcConfig()->SetHost("::1");
    auto limit = appConfig.MutablePQConfig()->AddValidRetentionLimits();
    limit->SetMinPeriodSeconds(0);
    limit->SetMaxPeriodSeconds(TDuration::Days(1).Seconds());
    limit->SetMinStorageMegabytes(0);
    limit->SetMaxStorageMegabytes(0);

    limit = appConfig.MutablePQConfig()->AddValidRetentionLimits();
    limit->SetMinPeriodSeconds(0);
    limit->SetMaxPeriodSeconds(TDuration::Days(7).Seconds());
    limit->SetMinStorageMegabytes(50_KB);
    limit->SetMaxStorageMegabytes(1_MB);

    MeteringFile = MakeHolder<TTempFileHandle>();
    appConfig.MutableMeteringConfig()->SetMeteringFilePath(MeteringFile->Name());

    if (secure) {
        appConfig.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        appConfig.MutableDomainsConfig()->MutableSecurityConfig()->SetEnforceUserTokenRequirement(true);
    }
    KikimrServer = std::unique_ptr<TKikimr>(new TKikimr(std::move(appConfig), {}, {}, false, nullptr, nullptr, 0));
    KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::KAFKA_PROXY, NActors::NLog::PRI_TRACE);
    KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);
    KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PQ_DESCRIBER, NActors::NLog::PRI_TRACE);
    KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PQ_FETCH_REQUEST, NActors::NLog::PRI_TRACE);
    KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NActors::NLog::PRI_TRACE);
    KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::TICKET_PARSER, NLog::PRI_TRACE);
    KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_CLIENT, NLog::PRI_TRACE);
    KikimrServer->GetRuntime()->SetLogPriority(NKikimrServices::GRPC_PROXY_NO_CONNECT_ACCESS, NLog::PRI_TRACE);

    if (!settings.EnableNativeKafkaBalancing) {
        KikimrServer->GetRuntime()->GetAppData().FeatureFlags.SetEnableKafkaNativeBalancing(false);
    }
    KikimrServer->GetRuntime()->GetAppData().FeatureFlags.SetEnableKafkaTransactions(true);
    KikimrServer->GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicCompactificationByKey(true);

    TClient client(*(KikimrServer->ServerSettings));
    if (secure) {
        client.SetSecurityToken("root@builtin");
    }

    ui16 grpc = KikimrServer->GetPort();
    TString location = TStringBuilder() << "localhost:" << grpc;
    auto driverConfig = TDriverConfig()
        .SetEndpoint(location)
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", TLOG_DEBUG).Release()));
    if (secure) {
        driverConfig.UseSecureConnection(TString(NYdbSslTestData::CaCrt));
        driverConfig.SetAuthToken("root@builtin");
    } else {
        driverConfig.SetDatabase("/Root/");
    }

    Driver = std::make_unique<TDriver>(std::move(driverConfig));

    UNIT_ASSERT_VALUES_EQUAL(
        NMsgBusProxy::MSTATUS_OK,
        client.AlterUserAttributes("/", "Root",
                                   {{"folder_id", DEFAULT_FOLDER_ID},
                                    {"cloud_id", DEFAULT_CLOUD_ID},
                                    {"kafka_api", settings.KafkaApiMode},
                                    {"database_id", "root"},
                                    {"serverless_rt_coordination_node_path", "/Coordinator/Root"},
                                    {"serverless_rt_base_resource_ru", "/ru_Root"}}));

    {
        auto status = client.CreateUser("/Root", "ouruser", "ourUserPassword");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        NYdb::NScheme::TSchemeClient schemeClient(*Driver);
        NYdb::NScheme::TPermissions permissions("ouruser", {"ydb.generic.read", "ydb.generic.write", "ydb.generic.full"});

        auto result = schemeClient
                          .ModifyPermissions(
                              "/Root", NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions))
                          .ExtractValueSync();
        Cerr << result.GetIssues().ToString() << "\n";
        UNIT_ASSERT(result.IsSuccess());
    }

    {
        auto status = client.CreateUser("/Root", "user123", "UsErPassword");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        NYdb::NScheme::TSchemeClient schemeClient(*Driver);
        NYdb::NScheme::TPermissions permissions("user123", {"ydb.generic.read", "ydb.generic.write", "ydb.generic.full"});

        auto result = schemeClient
                          .ModifyPermissions(
                              "/Root", NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions))
                          .ExtractValueSync();
        Cerr << result.GetIssues().ToString() << "\n";
        UNIT_ASSERT(result.IsSuccess());
    }

    {
        auto status = client.CreateUser("/Root", "usernorights", "dummyPass");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        NYdb::NScheme::TSchemeClient schemeClient(*Driver);
        NYdb::NScheme::TPermissions permissions("usernorights", {});

        auto result = schemeClient
                          .ModifyPermissions(
                              "/Root", NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions))
                          .ExtractValueSync();
        Cerr << result.GetIssues().ToString() << "\n";
        UNIT_ASSERT(result.IsSuccess());
    }

    {
        auto status = client.CreateUser("/Root", "useronlyreadrights", "AbAcAbA");
        UNIT_ASSERT_VALUES_EQUAL(status, NMsgBusProxy::MSTATUS_OK);

        NYdb::NScheme::TSchemeClient schemeClient(*Driver);
        NYdb::NScheme::TPermissions permissions("useronlyreadrights", {"ydb.generic.read"});

        auto result = schemeClient
                          .ModifyPermissions(
                              "/Root", NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions))
                          .ExtractValueSync();
        Cerr << result.GetIssues().ToString() << "\n";
        UNIT_ASSERT(result.IsSuccess());
    }

    {
        // Access Server Mock
        grpc::ServerBuilder builder;
        builder.AddListeningPort(accessServiceEndpoint, grpc::InsecureServerCredentials()).RegisterService(&accessServiceMock);
        AccessServer = builder.BuildAndStart();
    }
}

template <class TKikimr, bool secure>
TTestServer<TKikimr, secure>::TTestServer(const TString& kafkaApiMode, bool serverless, bool enableNativeKafkaBalancing,
            bool enableAutoTopicCreation, bool enableAutoConsumerCreation, bool enableQuoting, bool checkACL)
    : TTestServer(TTestServerSettings{kafkaApiMode, serverless, enableNativeKafkaBalancing, enableAutoTopicCreation, enableAutoConsumerCreation, enableQuoting, checkACL})
{}

// Explicit template instantiations
template class TTestServer<TKikimrWithGrpcAndRootSchema, false>;
template class TTestServer<TKikimrWithGrpcAndRootSchemaSecure, true>;
