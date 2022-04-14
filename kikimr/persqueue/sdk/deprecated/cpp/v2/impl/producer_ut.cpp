#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_server.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/test_utils.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/ut_utils/sdk_test_setup.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NPersQueue {

Y_UNIT_TEST_SUITE(TProducerTest) {
    Y_UNIT_TEST(NotStartedProducerCanBeDestructed) {
        // Test that producer doesn't hang on till shutdown
        TPQLib lib;
        TProducerSettings settings;
        settings.Server = TServerSetting{"localhost"};
        settings.Topic = "topic";
        settings.SourceId = "src";
        lib.CreateProducer(settings, {}, false);
    }

    TProducerSettings MakeProducerSettings(const TTestServer& testServer) {
        TProducerSettings producerSettings;
        producerSettings.ReconnectOnFailure = false;
        producerSettings.Topic = "topic1";
        producerSettings.SourceId = "123";
        producerSettings.Server = TServerSetting{"localhost", testServer.GrpcPort};
        producerSettings.Codec = ECodec::LZOP;
        return producerSettings;
    }

    Y_UNIT_TEST(CancelsOperationsAfterPQLibDeath) {
        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer(false);

        const size_t partitions = 1;
        testServer.AnnoyingClient->FullInit(!GrpcV1EnabledByDefault() ? DEFAULT_CLUSTERS_LIST : CLUSTERS_LIST_ONE_DC);
        testServer.AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--topic1", partitions);

        testServer.WaitInit("topic1");

        auto producer = testServer.PQLib->CreateProducer(MakeProducerSettings(testServer), testServer.PQLibSettings.DefaultLogger, false);
        UNIT_ASSERT(!producer->Start().GetValueSync().Response.HasError());
        auto isDead = producer->IsDead();
        UNIT_ASSERT(!isDead.HasValue());

        auto write1 = producer->Write(1, TString("blob1"));
        auto write2 = producer->Write(2, TString("blob2"));
        auto write3 = producer->Write(3, TString("blob3"));
        auto write4 = producer->Write(4, TString("blob4"));
        auto write5 = producer->Write(5, TString("blob5"));

        testServer.PQLib = nullptr;
        Cerr << "PQLib destroyed" << Endl;

        UNIT_ASSERT(write1.HasValue());
        UNIT_ASSERT(write2.HasValue());
        UNIT_ASSERT(write3.HasValue());
        UNIT_ASSERT(write4.HasValue());
        UNIT_ASSERT(write5.HasValue());

        UNIT_ASSERT(write1.GetValue().Response.HasError());
        UNIT_ASSERT(write2.GetValue().Response.HasError());
        UNIT_ASSERT(write3.GetValue().Response.HasError());
        UNIT_ASSERT(write4.GetValue().Response.HasError());
        UNIT_ASSERT(write5.GetValue().Response.HasError());

        auto write6 = producer->Write(6, TString("blob6"));
        UNIT_ASSERT(write6.HasValue());
        UNIT_ASSERT(write6.GetValue().Response.HasError());
    }

    Y_UNIT_TEST(WriteToDeadProducer) {
        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer(false);

        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        TPQLibSettings pqLibSettings;
        pqLibSettings.DefaultLogger = logger;
        THolder<TPQLib> PQLib = MakeHolder<TPQLib>(pqLibSettings);

        auto producer = PQLib->CreateProducer(MakeProducerSettings(testServer), logger, false);
        auto f = producer->Start();
        UNIT_ASSERT(f.GetValueSync().Response.HasError());
        Cerr << f.GetValueSync().Response << "\n";
        auto isDead = producer->IsDead();
        isDead.Wait();
        UNIT_ASSERT(isDead.HasValue());

        auto write = producer->Write(1, TString("blob"));

        Cerr << write.GetValueSync().Response << "\n";
        UNIT_ASSERT(write.GetValueSync().Response.HasError());
        UNIT_ASSERT_STRINGS_EQUAL(write.GetValueSync().Response.GetError().GetDescription(), "Destroyed");
    }

    Y_UNIT_TEST(Auth_StartProducerWithInvalidTokenFromGrpcMetadataPointOfView_StartFailedAndProduerDiedWithBadRequestErrors) {
        TPQLibSettings pqLibSettings;
        if (!std::getenv("PERSQUEUE_GRPC_API_V1_ENABLED"))
            return;

        TTestServer testServer(false);
        testServer.GrpcServerOptions.SetGRpcShutdownDeadline(TDuration::MilliSeconds(10));
        testServer.StartServer();
        TIntrusivePtr<TCerrLogger> logger = MakeIntrusive<TCerrLogger>(TLOG_DEBUG);
        pqLibSettings.DefaultLogger = logger;
        THolder<TPQLib> PQLib = MakeHolder<TPQLib>(pqLibSettings);

        auto fillCredentialsWithInvalidToken = [](NPersQueue::TCredentials* authInfo) {
            authInfo->set_tvm_service_ticket("token\n");
        };
        auto producerSettings = MakeProducerSettings(testServer);
        producerSettings.CredentialsProvider = std::make_shared<TCallbackCredentialsProvider>(std::move(fillCredentialsWithInvalidToken));
        auto producer = PQLib->CreateProducer(producerSettings, logger, false);


        Cerr << "Wait for producer start procedure end" << Endl;
        auto startResponse = producer->Start().GetValueSync().Response;

        UNIT_ASSERT(startResponse.has_error());
        UNIT_ASSERT_EQUAL_C(NPersQueue::NErrorCode::ERROR, startResponse.error().code(), startResponse);

        Cerr << "Wait for producer death" << Endl;
        auto deathCause = producer->IsDead().GetValueSync();


        UNIT_ASSERT_EQUAL_C(NPersQueue::NErrorCode::ERROR, deathCause.code(), deathCause);
    }

    Y_UNIT_TEST(Codecs_WriteWithNonDefaultCodecThatRequiresAdditionalConfiguration_ConsumerDiesWithBadRequestError) {
        SDKTestSetup setup{"Codecs_WriteWithNonDefaultCodecThatRequiresAdditionalConfiguration_ConsumerIsDeadWithBadRequestError"};
        auto log = setup.GetLog();
        auto producerSettings = setup.GetProducerSettings();
        // TTestServer::AnnoyingClient creates topic with default codecs set: raw, gzip, lzop. zstd not included
        producerSettings.Codec = ECodec::ZSTD;
        auto producer = setup.GetPQLib()->CreateProducer(producerSettings, nullptr, false);
        log << TLOG_INFO << "Wait for producer start";
        auto startResponse = producer->Start().GetValueSync().Response;
        UNIT_ASSERT_C(!startResponse.HasError(), startResponse);

        log << TLOG_INFO << "Wait for write response";
        auto writeResponse = producer->Write(NUnitTest::RandomString(250 * 1024, std::rand())).GetValueSync().Response;


        UNIT_ASSERT(writeResponse.HasError());
        Cerr << writeResponse << "\n";
        UNIT_ASSERT_EQUAL_C(NPersQueue::NErrorCode::BAD_REQUEST, writeResponse.GetError().GetCode(), writeResponse);
    }

    void StartProducerWithDiscovery(bool cds, const TString& preferredCluster = {}, bool addBrokenDatacenter = !GrpcV1EnabledByDefault()) {
        SDKTestSetup setup{"StartProducerWithDiscovery", false};
        setup.Start(true, addBrokenDatacenter);
        auto log = setup.GetLog();
        auto producerSettings = setup.GetProducerSettings();
        if (preferredCluster) {
            producerSettings.PreferredCluster = preferredCluster;
        }
        auto producer = setup.GetPQLib()->CreateProducer(producerSettings, nullptr, false);
        auto startResponse = producer->Start().GetValueSync().Response;
        UNIT_ASSERT_C(!startResponse.HasError(), startResponse);

        log << TLOG_INFO << "Wait for write response";
        auto writeResponse = producer->Write(1, TString("blob1")).GetValueSync().Response;
        UNIT_ASSERT(!writeResponse.HasError());

        producerSettings.Server.UseLogbrokerCDS = cds ? EClusterDiscoveryUsageMode::Use : EClusterDiscoveryUsageMode::DontUse;
        producer = setup.GetPQLib()->CreateProducer(producerSettings, nullptr, false);
        startResponse = producer->Start().GetValueSync().Response;
        UNIT_ASSERT_C(!startResponse.HasError(), startResponse);

        log << TLOG_INFO << "Wait for write response";
        writeResponse = producer->Write(2, TString("blob2")).GetValueSync().Response;
        UNIT_ASSERT(!writeResponse.HasError());
    }

    Y_UNIT_TEST(StartProducerWithCDS) {
        StartProducerWithDiscovery(true);
    }

    Y_UNIT_TEST(StartProducerWithoutCDS) {
        StartProducerWithDiscovery(false);
    }

    Y_UNIT_TEST(StartProducerWithCDSAndPreferAvailableCluster) {
        StartProducerWithDiscovery(true, "dc1");
    }

    Y_UNIT_TEST(StartProducerWithCDSAndPreferUnavailableCluster) {
        StartProducerWithDiscovery(true, "dc2", true);
    }

    Y_UNIT_TEST(StartProducerWithCDSAndPreferUnknownCluster) {
        SDKTestSetup setup{"PreferUnknownCluster"};
        auto producerSettings = setup.GetProducerSettings();
        producerSettings.Server.UseLogbrokerCDS = EClusterDiscoveryUsageMode::Use;
        producerSettings.PreferredCluster = "blablabla";
        auto producer = setup.GetPQLib()->CreateProducer(producerSettings, nullptr, false);
        auto startResponse = producer->Start().GetValueSync().Response;
        UNIT_ASSERT_C(startResponse.HasError(), startResponse);
    }
}
} // namespace NPersQueue
