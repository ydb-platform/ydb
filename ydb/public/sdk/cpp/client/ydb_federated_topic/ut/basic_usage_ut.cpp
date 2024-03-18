#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/managed_executor.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/ut_utils.h>
#include <ydb/public/sdk/cpp/client/ydb_federated_topic/ut/fds_mock.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <future>

namespace NYdb::NFederatedTopic::NTests {

Y_UNIT_TEST_SUITE(BasicUsage) {

    Y_UNIT_TEST(GetAllStartPartitionSessions) {
        size_t partitionsCount = 5;

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
            TEST_CASE_NAME, false, ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_DEBUG, 2, partitionsCount);

        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();

        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        std::shared_ptr<NYdb::NFederatedTopic::IFederatedReadSession> ReadSession;

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver);

        // Create read session.
        NYdb::NFederatedTopic::TFederatedReadSessionSettings readSettings;
        readSettings
            .ConsumerName("shared/user")
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        ReadSession = topicClient.CreateReadSession(readSettings);
        Cerr << "Session was created" << Endl;

        ReadSession->WaitEvent().Wait(TDuration::Seconds(1));
        TMaybe<NYdb::NFederatedTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
        Y_ASSERT(!event);

        auto fdsRequest = fdsMock.GetNextPendingRequest();
        Y_ASSERT(fdsRequest.has_value());
        // TODO check fdsRequest->Req db header

        Ydb::FederationDiscovery::ListFederationDatabasesResponse response;

        auto op = response.mutable_operation();
        op->set_status(Ydb::StatusIds::SUCCESS);
        response.mutable_operation()->set_ready(true);
        response.mutable_operation()->set_id("12345");

        Ydb::FederationDiscovery::ListFederationDatabasesResult mockResult;
        mockResult.set_control_plane_endpoint("cp.logbroker-federation:2135");
        mockResult.set_self_location("fancy_datacenter");
        auto c1 = mockResult.add_federation_databases();
        c1->set_name("dc1");
        c1->set_path("/Root");
        c1->set_id("account-dc1");
        c1->set_endpoint("localhost:" + ToString(fdsMock.Port));
        c1->set_location("dc1");
        c1->set_status(::Ydb::FederationDiscovery::DatabaseInfo::Status::DatabaseInfo_Status_AVAILABLE);
        c1->set_weight(1000);
        auto c2 = mockResult.add_federation_databases();
        c2->set_name("dc2");
        c2->set_path("/Root");
        c2->set_id("account-dc2");
        c2->set_endpoint("localhost:" + ToString(fdsMock.Port));
        c2->set_location("dc2");
        c2->set_status(::Ydb::FederationDiscovery::DatabaseInfo::Status::DatabaseInfo_Status_AVAILABLE);
        c2->set_weight(500);

        op->mutable_result()->PackFrom(mockResult);

        fdsRequest->Result.SetValue({std::move(response), grpc::Status::OK});

        for (size_t i = 0; i < partitionsCount; ++i) {
            ReadSession->WaitEvent().Wait();
            // Get event
            TMaybe<NYdb::NFederatedTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(true/*block - will block if no event received yet*/);
            Cerr << "Got new read session event: " << DebugString(*event) << Endl;

            auto* startPartitionSessionEvent = std::get_if<NYdb::NFederatedTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event);
            Y_ASSERT(startPartitionSessionEvent);
            startPartitionSessionEvent->Confirm();
        }

        ReadSession->Close(TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(WaitEventBlocksBeforeDiscovery) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();

        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        std::shared_ptr<NYdb::NFederatedTopic::IFederatedReadSession> ReadSession;

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver);

        // Create read session.
        NYdb::NFederatedTopic::TFederatedReadSessionSettings readSettings;
        readSettings
            .ConsumerName("shared/user")
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        Cerr << "Before ReadSession was created" << Endl;
        ReadSession = topicClient.CreateReadSession(readSettings);
        Cerr << "Session was created" << Endl;

        auto f = ReadSession->WaitEvent();
        Cerr << "Returned from WaitEvent" << Endl;
        // observer asyncInit should respect client/session timeouts
        UNIT_ASSERT(!f.Wait(TDuration::Seconds(1)));

        Cerr << "Session blocked successfully" << Endl;

        UNIT_ASSERT(ReadSession->Close(TDuration::MilliSeconds(10)));
        Cerr << "Session closed gracefully" << Endl;
    }

    Y_UNIT_TEST(RetryDiscoveryWithCancel) {
        // TODO register requests in mock, compare time for retries, reschedules

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();

        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        std::shared_ptr<NYdb::NFederatedTopic::IFederatedReadSession> ReadSession;

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        auto clientSettings = TFederatedTopicClientSettings()
            .RetryPolicy(NTopic::IRetryPolicy::GetFixedIntervalPolicy(
                TDuration::Seconds(10),
                TDuration::Seconds(10)
            ));
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver, clientSettings);

        bool answerOk = false;

        for (int i = 0; i < 6; ++i) {
            std::optional<TFederationDiscoveryServiceMock::TManualRequest> fdsRequest;
            do {
                Sleep(TDuration::MilliSeconds(50));
                fdsRequest = fdsMock.GetNextPendingRequest();

            } while (!fdsRequest.has_value());

            if (answerOk) {
                fdsRequest->Result.SetValue(fdsMock.ComposeOkResult());
            } else {
                fdsRequest->Result.SetValue({{}, grpc::Status(grpc::StatusCode::UNAVAILABLE, "mock 'unavailable'")});
            }

            answerOk = !answerOk;
        }
    }

    Y_UNIT_TEST(PropagateSessionClosed) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();

        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        std::shared_ptr<NYdb::NFederatedTopic::IFederatedReadSession> ReadSession;

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        auto clientSettings = TFederatedTopicClientSettings()
            .RetryPolicy(NTopic::IRetryPolicy::GetFixedIntervalPolicy(
                TDuration::Seconds(10),
                TDuration::Seconds(10)
            ));
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver, clientSettings);


        // Create read session.
        NYdb::NFederatedTopic::TFederatedReadSessionSettings readSettings;
        readSettings
            .ConsumerName("shared/user")
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        ReadSession = topicClient.CreateReadSession(readSettings);
        Cerr << "Session was created" << Endl;

        Sleep(TDuration::MilliSeconds(50));

        auto events = ReadSession->GetEvents(false);
        UNIT_ASSERT(events.empty());

        Ydb::FederationDiscovery::ListFederationDatabasesResponse Response;

        auto op = Response.mutable_operation();
        op->set_status(Ydb::StatusIds::SUCCESS);
        Response.mutable_operation()->set_ready(true);
        Response.mutable_operation()->set_id("12345");

        Ydb::FederationDiscovery::ListFederationDatabasesResult mockResult;
        mockResult.set_control_plane_endpoint("cp.logbroker-federation:2135");
        mockResult.set_self_location("fancy_datacenter");
        auto c1 = mockResult.add_federation_databases();
        c1->set_name("dc1");
        c1->set_path("/Root");
        c1->set_id("account-dc1");
        c1->set_endpoint("localhost:" + ToString(fdsMock.Port));
        c1->set_location("dc1");
        c1->set_status(::Ydb::FederationDiscovery::DatabaseInfo::Status::DatabaseInfo_Status_AVAILABLE);
        c1->set_weight(1000);
        auto c2 = mockResult.add_federation_databases();
        c2->set_name("dc2");
        c2->set_path("/Invalid");
        c2->set_id("account-dc2");
        c2->set_endpoint("localhost:" + ToString(fdsMock.Port));
        c2->set_location("dc2");
        c2->set_status(::Ydb::FederationDiscovery::DatabaseInfo::Status::DatabaseInfo_Status_AVAILABLE);
        c2->set_weight(500);

        op->mutable_result()->PackFrom(mockResult);

        std::optional<TFederationDiscoveryServiceMock::TManualRequest> fdsRequest;
        do {
            fdsRequest = fdsMock.GetNextPendingRequest();
            if (!fdsRequest.has_value()) {
                Sleep(TDuration::MilliSeconds(50));
            }
        } while (!fdsRequest.has_value());
        fdsRequest->Result.SetValue({Response, grpc::Status::OK});

        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(NPersQueue::ECodec::RAW);
        NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
        writeSettings.CompressionExecutor(executor);

        auto& client = setup->GetPersQueueClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);
        TString messageBase = "message----";


        ui64 count = 100u;
        for (auto i = 0u; i < count; i++) {
            auto res = session->Write(messageBase * (200 * 1024) + ToString(i));
            UNIT_ASSERT(res);

            events = ReadSession->GetEvents(true);
            UNIT_ASSERT(!events.empty());

            for (auto& e : events) {
                Cerr << ">>> Got event: " << DebugString(e) << Endl;
                if (auto* dataEvent = std::get_if<TReadSessionEvent::TDataReceivedEvent>(&e)) {
                    dataEvent->Commit();
                } else if (auto* startPartitionSessionEvent = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(&e)) {
                    startPartitionSessionEvent->Confirm();
                } else if (auto* stopPartitionSessionEvent = std::get_if<TReadSessionEvent::TStopPartitionSessionEvent>(&e)) {
                    stopPartitionSessionEvent->Confirm();
                }
            }
        }

        session->Close();
    }

    Y_UNIT_TEST(RecreateObserver) {
        // TODO register requests in mock, compare time for retries, reschedules

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();

        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        std::shared_ptr<NYdb::NFederatedTopic::IFederatedReadSession> ReadSession;

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        auto clientSettings = TFederatedTopicClientSettings()
            .RetryPolicy(NTopic::IRetryPolicy::GetNoRetryPolicy());
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver, clientSettings);


        // Create read session.
        NYdb::NFederatedTopic::TFederatedReadSessionSettings readSettings;
        readSettings
            .ConsumerName("shared/user")
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        ReadSession = topicClient.CreateReadSession(readSettings);
        Cerr << "Session was created" << Endl;

        ReadSession->WaitEvent().Wait(TDuration::Seconds(1));
        auto event = ReadSession->GetEvent(false);
        UNIT_ASSERT(!event.Defined());


        std::optional<TFederationDiscoveryServiceMock::TManualRequest> fdsRequest;
        do {
            fdsRequest = fdsMock.GetNextPendingRequest();
            if (!fdsRequest.has_value()) {
                Sleep(TDuration::MilliSeconds(50));
            }
        } while (!fdsRequest.has_value());

        fdsRequest->Result.SetValue({{}, grpc::Status(grpc::StatusCode::UNAVAILABLE, "mock 'unavailable'")});

        ReadSession->WaitEvent().Wait();
        event = ReadSession->GetEvent(false);
        UNIT_ASSERT(event.Defined());
        Cerr << ">>> Got event: " << DebugString(*event) << Endl;
        UNIT_ASSERT(std::holds_alternative<NTopic::TSessionClosedEvent>(*event));

        auto ReadSession2 = topicClient.CreateReadSession(readSettings);
        Cerr << "Session2 was created" << Endl;

        ReadSession2->WaitEvent().Wait(TDuration::Seconds(1));
        event = ReadSession2->GetEvent(false);
        UNIT_ASSERT(!event.Defined());

        do {
            fdsRequest = fdsMock.GetNextPendingRequest();
            if (!fdsRequest.has_value()) {
                Sleep(TDuration::MilliSeconds(50));
            }
        } while (!fdsRequest.has_value());

        fdsRequest->Result.SetValue(fdsMock.ComposeOkResult());

        event = ReadSession2->GetEvent(true);
        UNIT_ASSERT(event.Defined());
        Cerr << ">>> Got event: " << DebugString(*event) << Endl;
        UNIT_ASSERT(std::holds_alternative<TReadSessionEvent::TStartPartitionSessionEvent>(*event));

        // Cerr << ">>> Got event: " << DebugString(*event) << Endl;
        // UNIT_ASSERT(std::holds_alternative<NTopic::TSessionClosedEvent>(*event));
    }

    Y_UNIT_TEST(FallbackToSingleDb) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);

        setup->Start(true, true);

        std::shared_ptr<NYdb::NFederatedTopic::IFederatedReadSession> ReadSession;

        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(setup->GetDriver());

        // Create read session.
        NYdb::NFederatedTopic::TFederatedReadSessionSettings readSettings;
        readSettings
            .ConsumerName("shared/user")
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        ReadSession = topicClient.CreateReadSession(readSettings);
        Cerr << "Session was created" << Endl;

        ReadSession->WaitEvent().Wait(TDuration::Seconds(1));
        TMaybe<NYdb::NFederatedTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
        Y_ASSERT(event);
        Cerr << "Got new read session event: " << DebugString(*event) << Endl;

        auto* startPartitionSessionEvent = std::get_if<NYdb::NFederatedTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event);
        Y_ASSERT(startPartitionSessionEvent);
        startPartitionSessionEvent->Confirm();

        ReadSession->Close(TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(FallbackToSingleDbAfterBadRequest) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        setup->Start(true, true);
        TFederationDiscoveryServiceMock fdsMock;
        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        fdsMock.Port = setup->GetGrpcPort();

        Cerr << "PORTS " << fdsMock.Port << " " << newServicePort << Endl;
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        auto clientSettings = TFederatedTopicClientSettings();
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver, clientSettings);

        // Create read session.
        NYdb::NFederatedTopic::TFederatedReadSessionSettings readSettings;
        readSettings
            .ConsumerName("shared/user")
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        auto ReadSession = topicClient.CreateReadSession(readSettings);
        Cerr << "Session was created" << Endl;

        ReadSession->WaitEvent().Wait(TDuration::Seconds(1));
        TMaybe<NYdb::NFederatedTopic::TReadSessionEvent::TEvent> event = ReadSession->GetEvent(false);
        Y_ASSERT(!event);

        {
            auto fdsRequest = fdsMock.GetNextPendingRequest();
            Y_ASSERT(fdsRequest.has_value());
            Ydb::FederationDiscovery::ListFederationDatabasesResponse response;
            auto op = response.mutable_operation();
            op->set_status(Ydb::StatusIds::BAD_REQUEST);
            response.mutable_operation()->set_ready(true);
            response.mutable_operation()->set_id("12345");
            fdsRequest->Result.SetValue({std::move(response), grpc::Status::OK});
        }

        ReadSession->WaitEvent().Wait();
        TMaybe<NYdb::NFederatedTopic::TReadSessionEvent::TEvent> event2 = ReadSession->GetEvent(true);
        Cerr << "Got new read session event: " << DebugString(*event2) << Endl;

        auto* sessionEvent = std::get_if<NYdb::NFederatedTopic::TSessionClosedEvent>(&*event2);
        // At this point the SDK should connect to Topic API, but in this test we have it on a separate port,
        // so SDK connects back to FederationDiscovery service and the CLIENT_CALL_UNIMPLEMENTED status is expected.
        UNIT_ASSERT_EQUAL(sessionEvent->GetStatus(), EStatus::CLIENT_CALL_UNIMPLEMENTED);
        ReadSession->Close(TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(SimpleHandlers) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();

        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        std::shared_ptr<NYdb::NFederatedTopic::IFederatedReadSession> ReadSession;

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        auto clientSettings = TFederatedTopicClientSettings()
            .RetryPolicy(NTopic::IRetryPolicy::GetFixedIntervalPolicy(
                TDuration::Seconds(10),
                TDuration::Seconds(10)
            ));
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver, clientSettings);

        ui64 count = 300u;

        TString messageBase = "message----";
        TVector<TString> sentMessages;

        for (auto i = 0u; i < count; i++) {
            // sentMessages.emplace_back(messageBase * (i+1) + ToString(i));
            sentMessages.emplace_back(messageBase * (10 * i + 1));
        }

        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        auto totalReceived = 0u;

        auto f = checkedPromise.GetFuture();
        TAtomic check = 1;

        // Create read session.
        NYdb::NFederatedTopic::TFederatedReadSessionSettings readSettings;
        readSettings
            .ConsumerName("shared/user")
            .MaxMemoryUsageBytes(1_MB)
            .AppendTopics(setup->GetTestTopic());

        readSettings.FederatedEventHandlers_.SimpleDataHandlers([&](TReadSessionEvent::TDataReceivedEvent& ev) mutable {
            Cerr << ">>> event from dataHandler: " << DebugString(ev) << Endl;
            Y_VERIFY_S(AtomicGet(check) != 0, "check is false");
            auto& messages = ev.GetMessages();
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];
                UNIT_ASSERT_VALUES_EQUAL(message.GetData(), sentMessages[totalReceived]);
                totalReceived++;
            }
            if (totalReceived == sentMessages.size())
                checkedPromise.SetValue();
        });

        ReadSession = topicClient.CreateReadSession(readSettings);
        Cerr << ">>> Session was created" << Endl;

        Sleep(TDuration::MilliSeconds(50));

        auto events = ReadSession->GetEvents(false);
        UNIT_ASSERT(events.empty());

        std::optional<TFederationDiscoveryServiceMock::TManualRequest> fdsRequest;
        do {
            fdsRequest = fdsMock.GetNextPendingRequest();
            if (!fdsRequest.has_value()) {
                Sleep(TDuration::MilliSeconds(50));
            }
        } while (!fdsRequest.has_value());

        fdsRequest->Result.SetValue(fdsMock.ComposeOkResult());

        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(NPersQueue::ECodec::RAW);
        NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
        writeSettings.CompressionExecutor(executor);

        auto& client = setup->GetPersQueueClient();
        auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

        for (auto i = 0u; i < count; i++) {
            auto res = session->Write(sentMessages[i]);
            UNIT_ASSERT(res);
        }

        f.GetValueSync();
        ReadSession->Close();
        AtomicSet(check, 0);
    }

    Y_UNIT_TEST(ReadMirrored) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME, false);
        setup->Start(true, true);
        setup->CreateTopic(setup->GetTestTopic() + "-mirrored-from-dc2", setup->GetLocalCluster());
        setup->CreateTopic(setup->GetTestTopic() + "-mirrored-from-dc3", setup->GetLocalCluster());

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();

        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        std::shared_ptr<NYdb::NFederatedTopic::IFederatedReadSession> ReadSession;

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        auto clientSettings = TFederatedTopicClientSettings()
            .RetryPolicy(NTopic::IRetryPolicy::GetFixedIntervalPolicy(
                TDuration::Seconds(10),
                TDuration::Seconds(10)
            ));
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver, clientSettings);

        ui64 count = 5u;

        TString messageBase = "message----";
        TVector<TString> sentMessages;
        std::unordered_set<TString> sentSet;

        for (auto i = 0u; i < count; i++) {
            sentMessages.emplace_back(messageBase * (10 * i + 1));
            sentSet.emplace(sentMessages.back() + "-from-dc1");
            sentSet.emplace(sentMessages.back() + "-from-dc2");
            sentSet.emplace(sentMessages.back() + "-from-dc3");
        }

        NThreading::TPromise<void> checkedPromise = NThreading::NewPromise<void>();
        auto totalReceived = 0u;

        auto f = checkedPromise.GetFuture();
        TAtomic check = 1;

        // Create read session.
        NYdb::NFederatedTopic::TFederatedReadSessionSettings readSettings;
        readSettings
            .ReadMirrored("dc1")
            .ConsumerName("shared/user")
            .MaxMemoryUsageBytes(16_MB)
            .AppendTopics(setup->GetTestTopic());

        readSettings.FederatedEventHandlers_.SimpleDataHandlers([&](TReadSessionEvent::TDataReceivedEvent& ev) mutable {
            Cerr << ">>> event from dataHandler: " << DebugString(ev) << Endl;
            Y_VERIFY_S(AtomicGet(check) != 0, "check is false");
            auto& messages = ev.GetMessages();
            Cerr << ">>> get " << messages.size() << " messages in this event" << Endl;
            for (size_t i = 0u; i < messages.size(); ++i) {
                auto& message = messages[i];
                UNIT_ASSERT(message.GetFederatedPartitionSession()->GetReadSourceDatabaseName() == "dc1");
                UNIT_ASSERT(message.GetFederatedPartitionSession()->GetTopicPath() == setup->GetTestTopic());
                UNIT_ASSERT(message.GetData().EndsWith(message.GetFederatedPartitionSession()->GetTopicOriginDatabaseName()));

                UNIT_ASSERT(!sentSet.empty());
                UNIT_ASSERT_C(sentSet.erase(message.GetData()), "no such element is sentSet: " + message.GetData());
                totalReceived++;
            }
            if (totalReceived == 3 * sentMessages.size()) {
                UNIT_ASSERT(sentSet.empty());
                checkedPromise.SetValue();
            }
        });

        ReadSession = topicClient.CreateReadSession(readSettings);
        Cerr << ">>> Session was created" << Endl;

        Sleep(TDuration::MilliSeconds(50));

        auto events = ReadSession->GetEvents(false);
        UNIT_ASSERT(events.empty());

        std::optional<TFederationDiscoveryServiceMock::TManualRequest> fdsRequest;
        do {
            fdsRequest = fdsMock.GetNextPendingRequest();
            if (!fdsRequest.has_value()) {
                Sleep(TDuration::MilliSeconds(50));
            }
        } while (!fdsRequest.has_value());

        fdsRequest->Result.SetValue(fdsMock.ComposeOkResult());

        {
            NPersQueue::TWriteSessionSettings writeSettings;
            writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
            writeSettings.Codec(NPersQueue::ECodec::RAW);
            NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
            writeSettings.CompressionExecutor(executor);

            auto& client = setup->GetPersQueueClient();
            auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

            for (auto i = 0u; i < count; i++) {
                auto res = session->Write(sentMessages[i] + "-from-dc1");
                UNIT_ASSERT(res);
            }

            session->Close();

            Cerr << ">>> Writes to test-topic successful" << Endl;
        }

        {
            NPersQueue::TWriteSessionSettings writeSettings;
            writeSettings.Path(setup->GetTestTopic() + "-mirrored-from-dc2").MessageGroupId("src_id");
            writeSettings.Codec(NPersQueue::ECodec::RAW);
            NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
            writeSettings.CompressionExecutor(executor);

            auto& client = setup->GetPersQueueClient();
            auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

            for (auto i = 0u; i < count; i++) {
                auto res = session->Write(sentMessages[i] + "-from-dc2");
                UNIT_ASSERT(res);
            }

            session->Close();

            Cerr << ">>> Writes to test-topic-mirrored-from-dc2 successful" << Endl;
        }

        {
            NPersQueue::TWriteSessionSettings writeSettings;
            writeSettings.Path(setup->GetTestTopic() + "-mirrored-from-dc3").MessageGroupId("src_id");
            writeSettings.Codec(NPersQueue::ECodec::RAW);
            NPersQueue::IExecutor::TPtr executor = new NPersQueue::TSyncExecutor();
            writeSettings.CompressionExecutor(executor);

            auto& client = setup->GetPersQueueClient();
            auto session = client.CreateSimpleBlockingWriteSession(writeSettings);

            for (auto i = 0u; i < count; i++) {
                auto res = session->Write(sentMessages[i] + "-from-dc3");
                UNIT_ASSERT(res);
            }

            session->Close();

            Cerr << ">>> Writes to test-topic-mirrored-from-dc3 successful" << Endl;
        }

        f.GetValueSync();
        ReadSession->Close();
        AtomicSet(check, 0);
    }

    Y_UNIT_TEST(BasicWriteSession) {
        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
            TEST_CASE_NAME, false, ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_DEBUG, 2);

        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();

        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        std::shared_ptr<NYdb::NTopic::IWriteSession> WriteSession;

        // Create topic client.
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver);

        // Create write session.
        auto writeSettings = NTopic::TWriteSessionSettings()
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id");

        WriteSession = topicClient.CreateWriteSession(writeSettings);
        Cerr << "Session was created" << Endl;

        WriteSession->WaitEvent().Wait(TDuration::Seconds(1));
        auto event = WriteSession->GetEvent(false);
        Y_ASSERT(event);
        Cerr << "Got new write session event: " << DebugString(*event) << Endl;
        auto* readyToAcceptEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event);
        Y_ASSERT(readyToAcceptEvent);
        WriteSession->Write(std::move(readyToAcceptEvent->ContinuationToken), NTopic::TWriteMessage("hello"));

        WriteSession->WaitEvent().Wait(TDuration::Seconds(1));
        event = WriteSession->GetEvent(false);
        Y_ASSERT(event);
        Cerr << "Got new write session event: " << DebugString(*event) << Endl;

        readyToAcceptEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event);
        Y_ASSERT(readyToAcceptEvent);

        std::optional<TFederationDiscoveryServiceMock::TManualRequest> fdsRequest;
        do {
            fdsRequest = fdsMock.GetNextPendingRequest();
            if (!fdsRequest.has_value()) {
                Sleep(TDuration::MilliSeconds(50));
            }
        } while (!fdsRequest.has_value());

        fdsRequest->Result.SetValue(fdsMock.ComposeOkResult());

        WriteSession->WaitEvent().Wait(TDuration::Seconds(1));
        event = WriteSession->GetEvent(false);
        Y_ASSERT(event);
        Cerr << "Got new write session event: " << DebugString(*event) << Endl;

        auto* acksEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(&*event);
        Y_ASSERT(acksEvent);

        WriteSession->Close(TDuration::MilliSeconds(10));
    }

}

}
