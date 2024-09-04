#include <ydb/public/sdk/cpp/client/ydb_federated_topic/federated_topic.h>
#include <ydb/public/sdk/cpp/client/ydb_federated_topic/impl/federated_write_session.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/managed_executor.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/executor_impl.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/include/write_session.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>
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
                fdsRequest->Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());
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

        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue({Response, grpc::Status::OK});

        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(NPersQueue::ECodec::RAW);
        NPersQueue::IExecutor::TPtr executor = new NTopic::TSyncExecutor();
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

        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue({{}, grpc::Status(grpc::StatusCode::UNAVAILABLE, "mock 'unavailable'")});

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

        fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

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

        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

        NPersQueue::TWriteSessionSettings writeSettings;
        writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
        writeSettings.Codec(NPersQueue::ECodec::RAW);
        NPersQueue::IExecutor::TPtr executor = new NTopic::TSyncExecutor();
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

        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

        {
            NPersQueue::TWriteSessionSettings writeSettings;
            writeSettings.Path(setup->GetTestTopic()).MessageGroupId("src_id");
            writeSettings.Codec(NPersQueue::ECodec::RAW);
            NPersQueue::IExecutor::TPtr executor = new NTopic::TSyncExecutor();
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
            NPersQueue::IExecutor::TPtr executor = new NTopic::TSyncExecutor();
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
            NPersQueue::IExecutor::TPtr executor = new NTopic::TSyncExecutor();
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
            .DirectWriteToPartition(false)
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

        fdsRequest->Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

        WriteSession->WaitEvent().Wait(TDuration::Seconds(1));
        event = WriteSession->GetEvent(false);
        Y_ASSERT(event);
        Cerr << "Got new write session event: " << DebugString(*event) << Endl;

        auto* acksEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(&*event);
        Y_ASSERT(acksEvent);

        WriteSession->Close(TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(CloseWriteSessionImmediately) {
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
            .DirectWriteToPartition(false)
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id");

        WriteSession = topicClient.CreateWriteSession(writeSettings);
        Cerr << "Session was created" << Endl;

        WriteSession->Close(TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(WriteSessionCloseWaitsForWrites) {
        // Write a bunch of messages before the federation observer initialization.
        // Then as soon as the federation discovery service responds to the observer, close the write session.
        // The federated write session must wait for acks of all written messages.

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
            TEST_CASE_NAME, false, ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_DEBUG, 2);

        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();
        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver);

        // Create write session.
        auto writeSettings = NTopic::TWriteSessionSettings()
            .DirectWriteToPartition(false)
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id");

        int acks = 0;
        int messageCount = 100;

        auto gotAllAcks = NThreading::NewPromise();
        writeSettings.EventHandlers_.AcksHandler([&](NTopic::TWriteSessionEvent::TAcksEvent& ev) {
            acks += ev.Acks.size();
            if (acks == messageCount) {
                gotAllAcks.SetValue();
            }
        });

        auto WriteSession = topicClient.CreateWriteSession(writeSettings);
        Cerr << "Session was created" << Endl;

        for (int i = 0; i < messageCount; ++i) {
            auto event = WriteSession->GetEvent(true);
            auto* readyToAcceptEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event);
            WriteSession->Write(std::move(readyToAcceptEvent->ContinuationToken), NTopic::TWriteMessage("hello-" + ToString(i)));
        }

        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

        // Close method should wait until all messages have been acked.
        WriteSession->Close();
        gotAllAcks.GetFuture().Wait();
        UNIT_ASSERT_VALUES_EQUAL(acks, messageCount);
    }

    Y_UNIT_TEST(WriteSessionCloseIgnoresWrites) {
        // Create a federated topic client with NoRetryPolicy.
        // Make federation discovery service to respond with an UNAVAILABLE status.
        // It makes a federation observer object to become stale and the write session to close.

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
            TEST_CASE_NAME, false, ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_DEBUG, 2);

        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();
        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        TFederatedTopicClientSettings clientSettings;
        clientSettings.RetryPolicy(NPersQueue::IRetryPolicy::GetNoRetryPolicy());
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver, clientSettings);

        // Create write session.
        auto writeSettings = NTopic::TWriteSessionSettings()
            .DirectWriteToPartition(false)
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id");

        int acks = 0;
        writeSettings.EventHandlers_.AcksHandler([&acks](NTopic::TWriteSessionEvent::TAcksEvent& ev) {
            acks += ev.Acks.size();
        });

        auto WriteSession = topicClient.CreateWriteSession(writeSettings);
        Cerr << "Session was created" << Endl;

        int messageCount = 100;
        for (int i = 0; i < messageCount; ++i) {
            auto event = WriteSession->GetEvent(true);
            auto* readyToAcceptEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*event);
            WriteSession->Write(std::move(readyToAcceptEvent->ContinuationToken), NTopic::TWriteMessage("hello-" + ToString(i)));
        }

        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeUnavailableResult());

        // At this point the observer that federated write session works with should become stale, and the session closes.
        // No messages we have written and no federation discovery requests should be sent.

        Sleep(TDuration::Seconds(3));
        UNIT_ASSERT(!fdsMock.GetNextPendingRequest().has_value());
        WriteSession->Close();
        UNIT_ASSERT_VALUES_EQUAL(acks, 0);
    }

    Y_UNIT_TEST(PreferredDatabaseNoFallback) {
        // The test checks that the session keeps trying to connect to the preferred database
        // and does not fall back to other databases.

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
            TEST_CASE_NAME, false, ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_DEBUG, 2);

        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();
        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver);

        auto retryPolicy = std::make_shared<NPersQueue::NTests::TYdbPqTestRetryPolicy>();

        auto writeSettings = TFederatedWriteSessionSettings()
            .AllowFallback(false)
            .PreferredDatabase("dc2");

        writeSettings
            .RetryPolicy(retryPolicy)
            .DirectWriteToPartition(false)
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id");

        retryPolicy->Initialize();
        retryPolicy->ExpectBreakDown();

        auto writer = topicClient.CreateWriteSession(writeSettings);

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
        c2->set_status(::Ydb::FederationDiscovery::DatabaseInfo::Status::DatabaseInfo_Status_UNAVAILABLE);
        c2->set_weight(500);
        op->mutable_result()->PackFrom(mockResult);
        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue({std::move(response), grpc::Status::OK});

        Cerr << "=== Session was created, waiting for retries" << Endl;
        retryPolicy->WaitForRetriesSync(3);

        Cerr << "=== In the next federation discovery response dc2 will be available" << Endl;
        // fdsMock.PreparedResponse.Clear();
        // std::optional<TFederationDiscoveryServiceMock::TManualRequest> fdsRequest;
        fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

        Cerr << "=== Waiting for repair" << Endl;
        retryPolicy->WaitForRepairSync();

        Cerr << "=== Closing the session" << Endl;
        writer->Close(TDuration::MilliSeconds(10));
    }

    Y_UNIT_TEST(WriteSessionNoAvailableDatabase) {
        // Create a federated write session with NoRetryPolicy, PreferredDatabase set and AllowFallback=false.
        // Make federation discovery service to respond with an UNAVAILABLE status for the preferred database.
        // It makes the write session to close itself.

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
            TEST_CASE_NAME, false, ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_DEBUG, 2);

        setup->Start(true, true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();
        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << newServicePort);
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        TFederatedTopicClientSettings clientSettings;
        NYdb::NFederatedTopic::TFederatedTopicClient topicClient(driver, clientSettings);

        auto writeSettings = NTopic::TWriteSessionSettings()
            .DirectWriteToPartition(false)
            .RetryPolicy(NPersQueue::IRetryPolicy::GetNoRetryPolicy())
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id");

        auto WriteSession = topicClient.CreateWriteSession(writeSettings);
        Cerr << "Session was created" << Endl;

        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultUnavailableDatabases());

        {
            auto e = WriteSession->GetEvent(true);
            UNIT_ASSERT(e.Defined());
            Cerr << ">>> Got event: " << DebugString(*e) << Endl;
            UNIT_ASSERT(std::holds_alternative<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(*e));
        }
        {
            auto e = WriteSession->GetEvent(true);
            UNIT_ASSERT(e.Defined());
            Cerr << ">>> Got event: " << DebugString(*e) << Endl;
            UNIT_ASSERT(std::holds_alternative<NYdb::NTopic::TSessionClosedEvent>(*e));
        }

        WriteSession->Close();
    }

    NTopic::TContinuationToken GetToken(std::shared_ptr<NTopic::IWriteSession> writer) {
        auto e = writer->GetEvent(true);
        UNIT_ASSERT(e.Defined());
        Cerr << ">>> Got event: " << DebugString(*e) << Endl;
        auto* readyToAcceptEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*e);
        UNIT_ASSERT(readyToAcceptEvent);
        return std::move(readyToAcceptEvent->ContinuationToken);
    }

    Y_UNIT_TEST(WriteSessionSwitchDatabases) {
        // Test that the federated write session doesn't deadlock when reconnecting to another database,
        // if the updated state of the federation is different from the previous one.

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
            TEST_CASE_NAME, false, ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_DEBUG, 2);

        setup->Start(true);

        TFederationDiscoveryServiceMock fdsMock;
        fdsMock.Port = setup->GetGrpcPort();
        ui16 newServicePort = setup->GetPortManager()->GetPort(4285);
        auto grpcServer = setup->StartGrpcService(newServicePort, &fdsMock);

        auto driverConfig = NYdb::TDriverConfig()
            .SetEndpoint(TStringBuilder() << "localhost:" << newServicePort)
            .SetDatabase("/Root")
            .SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        auto driver = NYdb::TDriver(driverConfig);
        auto topicClient = NYdb::NFederatedTopic::TFederatedTopicClient(driver);

        auto writeSettings = NTopic::TFederatedWriteSessionSettings()
            .PreferredDatabase("dc1")
            .AllowFallback(true)
            .DirectWriteToPartition(false)
            .RetryPolicy(NPersQueue::IRetryPolicy::GetNoRetryPolicy())
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id");

        size_t successfulSessionClosedEvents = 0;
        size_t otherSessionClosedEvents = 0;

        writeSettings
            .EventHandlers_.SessionClosedHandler([&](const NTopic::TSessionClosedEvent &ev) {
                ++(ev.IsSuccess() ? successfulSessionClosedEvents : otherSessionClosedEvents);
            });

        writeSettings.EventHandlers_.HandlersExecutor(NTopic::CreateSyncExecutor());

        auto WriteSession = topicClient.CreateWriteSession(writeSettings);

        TMaybe<NTopic::TContinuationToken> token;

        auto fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

        {
            WriteSession->Write(GetToken(WriteSession), NTopic::TWriteMessage("hello 1"));
            token = GetToken(WriteSession);

            auto e = WriteSession->GetEvent(true);
            auto* acksEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(&*e);
            UNIT_ASSERT(acksEvent);
        }

        // Wait for two requests to the federation discovery service.
        // This way we ensure the federated write session has had enough time to request
        // the updated state of the federation from its federation observer.

        fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultWithUnavailableDatabase(1));

        fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultWithUnavailableDatabase(1));

        {
            UNIT_ASSERT(token.Defined());
            WriteSession->Write(std::move(*token), NTopic::TWriteMessage("hello 2"));

            token = GetToken(WriteSession);

            auto e = WriteSession->GetEvent(true);
            auto* acksEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(&*e);
            UNIT_ASSERT(acksEvent);
        }

        fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

        fdsRequest = fdsMock.WaitNextPendingRequest();
        fdsRequest.Result.SetValue(fdsMock.ComposeOkResultAvailableDatabases());

        {
            UNIT_ASSERT(token.Defined());
            WriteSession->Write(std::move(*token), NTopic::TWriteMessage("hello 3"));

            token = GetToken(WriteSession);

            auto e = WriteSession->GetEvent(true);
            auto* acksEvent = std::get_if<NYdb::NTopic::TWriteSessionEvent::TAcksEvent>(&*e);
            UNIT_ASSERT(acksEvent);
        }

        setup->ShutdownGRpc();

        WriteSession->Close(TDuration::Seconds(5));

        UNIT_ASSERT_VALUES_EQUAL(otherSessionClosedEvents, 1);
        UNIT_ASSERT_VALUES_EQUAL(successfulSessionClosedEvents, 0);
    }

    Y_UNIT_TEST(WriteSessionWriteInHandlers) {
        // Write messages from all event handlers. It shouldn't deadlock.

        auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(
            TEST_CASE_NAME, false, ::NPersQueue::TTestServer::LOGGED_SERVICES, NActors::NLog::PRI_DEBUG, 2);
        setup->Start(true, true);
        NYdb::TDriverConfig cfg;
        cfg.SetEndpoint(TStringBuilder() << "localhost:" << setup->GetGrpcPort());
        cfg.SetDatabase("/Root");
        cfg.SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        NYdb::TDriver driver(cfg);
        NYdb::NFederatedTopic::TFederatedTopicClient client(driver);

        auto writeSettings = NTopic::TWriteSessionSettings()
            .DirectWriteToPartition(false)
            .Path(setup->GetTestTopic())
            .MessageGroupId("src_id");

        std::shared_ptr<NTopic::IWriteSession> WriteSession;

        // 1. The session issues the first token: write a message inside AcksHandler.
        // 2. The session issues another token: close the session, write a message inside SessionClosedHandler.

        bool gotAcksEvent = false;
        std::optional<NTopic::TContinuationToken> token;
        auto [acksHandlerPromise, sessionClosedHandlerPromise] = std::tuple{NThreading::NewPromise(), NThreading::NewPromise()};
        auto [sentFromAcksHandler, sentFromSessionClosedHandler] = std::tuple{acksHandlerPromise.GetFuture(), sessionClosedHandlerPromise.GetFuture()};
        writeSettings.EventHandlers(
            NTopic::TWriteSessionSettings::TEventHandlers()
                .HandlersExecutor(NTopic::CreateSyncExecutor())
                .ReadyToAcceptHandler([&token](NTopic::TWriteSessionEvent::TReadyToAcceptEvent& e) {
                    Cerr << "=== Inside ReadyToAcceptHandler" << Endl;
                    token = std::move(e.ContinuationToken);
                })
                .AcksHandler([&token, &gotAcksEvent, &WriteSession, promise = std::move(acksHandlerPromise)](NTopic::TWriteSessionEvent::TAcksEvent&) mutable {
                    Cerr << "=== Inside AcksHandler" << Endl;
                    if (!gotAcksEvent) {
                        gotAcksEvent = true;
                        WriteSession->Write(std::move(*token), "From AcksHandler");
                        promise.SetValue();
                    }
                })
                .SessionClosedHandler([&token, &WriteSession, promise = std::move(sessionClosedHandlerPromise)](NTopic::TSessionClosedEvent const&) mutable {
                    Cerr << "=== Inside SessionClosedHandler" << Endl;
                    WriteSession->Write(std::move(*token), "From SessionClosedHandler");
                    promise.SetValue();
                })
        );

        Cerr << "=== Before CreateWriteSession" << Endl;
        WriteSession = client.CreateWriteSession(writeSettings);
        Cerr << "=== Session created" << Endl;

        WriteSession->Write(std::move(*token), "After CreateWriteSession");

        sentFromAcksHandler.Wait();
        Cerr << "=== AcksHandler has written a message, closing the session" << Endl;

        WriteSession->Close();
        sentFromSessionClosedHandler.Wait();
        Cerr << "=== SessionClosedHandler has 'written' a message" << Endl;
    }

    void AddDatabase(std::vector<std::shared_ptr<TDbInfo>>& dbInfos, int id, int weight) {
        auto db = std::make_shared<TDbInfo>();
        db->set_id(ToString(id));
        db->set_name("db" + ToString(id));
        db->set_location("dc" + ToString(id));
        db->set_weight(weight);
        db->set_status(Ydb::FederationDiscovery::DatabaseInfo_Status_AVAILABLE);
        dbInfos.push_back(db);
    }

    void EnableDatabase(std::vector<std::shared_ptr<TDbInfo>>& dbInfos, int id) {
        dbInfos[id - 1]->set_status(Ydb::FederationDiscovery::DatabaseInfo_Status_AVAILABLE);
    }

    void DisableDatabase(std::vector<std::shared_ptr<TDbInfo>>& dbInfos, int id) {
        dbInfos[id - 1]->set_status(Ydb::FederationDiscovery::DatabaseInfo_Status_UNAVAILABLE);
    }

    Y_UNIT_TEST(SelectDatabaseByHash) {
        std::vector<std::shared_ptr<TDbInfo>> dbInfos;
        using Settings = TFederatedWriteSessionSettings;

        {
            auto [db, status] = SelectDatabaseByHashImpl(Settings(), dbInfos);
            UNIT_ASSERT(!db);
            UNIT_ASSERT_EQUAL(status, EStatus::NOT_FOUND);
        }

        AddDatabase(dbInfos, 1, 0);

        {
            auto [db, status] = SelectDatabaseByHashImpl(Settings(), dbInfos);
            UNIT_ASSERT(!db);
            UNIT_ASSERT_EQUAL(status, EStatus::NOT_FOUND);
        }

        AddDatabase(dbInfos, 2, 100);

        {
            auto [db, status] = SelectDatabaseByHashImpl(Settings(), dbInfos);
            UNIT_ASSERT(db);
            UNIT_ASSERT_EQUAL(db->id(), "2");
            UNIT_ASSERT_EQUAL(status, EStatus::SUCCESS);
        }
    }

    Y_UNIT_TEST(SelectDatabase) {
        std::vector<std::shared_ptr<TDbInfo>> dbInfos;
        for (int i = 1; i < 11; ++i) {
            AddDatabase(dbInfos, i, 1000);
        }

        using Settings = TFederatedWriteSessionSettings;

        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | set         | not found       | -           | any           | NOT_FOUND   |
            */
            for (bool allow : {false, true}) {
                auto settings = Settings().PreferredDatabase("db0").AllowFallback(allow);
                auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc1");
                UNIT_ASSERT(!db);
                UNIT_ASSERT_EQUAL(status, EStatus::NOT_FOUND);
            }
        }

        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | set         | available       | -           | any           | preferred   |
            */
            for (bool allow : {false, true}) {
                auto settings = Settings().PreferredDatabase("db8").AllowFallback(allow);
                auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc1");
                UNIT_ASSERT(db);
                UNIT_ASSERT_EQUAL(db->id(), "8");
            }
        }

        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | set         | unavailable     | -           | false         | UNAVAILABLE |
            */
            DisableDatabase(dbInfos, 8);
            auto settings = Settings().PreferredDatabase("db8").AllowFallback(false);
            auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc1");
            UNIT_ASSERT(!db);
            UNIT_ASSERT_EQUAL(status, EStatus::UNAVAILABLE);
            EnableDatabase(dbInfos, 8);
        }

        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | set         | unavailable     | -           | true          | by hash     |
            */
            DisableDatabase(dbInfos, 8);
            auto settings = Settings().PreferredDatabase("db8").AllowFallback(true);
            auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc1");
            UNIT_ASSERT(db);
            UNIT_ASSERT_UNEQUAL(db->id(), "8");
            UNIT_ASSERT_EQUAL(status, EStatus::SUCCESS);
            EnableDatabase(dbInfos, 8);
        }

        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | unset       | -               | not found   | false         | NOT_FOUND   |
            */
            auto settings = Settings().AllowFallback(false);
            auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc0");
            UNIT_ASSERT(!db);
            UNIT_ASSERT_EQUAL(status, EStatus::NOT_FOUND);
        }
        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | unset       | -               | not found   | true          | by hash     |
            */
            auto settings = Settings().AllowFallback(true);
            auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc0");
            UNIT_ASSERT(db);
            UNIT_ASSERT_EQUAL(status, EStatus::SUCCESS);
        }

        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | unset       | -               | available   | any           | local       |
            */
            for (bool allow : {false, true}) {
                auto settings = Settings().AllowFallback(allow);
                auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc1");
                UNIT_ASSERT(db);
                UNIT_ASSERT_EQUAL(db->id(), "1");
            }
        }

        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | unset       | -               | unavailable | false         | UNAVAILABLE |
            */
            DisableDatabase(dbInfos, 1);
            auto settings = Settings().AllowFallback(false);
            auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc1");
            UNIT_ASSERT(!db);
            UNIT_ASSERT_EQUAL(status, EStatus::UNAVAILABLE);
            EnableDatabase(dbInfos, 1);
        }

        {
            /*
            | PreferredDb | Preferred state | Local state | AllowFallback | Return      |
            |-------------+-----------------+-------------+---------------+-------------|
            | unset       | -               | unavailable | true          | by hash     |
            */
            DisableDatabase(dbInfos, 1);
            auto settings = Settings().AllowFallback(true);
            auto [db, status] = SelectDatabaseImpl(settings, dbInfos, "dc1");
            UNIT_ASSERT(db);
            UNIT_ASSERT_UNEQUAL(db->id(), "1");
            UNIT_ASSERT_EQUAL(status, EStatus::SUCCESS);
            EnableDatabase(dbInfos, 1);
        }
    }

}

}
