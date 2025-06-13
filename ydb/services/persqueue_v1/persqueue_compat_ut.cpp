#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

using namespace NYdb;
using namespace NYdb::NPersQueue;


namespace NKikimr::NPersQueueTests {

class TPQv1CompatTestBase {
public:
    THolder<::NPersQueue::TTestServer> Server;
    THolder<TDriver> Driver;
    THolder<TPersQueueClient> PQClient;
    TString DbRoot;
    TString DbPath;

    TPQv1CompatTestBase()
    {
        Server = MakeHolder<::NPersQueue::TTestServer>(false);
        DbRoot = "/Root/LbCommunal";
        DbPath = "/Root/LbCommunal/account";
        Server->ServerSettings.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot(DbRoot);
        Server->ServerSettings.PQConfig.SetTestDatabaseRoot(DbRoot);

        Server->ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(false);

        Server->StartServer();
        //Server->EnableLogs()
        Server->EnableLogs({ NKikimrServices::KQP_PROXY }, NActors::NLog::PRI_EMERG);
        Server->EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_METACACHE });


        Server->AnnoyingClient->CreateTopicNoLegacy("rt3.dc2--account--topic1", 1, true, false);
        Server->AnnoyingClient->CreateTopicNoLegacy("rt3.dc1--account--topic1", 1, true);

        Server->WaitInit("account/topic1");
        Server->AnnoyingClient->MkDir("/Root", "LbCommunal");
        Server->AnnoyingClient->MkDir("/Root/LbCommunal", "account");
        Server->AnnoyingClient->CreateTopicNoLegacy(
                "/Root/LbCommunal/account/topic2", 1, true, true, {}, {}, "account"
        );
        Server->AnnoyingClient->CreateTopicNoLegacy(
                "/Root/LbCommunal/account/topic2-mirrored-from-dc2", 1, true, false, {}, {}, "account"
        );

        Server->AnnoyingClient->MkDir("/Root", "LbCommunal");
        Server->AnnoyingClient->MkDir("/Root/LbCommunal", "account2");
        Server->AnnoyingClient->CreateTopicNoLegacy(
                "/Root/LbCommunal/account2/topic3", 1, true, true, {}, {}, "account2"
        );
        Server->AnnoyingClient->CreateTopicNoLegacy(
                "/Root/LbCommunal/account2/topic3-mirrored-from-dc2", 1, true, false, {}, {}, "account2"
        );


        Server->AnnoyingClient->CreateConsumer("test-consumer");
        InitPQLib();
    }

    ~TPQv1CompatTestBase() {
        Server->ShutdownServer();
    }

    void InitPQLib() {
        TDriverConfig driverConfig(TStringBuilder() << "localhost:" << Server->GrpcPort);
        driverConfig.SetDatabase("/Root/");
        Driver.Reset(new TDriver(driverConfig));
        PQClient.Reset(new TPersQueueClient(*Driver));
    };

    void CreateTopic(const TString& path, const TString& account, bool canWrite, bool expectFail) {
        Server->AnnoyingClient->CreateTopicNoLegacy(
                path, 1, true, canWrite, {}, {}, account, expectFail
        );
    }

    std::shared_ptr<ISimpleBlockingWriteSession> CreateWriteSession(const TString& path) {
        Y_ABORT_UNLESS(PQClient);
        TWriteSessionSettings settings;
        settings.Path(path).MessageGroupId("test-src");
        return PQClient->CreateSimpleBlockingWriteSession(settings);
    }

    std::shared_ptr<IReadSession> CreateReadSession(const TVector<TString>& paths) {
        Y_ABORT_UNLESS(PQClient);


        TReadSessionSettings settings;
        settings.ConsumerName("test-consumer").ReadMirrored("dc1");

        for (const auto& path : paths) {
            settings.AppendTopics(TTopicReadSettings{path});
        }
        return PQClient->CreateReadSession(settings);
    }
};

Y_UNIT_TEST_SUITE(TPQCompatTest) {
    Y_UNIT_TEST(DiscoverTopics) {
        TPQv1CompatTestBase testServer;
        Cerr << "Create write session\n";
        auto ws = testServer.CreateWriteSession("account/topic2");
        Cerr << "Wait init seqNo\n";
        auto seqNo = ws->GetInitSeqNo();

        ws = testServer.CreateWriteSession("account/topic1");
        Cerr << "Wait init seqNo(2)\n";
        seqNo = ws->GetInitSeqNo();
        Y_UNUSED(seqNo);
    }

    void GetLocks(const THashSet<TString>& paths, std::shared_ptr<IReadSession>& readSession) {
        THashMap<TString, THashSet<TString>> clustersFound;
        for (auto i = 0u; i < paths.size() * 2; i++) {
            auto ev = readSession->GetEvent(true);
            Y_ABORT_UNLESS(ev.Defined());
            auto* lockEvent = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*ev);
            if (!lockEvent) {
                if (auto* otherEv = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent>(&*ev)) {
                    Cerr << "Got destroy event";
                } else if (auto* otherEv = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent>(&*ev)) {
                    Cerr << "Got part closed event\n";
                } else if (auto* otherEv = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*ev)) {
                    Cerr << "Got session closed event" << otherEv->DebugString() << Endl;
                }
            }
            UNIT_ASSERT(lockEvent);
            Cerr << "Got lock event: " << lockEvent->DebugString() << Endl;
            const auto& path = lockEvent->GetPartitionStream()->GetTopicPath();
            const auto& cluster = lockEvent->GetPartitionStream()->GetCluster();
            UNIT_ASSERT(paths.contains(path));
            auto& clusters = clustersFound[path];
            UNIT_ASSERT(!clusters.contains(cluster));
            clusters.insert(cluster);
            //lockEvent->Confirm();
        }
        UNIT_ASSERT_VALUES_EQUAL(clustersFound.size(), paths.size());
        for (const auto& [path, clusters] : clustersFound) {
            UNIT_ASSERT_VALUES_EQUAL(clusters.size(), 2);
            UNIT_ASSERT(clusters.contains("dc1"));
            UNIT_ASSERT(clusters.contains("dc2"));
        }

    }

    Y_UNIT_TEST(SetupLockSession) {
        TPQv1CompatTestBase testServer;
        {
            auto rs = testServer.CreateReadSession({"account/topic2"});
            GetLocks({"account/topic2"}, rs);
            rs->Close();
        }
        {
            auto rs = testServer.CreateReadSession({"account/topic1"});
            GetLocks({"account/topic1"}, rs);
            rs->Close();
        }
        {
            auto rs = testServer.CreateReadSession({"account/topic1", "account/topic2"});
            GetLocks({"account/topic1", "account/topic2"}, rs);
            rs->Close();
        }
        {
            auto rs = testServer.CreateReadSession({"account/topic1", "account/topic2", "account2/topic3"});
            GetLocks({"account/topic1", "account/topic2", "account2/topic3"}, rs);
            rs->Close();
        }

    }

    Y_UNIT_TEST(BadTopics) {
        TPQv1CompatTestBase testServer;
        testServer.CreateTopic("/Root/PQ/some-topic", "", true, true);
        testServer.CreateTopic("/Root/PQ/some-topic-mirrored-from-dc2", "", false, true);
        testServer.CreateTopic("/Root/PQ/.some-topic/mirrored-from-dc2", "", false, true);

        //Bad dc
        testServer.CreateTopic("/Root/PQ/rt3.bad--account--topic", "", true, true);
        testServer.CreateTopic("/Root/PQ/rt3.bad--account--topic", "", false, true);

        // Local, writes disabled
        testServer.CreateTopic("/Root/PQ/rt3.dc1--account--topic", "", false, true);

        // Non-local, writes enabled
        testServer.CreateTopic("/Root/PQ/rt3.dc2--account--topic", "", true, true);

        // Local topic with client write disabled
        testServer.CreateTopic("/Root/LbCommunal/some-topic", "account", false, true);
        // Non-local topic with client write enabled
        testServer.CreateTopic("/Root/LbCommunal/.some-topic/mirrored-from-dc2", "account", true, true);
        // this validation was relaxed
        testServer.CreateTopic("/Root/LbCommunal/some-topic-mirrored-from-dc2", "account", true, false);
        // No account
        testServer.CreateTopic("/Root/LbCommunal/some-topic", "", true, true);
        // Mirrored-from local
        testServer.CreateTopic("/Root/LbCommunal/.some-topic/mirrored-from-dc1", "account", false, true);
        // this validation was relaxed
        testServer.CreateTopic("/Root/LbCommunal/some-topic-mirrored-from-dc1", "account", false, false);
        // Bad mirrored names
        testServer.CreateTopic("/Root/LbCommunal/.some-topic/some-topic", "account", false, true);
        // Mirrored-from non-existing
        testServer.CreateTopic("/Root/LbCommunal/.some-topic/mirrored-from-dc777", "account", false, true);
        testServer.CreateTopic("/Root/LbCommunal/some-topic-mirrored-from-dc777", "account", false, true);

        // Just to verify it even creates anything at all
        testServer.CreateTopic("/Root/LbCommunal/account/some-topic-mirrored-from-dc2", "account", false, false);
    }

    Y_UNIT_TEST(CommitOffsets) {
        TPQv1CompatTestBase testServer;
        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel(
                         "localhost:" + ToString(testServer.Server->GrpcPort),
                         grpc::InsecureChannelCredentials()
                    );
        auto TopicStubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);

        {
            grpc::ClientContext rcontext;

            rcontext.AddMetadata("x-ydb-database", "/Root/LbCommunal/account");

            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("topic2");
            req.set_consumer("user");
            req.set_offset(0);

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        {
            grpc::ClientContext rcontext;

            rcontext.AddMetadata("x-ydb-database", "/Root/LbCommunal/account");

            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("topic2-mirrored-from-dc2");
            req.set_consumer("user");
            req.set_offset(0);

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::SUCCESS);
        }

    }

    Y_UNIT_TEST(LongProducerAndLongMessageGroupId) {
        TPQv1CompatTestBase testServer;
        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel(
                         "localhost:" + ToString(testServer.Server->GrpcPort),
                         grpc::InsecureChannelCredentials()
                    );
        auto TopicStubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);

        struct Data {
            TString ProducerId;
            bool error;
        };

        std::vector<Data> data = {
            {"producer-1", false},
            { TString(2_KB, '-'), false},
            { TString(2_KB + 1, '-'), true},
        };

        for(const auto& [producerId, error] : data) {
            Cerr << ">>>>> Case producerId.length()=" << producerId.length() << Endl;

            grpc::ClientContext wcontext;
            wcontext.AddMetadata("x-ydb-database", "/Root/LbCommunal/account");

            auto writeStream = TopicStubP_->StreamWrite(&wcontext);
            UNIT_ASSERT(writeStream);

            Ydb::Topic::StreamWriteMessage::FromClient req;
            Ydb::Topic::StreamWriteMessage::FromServer resp;

            req.mutable_init_request()->set_path("topic2");
            if (!producerId.Empty()) {
                req.mutable_init_request()->set_producer_id(producerId);
                req.mutable_init_request()->set_message_group_id(producerId);
            }

            UNIT_ASSERT(writeStream->Write(req));
            UNIT_ASSERT(writeStream->Read(&resp));
            Cerr << ">>>>> Response = " << resp.server_message_case() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kInitResponse);
            req.Clear();

            Cerr << ">>>>> Write message" << Endl;
            auto* write = req.mutable_write_request();
            write->set_codec(Ydb::Topic::CODEC_RAW);

            auto* msg = write->add_messages();
            msg->set_seq_no(1);
            msg->set_data("x");
            UNIT_ASSERT(writeStream->Write(req));
            UNIT_ASSERT(writeStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() ==  (error ? Ydb::Topic::StreamWriteMessage::FromServer::SERVER_MESSAGE_NOT_SET : Ydb::Topic::StreamWriteMessage::FromServer::kWriteResponse));
            resp.Clear();
        }
    }

    Y_UNIT_TEST(ReadWriteSessions) {
        TPQv1CompatTestBase testServer;
        std::shared_ptr<grpc::Channel> Channel_;
        Channel_ = grpc::CreateChannel(
                         "localhost:" + ToString(testServer.Server->GrpcPort),
                         grpc::InsecureChannelCredentials()
                    );
        auto TopicStubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);


        for (auto topic : std::vector<TString> {"topic2", "topic2-mirrored-from-dc2"}) {
            grpc::ClientContext wcontext;
            wcontext.AddMetadata("x-ydb-database", "/Root/LbCommunal/account");

            auto writeStream = TopicStubP_->StreamWrite(&wcontext);
            UNIT_ASSERT(writeStream);

            Ydb::Topic::StreamWriteMessage::FromClient req;
            Ydb::Topic::StreamWriteMessage::FromServer resp;

            req.mutable_init_request()->set_path(topic);
            req.mutable_init_request()->set_producer_id("A");
            req.mutable_init_request()->set_partition_id(0);
            UNIT_ASSERT(writeStream->Write(req));
            UNIT_ASSERT(writeStream->Read(&resp));

            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            if (topic == "topic2") {
                UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kInitResponse);
            } else {
                UNIT_ASSERT(resp.status() == Ydb::StatusIds::BAD_REQUEST);
                break;
            }
            req.Clear();

            auto* write = req.mutable_write_request();
            write->set_codec(Ydb::Topic::CODEC_RAW);

            auto* msg = write->add_messages();
            msg->set_seq_no(1);
            msg->set_data("x");
            UNIT_ASSERT(writeStream->Write(req));
            UNIT_ASSERT(writeStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kWriteResponse);
        }

        for (auto topic : std::vector<TString> {"topic2", "topic2-mirrored-from-dc2"}) {
            grpc::ClientContext rcontext;
            rcontext.AddMetadata("x-ydb-database", "/Root/LbCommunal/account");

            auto readStream = TopicStubP_->StreamRead(&rcontext);
            UNIT_ASSERT(readStream);
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path(topic);

            req.mutable_init_request()->set_consumer("user");

            UNIT_ASSERT(readStream->Write(req));
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);

            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().path() == topic);
        }

        for (ui32 i = 0; i < 2; ++i) {
            for (auto topic : std::vector<TString> {"account/topic2", "account/topic2-mirrored-from-dc2"}) {
                grpc::ClientContext rcontext;

                if (i > 0) {
                    rcontext.AddMetadata("x-ydb-database", "/Root/LbCommunal");
                }

                auto readStream = TopicStubP_->StreamRead(&rcontext);
                UNIT_ASSERT(readStream);
                Ydb::Topic::StreamReadMessage::FromClient req;
                Ydb::Topic::StreamReadMessage::FromServer resp;

                req.mutable_init_request()->add_topics_read_settings()->set_path(topic);

                req.mutable_init_request()->set_consumer("user");

                Cerr << "BEFORE PARSING " << topic << "\n";

                UNIT_ASSERT(readStream->Write(req));
                UNIT_ASSERT(readStream->Read(&resp));
                Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
                UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
                UNIT_ASSERT(readStream->Read(&resp));
                Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
                UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
                UNIT_ASSERT(resp.start_partition_session_request().partition_session().path() == topic);
            }
        }

    }
}

} // namespace NKikimr::NPersQueueTests
