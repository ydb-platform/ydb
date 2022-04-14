#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <kikimr/persqueue/sdk/deprecated/cpp/v2/persqueue.h>
#include <kikimr/yndx/grpc_services/persqueue/persqueue.h>
#include <kikimr/yndx/persqueue/msgbus_server/read_session_info.h>


namespace NKikimr {
namespace NPersQueueTests {

Y_UNIT_TEST_SUITE(TPersQueueProtocolCompatibility) {
    Y_UNIT_TEST(GetReadInfoFromV1AboutV0Session) {
        NKikimr::Tests::TServerSettings serverSettings = PQSettings(0);
        serverSettings.RegisterGrpcService<NKikimr::NGRpcService::TGRpcPersQueueService>(
            "pq",
            NKikimr::NMsgBusProxy::CreatePersQueueMetaCacheV2Id()
        );
        serverSettings.SetPersQueueGetReadSessionsInfoWorkerFactory(
            std::make_shared<NKikimr::NMsgBusProxy::TPersQueueGetReadSessionsInfoWorkerWithPQv0Factory>()
        );

        NPersQueue::TTestServer server(serverSettings);
        server.EnableLogs({ NKikimrServices::PERSQUEUE, NKikimrServices::PQ_READ_PROXY });
        server.AnnoyingClient->CreateTopic("rt3.dc1--topic1", 1);

        NPersQueue::TPQLibSettings pqSettings;
        pqSettings.DefaultLogger = new NPersQueue::TCerrLogger(NPersQueue::DEBUG_LOG_LEVEL);
        THolder<NPersQueue::TPQLib> PQLib = MakeHolder<NPersQueue::TPQLib>(pqSettings);

        NPersQueue::TConsumerSettings settings;
        settings.Server = NPersQueue::TServerSetting{"localhost", server.GrpcPort};
        settings.ClientId = "user";
        settings.Topics = {"topic1"};
        settings.UseLockSession = true;
        auto consumer = PQLib->CreateConsumer(settings);
        auto response = consumer->Start().GetValueSync();
        UNIT_ASSERT_C(response.Response.HasInit(), response.Response);

        auto msg = consumer->GetNextMessage();
        auto value = msg.ExtractValueSync();
        Cerr << value.Response << "\n";
        UNIT_ASSERT(value.Response.HasLock());
        value.ReadyToRead.SetValue(NPersQueue::TLockInfo{});
        auto lock = value.Response.GetLock();
        Cout << lock.DebugString() << Endl;
        {
            std::shared_ptr<grpc::Channel> channel;
            std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> stub;

            {
                channel = grpc::CreateChannel(
                    "localhost:" + ToString(server.GrpcPort),
                    grpc::InsecureChannelCredentials()
                );
                stub = Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
            }
            {
                Sleep(TDuration::Seconds(10));
                Ydb::PersQueue::V1::ReadInfoRequest request;
                Ydb::PersQueue::V1::ReadInfoResponse response;
                request.mutable_consumer()->set_path("user");
                request.set_get_only_original(true);
                request.add_topics()->set_path("topic1");
                grpc::ClientContext rcontext;
                auto status = stub->GetReadSessionsInfo(&rcontext, request, &response);
                UNIT_ASSERT(status.ok());
                Ydb::PersQueue::V1::ReadInfoResult res;
                response.operation().result().UnpackTo(&res);
                Cerr << "Read info response: " << response << Endl << res << Endl;
                UNIT_ASSERT_VALUES_EQUAL(res.topics_size(), 1);
                UNIT_ASSERT(res.topics(0).status() == Ydb::StatusIds::SUCCESS);
            }
        }

    }

} // Y_UNIT_TEST_SUITE(TPersQueueProtocolCompatibility)

} // namespace NPersQueueTests
} // namespace NKikimr
