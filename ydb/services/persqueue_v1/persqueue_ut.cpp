#include "actors/read_session_actor.h"
#include <ydb/services/persqueue_v1/ut/pq_data_writer.h>
#include <ydb/services/persqueue_v1/ut/api_test_setup.h>
#include <ydb/services/persqueue_v1/ut/rate_limiter_test_setup.h>
#include <ydb/services/persqueue_v1/ut/test_utils.h>
#include <ydb/services/persqueue_v1/ut/persqueue_test_fixture.h>
#include <ydb/services/persqueue_v1/ut/functions_executor_wrapper.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/sync_http_mon.h>
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/obfuscate/obfuscate.h>
#include <ydb/library/persqueue/tests/counters.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/digest/md5/md5.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/monlib/dynamic_counters/encode.h>
#include <google/protobuf/text_format.h>
#include <google/protobuf/util/message_differencer.h>

#include <util/string/join.h>
#include <util/system/sanitizers.h>
#include <util/generic/guid.h>

#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/api/grpc/ydb_topic_v1.grpc.pb.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/include/client.h>
#include <thread>


namespace NKikimr::NPersQueueTests {

using namespace Tests;
using namespace NKikimrClient;
using namespace Ydb::PersQueue;
using namespace Ydb::PersQueue::V1;
using namespace NThreading;
using namespace NNetClassifier;

TAutoPtr<IEventHandle> GetClassifierUpdate(TServer& server, const TActorId sender) {
    auto& actorSystem = *server.GetRuntime();
    actorSystem.Send(
            new IEventHandle(MakeNetClassifierID(), sender,
            new TEvNetClassifier::TEvSubscribe()
        ));

    TAutoPtr<IEventHandle> handle;
    actorSystem.GrabEdgeEvent<NNetClassifier::TEvNetClassifier::TEvClassifierUpdate>(handle);

    UNIT_ASSERT(handle);
    UNIT_ASSERT_VALUES_EQUAL(handle->Recipient, sender);

    return handle;
}

THolder<TTempFileHandle> CreateNetDataFile(const TString& content) {
    auto netDataFile = MakeHolder<TTempFileHandle>();

    netDataFile->Write(content.Data(), content.Size());
    netDataFile->FlushData();

    return netDataFile;
}

static TString FormNetData() {
    return "10.99.99.224/32\tSAS\n"
           "::1/128\tVLA\n";
}

NYdb::NPersQueue::TTopicReadSettings MakeTopicReadSettings(const TString& topic,
                                                           const TVector<ui32>& groupIds)
{
    NYdb::NPersQueue::TTopicReadSettings settings{topic};
    for (ui32 groupId : groupIds) {
        settings.AppendPartitionGroupIds(groupId);
    }
    return settings;
}

NYdb::NPersQueue::TReadSessionSettings MakeReadSessionSettings(const NYdb::NPersQueue::TTopicReadSettings& topicSettings,
                                                               const TString& consumer,
                                                               bool readOnlyOriginal)
{
    NYdb::NPersQueue::TReadSessionSettings settings;
    settings.AppendTopics(topicSettings);
    settings.ConsumerName(consumer);
    settings.ReadOnlyOriginal(readOnlyOriginal);
    return settings;
}

namespace {
    const static TString DEFAULT_TOPIC_NAME = "rt3.dc1--topic1";
    const static TString SHORT_TOPIC_NAME = "topic1";
}

#define MAKE_INSECURE_STUB(Service)                                       \
    std::shared_ptr<grpc::Channel> Channel_;                              \
    std::unique_ptr<Service::Stub> StubP_;                                \
                                                                          \
    {                                                                     \
        grpc::ChannelArguments args;                                      \
        args.SetMaxReceiveMessageSize(64_MB);                             \
        args.SetMaxSendMessageSize(64_MB);                                \
        Channel_ = grpc::CreateCustomChannel(                             \
            "localhost:" + ToString(server.Server->GrpcPort),             \
            grpc::InsecureChannelCredentials(),                           \
            args                                                          \
        );                                                                \
        StubP_ = Service::NewStub(Channel_);                              \
    }                                                                     \
    grpc::ClientContext rcontext;

Y_UNIT_TEST_SUITE(TPersQueueTest) {
    Y_UNIT_TEST(AllEqual) {
        using NGRpcProxy::V1::AllEqual;

        UNIT_ASSERT(AllEqual(0));
        UNIT_ASSERT(AllEqual(0, 0));
        UNIT_ASSERT(AllEqual(0, 0, 0));
        UNIT_ASSERT(AllEqual(1, 1, 1));
        UNIT_ASSERT(AllEqual(1, 1, 1, 1, 1, 1));
        UNIT_ASSERT(!AllEqual(1, 0));
        UNIT_ASSERT(!AllEqual(0, 1));
        UNIT_ASSERT(!AllEqual(0, 1, 0));
        UNIT_ASSERT(!AllEqual(1, 1, 0));
        UNIT_ASSERT(!AllEqual(0, 1, 1));
        UNIT_ASSERT(!AllEqual(1, 1, 1, 1, 1, 0));
    }

    Y_UNIT_TEST(SetupLockSession2) {
        Cerr << "=== Start server\n";
        TPersQueueV1TestServer server;
        Cerr << "=== Started server\n";
        SET_LOCALS;
        server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);

        const TString topicPath = server.GetTopicPathMultipleDC();

        auto driver = server.Server->AnnoyingClient->GetDriver();

        NYdb::NPersQueue::TReadSessionSettings settings;
        settings.ConsumerName("shared/user").AppendTopics(topicPath).ReadMirrored("dc1");
        Cerr << "=== Create reader\n";
        auto reader = CreateReader(*driver, settings);

        Cerr << "===Start reader event loop\n";
        for (ui32 i = 0; i < 2; ++i) {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << "===Got message: " << NYdb::NPersQueue::DebugString(*msg) << "\n";

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);

            UNIT_ASSERT(ev);

            UNIT_ASSERT(ev->GetPartitionStream()->GetTopicPath() == topicPath);
            UNIT_ASSERT(ev->GetPartitionStream()->GetCluster() == "dc1" || ev->GetPartitionStream()->GetCluster() == "dc2");
            UNIT_ASSERT(ev->GetPartitionStream()->GetPartitionId() == 0);

        }
        auto wait = reader->WaitEvent();
        UNIT_ASSERT(!wait.Wait(TDuration::Seconds(1)));
        Cerr << "======Altering topic\n";
        pqClient->AlterTopicNoLegacy("/Root/PQ/rt3.dc2--acc--topic2dc", 2);
        Cerr << "======Alter topic done\n";
        UNIT_ASSERT(wait.Wait(TDuration::Seconds(5)));

        auto msg = reader->GetEvent(true, 1);
        UNIT_ASSERT(msg);

        Cerr << NYdb::NPersQueue::DebugString(*msg) << "\n";

        auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);

        UNIT_ASSERT(ev);

        UNIT_ASSERT(ev->GetPartitionStream()->GetTopicPath() == topicPath);
        UNIT_ASSERT(ev->GetPartitionStream()->GetCluster() == "dc2");
        UNIT_ASSERT(ev->GetPartitionStream()->GetPartitionId() == 1);
    }


    Y_UNIT_TEST(SetupLockSession) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        MAKE_INSECURE_STUB(Ydb::PersQueue::V1::PersQueueService);
        server.EnablePQLogs({ NKikimrServices::PQ_METACACHE, NKikimrServices::PQ_READ_PROXY });
        server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);
        server.EnablePQLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD }, NLog::EPriority::PRI_ERROR);

        auto readStream = StubP_->MigrationStreamingRead(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            MigrationStreamingReadClientMessage  req;
            MigrationStreamingReadServerMessage resp;

            req.mutable_init_request()->add_topics_read_settings()->set_topic("acc/topic1");

            req.mutable_init_request()->set_consumer("user");
            req.mutable_init_request()->set_read_only_original(true);
            req.mutable_init_request()->mutable_read_params()->set_max_read_messages_count(1);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.response_case() == MigrationStreamingReadServerMessage::kInitResponse);
            //send some reads
            req.Clear();
            req.mutable_read();
            for (ui32 i = 0; i < 10; ++i) {
                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
        }
        Cerr << "===First block done\n";
        {
            Sleep(TDuration::Seconds(10));
            ReadInfoRequest request;
            ReadInfoResponse response;
            request.mutable_consumer()->set_path("user");
            request.set_get_only_original(true);
            request.add_topics()->set_path("acc/topic1");
            grpc::ClientContext rcontext;
            auto status = StubP_->GetReadSessionsInfo(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            ReadInfoResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << "Read info response: " << response << Endl << res << Endl;
            UNIT_ASSERT_VALUES_EQUAL(res.topics_size(), 1);
            UNIT_ASSERT(res.topics(0).status() == Ydb::StatusIds::SUCCESS);
        }
        Cerr << "===Second block done\n";

        ui64 assignId = 0;
        {
            MigrationStreamingReadClientMessage  req;
            MigrationStreamingReadServerMessage resp;

            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.response_case() == MigrationStreamingReadServerMessage::kAssigned);
            UNIT_ASSERT(resp.assigned().topic().path() == "acc/topic1");
            UNIT_ASSERT(resp.assigned().cluster() == "dc1");
            UNIT_ASSERT(resp.assigned().partition() == 0);

            assignId = resp.assigned().assign_id();

            req.Clear();
            req.mutable_start_read()->mutable_topic()->set_path("acc/topic1");
            req.mutable_start_read()->set_cluster("dc1");
            req.mutable_start_read()->set_partition(0);
            req.mutable_start_read()->set_assign_id(354235); // invalid id should receive no reaction

            req.mutable_start_read()->set_read_offset(10);
            UNIT_ASSERT_C(readStream->Write(req), "write fail");

            Sleep(TDuration::MilliSeconds(100));

            req.Clear();
            req.mutable_start_read()->mutable_topic()->set_path("acc/topic1");
            req.mutable_start_read()->set_cluster("dc1");
            req.mutable_start_read()->set_partition(0);
            req.mutable_start_read()->set_assign_id(assignId);

            req.mutable_start_read()->set_read_offset(10);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

        }
        Cerr << "===Third block done\n";

        auto driver = server.Server->AnnoyingClient->GetDriver();

        {
            auto writer = CreateSimpleWriter(*driver, "acc/topic1", "source");
            for (int i = 1; i < 17; ++i) {
                bool res = writer->Write("valuevaluevalue" + ToString(i), i);
                UNIT_ASSERT(res);
            }
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }
        Cerr << "===4th block done\n";

        //check read results
        MigrationStreamingReadServerMessage resp;
        for (ui32 i = 10; i < 16; ++i) {
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "Got read response " << resp << "\n";
            UNIT_ASSERT_C(resp.response_case() == MigrationStreamingReadServerMessage::kDataBatch, resp);
            UNIT_ASSERT(resp.data_batch().partition_data_size() == 1);
            UNIT_ASSERT(resp.data_batch().partition_data(0).batches_size() == 1);
            UNIT_ASSERT(resp.data_batch().partition_data(0).batches(0).message_data_size() == 1);
            UNIT_ASSERT(resp.data_batch().partition_data(0).batches(0).message_data(0).offset() == i);
        }
        //TODO: restart here readSession and read from position 10
        {
            MigrationStreamingReadClientMessage  req;
            MigrationStreamingReadServerMessage resp;

            auto cookie = req.mutable_commit()->add_cookies();
            cookie->set_assign_id(assignId);
            cookie->set_partition_cookie(1);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT_C(resp.response_case() == MigrationStreamingReadServerMessage::kCommitted, resp);
        }


        Cerr << "=== ===AlterTopic\n";
        pqClient->AlterTopic("rt3.dc1--acc--topic1", 10);
        {
            ReadInfoRequest request;
            ReadInfoResponse response;
            request.mutable_consumer()->set_path("user");
            request.set_get_only_original(false);
            request.add_topics()->set_path("acc/topic1");
            grpc::ClientContext rcontext;
            auto status = StubP_->GetReadSessionsInfo(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            ReadInfoResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << "Get read session info response: " << response << "\n" << res << "\n";
//            UNIT_ASSERT(res.sessions_size() == 1);
            UNIT_ASSERT_VALUES_EQUAL(res.topics_size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(res.topics(0).partitions_size(), 10);
        }
        Cerr << "=== ===AlterTopic block done\n";
        {
            ReadInfoRequest request;
            ReadInfoResponse response;
            request.mutable_consumer()->set_path("user");
            request.set_get_only_original(false);
            request.add_topics()->set_path("acc/topic1");
            grpc::ClientContext rcontext;

            pqClient->MarkNodeInHive(runtime, 0, false);
            pqClient->MarkNodeInHive(runtime, 1, false);

            pqClient->RestartBalancerTablet(runtime, "rt3.dc1--acc--topic1");
            auto status = StubP_->GetReadSessionsInfo(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            ReadInfoResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << "Read sessions info response: " << response << "\nResult: " << res << "\n";
            UNIT_ASSERT(res.topics().size() == 1);
            UNIT_ASSERT(res.topics(0).partitions(0).status() == Ydb::StatusIds::UNAVAILABLE);
        }
    }


    Y_UNIT_TEST(StreamReadCreateAndDestroyMsgs) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        MAKE_INSECURE_STUB(Ydb::Topic::V1::TopicService);
        server.EnablePQLogs({ NKikimrServices::PQ_METACACHE, NKikimrServices::PQ_READ_PROXY });
        server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);
        server.EnablePQLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD }, NLog::EPriority::PRI_ERROR);

        auto readStream = StubP_->StreamRead(&rcontext);
        UNIT_ASSERT(readStream);

        // add 2nd partition in this topic
        pqClient->AlterTopic("rt3.dc1--acc--topic1", 2);

        // init 1st read session
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path("acc/topic1");

            req.mutable_init_request()->set_consumer("user");

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
            //send some reads
            req.Clear();
            req.mutable_read_request()->set_bytes_size(256);
            for (ui32 i = 0; i < 10; ++i) {
                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
        }

        // await both CreatePartitionStreamRequest from Server
        // confirm both
        ui64 assignId = 0;
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            TVector<i64> partition_ids;
            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().path() == "acc/topic1");
            partition_ids.push_back(resp.start_partition_session_request().partition_session().partition_id());

            assignId = resp.start_partition_session_request().partition_session().partition_session_id();
            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignId);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            resp.Clear();
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().path() == "acc/topic1");
            partition_ids.push_back(resp.start_partition_session_request().partition_session().partition_id());

            std::sort(partition_ids.begin(), partition_ids.end());
            UNIT_ASSERT((partition_ids == TVector<i64>{0, 1}));

            assignId = resp.start_partition_session_request().partition_session().partition_session_id();

            req.Clear();

            // invalid id should receive no reaction
            req.mutable_start_partition_session_response()->set_partition_session_id(1124134);

            UNIT_ASSERT_C(readStream->Write(req), "write fail");

            Sleep(TDuration::MilliSeconds(100));

            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignId);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }

        Cerr << "=== Create second stream" << Endl;
        grpc::ClientContext rcontextSecond;
        auto readStreamSecond = StubP_->StreamRead(&rcontextSecond);
        UNIT_ASSERT(readStreamSecond);
        Cerr << "=== Second stream created" << Endl;

        // init 2nd read session
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path("acc/topic1");

            req.mutable_init_request()->set_consumer("user");

            if (!readStreamSecond->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStreamSecond->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
        }

        // await DestroyPartitionStreamRequest
        // confirm it
        // await CreatePartitionStream
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "=== Got response (expect destroy): " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStopPartitionSessionRequest);
            UNIT_ASSERT(resp.stop_partition_session_request().graceful());
            auto stream_id = resp.stop_partition_session_request().partition_session_id();

            req.Clear();
            req.mutable_stop_partition_session_response()->set_partition_session_id(stream_id);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            resp.Clear();
            UNIT_ASSERT(readStreamSecond->Read(&resp));
            Cerr << "=== Got response (expect create): " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().path() == "acc/topic1");

            assignId = resp.start_partition_session_request().partition_session().partition_session_id();
            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignId);

            if (!readStreamSecond->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }


        // kill balancer and await forceful parition stream destroy signal
        pqClient->RestartBalancerTablet(runtime, "rt3.dc1--acc--topic1");
        Cerr << "Balancer killed\n";
        {
            Ydb::Topic::StreamReadMessage::FromServer resp;

            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "=== Got response (expect forceful destroy): " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStopPartitionSessionRequest);
            UNIT_ASSERT(!resp.stop_partition_session_request().graceful());

            resp.Clear();
            UNIT_ASSERT(readStreamSecond->Read(&resp));
            Cerr << "=== Got response (expect forceful destroy): " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStopPartitionSessionRequest);
            UNIT_ASSERT(!resp.stop_partition_session_request().graceful());
        }
    }


    Y_UNIT_TEST(StreamReadCommitAndStatusMsgs) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        MAKE_INSECURE_STUB(Ydb::Topic::V1::TopicService);
        server.EnablePQLogs({ NKikimrServices::PQ_METACACHE, NKikimrServices::PQ_READ_PROXY });
        server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);
        server.EnablePQLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD }, NLog::EPriority::PRI_ERROR);

        auto readStream = StubP_->StreamRead(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path("acc/topic1");

            req.mutable_init_request()->set_consumer("user");

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
            //send some reads
            req.Clear();
            req.mutable_read_request()->set_bytes_size(256);
            for (ui32 i = 0; i < 10; ++i) {
                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
        }

        // await and confirm CreatePartitionStreamRequest from server
        i64 assignId = 0;
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT_VALUES_EQUAL(resp.start_partition_session_request().partition_session().path(), "acc/topic1");
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().partition_id() == 0);

            assignId = resp.start_partition_session_request().partition_session().partition_session_id();
            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignId);

            req.mutable_start_partition_session_response()->set_read_offset(10);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }

        // write to partition in 1 session
        auto driver = pqClient->GetDriver();
        {
            auto writer = CreateSimpleWriter(*driver, "acc/topic1", "source");
            for (int i = 1; i < 17; ++i) {
                bool res = writer->Write("valuevaluevalue" + ToString(i), i);
                UNIT_ASSERT(res);
            }
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        //check read results
        Ydb::Topic::StreamReadMessage::FromServer resp;
        for (ui32 i = 10; i < 16; ) {
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "Got read response " << resp << "\n";
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);
            UNIT_ASSERT_VALUES_EQUAL(resp.read_response().partition_data_size(), 1);
            UNIT_ASSERT_GE(resp.read_response().partition_data(0).batches_size(), 1);
            for (const auto& batch : resp.read_response().partition_data(0).batches()) {
                UNIT_ASSERT_GE(batch.message_data_size(), 1);
                UNIT_ASSERT_VALUES_EQUAL(batch.message_data(0).offset(), i);
                i += batch.message_data_size();
            }
        }

        // send commit, await commitDone
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            auto commit = req.mutable_commit_offset_request()->add_commit_offsets();
            commit->set_partition_session_id(assignId);

            auto offsets = commit->add_offsets();
            offsets->set_start(0);
            offsets->set_end(13);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kCommitOffsetResponse, resp);
            UNIT_ASSERT(resp.commit_offset_response().partitions_committed_offsets_size() == 1);
            UNIT_ASSERT(resp.commit_offset_response().partitions_committed_offsets(0).partition_session_id() == assignId);
            UNIT_ASSERT(resp.commit_offset_response().partitions_committed_offsets(0).committed_offset() == 13);
        }

        // send status request, await status
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_partition_session_status_request()->set_partition_session_id(assignId);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kPartitionSessionStatusResponse, resp);
            UNIT_ASSERT(resp.partition_session_status_response().partition_session_id() == assignId);
            UNIT_ASSERT(resp.partition_session_status_response().committed_offset() == 13);
            UNIT_ASSERT(resp.partition_session_status_response().partition_offsets().end() == 16);
            UNIT_ASSERT(resp.partition_session_status_response().write_time_high_watermark().seconds() > 0);
        }

        // send update token request, await response
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            const TString token = TString("test_user_0@") + BUILTIN_ACL_DOMAIN;;

            req.mutable_update_token_request()->set_token(token);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect UpdateTokenResponse, got response: " << resp.ShortDebugString() << Endl;

            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kUpdateTokenResponse, resp);
        }

        // write and read some more
        {
            auto writer = CreateSimpleWriter(*driver, "acc/topic1", "source");
            for (int i = 17; i < 37; ++i) {
                bool res = writer->Write("valuevaluevalue" + ToString(i), i);
                UNIT_ASSERT(res);
            }
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);

            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_read_request()->set_bytes_size(1_MB);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }
        // expect answer to read
        resp.Clear();
        UNIT_ASSERT(readStream->Read(&resp));
        Cerr << "Got response " << resp << "\n";
        UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);
    }


    Y_UNIT_TEST(UpdatePartitionLocation) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        MAKE_INSECURE_STUB(Ydb::Topic::V1::TopicService);
        server.EnablePQLogs({ NKikimrServices::PQ_METACACHE, NKikimrServices::PQ_READ_PROXY, NKikimrServices::PERSQUEUE});
        server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);
        server.EnablePQLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD }, NLog::EPriority::PRI_ERROR);

        auto readStream = StubP_->StreamRead(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path("acc/topic1");

            req.mutable_init_request()->set_consumer("user");
            req.mutable_init_request()->set_direct_read(true);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
        }

        // await and confirm CreatePartitionStreamRequest from server
        i64 assignId;
        i64 generation;
        {
            Ydb::Topic::StreamReadMessage::FromServer resp;

            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));

            Cerr << "GOT SERVER MESSAGE1: " << resp.DebugString() << "\n";

            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT_VALUES_EQUAL(resp.start_partition_session_request().partition_session().path(), "acc/topic1");
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().partition_id() == 0);
            UNIT_ASSERT(resp.start_partition_session_request().partition_location().generation() > 0);
            assignId = resp.start_partition_session_request().partition_session().partition_session_id();
            generation = resp.start_partition_session_request().partition_location().generation();
        }

        server.Server->AnnoyingClient->RestartPartitionTablets(server.Server->CleverServer->GetRuntime(), "rt3.dc1--acc--topic1");

        {
            Ydb::Topic::StreamReadMessage::FromServer resp;

            //update partition location
            UNIT_ASSERT(readStream->Read(&resp));

            Cerr << "GOT SERVER MESSAGE2: " << resp.DebugString() << "\n";

            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kUpdatePartitionSession);
            UNIT_ASSERT(resp.update_partition_session().partition_session_id() == assignId);
            UNIT_ASSERT(resp.update_partition_session().partition_location().generation() > generation);
        }
    }

    using namespace Ydb;
    class TDirectReadTestSetup {
        using Service = Ydb::Topic::V1::TopicService;
    private:
        std::shared_ptr<grpc::Channel> Channel;
        std::unique_ptr<Service::Stub> Stub;
        THolder<grpc::ClientContext> ControlContext;
        THolder<grpc::ClientContext> DirectContext;

    public:
        std::unique_ptr<grpc::ClientReaderWriter<Topic::StreamReadMessage_FromClient, Topic::StreamReadMessage_FromServer>> ControlStream;
        std::unique_ptr<grpc::ClientReaderWriter<Topic::StreamDirectReadMessage_FromClient, Topic::StreamDirectReadMessage_FromServer>> DirectStream;
        TString SessionId;

        TDirectReadTestSetup(TPersQueueV1TestServer& server)
            : DirectContext(MakeHolder<grpc::ClientContext>())
        {
            server.EnablePQLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PERSQUEUE });
            server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);
            server.EnablePQLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD }, NLog::EPriority::PRI_ERROR);

            Connect(server);
        }

        void Connect(TPersQueueV1TestServer& server) {
            grpc::ChannelArguments args;
            args.SetMaxReceiveMessageSize(64_MB);
            args.SetMaxSendMessageSize(64_MB);
            Channel = grpc::CreateCustomChannel(
                "localhost:" + ToString(server.Server->GrpcPort),
                grpc::InsecureChannelCredentials(),
                args
            );
            Stub = Service::NewStub(Channel);
        }

        void InitControlSession(const TString& topic) {
            // Send InitRequest, get InitResponse, send ReadRequest.

            ControlContext = MakeHolder<grpc::ClientContext>();
            ControlStream = Stub->StreamRead(ControlContext.Get());
            UNIT_ASSERT(ControlStream);

            Topic::StreamReadMessage::FromClient req;
            req.mutable_init_request()->add_topics_read_settings()->set_path(topic);
            req.mutable_init_request()->set_consumer("user");
            req.mutable_init_request()->set_direct_read(true);
            if (!ControlStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            Topic::StreamReadMessage::FromServer resp;
            UNIT_ASSERT(ControlStream->Read(&resp));
            Cerr << "Got init response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
            SessionId = resp.init_response().session_id();

            req.Clear();
            req.mutable_read_request()->set_bytes_size(40_MB);
            if (!ControlStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }

        std::pair<ui32, i64> GetNextAssign(const TString& topic) {
            // Get StartPartitionSessionRequest, send StartPartitionSessionResponse.

            Cerr << "Get next assign id\n";
            Topic::StreamReadMessage::FromServer resp;

            //lock partition
            UNIT_ASSERT(ControlStream->Read(&resp));

            Cerr << "GOT SERVER MESSAGE - start session: " << resp.DebugString() << "\n";

            UNIT_ASSERT(resp.server_message_case() == Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT_VALUES_EQUAL(resp.start_partition_session_request().partition_session().path(), topic);
            auto partitionId = resp.start_partition_session_request().partition_session().partition_id();
            UNIT_ASSERT(resp.start_partition_session_request().partition_location().generation() > 0);
            auto assignId = resp.start_partition_session_request().partition_session().partition_session_id();

            Topic::StreamReadMessage::FromClient req;
            req.mutable_start_partition_session_response()->set_partition_session_id(assignId);
            if (!ControlStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            return {partitionId, assignId};
        }

        void DoWrite(NYdb::TDriver* driver, const TString& topic, ui64 size, ui32 count,
                     const TString& srcId = "srcID", const std::optional<ui64>& partGroup = {})
        {
            auto writer = CreateSimpleWriter(*driver, topic, srcId, partGroup, {"raw"});

            for (ui32 i = 0; i < count; ++i) {
                TString data(size, 'x');
                UNIT_ASSERT(writer->Write(data));
            }
            writer->Close();
        }

        void DoRead(ui64 assignId, ui64& nextReadId, ui32& currTotalMessages, ui32 messageLimit) {
            // Get DirectReadResponse messages, send DirectReadAck messages.

            while (currTotalMessages < messageLimit) {
                Cerr << "Wait for direct read id: " << nextReadId << ", currently have " << currTotalMessages << " messages" << Endl;

                Ydb::Topic::StreamDirectReadMessage::FromServer resp;
                UNIT_ASSERT(DirectStream->Read(&resp));
                Cerr << "Got direct read response: " << resp.direct_read_response().direct_read_id() << Endl;
                UNIT_ASSERT_C(resp.status() == Ydb::StatusIds::SUCCESS, resp.DebugString());
                UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamDirectReadMessage::FromServer::kDirectReadResponse);
                UNIT_ASSERT_VALUES_EQUAL(resp.direct_read_response().direct_read_id(), nextReadId);

                Ydb::Topic::StreamReadMessage::FromClient req;
                req.mutable_direct_read_ack()->set_partition_session_id(assignId);
                req.mutable_direct_read_ack()->set_direct_read_id(nextReadId++);
                if (!ControlStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
                for (const auto& batch : resp.direct_read_response().partition_data().batches()) {
                    currTotalMessages += batch.message_data_size();
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(currTotalMessages, messageLimit);
        }

        void InitDirectSession(
            const TString& topic,
            const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS,
            const TString& consumer = "user"
        ) {
            // Send InitRequest, get InitResponse.
            if (DirectStream) {
                DirectStream->Finish();
                DirectStream = nullptr;
                DirectContext = MakeHolder<grpc::ClientContext>();
            }
            DirectStream = Stub->StreamDirectRead(DirectContext.Release());
            UNIT_ASSERT(DirectStream);

            Topic::StreamDirectReadMessage::FromClient req;
            req.mutable_init_request()->add_topics_read_settings()->set_path(topic);
            req.mutable_init_request()->set_consumer(consumer);
            req.mutable_init_request()->set_session_id(SessionId);
            if (!DirectStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            Topic::StreamDirectReadMessage::FromServer resp;
            UNIT_ASSERT(DirectStream->Read(&resp));
            Cerr << "Got direct read init response: " << resp.ShortDebugString() << Endl;

            UNIT_ASSERT_EQUAL(resp.status(), status);

            if (status != Ydb::StatusIds::SUCCESS) {
                return;
            }

            UNIT_ASSERT_EQUAL(resp.server_message_case(), Ydb::Topic::StreamDirectReadMessage::FromServer::kInitResponse);
        }

        void SendReadSessionAssign(const ui64 assignId, const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
            // Send StartDirectReadPartitionSessionRequest, get StartDirectReadPartitionSessionResponse.

            Cerr << "Send next assign to data session" << assignId << Endl;

            Topic::StreamDirectReadMessage::FromClient req;
            auto x = req.mutable_start_direct_read_partition_session_request();
            x->set_partition_session_id(assignId);
            x->set_last_direct_read_id(0);
            x->set_generation(1);
            if (!DirectStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            Topic::StreamDirectReadMessage::FromServer resp;
            UNIT_ASSERT(DirectStream->Read(&resp));
            Cerr << "Got response: " << resp.ShortDebugString() << Endl;

            UNIT_ASSERT_EQUAL(resp.status(), status);

            if (status != Ydb::StatusIds::SUCCESS) {
                return;
            }

            UNIT_ASSERT_EQUAL(resp.server_message_case(), Ydb::Topic::StreamDirectReadMessage::FromServer::kStartDirectReadPartitionSessionResponse);
            UNIT_ASSERT_EQUAL(resp.start_direct_read_partition_session_response().partition_session_id(), static_cast<i64>(assignId));
            UNIT_ASSERT_EQUAL(resp.start_direct_read_partition_session_response().generation(), 1);
        }
    };

    THolder<TEvPQ::TEvGetFullDirectReadData> RequestCacheData(TTestActorRuntime* runtime, TEvPQ::TEvGetFullDirectReadData* request) {
        const auto& edgeId = runtime->AllocateEdgeActor();
        runtime->Send(NPQ::MakePQDReadCacheServiceActorId(), edgeId, request);
        auto resp = runtime->GrabEdgeEvent<TEvPQ::TEvGetFullDirectReadData>();
        UNIT_ASSERT(resp);
        return resp;
    }

    Y_UNIT_TEST(DirectReadPreCached) {
TPersQueueV1TestServer server{{.CheckACL=true, .NodeCount=1}};
        SET_LOCALS;
        TDirectReadTestSetup setup{server};
        setup.DoWrite(pqClient->GetDriver(), "acc/topic1", 1_MB, 30);

        setup.InitControlSession("acc/topic1");
        auto [partitionId, assignId] = setup.GetNextAssign("acc/topic1");
        UNIT_ASSERT_VALUES_EQUAL(partitionId, 0);
        setup.InitDirectSession("acc/topic1");

        // Without retrying the test is flaky as the cachedData might be empty.
        THolder<TEvPQ::TEvGetFullDirectReadData> cachedData;
        while (true) {
            cachedData = RequestCacheData(runtime, new TEvPQ::TEvGetFullDirectReadData());
            if (!cachedData->Data.empty()) {
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.size(), 1);
        setup.SendReadSessionAssign(assignId);

        ui32 totalMsg = 0;
        ui64 nextReadId = 1;
        setup.DoRead(assignId, nextReadId, totalMsg, 30);

        Sleep(TDuration::Seconds(1));
        cachedData = RequestCacheData(runtime, new TEvPQ::TEvGetFullDirectReadData());
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.begin()->second.StagedReads.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.begin()->second.Reads.size(), 0);
    }

    Y_UNIT_TEST(DirectReadNotCached) {
        TPersQueueV1TestServer server{{.CheckACL=true, .NodeCount=1}};
        SET_LOCALS;
        TDirectReadTestSetup setup{server};

        setup.InitControlSession("acc/topic1");
        auto [partitionId, assignId] = setup.GetNextAssign("acc/topic1");
        UNIT_ASSERT_VALUES_EQUAL(partitionId, 0);
        setup.InitDirectSession("acc/topic1");
        setup.SendReadSessionAssign(assignId);

        ui32 totalMsg = 0;
        ui64 nextReadId = 1;
        Sleep(TDuration::Seconds(3));
        setup.DoWrite(pqClient->GetDriver(), "acc/topic1", 1_MB, 50);
        setup.DoRead(assignId, nextReadId, totalMsg, 40);

        Topic::StreamReadMessage::FromClient req;
        req.mutable_read_request()->set_bytes_size(40_MB);
        if (!setup.ControlStream->Write(req)) {
            ythrow yexception() << "write fail";
        }
        setup.DoRead(assignId, nextReadId, totalMsg, 50);

        Sleep(TDuration::Seconds(1));
        auto cachedData = RequestCacheData(runtime, new TEvPQ::TEvGetFullDirectReadData());
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.begin()->second.StagedReads.size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.begin()->second.Reads.size(), 0);
    }

    Y_UNIT_TEST(DirectReadBadCases) {
        TPersQueueV1TestServer server{{.CheckACL=true, .NodeCount=1}};
        SET_LOCALS;
        TDirectReadTestSetup setup{server};
        setup.InitControlSession("acc/topic1");
        auto sessionId = setup.SessionId;
        auto assignId = setup.GetNextAssign("acc/topic1").second;
        setup.SessionId = "bad-session";
        Cerr << "First init bad session\n";
        setup.InitDirectSession("acc/topic1");
        // No control session:
        setup.SendReadSessionAssign(assignId, Ydb::StatusIds::BAD_REQUEST);
        setup.SessionId = sessionId;
        Cerr << "Init bad topic session\n";
        setup.InitDirectSession("acc/topic-bad", Ydb::StatusIds::SCHEME_ERROR);
        //setup.InitReadSession("acc/topic1", Ydb::StatusIds::SCHEME_ERROR, "bad-user"); //ToDo - enable ACL (read rules) check

        setup.ControlStream->WritesDone();
        Cerr << "Close control session\n";
        setup.ControlStream->Finish();
        Cerr << "Close control session - done\n";
        setup.ControlStream = nullptr;

        setup.DoWrite(pqClient->GetDriver(), "acc/topic1", 100_KB, 10);
        Cerr << "Init read session\n";
        setup.InitDirectSession("acc/topic1");
        // No control session:
        setup.SendReadSessionAssign(assignId, Ydb::StatusIds::BAD_REQUEST);

        auto cachedData = RequestCacheData(runtime, new TEvPQ::TEvGetFullDirectReadData());
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.size(), 0);
    }

    Y_UNIT_TEST(DirectReadStop) {
        TPersQueueV1TestServer server{{.CheckACL=true, .NodeCount=1}};
        SET_LOCALS;

        server.Server->AnnoyingClient->AlterTopicNoLegacy("Root/PQ/rt3.dc1--acc--topic1", 2);

        TDirectReadTestSetup setup{server};
        setup.DoWrite(pqClient->GetDriver(), "acc/topic1", 100_KB, 1, "src1", 0);
        setup.DoWrite(pqClient->GetDriver(), "acc/topic1", 100_KB, 1, "src2", 1);

        setup.InitControlSession("acc/topic1");
        auto [partitionId1, assignId1] = setup.GetNextAssign("acc/topic1");
        auto [partitionId2, assignId2] = setup.GetNextAssign("acc/topic1");
        UNIT_ASSERT(partitionId1 + partitionId2 == 1); // partitions 0 and 1;
        UNIT_ASSERT(assignId1 != assignId2);

        auto readData = [&](i64 assignId) {
            Cerr << "Wait for direct read" << Endl;
            Ydb::Topic::StreamDirectReadMessage::FromServer resp;
            UNIT_ASSERT(setup.DirectStream->Read(&resp));
            Cerr << "Got direct read response: " << resp.direct_read_response().direct_read_id() << Endl;
            UNIT_ASSERT_C(resp.status() == Ydb::StatusIds::SUCCESS, resp.DebugString());
            UNIT_ASSERT_EQUAL(resp.server_message_case(), Ydb::Topic::StreamDirectReadMessage::FromServer::kDirectReadResponse);
            UNIT_ASSERT_EQUAL(resp.direct_read_response().direct_read_id(), 1);
            i64 id = resp.direct_read_response().partition_session_id();
            UNIT_ASSERT_EQUAL(id, assignId);

            Ydb::Topic::StreamReadMessage::FromClient req;
            req.mutable_direct_read_ack()->set_partition_session_id(id);
            req.mutable_direct_read_ack()->set_direct_read_id(1);
            if (!setup.ControlStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        };

        setup.InitDirectSession("acc/topic1");
        setup.SendReadSessionAssign(assignId1);
        readData(assignId1);
        setup.SendReadSessionAssign(assignId2);
        readData(assignId2);

        // Read from both parts so that LastReadId goes forward;

        NYdb::NTopic::TTopicClient topicClient(*pqClient->GetDriver());
        NYdb::NTopic::TReadSessionSettings rSettings;
        rSettings.ConsumerName("user").AppendTopics({"acc/topic1"});
        auto readSession = topicClient.CreateReadSession(rSettings);

        auto assignId = 0;
        {
            Topic::StreamReadMessage::FromServer resp;

            //lock partition
            UNIT_ASSERT(setup.ControlStream->Read(&resp));

            Cerr << "GOT SERVER MESSAGE (stop session): " << resp.DebugString() << "\n";

            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStopPartitionSessionRequest);
            UNIT_ASSERT_VALUES_EQUAL(resp.stop_partition_session_request().graceful(), true);
            UNIT_ASSERT_VALUES_EQUAL(resp.stop_partition_session_request().last_direct_read_id(), 1);

            assignId = resp.stop_partition_session_request().partition_session_id();
            UNIT_ASSERT(assignId == assignId1 || assignId == assignId2);

            Topic::StreamReadMessage::FromClient req;
            req.mutable_stop_partition_session_response()->set_partition_session_id(assignId);
            req.mutable_stop_partition_session_response()->set_graceful(true);
            if (!setup.ControlStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }

        {
            Ydb::Topic::StreamReadMessage::FromServer resp;

            //lock partition
            UNIT_ASSERT(setup.ControlStream->Read(&resp));

            Cerr << "GOT SERVER MESSAGE (stop session 2): " << resp.DebugString() << "\n";

            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStopPartitionSessionRequest);
            UNIT_ASSERT_VALUES_EQUAL(resp.stop_partition_session_request().graceful(), false);
            UNIT_ASSERT_VALUES_EQUAL(resp.stop_partition_session_request().partition_session_id(), assignId);
            Ydb::Topic::StreamReadMessage::FromClient req;
            req.mutable_stop_partition_session_response()->set_partition_session_id(assignId);
            req.mutable_stop_partition_session_response()->set_graceful(false);
            if (!setup.ControlStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }
    }

    Y_UNIT_TEST(DirectReadCleanCache) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        TString topicPath{"/Root/PQ/rt3.dc1--acc--topic2"};
        server.Server->AnnoyingClient->CreateTopicNoLegacy(topicPath, 1);
        auto pathDescr = server.Server->AnnoyingClient->Ls(topicPath)->Record.GetPathDescription().GetPersQueueGroup();
        auto tabletId = pathDescr.GetPartitions(0).GetTabletId();
        Cerr << "PQ descr: " << pathDescr.DebugString() << Endl;


        TDirectReadTestSetup setup{server};

        setup.InitControlSession("acc/topic2");
        setup.InitDirectSession("acc/topic2");
        auto pair = setup.GetNextAssign("acc/topic2");
        UNIT_ASSERT_VALUES_EQUAL(pair.first, 0);
        auto assignId = pair.second;
        setup.SendReadSessionAssign(assignId);
        // auto cachedData = RequestCacheData(runtime, new TEvPQ::TEvGetFullDirectReadData());
        // UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.size(), 1);
        setup.DoWrite(pqClient->GetDriver(), "acc/topic2", 10_MB, 1);
        Ydb::Topic::StreamDirectReadMessage::FromServer resp;
        Cerr << "Request initial read data\n";
        UNIT_ASSERT(setup.DirectStream->Read(&resp));

        Cerr << "Request cache data\n";
        auto cachedData = RequestCacheData(runtime, new TEvPQ::TEvGetFullDirectReadData());
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.size(), 1);
        Cerr << "Kill the tablet\n";
        server.Server->AnnoyingClient->KillTablet(*(server.Server->CleverServer), tabletId);
        Cerr << "Get session closure\n";
        resp.Clear();
        UNIT_ASSERT(setup.DirectStream->Read(&resp));
        UNIT_ASSERT_C(resp.status() == Ydb::StatusIds::SESSION_EXPIRED, resp.status());
        Cerr << "Check caching service data empty\n";
        cachedData = RequestCacheData(runtime, new TEvPQ::TEvGetFullDirectReadData());
        UNIT_ASSERT_VALUES_EQUAL(cachedData->Data.size(), 0);
    }

    Y_UNIT_TEST(StreamReadManyUpdateTokenAndRead) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        MAKE_INSECURE_STUB(Ydb::Topic::V1::TopicService);
        server.EnablePQLogs({ NKikimrServices::PQ_METACACHE, NKikimrServices::PQ_READ_PROXY });
        server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);
        server.EnablePQLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD }, NLog::EPriority::PRI_ERROR);

        rcontext.AddMetadata("x-ydb-auth-ticket", "user@" BUILTIN_ACL_DOMAIN);

        TVector<std::pair<TString, TVector<TString>>> permissions;
        permissions.push_back({"user@" BUILTIN_ACL_DOMAIN, {"ydb.generic.read"}});
        for (ui32 i = 0; i < 10; ++i) {
            permissions.push_back({"test_user_" + ToString(i) + "@" + BUILTIN_ACL_DOMAIN, {"ydb.generic.read"}});
        }
        server.ModifyTopicACL("/Root/PQ/rt3.dc1--acc--topic1", permissions);

        TVector<std::pair<TString, TVector<TString>>> consumerPermissions;
        consumerPermissions.push_back({"user@" BUILTIN_ACL_DOMAIN, {"ydb.generic.read", "ydb.granular.write_attributes"}});
        for (ui32 i = 0; i < 10; ++i) {
            consumerPermissions.push_back({"test_user_" + ToString(i) + "@" + BUILTIN_ACL_DOMAIN, {"ydb.generic.read", "ydb.granular.write_attributes"}});
        }
        server.ModifyTopicACL("/Root/PQ/shared/user", consumerPermissions);

        auto readStream = StubP_->StreamRead(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path("acc/topic1");

            req.mutable_init_request()->set_consumer("user");

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
            // send some reads
            req.Clear();
            req.mutable_read_request()->set_bytes_size(200_MB);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }

        // await and confirm CreatePartitionStreamRequest from server
        i64 assignId = 0;
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT_VALUES_EQUAL(resp.start_partition_session_request().partition_session().path(), "acc/topic1");
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().partition_id() == 0);

            assignId = resp.start_partition_session_request().partition_session().partition_session_id();
            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignId);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }

        // write to partition in 1 session
        auto driver = pqClient->GetDriver();
        auto writer = CreateSimpleWriter(*driver, "acc/topic1", "source", /*partitionGroup=*/{}, /*codec=*/{"raw"});

        //check read results
        Ydb::Topic::StreamReadMessage::FromClient req;
        Ydb::Topic::StreamReadMessage::FromServer resp;

        int seqNo = 1;
        for (ui32 i = 0; i < 10; ++i) {
            bool result = writer->Write(TString(2_KB, 'x'), seqNo++);
            UNIT_ASSERT(result);

            resp.Clear();
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect ReadResponse, got " << resp << "\n";
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);

            // send update token request, await response
            const TString token = "test_user_" + ToString(i) + "@" + BUILTIN_ACL_DOMAIN;
            req.Clear();
            resp.Clear();
            req.mutable_update_token_request()->set_token(token);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect UpdateTokenResponse, got response: " << resp.ShortDebugString() << Endl;

            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kUpdateTokenResponse, resp);
        }

        {
            bool result = writer->Write(TString(2_KB, 'x'), seqNo++);
            UNIT_ASSERT(result);

            resp.Clear();
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect ReadResponse, got " << resp << "\n";
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);

            const TString token = "user_without_rights@" BUILTIN_ACL_DOMAIN;
            req.Clear();
            resp.Clear();
            req.mutable_update_token_request()->set_token(token);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect UpdateTokenResponse, got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kUpdateTokenResponse, resp);

            result = writer->Write(TString(2_KB, 'x'), seqNo++);
            UNIT_ASSERT(result);

            // why successful?
            resp.Clear();
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect ReadResponse, got " << resp.ShortDebugString() << "\n";
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);

            // await checking auth because of timeout
            resp.Clear();
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect UNAUTHORIZED, got response: " << resp.ShortDebugString() << Endl;

            UNIT_ASSERT_C(resp.status() == Ydb::StatusIds::UNAUTHORIZED, resp);
        }

        bool res = writer->Close(TDuration::Seconds(10));
        UNIT_ASSERT(res);

    }

    Y_UNIT_TEST(TopicServiceCommitOffset) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        MAKE_INSECURE_STUB(Ydb::Topic::V1::TopicService);
        server.EnablePQLogs({ NKikimrServices::PQ_METACACHE, NKikimrServices::PQ_READ_PROXY });
        server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);
        server.EnablePQLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD }, NLog::EPriority::PRI_ERROR);
        server.EnablePQLogs({ NKikimrServices::PERSQUEUE }, NLog::EPriority::PRI_DEBUG);

        auto driver = pqClient->GetDriver();
        {
            auto writer = CreateSimpleWriter(*driver, "acc/topic1", "source");
            for (int i = 1; i < 17; ++i) {
                bool res = writer->Write("valuevaluevalue" + ToString(i), i);
                UNIT_ASSERT(res);
            }
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        auto TopicStubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);

        grpc::ClientContext readContext;
        auto readStream = TopicStubP_ -> StreamRead(&readContext);
        UNIT_ASSERT(readStream);

        i64 assignId = 0;
        // init read session
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path("acc/topic1");

            req.mutable_init_request()->set_consumer("user");

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);

            req.Clear();
            // await and confirm StartPartitionSessionRequest from server
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT_VALUES_EQUAL(resp.start_partition_session_request().partition_session().path(), "acc/topic1");
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().partition_id() == 0);
            UNIT_ASSERT(resp.start_partition_session_request().committed_offset() == 0);

            assignId = resp.start_partition_session_request().partition_session().partition_session_id();
            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignId);

            req.mutable_start_partition_session_response()->set_read_offset(0);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            //send some reads
            req.Clear();
            req.mutable_read_request()->set_bytes_size(1);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            resp.Clear();
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "Got read response " << resp << "\n";
            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);
            UNIT_ASSERT(resp.read_response().partition_data_size() == 1);
            UNIT_ASSERT(resp.read_response().partition_data(0).batches_size() == 1);
            UNIT_ASSERT(resp.read_response().partition_data(0).batches(0).message_data_size() >= 1);
        }

        // commit offset
        {
            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("acc/topic1");
            req.set_consumer("user");
            req.set_offset(5);
            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::SUCCESS);
        }


        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_read_request()->set_bytes_size(10000);

            // auto commit = req.mutable_commit_offset_request()->add_commit_offsets();
            // commit->set_partition_session_id(assignId);

            // auto offsets = commit->add_offsets();
            // offsets->set_start(0);
            // offsets->set_end(7);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "=== Got response (expect session expired): " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(resp.status(), Ydb::StatusIds::SESSION_EXPIRED);
        }
    }

    Y_UNIT_TEST(TopicServiceCommitOffsetBadOffsets) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        MAKE_INSECURE_STUB(Ydb::Topic::V1::TopicService);
        server.EnablePQLogs({ NKikimrServices::PQ_METACACHE, NKikimrServices::PQ_READ_PROXY });
        server.EnablePQLogs({ NKikimrServices::KQP_PROXY }, NLog::EPriority::PRI_EMERG);
        server.EnablePQLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD }, NLog::EPriority::PRI_ERROR);
        server.EnablePQLogs({ NKikimrServices::PERSQUEUE }, NLog::EPriority::PRI_DEBUG);

        auto TopicStubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);

        {
            Ydb::Topic::CreateTopicRequest request;
            Ydb::Topic::CreateTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/rt3.dc1--acc--topic2");

            request.set_retention_storage_mb(1);

            request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
            request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_GZIP);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            server.Server->AnnoyingClient->WaitTopicInit("acc/topic2");
            server.Server->AnnoyingClient->AddTopic("acc/topic2");
        }

        auto driver = pqClient->GetDriver();
        {
            auto writer = CreateSimpleWriter(*driver, "acc/topic2", "source", /*partitionGroup=*/{}, /*codec=*/{"raw"});
            TString blob{1_MB, 'x'};
            for (int i = 1; i <= 20; ++i) {
                bool res = writer->Write(blob + ToString(i), i);
                UNIT_ASSERT(res);
            }

            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        {
            using namespace NYdb::NTopic;
            auto settings = TDescribeTopicSettings().IncludeStats(true);
            auto client = TTopicClient(server.Server->GetDriver());
            auto desc = client.DescribeTopic("/Root/PQ/rt3.dc1--acc--topic2", settings)
                            .ExtractValueSync()
                            .GetTopicDescription();
            Cerr << ">>>Describe result: partitions count is " << desc.GetTotalPartitionsCount() << Endl;
            for (const auto& partInfo: desc.GetPartitions()) {
                Cerr << ">>>Describe result: partition id = " << partInfo.GetPartitionId() << ", ";
                auto stats = partInfo.GetPartitionStats();
                UNIT_ASSERT(stats.Defined());
                Cerr << "offsets: [ " << stats.Get()->GetStartOffset() << ", " << stats.Get()->GetEndOffset() << " )" << Endl;
            }

            TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAddConsumer("first-consumer")
                .EndAddConsumer()
                .BeginAddConsumer("second-consumer").Important(true)
                .EndAddConsumer();
            auto res = client.AlterTopic("/Root/PQ/rt3.dc1--acc--topic2", alterSettings);
            res.Wait();
            Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
            UNIT_ASSERT(res.GetValue().IsSuccess());

        }
        // unimportant consumer
        // commit to future - expect bad request
        {
            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("acc/topic2");
            req.set_consumer("first-consumer");
            req.set_offset(25);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }

        // commit to past - expect bad request
        {
            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("acc/topic2");
            req.set_consumer("first-consumer");
            req.set_offset(3);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }

        // commit to valid offset - expect successful commit
        {
            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("acc/topic2");
            req.set_consumer("first-consumer");
            req.set_offset(18);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        // important consumer
        // normal commit - expect successful commit
        {
            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("acc/topic2");
            req.set_consumer("second-consumer");
            req.set_offset(18);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::SUCCESS);
        }

        // commit to past - expect error
        {
            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("acc/topic2");
            req.set_consumer("second-consumer");
            req.set_offset(3);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }

        // commit to future - expect bad request
        {
            Ydb::Topic::CommitOffsetRequest req;
            Ydb::Topic::CommitOffsetResponse resp;

            req.set_path("acc/topic2");
            req.set_consumer("second-consumer");
            req.set_offset(25);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CommitOffset(&rcontext, req, &resp);

            Cerr << resp << "\n";
            UNIT_ASSERT(status.ok());
            UNIT_ASSERT_VALUES_EQUAL(resp.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
    }


    Y_UNIT_TEST(TopicServiceReadBudget) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        MAKE_INSECURE_STUB(Ydb::Topic::V1::TopicService);
        server.EnablePQLogs({NKikimrServices::PQ_METACACHE, NKikimrServices::PQ_READ_PROXY});
        server.EnablePQLogs({NKikimrServices::KQP_PROXY}, NLog::EPriority::PRI_EMERG);
        server.EnablePQLogs({NKikimrServices::FLAT_TX_SCHEMESHARD}, NLog::EPriority::PRI_ERROR);

        auto readStream = StubP_ -> StreamRead(&rcontext);
        UNIT_ASSERT(readStream);

        auto driver = pqClient -> GetDriver();
        auto writer = CreateSimpleWriter(*driver, "acc/topic1", "source", /*partitionGroup=*/{}, /*codec=*/{"raw"});

        Ydb::Topic::StreamReadMessage::FromClient req;
        Ydb::Topic::StreamReadMessage::FromServer resp;

        auto WriteSome = [&](ui64 size) {
            TString data(size, 'x');
            UNIT_ASSERT(writer->Write(data));
        };

        i64 budget = 0;
        auto AwaitExpected = [&](int count) {
            while (count > 0) {
                UNIT_ASSERT(readStream->Read(&resp));
                Cerr << "Got read response " << resp << "\n";
                UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse,
                              resp);
                UNIT_ASSERT(resp.read_response().partition_data_size() == 1);

                for (int i = 0; i < resp.read_response().partition_data(0).batches_size(); ++i) {
                    int got = resp.read_response().partition_data(0).batches(i).message_data_size();
                    Cerr << "TAGX got response batch " << i << " with " << got << " messages, awaited for " << count << " more\n";
                    UNIT_ASSERT(got >= 1 && got <= count);
                    count -= got;
                }
                budget -= resp.read_response().bytes_size();
                Cerr << "TAGX Budget deced, now " << budget << "\n";
            }
        };

        // init read session
        {
            req.mutable_init_request()->add_topics_read_settings()->set_path("acc/topic1");
            req.mutable_init_request()->set_consumer("user");

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
        }

        WriteSome(10_KB);

        req.Clear();
        req.mutable_read_request()->set_bytes_size(50_KB);
        budget += 50_KB;
        Cerr << "TAGX Budget inced with 50k, now " << budget << "\n";
        if (!readStream->Write(req)) {
            ythrow yexception() << "write fail";
        }

        // await and confirm CreatePartitionStreamRequest from server
        i64 assignId = 0;
        {
            // lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            UNIT_ASSERT(resp.server_message_case() ==
                        Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT_VALUES_EQUAL(resp.start_partition_session_request().partition_session().path(), "acc/topic1");
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().partition_id() == 0);

            assignId = resp.start_partition_session_request().partition_session().partition_session_id();
            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignId);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }

        AwaitExpected(1);

        for (int i = 0; i < 3; ++i) {
            WriteSome(10_KB);
        }

        AwaitExpected(3);

        for (int i = 0; i < 6; ++i) {
            WriteSome(10_KB);
        }

        AwaitExpected(1);

        req.Clear();
        req.mutable_read_request()->set_bytes_size(25_KB);
        budget += 25_KB;
        Cerr << "TAGX Budget inced with 25k, now " << budget << "\n";
        if (!readStream->Write(req)) {
            ythrow yexception() << "write fail";
        }

        AwaitExpected(3); //why 3? 2!

        req.Clear();
        req.mutable_read_request()->set_bytes_size(7_KB);
        budget += 7_KB;
        Cerr << "TAGX Budget inced with 7k, now " << budget << "\n";

        if (!readStream->Write(req)) {
            ythrow yexception() << "write fail";
        }

        AwaitExpected(1);

        req.Clear();
        req.mutable_read_request()->set_bytes_size(14_KB);
        budget += 14_KB;
        Cerr << "TAGX Budget inced with 14k, now " << budget << "\n";
        if (!readStream->Write(req)) {
            ythrow yexception() << "write fail";
        }

        AwaitExpected(1);

        UNIT_ASSERT(writer->Close(TDuration::Seconds(10)));
    } // Y_UNIT_TEST(TopicServiceReadBudget)

    Y_UNIT_TEST(TopicServiceSimpleHappyWrites) {
        NPersQueue::TTestServer server;
        server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });
        TString topic3 = "acc/topic3";

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> TopicStubP_;

        {
            Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            TopicStubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);
        }

        {
            Ydb::Topic::CreateTopicRequest request;
            Ydb::Topic::CreateTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/rt3.dc1--acc--topic3");

            request.mutable_partitioning_settings()->set_min_active_partitions(2);
            request.mutable_retention_period()->set_seconds(TDuration::Days(1).Seconds());
            (*request.mutable_attributes())["_max_partition_storage_size"] = "1000";
            request.set_partition_write_speed_bytes_per_second(1000);
            request.set_partition_write_burst_bytes(1000);

            request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
            request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_CUSTOM + 42);

            auto consumer = request.add_consumers();
            consumer->set_name("first-consumer");
            consumer->set_important(false);
            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            server.AnnoyingClient->WaitTopicInit(topic3);
            server.AnnoyingClient->AddTopic(topic3);
        }

        grpc::ClientContext rcontextWrite1;
        auto writeStream1 = TopicStubP_->StreamWrite(&rcontextWrite1);
        UNIT_ASSERT(writeStream1);

        grpc::ClientContext rcontextWrite2;
        auto writeStream2 = TopicStubP_->StreamWrite(&rcontextWrite2);
        UNIT_ASSERT(writeStream2);

        grpc::ClientContext rcontext;
        auto readStream = TopicStubP_ -> StreamRead(&rcontext);
        UNIT_ASSERT(readStream);

        // init write session 1
        {
            Ydb::Topic::StreamWriteMessage::FromClient req;
            Ydb::Topic::StreamWriteMessage::FromServer resp;

            req.mutable_init_request()->set_path("acc/topic3");

            req.mutable_init_request()->set_producer_id("A");
            req.mutable_init_request()->set_partition_id(0);

            if (!writeStream1->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(writeStream1->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kInitResponse);
            UNIT_ASSERT_C(resp.init_response().partition_id() == 0, "unexpected partition_id");
            //send some reads
            req.Clear();

            auto* write = req.mutable_write_request();
            write->set_codec(Ydb::Topic::CODEC_RAW);

            for (ui32 i = 0; i < 10; ++i) {
                auto* msg = write->add_messages();
                msg->set_seq_no(i + 1);
                msg->set_data(TString("x") * (i + 1));
                *msg->mutable_created_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(TInstant::Now().MilliSeconds());
                msg->set_uncompressed_size(msg->data().size());
            }
            if (!writeStream1->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(writeStream1->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kWriteResponse);
        }

        // init write session 2
        {
            Ydb::Topic::StreamWriteMessage::FromClient req;
            Ydb::Topic::StreamWriteMessage::FromServer resp;

            req.mutable_init_request()->set_path("acc/topic3");

            req.mutable_init_request()->set_producer_id("B");
            req.mutable_init_request()->set_partition_id(1);

            if (!writeStream2->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(writeStream2->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kInitResponse);
            UNIT_ASSERT_C(resp.init_response().partition_id() == 1, "unexpected partition_id");
            //send some reads
            req.Clear();

            auto* write = req.mutable_write_request();
            write->set_codec(Ydb::Topic::CODEC_CUSTOM + 42);

            for (ui32 i = 0; i < 10; ++i) {
                auto* msg = write->add_messages();
                msg->set_seq_no(i + 1);
                msg->set_data(TString("y") * (i + 1));
                *msg->mutable_created_at() = ::google::protobuf::util::TimeUtil::MillisecondsToTimestamp(TInstant::Now().MilliSeconds());
                msg->set_uncompressed_size(msg->data().size());
            }
            if (!writeStream2->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(writeStream2->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kWriteResponse);
        }

        // init 1st read session
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path("acc/topic3");

            req.mutable_init_request()->set_consumer("user");

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
            //send some reads
            req.Clear();
            req.mutable_read_request()->set_bytes_size(256);
            for (ui32 i = 0; i < 10; ++i) {
                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
        }

        // await both CreatePartitionStreamRequest from Server
        // confirm both
        {
            Ydb::Topic::StreamReadMessage::FromClient req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            TVector<i64> partition_ids;
            //lock partition
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect 1st start part, Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().path() == "acc/topic3");
            partition_ids.push_back(resp.start_partition_session_request().partition_session().partition_id());

            ui64 assignIdFirst = resp.start_partition_session_request().partition_session().partition_session_id();

            resp.Clear();
            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "===Expect 2nd start part, Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest);
            UNIT_ASSERT(resp.start_partition_session_request().partition_session().path() == "acc/topic3");
            partition_ids.push_back(resp.start_partition_session_request().partition_session().partition_id());

            std::sort(partition_ids.begin(), partition_ids.end());
            UNIT_ASSERT((partition_ids == TVector<i64>{0, 1}));

            ui64 assignIdSecond = resp.start_partition_session_request().partition_session().partition_session_id();

            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignIdFirst);
            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }

            req.Clear();

            // invalid id should receive no reaction
            req.mutable_start_partition_session_response()->set_partition_session_id(1124134);

            UNIT_ASSERT_C(readStream->Write(req), "write fail");

            Sleep(TDuration::MilliSeconds(100));

            req.Clear();
            req.mutable_start_partition_session_response()->set_partition_session_id(assignIdSecond);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
        }

        Ydb::Topic::StreamReadMessage::FromServer resp;
        UNIT_ASSERT(readStream->Read(&resp));
        Cerr << "Got read response " << resp << "\n";
        UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);

        // second partition data goes to separate response - remove when reads return data from many partitions
        UNIT_ASSERT(readStream->Read(&resp));
        Cerr << "Got read response " << resp << "\n";
        UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);
    }

    Y_UNIT_TEST(SetupWriteSession) {
        NPersQueue::TTestServer server{PQSettings(0, 2), false};
        server.ServerSettings.SetEnableSystemViews(false);
        server.StartServer();

        server.EnableLogs({ NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_INFO);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);

        TPQDataWriter writer("source", server);

        ui32 p = writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"});

        server.AnnoyingClient->AlterTopic(DEFAULT_TOPIC_NAME, 15);

        ui32 pp = writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue2"});
        UNIT_ASSERT_VALUES_EQUAL(p, pp);

        writer.Write(SHORT_TOPIC_NAME, {"1", "2", "3", "4", "5"});

        writer.Write("topic2", {"valuevaluevalue1"}, true);

        p = writer.InitSession("sid1", 2, true);
        pp = writer.InitSession("sid1", 0, true);

        UNIT_ASSERT(p = pp);
        UNIT_ASSERT(p == 1);

        {
            p = writer.InitSession("sidx", 0, true);
            pp = writer.InitSession("sidx", 0, true);

            UNIT_ASSERT(p == pp);
        }

        writer.InitSession("sid1", 3, false);

        //check round robin;
        TMap<ui32, ui32> ss;
        for (ui32 i = 0; i < 15*5; ++i) {
            ss[writer.InitSession("sid_rand_" + ToString<ui32>(i), 0, true)]++;
        }
        for (auto& s : ss) {
            Cerr << "Round robin check: " << s.first << ":" << s.second << "\n";
            UNIT_ASSERT(s.second >= 4 && s.second <= 6);
        }
    }

    Y_UNIT_TEST(StoreNoMoreThanXSourceIDs) {
        ui16 X = 4;
        ui64 SOURCEID_COUNT_DELETE_BATCH_SIZE = 100;
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PERSQUEUE, NKikimrServices::PQ_WRITE_PROXY });
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 8_MB, 86400, 20000000, "", 200000000, {}, {}, {}, X, 86400);

        auto driver = server.AnnoyingClient->GetDriver();

        auto writer1 = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "test source ID " << 0, {}, {}, true);
        writer1->GetInitSeqNo();

        bool res = writer1->Write("x", 1);
        UNIT_ASSERT(res);

        Sleep(TDuration::Seconds(5));

        auto writer2 = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "test source ID Del " << 0);
        writer2->GetInitSeqNo();

        res = writer2->Write("x", 1);
        UNIT_ASSERT(res);

        Sleep(TDuration::Seconds(5));

        res = writer1->Write("x", 2);
        UNIT_ASSERT(res);

        Sleep(TDuration::Seconds(5));

        for (ui32 nProducer=1; nProducer < X + SOURCEID_COUNT_DELETE_BATCH_SIZE + 1; ++nProducer) {
            auto writer = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "test source ID " << nProducer);

            res = writer->Write("x", 1);
            UNIT_ASSERT(res);

            UNIT_ASSERT(writer->IsAlive());

            res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);

        }

        res = writer1->Write("x", 3);
        UNIT_ASSERT(res);
        res = writer1->Close(TDuration::Seconds(5));
        UNIT_ASSERT(res);

        res = writer2->Write("x", 4);
        UNIT_ASSERT(res);

        UNIT_ASSERT(!writer2->Close());
    }

    Y_UNIT_TEST(EachMessageGetsExactlyOneAcknowledgementInCorrectOrder) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic("rt3.dc1--topic", 1);

        auto driver = server.AnnoyingClient->GetDriver();

        auto writer = CreateSimpleWriter(*driver, "topic", "test source ID");

        bool res = true;

        ui32 messageCount = 1000;

        for (ui32 sequenceNumber = 1; sequenceNumber <= messageCount; ++sequenceNumber) {
                res = writer->Write("x", sequenceNumber);
                UNIT_ASSERT(res);
        }
        UNIT_ASSERT(writer->IsAlive());
        res = writer->Close(TDuration::Seconds(10));
        UNIT_ASSERT(res);
    }

    Y_UNIT_TEST(BadTopic) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic("rt3.dc1--topic", 1);

        auto driver = server.AnnoyingClient->GetDriver();

        auto writer = CreateSimpleWriter(*driver, "/topic/", "test source ID");
        bool gotException = false;
        try {
        writer->GetInitSeqNo();
        } catch(...) {
            gotException = true;
        }
        UNIT_ASSERT(gotException);
    }


    Y_UNIT_TEST(SetupWriteSessionOnDisabledCluster) {
        TPersQueueV1TestServer server;
        SET_LOCALS;

        TPQDataWriter writer("source", *server.Server);

        pqClient->DisableDC();

        Sleep(TDuration::Seconds(5));
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"}, true);
    }

    Y_UNIT_TEST(CloseActiveWriteSessionOnClusterDisable) {
        NPersQueue::TTestServer server;

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);

        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });


        TPQDataWriter writer2("source", server);

        auto driver = server.AnnoyingClient->GetDriver();

        auto writer = CreateWriter(*driver, SHORT_TOPIC_NAME, "123", 0, "raw");

        auto msg = writer->GetEvent(true);
        UNIT_ASSERT(msg); // ReadyToAcceptEvent

        Cerr << DebugString(*msg) << "\n";

        server.AnnoyingClient->DisableDC();

        UNIT_ASSERT(writer->WaitEvent().Wait(TDuration::Seconds(30)));
        msg = writer->GetEvent(true);
        UNIT_ASSERT(msg);


        Cerr << DebugString(*msg) << "\n";

        auto ev = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*msg);

        UNIT_ASSERT(ev);

        Cerr << "is dead res: " << ev->DebugString() << "\n";
        UNIT_ASSERT_EQUAL(ev->GetIssues().back().GetCode(), Ydb::PersQueue::ErrorCode::CLUSTER_DISABLED);
    }

    Y_UNIT_TEST(BadSids) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });
        TPQDataWriter writer2("source", server);
        TString topic = SHORT_TOPIC_NAME;

        auto driver = server.AnnoyingClient->GetDriver();

        auto writer = CreateSimpleWriter(*driver, topic, "base64:a***");
        UNIT_ASSERT(!writer->Write("x"));
        writer = CreateSimpleWriter(*driver, topic, "base64:aa==");
        UNIT_ASSERT(!writer->Write("x"));
        writer = CreateSimpleWriter(*driver, topic, "base64:a");
        UNIT_ASSERT(!writer->Write("x"));
        writer = CreateSimpleWriter(*driver, topic, "base64:aa");
        UNIT_ASSERT(writer->Write("x"));
        UNIT_ASSERT(writer->Close());
    }

    Y_UNIT_TEST(ReadFromSeveralPartitions) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);

        TPQDataWriter writer("source1", server);
        Cerr << "===Writer started\n";
        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> StubP_;

        Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
        StubP_ = Ydb::PersQueue::V1::PersQueueService::NewStub(Channel_);


        //Write some data
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"});

        TPQDataWriter writer2("source2", server);
        writer2.Write(SHORT_TOPIC_NAME, {"valuevaluevalue2"});
        Cerr << "===Writer - writes done\n";

        grpc::ClientContext rcontext;
        auto readStream = StubP_->MigrationStreamingRead(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            MigrationStreamingReadClientMessage  req;
            MigrationStreamingReadServerMessage resp;

            req.mutable_init_request()->add_topics_read_settings()->set_topic(SHORT_TOPIC_NAME);

            req.mutable_init_request()->set_consumer("user");
            req.mutable_init_request()->set_read_only_original(true);

            req.mutable_init_request()->mutable_read_params()->set_max_read_messages_count(1000);

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            Cerr << "===Try to get read response\n";

            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "Read server response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.response_case() == MigrationStreamingReadServerMessage::kInitResponse);

            //send some reads
            Sleep(TDuration::Seconds(5));
            for (ui32 i = 0; i < 10; ++i) {
                req.Clear();
                req.mutable_read();

                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
        }

        //check read results
        MigrationStreamingReadServerMessage resp;
        for (ui32 i = 0; i < 2;) {
            MigrationStreamingReadServerMessage resp;
            UNIT_ASSERT(readStream->Read(&resp));
            if (resp.response_case() == MigrationStreamingReadServerMessage::kAssigned) {
                auto assignId = resp.assigned().assign_id();
                MigrationStreamingReadClientMessage req;
                req.mutable_start_read()->mutable_topic()->set_path(SHORT_TOPIC_NAME);
                req.mutable_start_read()->set_cluster("dc1");
                req.mutable_start_read()->set_assign_id(assignId);
                UNIT_ASSERT(readStream->Write(req));
                continue;
            }

            UNIT_ASSERT_C(resp.response_case() == MigrationStreamingReadServerMessage::kDataBatch, resp);
            i += resp.data_batch().partition_data_size();
        }
    }

    Y_UNIT_TEST(ReadFromSeveralPartitionsMigrated) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);

        TPQDataWriter writer("source1", server);
        Cerr << "===Writer started\n";
        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> StubP_;

        Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
        StubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);


        //Write some data
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"});

        TPQDataWriter writer2("source2", server);
        writer2.Write(SHORT_TOPIC_NAME, {"valuevaluevalue2"});
        Cerr << "===Writer - writes done\n";

        grpc::ClientContext rcontext;
        auto readStream = StubP_->StreamRead(&rcontext);
        UNIT_ASSERT(readStream);

        // init read session
        {
            Ydb::Topic::StreamReadMessage::FromClient  req;
            Ydb::Topic::StreamReadMessage::FromServer resp;

            req.mutable_init_request()->add_topics_read_settings()->set_path(SHORT_TOPIC_NAME);

            req.mutable_init_request()->set_consumer("user");

            if (!readStream->Write(req)) {
                ythrow yexception() << "write fail";
            }
            Cerr << "===Try to get read response\n";

            UNIT_ASSERT(readStream->Read(&resp));
            Cerr << "Read server response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);

            //send some reads
            Sleep(TDuration::Seconds(5));
            for (ui32 i = 0; i < 10; ++i) {
                req.Clear();
                req.mutable_read_request()->set_bytes_size(256000);

                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }
            }
        }

        //check read results
        for (ui32 i = 0; i < 2;) {
            Ydb::Topic::StreamReadMessage::FromServer resp;
            UNIT_ASSERT(readStream->Read(&resp));
            if (resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest) {
                auto assignId = resp.start_partition_session_request().partition_session().partition_session_id();
                Ydb::Topic::StreamReadMessage::FromClient req;
                req.mutable_start_partition_session_response()->set_partition_session_id(assignId);
                UNIT_ASSERT(readStream->Write(req));
                continue;
            }

            UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);
            i += resp.read_response().partition_data_size();
        }
    }


    void SetupReadSessionTest() {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        server.AnnoyingClient->CreateTopicNoLegacy("rt3.dc2--topic1", 2, true, false);

        TPQDataWriter writer("source1", server);

        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue0"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue2"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue3"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue4"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue5"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue6"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue7"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue8"});
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue9"});

        writer.Read(SHORT_TOPIC_NAME, "user", "", false, false);
    }

    Y_UNIT_TEST(SetupReadSession) {
        SetupReadSessionTest();
    }


    Y_UNIT_TEST(WriteExisting) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE });

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        {
            THolder<NMsgBusProxy::TBusPersQueue> request = TRequestDescribePQ().GetRequest({});

            NKikimrClient::TResponse response;

            auto channel = grpc::CreateChannel("localhost:"+ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            auto stub(NKikimrClient::TGRpcServer::NewStub(channel));
            grpc::ClientContext context;
            auto status = stub->PersQueueRequest(&context, request->Record, &response);

            UNIT_ASSERT(status.ok());
        }

        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 1, "valuevaluevalue1", "");
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 2, "valuevaluevalue1", "");
    }

    Y_UNIT_TEST(WriteExistingBigValue) {
        auto settings = PQSettings(0).SetDomainName("Root").SetNodeCount(2);
        settings.PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);
        NPersQueue::TTestServer server{settings};
        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE });
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2, 8_MB, 86400, 100000);


        TInstant now(Now());

        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 1, TString(1000000, 'a'));
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 2, TString(1, 'a'));
        UNIT_ASSERT(TInstant::Now() - now > TDuration::MilliSeconds(5990)); //speed limit is 200kb/s and burst is 200kb, so to write 1mb it will take at least 4 seconds
    }

    Y_UNIT_TEST(WriteEmptyData) {
        NPersQueue::TTestServer server{PQSettings(0).SetDomainName("Root").SetNodeCount(2)};

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        server.EnableLogs({ NKikimrServices::PERSQUEUE });

        // empty data and sourceId
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "", 1, "", "", NMsgBusProxy::MSTATUS_ERROR);
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "a", 1, "", "", NMsgBusProxy::MSTATUS_ERROR);
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "", 1, "a", "", NMsgBusProxy::MSTATUS_ERROR);
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "a", 1, "a", "", NMsgBusProxy::MSTATUS_OK);
    }


    Y_UNIT_TEST(WriteNonExistingPartition) {
        NPersQueue::TTestServer server{PQSettings(0).SetDomainName("Root").SetNodeCount(2)};
        server.EnableLogs({ NKikimrServices::PQ_METACACHE });
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE });

        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 100500, "abacaba", 1, "valuevaluevalue1", "",
                NMsgBusProxy::MSTATUS_ERROR, NMsgBusProxy::MSTATUS_ERROR
        );
    }

    Y_UNIT_TEST(WriteNonExistingTopic) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE });

        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME + "000", 1, "abacaba", 1, "valuevaluevalue1", "",
                NMsgBusProxy::MSTATUS_ERROR, NMsgBusProxy::MSTATUS_ERROR
        );
    }

    Y_UNIT_TEST(SchemeshardRestart) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(1));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        TString secondTopic = "rt3.dc1--topic2";
        server.AnnoyingClient->CreateTopic(secondTopic, 2);

        // force topic1 into cache and establish pipe from cache to schemeshard
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 1, "abacaba", 1, "valuevaluevalue1");

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD,
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->RestartSchemeshard(server.CleverServer->GetRuntime());

        server.AnnoyingClient->WriteToPQ(secondTopic, 1, "abacaba", 1, "valuevaluevalue1");
    }

    Y_UNIT_TEST(WriteAfterAlter) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);


        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 5, "abacaba", 1, "valuevaluevalue1", "",
                NMsgBusProxy::MSTATUS_ERROR,  NMsgBusProxy::MSTATUS_ERROR
        );

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD,
            NKikimrServices::PERSQUEUE,
            NKikimrServices::PQ_METACACHE });

        server.AnnoyingClient->AlterTopic(DEFAULT_TOPIC_NAME, 10);
        Sleep(TDuration::Seconds(1));
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacaba", 1, "valuevaluevalue1");
        server.AnnoyingClient->WriteToPQ(
                DEFAULT_TOPIC_NAME, 15, "abacaba", 1, "valuevaluevalue1", "",
                NMsgBusProxy::MSTATUS_ERROR,  NMsgBusProxy::MSTATUS_ERROR
        );

        server.AnnoyingClient->AlterTopic(DEFAULT_TOPIC_NAME, 20);
        Sleep(TDuration::Seconds(1));
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacaba", 1, "valuevaluevalue1");
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 15, "abacaba", 1, "valuevaluevalue1");
    }

    Y_UNIT_TEST(Delete) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE});

        // Delete non-existing
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME, NPersQueue::NErrorCode::UNKNOWN_TOPIC);

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        // Delete existing
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);

        // Double delete - "What Is Dead May Never Die"
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME, NPersQueue::NErrorCode::UNKNOWN_TOPIC);

        // Resurrect deleted topic
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);
    }

    // expects that L2 size is 32Mb
    Y_UNIT_TEST(Cache) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root").SetGrpcMaxMessageSize(18_MB));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 8_MB, 86400);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE });

        TString value(1_MB, 'x');
        for (ui32 i = 0; i < 32; ++i)
            server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", i}, value);

        auto info0 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 0, 16, "user"}, 16);
        auto info16 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 16, 16, "user"}, 16);

        UNIT_ASSERT_VALUES_EQUAL(info0.BlobsFromCache, 3);
        UNIT_ASSERT_VALUES_EQUAL(info16.BlobsFromCache, 2);
        UNIT_ASSERT_VALUES_EQUAL(info0.BlobsFromDisk + info16.BlobsFromDisk, 0);

        for (ui32 i = 0; i < 8; ++i)
            server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", 32+i}, value);

        info0 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 0, 16, "user"}, 16);
        info16 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 16, 16, "user"}, 16);

        ui32 fromDisk = info0.BlobsFromDisk + info16.BlobsFromDisk;
        ui32 fromCache = info0.BlobsFromCache + info16.BlobsFromCache;
        UNIT_ASSERT(fromDisk > 0);
        UNIT_ASSERT(fromDisk < 5);
        UNIT_ASSERT(fromCache > 0);
        UNIT_ASSERT(fromCache < 5);
    }

    Y_UNIT_TEST(CacheHead) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root").SetGrpcMaxMessageSize(16_MB));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 6_MB, 86400);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE });

        ui64 seqNo = 0;
        for (ui32 blobSizeKB = 256; blobSizeKB < 4096; blobSizeKB *= 2) {
            static const ui32 maxEventKB = 24_KB;
            ui32 blobSize = blobSizeKB * 1_KB;
            ui32 count = maxEventKB / blobSizeKB;
            count -= count%2;
            ui32 half = count/2;

            ui64 offset = seqNo;
            TString value(blobSize, 'a');
            for (ui32 i = 0; i < count; ++i)
                server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", seqNo++}, value);

            auto info_half1 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, offset, half, "user1"}, half);
            auto info_half2 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, offset, half, "user1"}, half);

            UNIT_ASSERT(info_half1.BlobsFromCache > 0);
            UNIT_ASSERT(info_half2.BlobsFromCache > 0);
            UNIT_ASSERT_VALUES_EQUAL(info_half1.BlobsFromDisk, 0);
            UNIT_ASSERT_VALUES_EQUAL(info_half2.BlobsFromDisk, 0);
        }
    }

    Y_UNIT_TEST(SameOffset) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root"));
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 6_MB, 86400);
        TString secondTopic = DEFAULT_TOPIC_NAME + "2";
        server.AnnoyingClient->CreateTopic(secondTopic, 1, 6_MB, 86400);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE });

        ui32 valueSize = 128;
        TString value1(valueSize, 'a');
        TString value2(valueSize, 'b');
        server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", 0}, value1);
        server.AnnoyingClient->WriteToPQ({secondTopic, 0, "source1", 0}, value2);

        // avoid reading from head
        TString mb(1_MB, 'x');
        for (ui32 i = 1; i < 16; ++i) {
            server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 0, "source1", i}, mb);
            server.AnnoyingClient->WriteToPQ({secondTopic, 0, "source1", i}, mb);
        }

        auto info1 = server.AnnoyingClient->ReadFromPQ({DEFAULT_TOPIC_NAME, 0, 0, 1, "user1"}, 1);
        auto info2 = server.AnnoyingClient->ReadFromPQ({secondTopic, 0, 0, 1, "user1"}, 1);

        UNIT_ASSERT_VALUES_EQUAL(info1.BlobsFromCache, 1);
        UNIT_ASSERT_VALUES_EQUAL(info2.BlobsFromCache, 1);
        UNIT_ASSERT_VALUES_EQUAL(info1.Values.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(info2.Values.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(info1.Values[0].size(), valueSize);
        UNIT_ASSERT_VALUES_EQUAL(info2.Values[0].size(), valueSize);
        UNIT_ASSERT(info1.Values[0] == value1);
        UNIT_ASSERT(info2.Values[0] == value2);
    }


    Y_UNIT_TEST(FetchRequest) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root"));
        TString secondTopic = DEFAULT_TOPIC_NAME + "2";

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        server.AnnoyingClient->CreateTopic(secondTopic, 10);

        ui32 valueSize = 128;
        TString value1(valueSize, 'a');
        TString value2(valueSize, 'b');
        server.AnnoyingClient->WriteToPQ({secondTopic, 5, "source1", 0}, value2);
        server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 1, "source1", 0}, value1);
        server.AnnoyingClient->WriteToPQ({DEFAULT_TOPIC_NAME, 1, "source1", 1}, value2);

        server.EnableLogs({ NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PERSQUEUE });
        TInstant tm(TInstant::Now());
        server.AnnoyingClient->FetchRequestPQ({{secondTopic, 5, 0, 400},{DEFAULT_TOPIC_NAME, 1, 0, 400},{DEFAULT_TOPIC_NAME, 3, 0, 400}}, 400, 1000000);
        UNIT_ASSERT((TInstant::Now() - tm).Seconds() < 1);
        tm = TInstant::Now();
        server.AnnoyingClient->FetchRequestPQ({{secondTopic, 5, 1, 400}}, 400, 5000);
        UNIT_ASSERT((TInstant::Now() - tm).Seconds() > 2);
        server.AnnoyingClient->FetchRequestPQ({{secondTopic, 5, 0, 400},{DEFAULT_TOPIC_NAME, 1, 0, 400},{DEFAULT_TOPIC_NAME, 3, 0, 400}}, 1, 1000000);
        server.AnnoyingClient->FetchRequestPQ({{secondTopic, 5, 500, 400},{secondTopic, 4, 0, 400},{DEFAULT_TOPIC_NAME, 1, 0, 400}}, 400, 1000000);
    }

    Y_UNIT_TEST(Init) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        if (!true) {
            server.EnableLogs( {
                NKikimrServices::FLAT_TX_SCHEMESHARD,
                NKikimrServices::TX_DATASHARD,
                NKikimrServices::HIVE,
                NKikimrServices::PERSQUEUE,
                NKikimrServices::TABLET_MAIN,
                NKikimrServices::BS_PROXY_DISCOVER,
                NKikimrServices::PIPE_CLIENT,
                NKikimrServices::PQ_METACACHE });
        }

        server.AnnoyingClient->DescribeTopic({});
        server.AnnoyingClient->TestCase({}, 0, 0, true);

        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        server.AnnoyingClient->AlterTopic(DEFAULT_TOPIC_NAME, 20);
        TString secondTopic = DEFAULT_TOPIC_NAME + "2";
        TString thirdTopic = DEFAULT_TOPIC_NAME + "3";
        server.AnnoyingClient->CreateTopic(secondTopic, 25);

        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacaba", 1, "valuevaluevalue1");
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacaba", 2, "valuevaluevalue2");
        server.AnnoyingClient->WriteToPQ(DEFAULT_TOPIC_NAME, 5, "abacabae", 1, "valuevaluevalue3");
        server.AnnoyingClient->ReadFromPQ(DEFAULT_TOPIC_NAME, 5, 0, 10, 3);

        server.AnnoyingClient->SetClientOffsetPQ(DEFAULT_TOPIC_NAME, 5, 2);

        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {5}}}, 1, 1, true);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {0}}}, 1, 0, true);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {}}}, 20, 1, true);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {5, 5}}}, 0, 0, false);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {111}}}, 0, 0, false);
        server.AnnoyingClient->TestCase({}, 45, 1, true);
        server.AnnoyingClient->TestCase({{thirdTopic, {}}}, 0, 0, false);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {}}, {thirdTopic, {}}}, 0, 0, false);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {}}, {secondTopic, {}}}, 45, 1, true);
        server.AnnoyingClient->TestCase({{DEFAULT_TOPIC_NAME, {0, 3, 5}}, {secondTopic, {1, 4, 6, 8}}}, 7, 1, true);

        server.AnnoyingClient->DescribeTopic({DEFAULT_TOPIC_NAME});
        server.AnnoyingClient->DescribeTopic({secondTopic});
        server.AnnoyingClient->DescribeTopic({secondTopic, DEFAULT_TOPIC_NAME});
        server.AnnoyingClient->DescribeTopic({});
        server.AnnoyingClient->DescribeTopic({thirdTopic}, true);
    }

    void WaitResolveSuccess(TFlatMsgBusPQClient& annoyingClient, TString topic, ui32 numParts) {
        const TInstant start = TInstant::Now();
        while (true) {
            TAutoPtr<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
            auto req = request->Record.MutableMetaRequest();
            auto partOff = req->MutableCmdGetPartitionLocations();
            auto treq = partOff->AddTopicRequest();
            treq->SetTopic(topic);
            for (ui32 i = 0; i < numParts; ++i)
                treq->AddPartition(i);

            TAutoPtr<NBus::TBusMessage> reply;
            NBus::EMessageStatus status = annoyingClient.SyncCall(request, reply);
            UNIT_ASSERT_VALUES_EQUAL(status, NBus::MESSAGE_OK);
            const NMsgBusProxy::TBusResponse* response = dynamic_cast<NMsgBusProxy::TBusResponse*>(reply.Get());
            UNIT_ASSERT(response);
            if (response->Record.GetStatus() == NMsgBusProxy::MSTATUS_OK)
                break;
            UNIT_ASSERT(TInstant::Now() - start < ::DEFAULT_DISPATCH_TIMEOUT);
            Sleep(TDuration::MilliSeconds(10));
        }
    }

    Y_UNIT_TEST(WhenDisableNodeAndCreateTopic_ThenAllPartitionsAreOnOtherNode) {
        NPersQueue::TTestServer server(PQSettings(0).SetDomainName("Root").SetNodeCount(2));
        server.EnableLogs({ NKikimrServices::PERSQUEUE, NKikimrServices::HIVE });
        TString unusedTopic = "rt3.dc1--unusedtopic";
        server.AnnoyingClient->CreateTopic(unusedTopic, 1);
        WaitResolveSuccess(*server.AnnoyingClient, unusedTopic, 1);

        // Act
        // Disable node #0
        server.AnnoyingClient->MarkNodeInHive(server.CleverServer->GetRuntime(), 0, false);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 3);
        WaitResolveSuccess(*server.AnnoyingClient, DEFAULT_TOPIC_NAME, 3);

        // Assert that all partitions are on node #1
        const ui32 node1Id = server.CleverServer->GetRuntime()->GetNodeId(1);
        UNIT_ASSERT_VALUES_EQUAL(
            server.AnnoyingClient->GetPartLocation({{DEFAULT_TOPIC_NAME, {0, 1}}}, 2, true),
            TVector<ui32>({node1Id, node1Id})
        );
    }

    void PrepareForGrpc(NPersQueue::TTestServer& server) {
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);
    }

    class TTestCredentialsProvider : public NYdb::ICredentialsProvider {
        public:

        TTestCredentialsProvider(const NYdb::TStringType& token)
        : Token(token)
        {}

        virtual ~TTestCredentialsProvider()
        {}

        NYdb::TStringType GetAuthInfo() const override {
            return Token;
        }

        void SetToken(const NYdb::TStringType& token) {
            Token = token;
        }
        bool IsValid() const override {
            return true;
        }

        NYdb::TStringType Token;
    };

    class TTestCredentialsProviderFactory : public NYdb::ICredentialsProviderFactory {
    public:
        TTestCredentialsProviderFactory(const NYdb::TStringType& token)
        : CredentialsProvider(new TTestCredentialsProvider(token))
        {}

        TTestCredentialsProviderFactory(const TTestCredentialsProviderFactory&) = delete;
        TTestCredentialsProviderFactory& operator = (const TTestCredentialsProviderFactory&) = delete;

        virtual ~TTestCredentialsProviderFactory()
        {}

        std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
            return CredentialsProvider;
        }

        NYdb::TStringType GetClientIdentity() const override {
            return CreateGuidAsString();
        }

        void SetToken(const NYdb::TStringType& token) {
            CredentialsProvider->SetToken(token);
        }
    private:
        std::shared_ptr<TTestCredentialsProvider> CredentialsProvider;


    };


    Y_UNIT_TEST(CheckACLForGrpcWrite) {
        NPersQueue::TTestServer server(PQSettings(0, 1));
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });
        PrepareForGrpc(server);

        TPQDataWriter writer("source1", server);
        TPQDataWriter writer2("source1", server);

        server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"}, true, TString()); // Fail if user set empty token
        writer.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"}, true, "topic1@" BUILTIN_ACL_DOMAIN);

        auto driver = server.AnnoyingClient->GetDriver();


        ModifyTopicACL(driver, "/Root/PQ/" + DEFAULT_TOPIC_NAME, {{"topic1@" BUILTIN_ACL_DOMAIN, {"ydb.generic.write"}}});


        writer2.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"}, false, "topic1@" BUILTIN_ACL_DOMAIN);
        writer2.Write(SHORT_TOPIC_NAME, {"valuevaluevalue1"}, true, "invalid_ticket");


        for (ui32 i = 0; i < 2; ++i) {
            std::shared_ptr<NYdb::ICredentialsProviderFactory> creds = std::make_shared<TTestCredentialsProviderFactory>(NYdb::TStringType("topic1@" BUILTIN_ACL_DOMAIN));
            dynamic_cast<TTestCredentialsProviderFactory*>(creds.get())->SetToken(NYdb::TStringType("topic1@" BUILTIN_ACL_DOMAIN));

            auto writer = CreateWriter(*driver, SHORT_TOPIC_NAME, "123", {}, {}, {}, creds);

            auto msg = writer->GetEvent(true);
            UNIT_ASSERT(msg); // ReadyToAcceptEvent

            Cerr << DebugString(*msg) << "\n";

            auto ev = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
            UNIT_ASSERT(ev);

            writer->Write(std::move(ev->ContinuationToken), "a");

            msg = writer->GetEvent(true);
            UNIT_ASSERT(msg);
            Cerr << DebugString(*msg) << "\n";
            ev = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
            UNIT_ASSERT(ev);

            msg = writer->GetEvent(true);
            UNIT_ASSERT(msg);
            Cerr << DebugString(*msg) << "\n";
            auto ack = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TAcksEvent>(&*msg);
            UNIT_ASSERT(ack);

            NYdb::TStringType token = i == 0 ? "user_without_rights@" BUILTIN_ACL_DOMAIN : "invalid_ticket";
            Cerr << "Set token " << token << "\n";

            dynamic_cast<TTestCredentialsProviderFactory*>(creds.get())->SetToken(token);

            writer->Write(std::move(ev->ContinuationToken), "a");
            ui32 events = 0;
            while(true) {
                UNIT_ASSERT(writer->WaitEvent().Wait(TDuration::Seconds(10)));
                msg = writer->GetEvent(true);
                UNIT_ASSERT(msg);
                Cerr << DebugString(*msg) << "\n";
                if (std::holds_alternative<NYdb::NPersQueue::TSessionClosedEvent>(*msg))
                    break;
                UNIT_ASSERT(++events <= 2); // Before close only one ack and one ready-to-accept can be received
            }
        }
    }


    Y_UNIT_TEST(CheckACLForGrpcRead) {
        NPersQueue::TTestServer server(PQSettings(0, 1));
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_METACACHE });
        //server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_METACACHE });
        server.EnableLogs({ NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_INFO);
        TString topic2 = DEFAULT_TOPIC_NAME + "2";
        TString shortTopic2Name = "topic12";
        PrepareForGrpc(server);

        server.AnnoyingClient->CreateTopic(topic2, 1, 8_MB, 86400, 20000000, "", 200000000, {"user1", "user2"});
        server.WaitInit(shortTopic2Name);
        server.AnnoyingClient->CreateConsumer("user1");
        server.AnnoyingClient->CreateConsumer("user2");
        server.AnnoyingClient->CreateConsumer("user5");
        server.AnnoyingClient->GrantConsumerAccess("user1", "user2@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->GrantConsumerAccess("user1", "user3@" BUILTIN_ACL_DOMAIN);

        server.AnnoyingClient->GrantConsumerAccess("user1", "1@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->GrantConsumerAccess("user2", "2@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->GrantConsumerAccess("user5", "1@" BUILTIN_ACL_DOMAIN);
        server.AnnoyingClient->GrantConsumerAccess("user5", "2@" BUILTIN_ACL_DOMAIN);
        Cerr << "=== Create writer\n";
        TPQDataWriter writer("source1", server);

        server.CleverServer->GetRuntime()->GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);

        auto driver = server.AnnoyingClient->GetDriver();

        ModifyTopicACL(driver, "/Root/PQ/" + topic2, {{"1@" BUILTIN_ACL_DOMAIN, {"ydb.generic.read"}},
                                                      {"2@" BUILTIN_ACL_DOMAIN, {"ydb.generic.read"}},
                                                      {"user1@" BUILTIN_ACL_DOMAIN, {"ydb.generic.read"}},
                                                      {"user2@" BUILTIN_ACL_DOMAIN, {"ydb.generic.read"}}});
        auto ticket1 = "1@" BUILTIN_ACL_DOMAIN;
        auto ticket2 = "2@" BUILTIN_ACL_DOMAIN;

        Cerr << "=== Writer - do reads\n";
        writer.Read(shortTopic2Name, "user1", ticket1, false, false, true);

        writer.Read(shortTopic2Name, "user1", "user2@" BUILTIN_ACL_DOMAIN, false, false, true);
        writer.Read(shortTopic2Name, "user1", "user3@" BUILTIN_ACL_DOMAIN, true, false, true); //for topic
        writer.Read(shortTopic2Name, "user1", "user1@" BUILTIN_ACL_DOMAIN, true, false, true); //for consumer
        writer.Read(shortTopic2Name, "user2", ticket1, true, false, true);
        writer.Read(shortTopic2Name, "user2", ticket2, false, false, true);

        writer.Read(shortTopic2Name, "user5", ticket1, true, false, true);
        writer.Read(shortTopic2Name, "user5", ticket2, true, false, true);

        ModifyTopicACL(driver, "/Root/PQ/" + topic2, {{"user3@" BUILTIN_ACL_DOMAIN, {"ydb.generic.read"}}});


        Cerr << "==== Writer - read\n";
        writer.Read(shortTopic2Name, "user1", "user3@" BUILTIN_ACL_DOMAIN, false, true, true);

        auto Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
        auto StubP_ = Ydb::PersQueue::V1::PersQueueService::NewStub(Channel_);

/*        auto driver = server.AnnoyingClient->GetDriver();

        Cerr << "==== Start consuming loop\n";
        for (ui32 i = 0; i < 2; ++i) {

            std::shared_ptr<NYdb::ICredentialsProviderFactory> creds = std::make_shared<TTestCredentialsProviderFactory>(NYdb::TStringType("user3@" BUILTIN_ACL_DOMAIN));

            NYdb::NPersQueue::TReadSessionSettings settings;
            settings.ConsumerName("user1").AppendTopics(shortTopic2Name).ReadOriginal({"dc1"});
            auto reader = CreateReader(*driver, settings, creds);

            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << NYdb::NPersQueue::DebugString(*msg) << "\n";

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);

            UNIT_ASSERT(ev);
            dynamic_cast<TTestCredentialsProviderFactory*>(creds.get())->SetToken(i == 0 ? "user_without_rights@" BUILTIN_ACL_DOMAIN : "invalid_ticket");

            ev->Confirm();

            Cerr << "=== Wait for consumer death (" << i << ")" << Endl;

            msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << NYdb::NPersQueue::DebugString(*msg) << "\n";

            auto closeEv = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*msg);

            UNIT_ASSERT(closeEv);
        }
*/
        Cerr << "==== Start second loop\n";
        server.AnnoyingClient->CreateTopic("rt3.dc1--account--test-topic123", 1);
        for (ui32 i = 0; i < 3; ++i){
            server.AnnoyingClient->GetClientInfo({topic2}, "user1", true);

            ReadInfoRequest request;
            ReadInfoResponse response;
            request.mutable_consumer()->set_path("user1");
            request.set_get_only_original(true);
            request.add_topics()->set_path(shortTopic2Name);
            grpc::ClientContext rcontext;
            if (i == 0) {
                rcontext.AddMetadata("x-ydb-auth-ticket", "user_without_rights@" BUILTIN_ACL_DOMAIN);
            }
            if (i == 1) {
                rcontext.AddMetadata("x-ydb-auth-ticket", "invalid_ticket");
            }
            if (i == 2) {
                rcontext.AddMetadata("x-ydb-auth-ticket", "user3@" BUILTIN_ACL_DOMAIN);
            }
            auto status = StubP_->GetReadSessionsInfo(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            ReadInfoResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << "Response: " << response << "\n" << res << "\n";
            UNIT_ASSERT(response.operation().ready() == true);
            UNIT_ASSERT(response.operation().status() == (i < 2) ? Ydb::StatusIds::UNAUTHORIZED : Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(EventBatching) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        PrepareForGrpc(server);

        auto driver = server.AnnoyingClient->GetDriver();
        auto decompressor = CreateSyncExecutorWrapper();

        NYdb::NPersQueue::TReadSessionSettings settings;
        settings.ConsumerName("shared/user").AppendTopics(SHORT_TOPIC_NAME).ReadOriginal({"dc1"});
        settings.DecompressionExecutor(decompressor);
        auto reader = CreateReader(*driver, settings);

        for (ui32 i = 0; i < 2; ++i) {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);
            UNIT_ASSERT(ev);

            ev->Confirm();
        }

        auto writeDataAndWaitForDecompressionTasks = [&](const TString &message,
                                                         const TString &sourceId,
                                                         ui32 partitionId,
                                                         size_t tasksCount) {
            //
            // write data
            //
            auto writer = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, sourceId, partitionId, "raw");
            writer->Write(message, 1);

            writer->Close(TDuration::Seconds(10));

            //
            // wait for decompression tasks
            //
            while (decompressor->GetFuncsCount() < tasksCount) {
                Sleep(TDuration::Seconds(1));
            }
        };

        //
        // stream #1: [0-, 2-]
        // stream #2: [1-, 3-]
        // session  : []
        //
        writeDataAndWaitForDecompressionTasks("111", "source_id_0", 1, 1); // 0
        writeDataAndWaitForDecompressionTasks("333", "source_id_1", 2, 2); // 1
        writeDataAndWaitForDecompressionTasks("222", "source_id_2", 1, 3); // 2
        writeDataAndWaitForDecompressionTasks("444", "source_id_3", 2, 4); // 3

        //
        // stream #1: [0+, 2+]
        // stream #2: [1+, 3+]
        // session  : [(#1: 1), (#2: 1), (#1, 1)]
        //
        decompressor->StartFuncs({0, 3, 1, 2});

        auto messages = reader->GetEvents(true);
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);

        {
            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&messages[0]);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetMessages().size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(ev->GetMessages()[0].GetData(), "111");
        }

        {
            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&messages[1]);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetMessages().size(), 2);

            UNIT_ASSERT_VALUES_EQUAL(ev->GetMessages()[0].GetData(), "333");
            UNIT_ASSERT_VALUES_EQUAL(ev->GetMessages()[1].GetData(), "444");
        }

        {
            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&messages[2]);
            UNIT_ASSERT(ev);
            UNIT_ASSERT_VALUES_EQUAL(ev->GetMessages().size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(ev->GetMessages()[0].GetData(), "222");
        }

        //
        // stream #1: []
        // stream #2: []
        // session  : []
        //
        auto msg = reader->GetEvent(false);
        UNIT_ASSERT(!msg);
    }

    Y_UNIT_TEST(CheckKillBalancer) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        auto driver = server.AnnoyingClient->GetDriver();
        auto decompressor = CreateThreadPoolExecutorWrapper(2);

        NYdb::NPersQueue::TReadSessionSettings settings;
        settings.ConsumerName("shared/user").AppendTopics(SHORT_TOPIC_NAME).ReadOriginal({"dc1"});
        settings.DecompressionExecutor(decompressor);
        auto reader = CreateReader(*driver, settings);

        auto counters = reader->GetCounters();

        auto DumpCounters = [&](const char *message) {
            Cerr << "===== " << message << " =====" << Endl;
            Cerr << "MessagesInflight: " << counters->MessagesInflight->Val() << Endl;
            Cerr << "BytesInflightUncompressed: " << counters->BytesInflightUncompressed->Val() << Endl;
            Cerr << "BytesInflightCompressed: " << counters->BytesInflightCompressed->Val() << Endl;
            Cerr << "BytesInflightTotal: " << counters->BytesInflightTotal->Val() << Endl;
            Cerr << "============" << Endl;
        };

        DumpCounters("CreateReader");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);

        for (ui32 i = 0; i < 2; ++i) {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << NYdb::NPersQueue::DebugString(*msg) << "\n";

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);

            UNIT_ASSERT(ev);

            ev->Confirm();
        }


        for (ui32 i = 0; i < 10; ++i) {
            auto writer = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "source" << i);
            bool res = writer->Write("valuevaluevalue", 1);
            UNIT_ASSERT(res);
            res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        DumpCounters("Write");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 10);

        ui32 createEv = 0, destroyEv = 0, dataEv = 0;
        std::vector<ui32> gotDestroy{0, 0};

        auto doRead = [&]() {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << "Got message: " << NYdb::NPersQueue::DebugString(*msg) << "\n";


            if (std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&*msg)) {
                ++dataEv;
                return;
            }

            auto ev1 = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent>(&*msg);
            auto ev2 = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);

            UNIT_ASSERT(ev1 || ev2);

            if (ev1) {
                ++destroyEv;
                UNIT_ASSERT(ev1->GetPartitionStream()->GetPartitionId() < 2);
                gotDestroy[ev1->GetPartitionStream()->GetPartitionId()]++;
            }
            if (ev2) {
                ev2->Confirm(ev2->GetEndOffset());
                ++createEv;
                UNIT_ASSERT(ev2->GetPartitionStream()->GetPartitionId() < 2);
                UNIT_ASSERT_VALUES_EQUAL(gotDestroy[ev2->GetPartitionStream()->GetPartitionId()], 1);

            }
        };

        decompressor->StartFuncs({0, 1, 2, 3, 4});

        DumpCounters("StartFuncs-1");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 10);

        for (ui32 i = 0; i < 5; ++i) {
            doRead();
        }

        UNIT_ASSERT_VALUES_EQUAL(dataEv, 5);

        DumpCounters("doRead(5)");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 5);

        Cerr << ">>>> Restart balancer" << Endl;
        server.AnnoyingClient->RestartBalancerTablet(server.CleverServer->GetRuntime(), DEFAULT_TOPIC_NAME);
        Cerr << ">>>> Balancer restarted" << Endl;

        Sleep(TDuration::Seconds(5));

        decompressor->StartFuncs({5, 6, 7, 8, 9});

        DumpCounters("StartFuncs-2");

        for (ui32 i = 0; i < 4; ++i) {
            doRead();
        }

        UNIT_ASSERT_VALUES_EQUAL(createEv, 2);
        UNIT_ASSERT_VALUES_EQUAL(destroyEv, 2);
        UNIT_ASSERT_VALUES_EQUAL(dataEv, 5);

        DumpCounters("doRead(4)");

        Sleep(TDuration::Seconds(5));

        auto msg = reader->GetEvent(false, 1);
        UNIT_ASSERT(!msg);

        UNIT_ASSERT(!reader->WaitEvent().Wait(TDuration::Seconds(1)));

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), 0);

        DumpCounters("End");
    }

    Y_UNIT_TEST(CheckDeleteTopic) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 2);

        auto driver = server.AnnoyingClient->GetDriver();
        auto decompressor = CreateThreadPoolExecutorWrapper(2);

        NYdb::NPersQueue::TReadSessionSettings settings;
        settings.ConsumerName("shared/user").AppendTopics(SHORT_TOPIC_NAME).ReadOriginal({"dc1"});
        settings.DecompressionExecutor(decompressor);
        auto reader = CreateReader(*driver, settings);

        auto counters = reader->GetCounters();

        auto DumpCounters = [&](const char *message) {
            Cerr << "===== " << message << " =====" << Endl;
            Cerr << "MessagesInflight: " << counters->MessagesInflight->Val() << Endl;
            Cerr << "BytesInflightUncompressed: " << counters->BytesInflightUncompressed->Val() << Endl;
            Cerr << "BytesInflightCompressed: " << counters->BytesInflightCompressed->Val() << Endl;
            Cerr << "BytesInflightTotal: " << counters->BytesInflightTotal->Val() << Endl;
            Cerr << "============" << Endl;
        };

        DumpCounters("CreateReader");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);

        for (ui32 i = 0; i < 2; ++i) {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << NYdb::NPersQueue::DebugString(*msg) << "\n";

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);

            UNIT_ASSERT(ev);

            ev->Confirm();
        }


        for (ui32 i = 0; i < 10; ++i) {
            auto writer = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "source" << i);
            bool res = writer->Write("valuevaluevalue", 1);
            UNIT_ASSERT(res);
            res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        DumpCounters("Write");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 10);

        ui32 createEv = 0, destroyEv = 0, dataEv = 0;
        std::vector<ui32> gotDestroy{0, 0};

        auto doRead = [&]() {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << "Got message: " << NYdb::NPersQueue::DebugString(*msg) << "\n";


            if (std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&*msg)) {
                ++dataEv;
                return;
            }

            auto ev1 = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent>(&*msg);
            auto ev2 = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);

            UNIT_ASSERT(ev1 || ev2);

            if (ev1) {
                ++destroyEv;
                UNIT_ASSERT(ev1->GetPartitionStream()->GetPartitionId() < 2);
                gotDestroy[ev1->GetPartitionStream()->GetPartitionId()]++;
            }
            if (ev2) {
                ev2->Confirm(ev2->GetEndOffset());
                ++createEv;
                UNIT_ASSERT(ev2->GetPartitionStream()->GetPartitionId() < 2);
                UNIT_ASSERT_VALUES_EQUAL(gotDestroy[ev2->GetPartitionStream()->GetPartitionId()], 1);

            }
        };

        decompressor->StartFuncs({0, 1, 2, 3, 4});

        DumpCounters("StartFuncs-1");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 10);

        for (ui32 i = 0; i < 5; ++i) {
            doRead();
        }

        UNIT_ASSERT_VALUES_EQUAL(dataEv, 5);

        DumpCounters("doRead(5)");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 5);

        Cerr << ">>>> Delete topic" << Endl;
        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);
        Cerr << ">>>> Topic deleted" << Endl;

        Sleep(TDuration::Seconds(5));

        doRead();
        doRead();

        //
        // there should be 2 TPartitionStreamClosedEvent events in the queue
        //
        UNIT_ASSERT_VALUES_EQUAL(createEv, 0);
        UNIT_ASSERT_VALUES_EQUAL(destroyEv, 2);
        UNIT_ASSERT_VALUES_EQUAL(dataEv, 5);

        UNIT_ASSERT_VALUES_EQUAL(decompressor->GetExecutedCount(), 5);

        decompressor->StartFuncs({5, 6, 7, 8, 9});

        DumpCounters("StartFuncs-2");

        while (decompressor->GetExecutedCount() < 10) {
            Sleep(TDuration::Seconds(1));
        }

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), 0);

        DumpCounters("End");
    }

    Y_UNIT_TEST(NoDecompressionMemoryLeaks) {

        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        auto driver = server.AnnoyingClient->GetDriver();
        auto decompressor = CreateThreadPoolExecutorWrapper(2);

        NYdb::NPersQueue::TReadSessionSettings settings;
        settings.ConsumerName("shared/user").AppendTopics(SHORT_TOPIC_NAME).ReadOriginal({"dc1"});
        settings.DecompressionExecutor(decompressor);
        settings.MaxMemoryUsageBytes(5_MB);
        settings.Decompress(true);

        auto reader = CreateReader(*driver, settings);

        //
        // there should be 1 TCreatePartitionStreamEvent events in the queue
        //
        {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << ">>>> message: " << NYdb::NPersQueue::DebugString(*msg) << Endl;

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);
            UNIT_ASSERT(ev);

            ev->Confirm();
        }

        for (ui32 i = 0; i < 10; ++i) {
            auto writer = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "source" << i);

            std::string message(1_MB - 1_KB, 'x');

            bool res = writer->Write(message, 1);
            UNIT_ASSERT(res);

            res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        decompressor->StartFuncs({0, 1, 2});
        Sleep(TDuration::Seconds(1));
    }

    enum WhenTheTopicIsDeletedMode {
        AFTER_WRITES,
        AFTER_START_TASKS,
        AFTER_DOREAD
    };

    void WhenTheTopicIsDeletedImpl(WhenTheTopicIsDeletedMode mode, i64 maxMemoryUsageSize, bool decompress, i64 decompressedSize, i64 compressedSize) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        auto driver = server.AnnoyingClient->GetDriver();
        auto decompressor = CreateThreadPoolExecutorWrapper(2);

        NYdb::NPersQueue::TReadSessionSettings settings;
        settings.ConsumerName("shared/user").AppendTopics(SHORT_TOPIC_NAME).ReadOriginal({"dc1"});
        settings.DecompressionExecutor(decompressor);
        settings.MaxMemoryUsageBytes(maxMemoryUsageSize);
        settings.Decompress(decompress);

        auto reader = CreateReader(*driver, settings);
        auto counters = reader->GetCounters();

        auto DumpCounters = [&](const char *message) {
            Cerr << "===== " << message << " =====" << Endl;
            Cerr << "MessagesInflight: " << counters->MessagesInflight->Val() << Endl;
            Cerr << "BytesInflightUncompressed: " << counters->BytesInflightUncompressed->Val() << Endl;
            Cerr << "BytesInflightCompressed: " << counters->BytesInflightCompressed->Val() << Endl;
            Cerr << "BytesInflightTotal: " << counters->BytesInflightTotal->Val() << Endl;
            Cerr << "============" << Endl;
        };

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);

        //
        // there should be 1 TCreatePartitionStreamEvent events in the queue
        //
        {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << ">>>> message: " << NYdb::NPersQueue::DebugString(*msg) << Endl;

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);
            UNIT_ASSERT(ev);

            ev->Confirm();
        }

        for (ui32 i = 0; i < 2; ++i) {
            std::optional<TString> codec;
            if (!decompress) {
                codec = "raw";
            }

            auto writer = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "source" << i, {}, codec);

            std::string message(decompressedSize, 'x');

            bool res = writer->Write(message, 1);
            UNIT_ASSERT(res);

            res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        Sleep(TDuration::Seconds(1));

        DumpCounters("write");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), compressedSize);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), compressedSize);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);

        if (mode == AFTER_WRITES) {
            Cerr << ">>>> Delete topic" << Endl;
            server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);
            Cerr << ">>>> Topic deleted" << Endl;

            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);
            UNIT_ASSERT(std::get_if<NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent>(&*msg));

            decompressor->RunAllTasks();
            Sleep(TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), 0);

            return;
        }

        ui32 dataEv = 0;

        auto doRead = [&]() {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            if (!std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&*msg)) {
                UNIT_FAIL("a TDataReceivedEvent event is expected");
            }

            ++dataEv;
        };

        decompressor->StartFuncs({0});
        Sleep(TDuration::Seconds(1));

        DumpCounters("task #0");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 2);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), compressedSize + decompressedSize);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), compressedSize);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), decompressedSize);

        if (mode == AFTER_START_TASKS) {
            Cerr << ">>>> Delete topic" << Endl;
            server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);
            Cerr << ">>>> Topic deleted" << Endl;

            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);
            UNIT_ASSERT(std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent>(&*msg));

            msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);
            UNIT_ASSERT(std::get_if<NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent>(&*msg));

            decompressor->RunAllTasks();
            Sleep(TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), 0);

            return;
        }

        doRead();
        Sleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(dataEv, 1);

        DumpCounters("read");

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 1);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), compressedSize);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), compressedSize);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);

        if (mode == AFTER_DOREAD) {
            Cerr << ">>>> Delete topic" << Endl;
            server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME);
            Cerr << ">>>> Topic deleted" << Endl;

            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);
            UNIT_ASSERT(std::get_if<NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamClosedEvent>(&*msg));

            decompressor->RunAllTasks();
            Sleep(TDuration::Seconds(1));

            UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), 0);
            UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), 0);

            return;
        }

        UNIT_FAIL("incorrect mode");
    }

    Y_UNIT_TEST(WhenTheTopicIsDeletedBeforeDataIsDecompressed_Compressed) {
        WhenTheTopicIsDeletedImpl(AFTER_WRITES, 1_MB + 1, true, 1_MB - 1_KB, 1050);
    }

    Y_UNIT_TEST(WhenTheTopicIsDeletedAfterDecompressingTheData_Compressed) {
        WhenTheTopicIsDeletedImpl(AFTER_START_TASKS, 1_MB + 1, true, 1_MB - 1_KB, 1050);
    }

    Y_UNIT_TEST(WhenTheTopicIsDeletedAfterReadingTheData_Compressed) {
        WhenTheTopicIsDeletedImpl(AFTER_DOREAD, 1_MB + 1, true, 1_MB - 1_KB, 1050);
    }

    Y_UNIT_TEST(WhenTheTopicIsDeletedBeforeDataIsDecompressed_Uncompressed) {
        WhenTheTopicIsDeletedImpl(AFTER_WRITES, 1_MB + 1, false, 1_MB - 1_KB, 1_MB - 1_KB);
    }

    Y_UNIT_TEST(WhenTheTopicIsDeletedAfterDecompressingTheData_Uncompressed) {
        WhenTheTopicIsDeletedImpl(AFTER_START_TASKS, 1_MB + 1, false, 1_MB - 1_KB, 1_MB - 1_KB);
    }

    Y_UNIT_TEST(WhenTheTopicIsDeletedAfterReadingTheData_Uncompressed) {
        WhenTheTopicIsDeletedImpl(AFTER_DOREAD, 1_MB + 1, false, 1_MB - 1_KB, 1_MB - 1_KB);
    }

    Y_UNIT_TEST(CheckDecompressionTasksWithoutSession) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        auto driver = server.AnnoyingClient->GetDriver();
        auto decompressor = CreateThreadPoolExecutorWrapper(2);

        NYdb::NPersQueue::TReadSessionSettings settings;
        settings.ConsumerName("shared/user").AppendTopics(SHORT_TOPIC_NAME).ReadOriginal({"dc1"});
        settings.DecompressionExecutor(decompressor);

        auto reader = CreateReader(*driver, settings);
        auto counters = reader->GetCounters();

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);

        {
            auto msg = reader->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);
            UNIT_ASSERT(ev);

            ev->Confirm();
        }

        for (ui32 i = 0; i < 2; ++i) {
            auto writer = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "source" << i);

            bool res = writer->Write("abracadabra", 1);
            UNIT_ASSERT(res);

            res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 2);

        reader = nullptr;

        decompressor->RunAllTasks();
        Sleep(TDuration::Seconds(1));

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 2);
    }

    Y_UNIT_TEST(TestWriteStat) {
        auto testWriteStat = [](const TString& originallyProvidedConsumerName,
                                const TString& consumerName,
                                const TString& consumerPath) {
            auto checkCounters = [](auto monPort, const TString& session,
                                    const std::set<std::string>& canonicalSensorNames,
                                    const TString& clientDc, const TString& originDc,
                                    const TString& client, const TString& consumerPath) {
                NJson::TJsonValue counters;

                if (clientDc.empty() && originDc.empty()) {
                    counters = GetClientCountersLegacy(monPort, "pqproxy", session, client, consumerPath);
                } else {
                    counters = GetCountersLegacy(monPort, "pqproxy", session, "account/topic1",
                                                 clientDc, originDc, client, consumerPath);
                }
                const auto sensors = counters["sensors"].GetArray();
                std::set<std::string> sensorNames;
                std::transform(sensors.begin(), sensors.end(),
                               std::inserter(sensorNames, sensorNames.begin()),
                               [](auto& el) {
                                   return el["labels"]["sensor"].GetString();
                               });
                auto equal = sensorNames == canonicalSensorNames;
                UNIT_ASSERT(equal);
            };

            auto settings = PQSettings(0, 1, "10");
            settings.PQConfig.MutableQuotingConfig()->SetEnableQuoting(true);

            NPersQueue::TTestServer server{settings, false};
            auto netDataUpdated = server.PrepareNetDataFile(FormNetData());
            UNIT_ASSERT(netDataUpdated);
            server.StartServer();

            const auto monPort = TPortManager().GetPort();
            auto Counters = server.CleverServer->GetGRpcServerRootCounters();
            NActors::TSyncHttpMon Monitoring({
                .Port = monPort,
                .Address = "localhost",
                .Threads = 3,
                .Title = "root",
                .Host = "localhost",
            });
            Monitoring.RegisterCountersPage("counters", "Counters", Counters);
            Monitoring.Start();

            server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::NET_CLASSIFIER });
            server.EnableLogs({ NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_ERROR);

            auto sender = server.CleverServer->GetRuntime()->AllocateEdgeActor();

            GetClassifierUpdate(*server.CleverServer, sender); //wait for initializing

            server.AnnoyingClient->CreateTopic("rt3.dc1--account--topic1", 10, 10000, 10000, 2000);

            auto driver = server.AnnoyingClient->GetDriver();

            auto writer = CreateWriter(*driver, "account/topic1", "base64:AAAAaaaa____----12", 0, "raw");

            auto msg = writer->GetEvent(true);
            UNIT_ASSERT(msg); // ReadyToAcceptEvent

            auto ev = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
            UNIT_ASSERT(ev);

            TInstant st(TInstant::Now());
            for (ui32 i = 1; i <= 5; ++i) {
                writer->Write(std::move(ev->ContinuationToken), TString(2000, 'a'));
                msg = writer->GetEvent(true);
                UNIT_ASSERT(msg); // ReadyToAcceptEvent

                ev = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
                UNIT_ASSERT(ev);

                msg = writer->GetEvent(true);

                Cerr << DebugString(*msg) << "\n";

                auto ack = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TAcksEvent>(&*msg);
                UNIT_ASSERT(ack);
                if (i == 5) {
                    UNIT_ASSERT(TInstant::Now() - st > TDuration::Seconds(3));
                    UNIT_ASSERT(!ack->Acks.empty());
                    UNIT_ASSERT(ack->Acks.back().Stat);
                }
            }
            checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                          "writeSession",
                          {
                              "BytesWrittenOriginal",
                              "CompactedBytesWrittenOriginal",
                              "DiscardedBytes",
                              "DiscardedMessages",
                              "MessagesWrittenOriginal",
                              "UncompressedBytesWrittenOriginal"
                          },
                          "", "cluster", "", ""
                          );

            checkCounters(monPort,
                          "writeSession",
                          {
                              "BytesInflight",
                              "BytesInflightTotal",
                              "MessagesWrittenByCodec",
                              "Errors",
                              "SessionsActive",
                              "SessionsCreated",
                              // "WithoutAuth" - this counter just not present in this test
                          },
                          "", "cluster", "", ""
                          );

            {
                NYdb::NPersQueue::TReadSessionSettings settings;
                settings.ConsumerName(originallyProvidedConsumerName)
                    .AppendTopics(TString("account/topic1")).ReadOriginal({"dc1"});

                auto reader = CreateReader(*driver, settings);

                auto msg = GetNextMessageSkipAssignment(reader);
                UNIT_ASSERT(msg);

                Cerr << NYdb::NPersQueue::DebugString(*msg) << "\n";

                checkCounters(monPort,
                              "readSession",
                              {
                                  "Commits",
                                  "PartitionsErrors",
                                  "PartitionsInfly",
                                  "PartitionsLocked",
                                  "PartitionsReleased",
                                  "PartitionsToBeLocked",
                                  "PartitionsToBeReleased",
                                  "WaitsForData",
                              },
                              "", "cluster", "", ""
                              );

                checkCounters(monPort,
                              "readSession",
                              {
                                  "BytesInflight",
                                  "Errors",
                                  "PipeReconnects",
                                  "SessionsActive",
                                  "SessionsCreated",
                                  "PartsPerSession"
                              },
                              "", "", consumerName, consumerPath
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesReadFromDC"
                              },
                              "Vla", "", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesRead",
                                  "MessagesRead"
                              },
                              "", "Dc1", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesRead",
                                  "MessagesRead"
                              },
                              "", "cluster", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesRead",
                                  "MessagesRead"
                              },
                              "", "cluster", "", ""
                              );

                checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                              "readSession",
                              {
                                  "BytesRead",
                                  "MessagesRead"
                              },
                              "", "Dc1", consumerName, consumerPath
                              );
            }
        };

        testWriteStat("some@random@consumer", "some@random@consumer", "some/random/consumer");
        testWriteStat("some@user", "some@user", "some/user");
        testWriteStat("shared@user", "shared@user", "shared/user");
        testWriteStat("shared/user", "user", "shared/user");
        testWriteStat("user", "user", "shared/user");
        testWriteStat("some@random/consumer", "some@random@consumer", "some/random/consumer");
        testWriteStat("/some/user", "some@user", "some/user");
    }


    Y_UNIT_TEST(TestWriteSessionsConflicts) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        TPQDataWriter writer("source", server);

        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });

        TString topic = SHORT_TOPIC_NAME;
        TString sourceId = "123";


        auto driver = server.AnnoyingClient->GetDriver();

        auto writer1 = CreateWriter(*driver, topic, sourceId);

        auto msg = writer1->GetEvent(true);
        UNIT_ASSERT(msg); // ReadyToAcceptEvent

        Cerr << DebugString(*msg) << "\n";

        auto ev = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
        UNIT_ASSERT(ev);

        auto writer2 = CreateWriter(*driver, topic, sourceId);

        msg = writer2->GetEvent(true);
        UNIT_ASSERT(msg); // ReadyToAcceptEvent

        Cerr << DebugString(*msg) << "\n";

        ev = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
        UNIT_ASSERT(ev);

        // First session dies.
        UNIT_ASSERT(writer1->WaitEvent().Wait(TDuration::Seconds(10)));

        msg = writer1->GetEvent(true);
        UNIT_ASSERT(msg);

        Cerr << DebugString(*msg) << "\n";

        auto closeEv = std::get_if<NYdb::NPersQueue::TSessionClosedEvent>(&*msg);
        UNIT_ASSERT(closeEv);

        UNIT_ASSERT(!writer2->WaitEvent().Wait(TDuration::Seconds(1)));
    }

/*
    Y_UNIT_TEST(TestLockErrors) {
        return;  // Test is ignored. FIX: KIKIMR-7881

        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        auto pqLib = TPQLib::WithCerrLogger();

        {
            auto [producer, pcResult] = CreateProducer(pqLib, server.GrpcPort, SHORT_TOPIC_NAME, "123");
            for (ui32 i = 1; i <= 11; ++i) {
                auto f = producer->Write(i,  TString(10, 'a'));
                f.Wait();
            }
        }

        TConsumerSettings ss;
        ss.Consumer = "user";
        ss.Server = TServerSetting{"localhost", server.GrpcPort};
        ss.Topics.push_back({SHORT_TOPIC_NAME, {}});
        ss.MaxCount = 1;
        ss.Unpack = false;

        auto [consumer, ccResult] = CreateConsumer(pqLib, ss);
        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.assigned().partition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().read_offset() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().end_offset() == 11);

        auto pp = msg.GetValue().StartRead;
        pp.SetValue(TAssignInfo{0, 5, false});
        auto future = consumer->IsDead();
        future.Wait();
        Cerr << future.GetValue() << "\n";

        std::tie(consumer, ccResult) = CreateConsumer(pqLib, ss);
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.assigned().partition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().read_offset() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().end_offset() == 11);

        pp = msg.GetValue().StartRead;
        pp.SetValue(TAssignInfo{12, 12, false});
        future = consumer->IsDead();
        future.Wait();
        Cerr << future.GetValue() << "\n";

        std::tie(consumer, ccResult) = CreateConsumer(pqLib, ss);
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.assigned().partition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().read_offset() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().end_offset() == 11);

        pp = msg.GetValue().StartRead;
        pp.SetValue(TAssignInfo{6, 7, false});
        future = consumer->IsDead();
        future.Wait();
        Cerr << future.GetValue() << "\n";

        std::tie(consumer, ccResult) = CreateConsumer(pqLib, ss);
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.assigned().partition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().read_offset() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().end_offset() == 11);
        auto assignId = msg.GetValue().Response.assigned().assign_id();
        pp = msg.GetValue().StartRead;
        pp.SetValue(TAssignInfo{5, 0, false});
        consumer->Commit({{assignId, 0}});
        while (true) {
            msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << msg.GetValue().Response << "\n";
            if (msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kCommitted) {
                UNIT_ASSERT(msg.GetValue().Response.committed().cookies_size() == 1);
                UNIT_ASSERT(msg.GetValue().Response.committed().cookies(0).assign_id() == assignId);
                UNIT_ASSERT(msg.GetValue().Response.committed().cookies(0).partition_cookie() == 0);
                break;
            }
        }

        std::tie(consumer, ccResult) = CreateConsumer(pqLib, ss);
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.assigned().partition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().read_offset() == 5);
        UNIT_ASSERT(msg.GetValue().Response.assigned().end_offset() == 11);

        pp = msg.GetValue().StartRead;
        pp.SetValue(TAssignInfo{11, 11, false});
        Sleep(TDuration::Seconds(5));

        std::tie(consumer, ccResult) = CreateConsumer(pqLib, ss);
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.assigned().partition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().read_offset() == 11);
        UNIT_ASSERT(msg.GetValue().Response.assigned().end_offset() == 11);

        pp = msg.GetValue().StartRead;
        pp.SetValue(TAssignInfo{1, 0, true});
        future = consumer->IsDead();
        future.Wait();
        Cerr << future.GetValue() << "\n";

        std::tie(consumer, ccResult) = CreateConsumer(pqLib, ss);
        msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.assigned().partition() == 0);
        UNIT_ASSERT(msg.GetValue().Response.assigned().read_offset() == 11);
        UNIT_ASSERT(msg.GetValue().Response.assigned().end_offset() == 11);

        pp = msg.GetValue().StartRead;
        pp.SetValue(TAssignInfo{0, 0, false});
        future = consumer->IsDead();
        UNIT_ASSERT(!future.Wait(TDuration::Seconds(5)));
    }
*/

    Y_UNIT_TEST(TestBigMessage) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY });

        TPQDataWriter writer2("source", server);

        auto driver = server.AnnoyingClient->GetDriver();

        auto writer = CreateWriter(*driver, SHORT_TOPIC_NAME, "123", 0, "raw");

        auto msg = writer->GetEvent(true);
        UNIT_ASSERT(msg); // ReadyToAcceptEvent

        auto ev = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
        UNIT_ASSERT(ev);

        writer->Write(std::move(ev->ContinuationToken), TString(60_MB, 'a')); //TODO: Increase GRPC_ARG_MAX_SEND_MESSAGE_LENGTH
        {
            msg = writer->GetEvent(true);
            UNIT_ASSERT(msg); // ReadyToAcceptEvent
            auto ev2 = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
            UNIT_ASSERT(ev2);
        }
        {
            msg = writer->GetEvent(true);
            UNIT_ASSERT(msg); // ReadyToAcceptEvent
            auto ev2 = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TAcksEvent>(&*msg);
            UNIT_ASSERT(ev2);
        }
    }

/*
    void TestRereadsWhenDataIsEmptyImpl(bool withWait) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        TPQDataWriter writer("source", server);
        auto pqLib = TPQLib::WithCerrLogger();

        // Write nonempty data
        NKikimr::NPersQueueTests::TRequestWritePQ writeReq(DEFAULT_TOPIC_NAME, 0, "src", 4);

        auto write = [&](const TString& data, bool empty = false) {
            NKikimrPQClient::TDataChunk dataChunk;
            dataChunk.SetCreateTime(42);
            dataChunk.SetSeqNo(++writeReq.SeqNo);
            dataChunk.SetData(data);
            if (empty) {
                dataChunk.SetChunkType(NKikimrPQClient::TDataChunk::GROW); // this guarantees that data will be threated as empty
            }
            TString serialized;
            UNIT_ASSERT(dataChunk.SerializeToString(&serialized));
            server.AnnoyingClient->WriteToPQ(writeReq, serialized);
        };
        write("data1");
        write("data2", true);
        if (!withWait) {
            write("data3");
        }

        ui32 maxCount = 1;
        bool unpack = false;
        ui32 maxInflyRequests = 1;
        ui32 maxMemoryUsage = 1;
        auto [consumer, ccResult] = CreateConsumer(
                pqLib, server.GrpcPort, "user", {SHORT_TOPIC_NAME, {}},
                maxCount, unpack, {}, maxInflyRequests, maxMemoryUsage
        );
        UNIT_ASSERT_C(ccResult.Response.response_case() == MigrationStreamingReadServerMessage::kInitResponse, ccResult.Response);

        auto msg1 = GetNextMessageSkipAssignment(consumer).GetValueSync().Response;

        auto assertHasData = [](const MigrationStreamingReadServerMessage& msg, const TString& data) {
            const auto& d = msg.data_batch();
            UNIT_ASSERT_VALUES_EQUAL_C(d.partition_data_size(), 1, msg);
            UNIT_ASSERT_VALUES_EQUAL_C(d.partition_data(0).batches_size(), 1, msg);
            UNIT_ASSERT_VALUES_EQUAL_C(d.partition_data(0).batches(0).message_data_size(), 1, msg);
            UNIT_ASSERT_VALUES_EQUAL_C(d.partition_data(0).batches(0).message_data(0).data(), data, msg);
        };
        UNIT_ASSERT_VALUES_EQUAL_C(msg1.data_batch().partition_data(0).cookie().partition_cookie(), 1, msg1);
        assertHasData(msg1, "data1");

        auto resp2Future = consumer->GetNextMessage();
        if (withWait) {
            // no data
            UNIT_ASSERT(!resp2Future.HasValue());
            UNIT_ASSERT(!resp2Future.HasException());

            // waits and data doesn't arrive
            Sleep(TDuration::MilliSeconds(100));
            UNIT_ASSERT(!resp2Future.HasValue());
            UNIT_ASSERT(!resp2Future.HasException());

            // write data
            write("data3");
        }
        const auto& msg2 = resp2Future.GetValueSync().Response;
        UNIT_ASSERT_VALUES_EQUAL_C(msg2.data_batch().partition_data(0).cookie().partition_cookie(), 2, msg2);

        assertHasData(msg2, "data3");
    }

    Y_UNIT_TEST(TestRereadsWhenDataIsEmpty) {
        TestRereadsWhenDataIsEmptyImpl(false);
    }

    Y_UNIT_TEST(TestRereadsWhenDataIsEmptyWithWait) {
        TestRereadsWhenDataIsEmptyImpl(true);
    }


    Y_UNIT_TEST(TestLockAfterDrop) {
        NPersQueue::TTestServer server{false};
        server.GrpcServerOptions.SetMaxMessageSize(130_MB);
        server.StartServer();
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.WaitInit(SHORT_TOPIC_NAME);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        auto pqLib = TPQLib::WithCerrLogger();

        auto [producer, pcResult] = CreateProducer(pqLib, server.GrpcPort, SHORT_TOPIC_NAME, "123");
        auto f = producer->Write(1,  TString(1_KB, 'a'));
        f.Wait();

        ui32 maxCount = 1;
        bool unpack = false;
        auto [consumer, ccResult] = CreateConsumer(pqLib, server.GrpcPort, "user", {SHORT_TOPIC_NAME, {}}, maxCount, unpack);
        Cerr << ccResult.Response << "\n";

        auto msg = consumer->GetNextMessage();
        msg.Wait();
        UNIT_ASSERT_C(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned, msg.GetValue().Response);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.assigned().partition() == 0);

        server.CleverServer->GetRuntime()->ResetScheduledCount();
        server.AnnoyingClient->RestartPartitionTablets(server.CleverServer->GetRuntime(), DEFAULT_TOPIC_NAME);

        msg.GetValue().StartRead.SetValue({0,0,false});

        msg = consumer->GetNextMessage();
        UNIT_ASSERT(msg.Wait(TDuration::Seconds(10)));

        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kDataBatch);
    }


    Y_UNIT_TEST(TestMaxNewTopicModel) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->AlterUserAttributes("/", "Root", {{"__extra_path_symbols_allowed", "@"}});
        server.AnnoyingClient->CreateTopic("rt3.dc1--aaa@bbb@ccc--topic", 1);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        auto pqLib = TPQLib::WithCerrLogger();

        {
            auto [producer, pcResult] = CreateProducer(pqLib, server.GrpcPort, "aaa/bbb/ccc/topic", "123");
            UNIT_ASSERT_C(pcResult.Response.server_message_case() == StreamingWriteServerMessage::kInitResponse, pcResult.Response);
            for (ui32 i = 1; i <= 11; ++i) {
                auto f = producer->Write(i,  TString(10, 'a'));
                f.Wait();
                UNIT_ASSERT_C(f.GetValue().Response.server_message_case() == StreamingWriteServerMessage::kBatchWriteResponse, f.GetValue().Response);
            }
        }

        ui32 maxCount = 1;
        bool unpack = false;
        auto [consumer, ccResult] = CreateConsumer(pqLib, server.GrpcPort, "user", {"aaa/bbb/ccc/topic", {}}, maxCount, unpack);
        UNIT_ASSERT_C(ccResult.Response.response_case() == MigrationStreamingReadServerMessage::kInitResponse, ccResult.Response);

        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
    }


    Y_UNIT_TEST(TestReleaseWithAssigns) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 3);

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        auto pqLib = TPQLib::WithCerrLogger();

        TPQDataWriter writer("source", server);

        for (ui32 i = 1; i <= 3; ++i) {
            TString sourceId = "123" + ToString<int>(i);
            ui32 partitionGroup = i;
            auto [producer, pcResult] = CreateProducer(pqLib, server.GrpcPort, SHORT_TOPIC_NAME, sourceId, partitionGroup);

            UNIT_ASSERT(pcResult.Response.server_message_case() == StreamingWriteServerMessage::kInitResponse);
            auto f = producer->Write(i,  TString(10, 'a'));
            f.Wait();
        }

        TConsumerSettings ss;
        ss.Consumer = "user";
        ss.Server = TServerSetting{"localhost", server.GrpcPort};
        ss.Topics.push_back({SHORT_TOPIC_NAME, {}});
        ss.ReadMirroredPartitions = false;
        ss.MaxCount = 3;
        ss.Unpack = false;

        auto [consumer, ccResult] = CreateConsumer(pqLib, ss);
        Cerr << ccResult.Response << "\n";

        for (ui32 i = 1; i <= 3; ++i) {
            auto msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << msg.GetValue().Response << "\n";
            UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
            UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
            UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");
        }

        auto [consumer2, ccResult2] = CreateConsumer(pqLib, ss);
        Cerr << ccResult2.Response << "\n";

        auto msg = consumer->GetNextMessage();
        auto msg2 = consumer2->GetNextMessage();

        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kRelease);
        UNIT_ASSERT(msg.GetValue().Response.release().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.release().cluster() == "dc1");

        UNIT_ASSERT(!msg2.Wait(TDuration::Seconds(1)));

        msg.GetValue().Release.SetValue();

        msg2.Wait();
        Cerr << msg2.GetValue().Response << "\n";

        UNIT_ASSERT(msg2.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg2.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg2.GetValue().Response.assigned().cluster() == "dc1");
    }

    Y_UNIT_TEST(TestSilentRelease) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 3);

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });
        auto pqLib = TPQLib::WithCerrLogger();

        TPQDataWriter writer("source", server);

        TVector<std::pair<ui64, ui64>> cookies;

        for (ui32 i = 1; i <= 3; ++i) {
            TString sourceId = "123" + ToString<int>(i);
            ui32 partitionGroup = i;

            auto [producer, pcResult] = CreateProducer(pqLib, server.GrpcPort, SHORT_TOPIC_NAME, sourceId, partitionGroup);
            Cerr << "===Response: " << pcResult.Response << Endl;
            UNIT_ASSERT(pcResult.Response.server_message_case() == StreamingWriteServerMessage::kInitResponse);
            auto f = producer->Write(i,  TString(10, 'a'));
            f.Wait();
        }

        TConsumerSettings ss;
        ss.Consumer = "user";
        ss.Server = TServerSetting{"localhost", server.GrpcPort};
        ss.Topics.push_back({SHORT_TOPIC_NAME, {}});
        ss.ReadMirroredPartitions = false;
        ss.MaxCount = 1;
        ss.Unpack = false;

        auto [consumer, ccResult] = CreateConsumer(pqLib, ss);
        Cerr << ccResult.Response << "\n";

        for (ui32 i = 1; i <= 3; ++i) {
            auto msg = GetNextMessageSkipAssignment(consumer);
            Cerr << msg.GetValueSync().Response << "\n";
            UNIT_ASSERT(msg.GetValueSync().Response.response_case() == MigrationStreamingReadServerMessage::kDataBatch);
            for (auto& p : msg.GetValue().Response.data_batch().partition_data()) {
                cookies.emplace_back(p.cookie().assign_id(), p.cookie().partition_cookie());
            }
        }

        auto [consumer2, ccResult2] = CreateConsumer(pqLib, ss);
        Cerr << ccResult2.Response << "\n";

        auto msg = consumer->GetNextMessage();
        auto msg2 = consumer2->GetNextMessage();
        UNIT_ASSERT(!msg2.Wait(TDuration::Seconds(1)));
        consumer->Commit(cookies);

        if (msg.GetValueSync().Release.Initialized()) {
            msg.GetValueSync().Release.SetValue();
        }

        msg2.Wait();
        Cerr << msg2.GetValue().Response << "\n";

        UNIT_ASSERT(msg2.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg2.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg2.GetValue().Response.assigned().cluster() == "dc1");
        UNIT_ASSERT(msg2.GetValue().Response.assigned().read_offset() == 1);
    }

*/

/*
    Y_UNIT_TEST(TestDoubleRelease) {
        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(
                DEFAULT_TOPIC_NAME, 1, 8000000, 86400, 50000000, "", 50000000, {"user1"}, {"user1", "user2"}
        );
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::TABLET_AGGREGATOR });
        auto pqLib = TPQLib::WithCerrLogger();
        TPQDataWriter writer("source", server);

        TConsumerSettings ss;
        ss.Consumer = "user";
        ss.Server = TServerSetting{"localhost", server.GrpcPort};
        ss.Topics.push_back({SHORT_TOPIC_NAME, {}});
        ss.ReadMirroredPartitions = false;
        ss.MaxCount = 3;
        ss.Unpack = false;

        auto [consumer, ccResult] = CreateConsumer(pqLib, ss);
        Cerr << ccResult.Response << "\n";

        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kAssigned);
        UNIT_ASSERT(msg.GetValue().Response.assigned().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.assigned().cluster() == "dc1");

        msg = consumer->GetNextMessage();
        UNIT_ASSERT(!msg.Wait(TDuration::Seconds(1)));

        THolder<IConsumer> consumer2;
        do {
            std::tie(consumer2, ccResult) = CreateConsumer(pqLib, ss);
            Cerr << ccResult.Response << "\n";
        } while(!msg.Wait(TDuration::Seconds(1)));

        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kRelease);
        UNIT_ASSERT(msg.GetValue().Response.release().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.release().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.release().forceful_release() == false);

        msg = consumer->GetNextMessage();
        UNIT_ASSERT(!msg.Wait(TDuration::Seconds(1)));

        server.AnnoyingClient->RestartBalancerTablet(server.CleverServer->GetRuntime(), DEFAULT_TOPIC_NAME);
        UNIT_ASSERT(msg.Wait(TDuration::Seconds(1)));

        Cerr << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Response.response_case() == MigrationStreamingReadServerMessage::kRelease);
        UNIT_ASSERT(msg.GetValue().Response.release().topic().path() == SHORT_TOPIC_NAME);
        UNIT_ASSERT(msg.GetValue().Response.release().cluster() == "dc1");
        UNIT_ASSERT(msg.GetValue().Response.release().forceful_release() == true);

        THolder<TEvTabletCounters::TEvTabletLabeledCountersResponse> response;
        TActorId edge = server.CleverServer->GetRuntime()->AllocateEdgeActor();

        do {

            auto actorId = NKikimr::CreateClusterLabeledCountersAggregatorActor(edge, TTabletTypes::PersQueue, 3, "rt3.*--*,user*!!/!!*!!/rt3.*--*", 0); // remove !!
            server.CleverServer->GetRuntime()->Register(actorId);
            response = server.CleverServer->GetRuntime()->
                            GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>();

           Cerr << "FINAL RESPONSE :\n" << response->Record.DebugString() << Endl;
        } while (response->Record.LabeledCountersByGroupSize() == 0);

        Cerr << "MULITREQUEST\n";

        auto actorId = NKikimr::CreateClusterLabeledCountersAggregatorActor(edge, TTabletTypes::PersQueue, 3, "rt3.*--*,user*!!/!!*!!/rt3.*--*", 3); // remove !!
        server.CleverServer->GetRuntime()->Register(actorId);
        response = server.CleverServer->GetRuntime()->
                        GrabEdgeEvent<TEvTabletCounters::TEvTabletLabeledCountersResponse>();

        Cerr << "FINAL RESPONSE2 :\n" << response->Record.DebugString() << Endl;
        UNIT_ASSERT(response->Record.LabeledCountersByGroupSize());

    }

    Y_UNIT_TEST(TestUncompressedSize) {
        TRateLimiterTestSetup setup(NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE);

        setup.CreateTopic("account/topic");

        THolder<IProducer> producer = setup.StartProducer("account/topic", true);

        TString data = TString("12345") * 100;
        for (ui32 i = 0; i < 100; ++i) {
            producer->Write(data);
        }
        auto writeResult = producer->Write(data);
        const auto& writeResponse = writeResult.GetValueSync().Response;
        UNIT_ASSERT_EQUAL_C(Ydb::StatusIds::SUCCESS, writeResponse.status(), "Response: " << writeResponse);

        auto pqLib = TPQLib::WithCerrLogger();
        using namespace NPersQueue;
        auto [consumer, ccResult] = CreateConsumer(pqLib, setup.GetGrpcPort(), "shared/user", {"/account/topic" , {}}, 1000, false);
        Cerr << ccResult.Response << "\n";

        auto msg = consumer->GetNextMessage();
        msg.Wait();
        Cerr << "ASSIGN RESPONSE: " << msg.GetValue().Response << "\n";
        UNIT_ASSERT(msg.GetValue().Type == EMT_ASSIGNED);
        auto pp = msg.GetValue().StartRead;
        pp.SetValue(TAssignInfo());
        Cerr << "START READ\n";
        ui32 count = 0;
        do {
            msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << "GOT LAST MESSAGE: " << msg.GetValue().Response << "\n";
            for (auto& pd : msg.GetValue().Response.data_batch().partition_data()) {
                for (auto & b : pd.batches()) {
                    for (auto& md : b.message_data()) {
                        UNIT_ASSERT(md.uncompressed_size() == 500);
                        ++count;
                    }
                }
            }
            Cerr << count << "\n";
        } while (count < 100);
    }

    Y_UNIT_TEST(TestReadQuotasSimple) {
        TRateLimiterTestSetup setup(NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE, 1000, 1000, true);

        const TString topicPath = "acc/topic1";
        const TString consumerPath = "acc2/reader1";
        setup.CreateTopic(topicPath);
        setup.CreateConsumer(consumerPath);

        THolder<IProducer> producer = setup.StartProducer(topicPath, true);

        auto pqLib = TPQLib::WithCerrLogger();

        auto [consumer, ccResult] = CreateConsumer(
                pqLib, setup.GetGrpcPort(), consumerPath, {topicPath , {}}, 1000, false
        );
        Cerr << ccResult.Response << "\n";

        {
            auto msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << "consumer assign response: " << msg.GetValue().Response << "\n";
            UNIT_ASSERT(msg.GetValue().Type == EMT_ASSIGNED);
            msg.GetValue().StartRead.SetValue(TAssignInfo());
        }

        TVector<NThreading::TFuture<Ydb::PersQueue::TProducerCommitResponse>> writeResults;
        TVector<NThreading::TFuture<Ydb::PersQueue::TConsumerMessage>> readResults;

        for (ui32 readBatches = 0; readBatches < 10; ++readBatches) {
            auto msg = consumer->GetNextMessage();
            while (!msg.HasValue()) {
                producer->Write(TString(std::string(10000, 'A')));
                Sleep(TDuration::MilliSeconds(10));
            }
            const auto& response = msg.GetValue().Response;
            Cerr << "next read response: " << response << "\n";

            for (auto& data : response.data_batch().partition_data()) {
                for (auto& batch : data.batches()) {
                    UNIT_ASSERT(batch.message_data_size() > 0);
                }
            }
        }
    }

    Y_UNIT_TEST(TestReadWithQuoterWithoutResources) {
        if (NSan::ASanIsOn()) {
            return;
        }
        TRateLimiterTestSetup setup(NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE, 1000, 1000, true);

        const TString topicPath = "acc/topic1";
        const TString consumerPath = "acc2/reader1"; // don't create kesus resources
        setup.CreateTopic(topicPath);

        THolder<IProducer> producer = setup.StartProducer(topicPath, true);

        TPQLibSettings pqLibSettings({ .DefaultLogger = new TCerrLogger(DEBUG_LOG_LEVEL) });
        TPQLib PQLib(pqLibSettings);
        auto [consumer, ccResult] = CreateConsumer(PQLib, setup.GetGrpcPort(), consumerPath, {topicPath , {}}, 1000, false);
        Cerr << ccResult.Response << "\n";

        {
            auto msg = consumer->GetNextMessage();
            msg.Wait();
            Cerr << "consumer assign  response: " << msg.GetValue().Response << "\n";
            UNIT_ASSERT(msg.GetValue().Type == EMT_ASSIGNED);
            msg.GetValue().StartRead.SetValue(TAssignInfo());
        }

        TVector<NThreading::TFuture<Ydb::PersQueue::TProducerCommitResponse>> writeResults;
        TVector<NThreading::TFuture<Ydb::PersQueue::TConsumerMessage>> readResults;

        for (ui32 readBatches = 0; readBatches < 10; ++readBatches) {
            auto msg = consumer->GetNextMessage();
            while (!msg.HasValue()) {
                producer->Write(TString(std::string(10000, 'A')));
                Sleep(TDuration::MilliSeconds(10));
            }
            const auto& response = msg.GetValue().Response;
            Cerr << "next read response: " << response << "\n";

            for (auto& data : response.data_batch().partition_data()) {
                for (auto& batch : data.batches()) {
                    UNIT_ASSERT(batch.message_data_size() > 0);
                }
            }
        }
    }

    Y_UNIT_TEST(TestDeletionOfTopic) {
        if (NSan::ASanIsOn()) {
            return;
        }

        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.WaitInit(SHORT_TOPIC_NAME);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY });

        server.AnnoyingClient->DeleteTopic2(DEFAULT_TOPIC_NAME, NPersQueue::NErrorCode::OK, false);
        auto pqLib = TPQLib::WithCerrLogger();

        ui32 maxCount = 1;
        bool unpack = false;
        auto [consumer, ccResult] = CreateConsumer(pqLib, server.GrpcPort, "user", {SHORT_TOPIC_NAME, {}}, maxCount, unpack);
        Cerr << "Consumer create response: " << ccResult.Response << "\n";

        auto isDead = consumer->IsDead();
        isDead.Wait();
        Cerr << "Is dead future value: " << isDead.GetValue() << "\n";
        UNIT_ASSERT_EQUAL(ccResult.Response.Getissues(0).issue_code(), Ydb::PersQueue::ErrorCode::UNKNOWN_TOPIC);
    }
*/
    Y_UNIT_TEST(Codecs_InitWriteSession_DefaultTopicSupportedCodecsInInitResponse) {
        APITestSetup setup{TEST_CASE_NAME};
        grpc::ClientContext context;
        auto session = setup.GetPersQueueService()->StreamingWrite(&context);


        auto serverMessage = setup.InitSession(session);


        auto defaultSupportedCodecs = TVector<Ydb::PersQueue::V1::Codec>{ Ydb::PersQueue::V1::CODEC_RAW, Ydb::PersQueue::V1::CODEC_GZIP, Ydb::PersQueue::V1::CODEC_LZOP };
        auto topicSupportedCodecs = serverMessage.init_response().supported_codecs();
        UNIT_ASSERT_VALUES_EQUAL_C(defaultSupportedCodecs.size(), topicSupportedCodecs.size(), serverMessage.init_response());
        UNIT_ASSERT_C(Equal(defaultSupportedCodecs.begin(), defaultSupportedCodecs.end(), topicSupportedCodecs.begin()), serverMessage.init_response());
    }

    Y_UNIT_TEST(Codecs_WriteMessageWithDefaultCodecs_MessagesAreAcknowledged) {
        APITestSetup setup{TEST_CASE_NAME};
        auto log = setup.GetLog();
        StreamingWriteClientMessage clientMessage;
        auto* writeRequest = clientMessage.mutable_write_request();
        auto sequenceNumber = 1;
        const auto message = NUnitTest::RandomString(250_KB, std::rand());
        auto compress = [](TString data, i32 codecID) {
            Y_UNUSED(codecID);
            return TString(data);
        };
        TVector<char> defaultCodecs{0, 1, 2};
        for (const auto& codecID : defaultCodecs) {
            auto compressedMessage = compress(message, codecID);

            writeRequest->add_sequence_numbers(sequenceNumber++);
            writeRequest->add_message_sizes(message.size());
            writeRequest->add_created_at_ms(TInstant::Now().MilliSeconds());
            writeRequest->add_sent_at_ms(TInstant::Now().MilliSeconds());
            writeRequest->add_blocks_offsets(0);
            writeRequest->add_blocks_part_numbers(0);
            writeRequest->add_blocks_message_counts(1);
            writeRequest->add_blocks_uncompressed_sizes(message.size());
            writeRequest->add_blocks_headers(TString(1, codecID));
            writeRequest->add_blocks_data(compressedMessage);
        }
        auto session = setup.InitWriteSession();


        AssertSuccessfullStreamingOperation(session.first->Write(clientMessage), session.first, &clientMessage);


        StreamingWriteServerMessage serverMessage;
        log << TLOG_INFO << "Wait for write acknowledgement";
        AssertSuccessfullStreamingOperation(session.first->Read(&serverMessage), session.first);
        UNIT_ASSERT_C(serverMessage.server_message_case() == StreamingWriteServerMessage::kBatchWriteResponse, serverMessage);
        UNIT_ASSERT_VALUES_EQUAL_C(defaultCodecs.size(), serverMessage.batch_write_response().offsets_size(), serverMessage);
    }

    Y_UNIT_TEST(Codecs_WriteMessageWithNonDefaultCodecThatHasToBeConfiguredAdditionally_SessionClosedWithBadRequestError) {
        APITestSetup setup{TEST_CASE_NAME};
        auto log = setup.GetLog();
        StreamingWriteClientMessage clientMessage;
        auto* writeRequest = clientMessage.mutable_write_request();
        const auto message = NUnitTest::RandomString(250_KB, std::rand());
        const auto codecID = 3;
        writeRequest->add_sequence_numbers(1);
        writeRequest->add_message_sizes(message.size());
        writeRequest->add_created_at_ms(TInstant::Now().MilliSeconds());
        writeRequest->add_sent_at_ms(TInstant::Now().MilliSeconds());
        writeRequest->add_blocks_offsets(0);
        writeRequest->add_blocks_part_numbers(0);
        writeRequest->add_blocks_message_counts(1);
        writeRequest->add_blocks_uncompressed_sizes(message.size());
        writeRequest->add_blocks_headers(TString(1, codecID));
        writeRequest->add_blocks_data(message);
        auto session = setup.InitWriteSession();


        AssertSuccessfullStreamingOperation(session.first->Write(clientMessage), session.first, &clientMessage);

        log << TLOG_INFO << "Wait for session to die";
        AssertStreamingSessionDead(session.first, Ydb::StatusIds::BAD_REQUEST, Ydb::PersQueue::ErrorCode::BAD_REQUEST);
    }

    StreamingWriteClientMessage::InitRequest GenerateSessionSetupWithPreferredCluster(const TString preferredCluster) {
        StreamingWriteClientMessage::InitRequest sessionSetup;
        sessionSetup.set_preferred_cluster(preferredCluster);
        sessionSetup.set_message_group_id("test-message-group-id-" + preferredCluster);
        return sessionSetup;
    };

    Y_UNIT_TEST(PreferredCluster_TwoEnabledClustersAndWriteSessionsWithDifferentPreferredCluster_SessionWithMismatchedClusterDiesAndOthersAlive) {
        APITestSetup setup{TEST_CASE_NAME};
        auto log = setup.GetLog();

        setup.GetPQConfig().SetClustersUpdateTimeoutSec(0);
        setup.GetPQConfig().SetRemoteClusterEnabledDelaySec(0);
        setup.GetPQConfig().SetCloseClientSessionWithEnabledRemotePreferredClusterDelaySec(0);

        auto sessionWithNoPreferredCluster = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster(TString()));
        auto sessionWithLocalPreffedCluster = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster(setup.GetLocalCluster()));
        auto sessionWithRemotePrefferedCluster = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster(setup.GetRemoteCluster()), true);

        grpc::ClientContext context;
        auto sessionWithNoInitialization = setup.GetPersQueueService()->StreamingWrite(&context);

        log << TLOG_INFO << "Wait for session with remote preferred cluster to die";
        AssertStreamingSessionDead(sessionWithRemotePrefferedCluster.Stream, Ydb::StatusIds::ABORTED, Ydb::PersQueue::ErrorCode::PREFERRED_CLUSTER_MISMATCHED, sessionWithRemotePrefferedCluster.FirstMessage);
        AssertStreamingSessionAlive(sessionWithNoPreferredCluster.first);
        AssertStreamingSessionAlive(sessionWithLocalPreffedCluster.first);

        setup.InitSession(sessionWithNoInitialization);
        AssertStreamingSessionAlive(sessionWithNoInitialization);
    }

    Y_UNIT_TEST(PreferredCluster_DisabledRemoteClusterAndWriteSessionsWithDifferentPreferredClusterAndLaterRemoteClusterEnabled_SessionWithMismatchedClusterDiesAfterPreferredClusterEnabledAndOtherSessionsAlive) {
        APITestSetup setup{TEST_CASE_NAME};
        auto log = setup.GetLog();
        log << TLOG_INFO << "Disable remote cluster " << setup.GetRemoteCluster().Quote();
        setup.GetFlatMsgBusPQClient().UpdateDC(setup.GetRemoteCluster(), false, false);
        setup.GetPQConfig().SetClustersUpdateTimeoutSec(0);
        setup.GetPQConfig().SetRemoteClusterEnabledDelaySec(0);
        setup.GetPQConfig().SetCloseClientSessionWithEnabledRemotePreferredClusterDelaySec(0);
        auto sessionWithNoPreferredCluster = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster(TString()));
        auto sessionWithLocalPreffedCluster = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster(setup.GetLocalCluster()));
        auto sessionWithRemotePrefferedCluster = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster(setup.GetRemoteCluster()));
        AssertStreamingSessionAlive(sessionWithNoPreferredCluster.first);
        AssertStreamingSessionAlive(sessionWithLocalPreffedCluster.first);
        AssertStreamingSessionAlive(sessionWithRemotePrefferedCluster.first);

        log << TLOG_INFO << "Enable remote cluster " << setup.GetRemoteCluster().Quote();
        setup.GetFlatMsgBusPQClient().UpdateDC(setup.GetRemoteCluster(), false, true);

        log << TLOG_INFO << "Wait for session with remote preferred cluster to die";
        AssertStreamingSessionDead(sessionWithRemotePrefferedCluster.first, Ydb::StatusIds::ABORTED, Ydb::PersQueue::ErrorCode::PREFERRED_CLUSTER_MISMATCHED);
        AssertStreamingSessionAlive(sessionWithNoPreferredCluster.first);
        AssertStreamingSessionAlive(sessionWithLocalPreffedCluster.first);
    }

    Y_UNIT_TEST(PreferredCluster_EnabledRemotePreferredClusterAndCloseClientSessionWithEnabledRemotePreferredClusterDelaySec_SessionDiesOnlyAfterDelay) {
        APITestSetup setup{TEST_CASE_NAME};
        auto log = setup.GetLog();
        setup.GetPQConfig().SetClustersUpdateTimeoutSec(0);
        setup.GetPQConfig().SetRemoteClusterEnabledDelaySec(0);
        setup.GetPQConfig().SetCloseClientSessionWithEnabledRemotePreferredClusterDelaySec(3);

        const auto edgeActorID = setup.GetServer().GetRuntime()->AllocateEdgeActor();

        setup.GetServer().GetRuntime()->Send(new IEventHandle(NPQ::NClusterTracker::MakeClusterTrackerID(), edgeActorID, new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe));
        log << TLOG_INFO << "Wait for cluster tracker event";
        auto clustersUpdate = setup.GetServer().GetRuntime()->GrabEdgeEvent<NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate>();

        TInstant now = TInstant::Now();
        auto session = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster(setup.GetRemoteCluster()));

        AssertStreamingSessionDead(session.first, Ydb::StatusIds::ABORTED, Ydb::PersQueue::ErrorCode::PREFERRED_CLUSTER_MISMATCHED);
        UNIT_ASSERT(TInstant::Now() - now > TDuration::Seconds(3));
    }

    Y_UNIT_TEST(PreferredCluster_NonExistentPreferredCluster_SessionDiesOnlyAfterDelay) {
        APITestSetup setup{TEST_CASE_NAME};
        auto log = setup.GetLog();
        setup.GetPQConfig().SetClustersUpdateTimeoutSec(0);
        setup.GetPQConfig().SetRemoteClusterEnabledDelaySec(0);
        setup.GetPQConfig().SetCloseClientSessionWithEnabledRemotePreferredClusterDelaySec(2);

        TInstant now(TInstant::Now());
        auto session = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster("non-existent-cluster"));
        AssertStreamingSessionAlive(session.first);

        AssertStreamingSessionDead(session.first, Ydb::StatusIds::ABORTED, Ydb::PersQueue::ErrorCode::PREFERRED_CLUSTER_MISMATCHED);
        UNIT_ASSERT(TInstant::Now() - now > TDuration::MilliSeconds(1999));
    }

    Y_UNIT_TEST(PreferredCluster_EnabledRemotePreferredClusterAndRemoteClusterEnabledDelaySec_SessionDiesOnlyAfterDelay) {
        APITestSetup setup{TEST_CASE_NAME};
        auto log = setup.GetLog();
        setup.GetPQConfig().SetClustersUpdateTimeoutSec(0);
        setup.GetPQConfig().SetCloseClientSessionWithEnabledRemotePreferredClusterDelaySec(2);
        const auto edgeActorID = setup.GetServer().GetRuntime()->AllocateEdgeActor();

        setup.GetPQConfig().SetRemoteClusterEnabledDelaySec(0);

        TInstant now(TInstant::Now());
        auto session = setup.InitWriteSession(GenerateSessionSetupWithPreferredCluster(setup.GetRemoteCluster()));

        setup.GetServer().GetRuntime()->Send(new IEventHandle(NPQ::NClusterTracker::MakeClusterTrackerID(), edgeActorID, new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe));
        log << TLOG_INFO << "Wait for cluster tracker event";
        auto clustersUpdate = setup.GetServer().GetRuntime()->GrabEdgeEvent<NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate>();
        AssertStreamingSessionAlive(session.first);

        AssertStreamingSessionDead(session.first, Ydb::StatusIds::ABORTED, Ydb::PersQueue::ErrorCode::PREFERRED_CLUSTER_MISMATCHED);

        UNIT_ASSERT(TInstant::Now() - now > TDuration::MilliSeconds(1999));
    }

    Y_UNIT_TEST(PreferredCluster_RemotePreferredClusterEnabledWhileSessionInitializing_SessionDiesOnlyAfterInitializationAndDelay) {
        APITestSetup setup{TEST_CASE_NAME};
        auto log = setup.GetLog();
        setup.GetPQConfig().SetClustersUpdateTimeoutSec(0);
        setup.GetPQConfig().SetRemoteClusterEnabledDelaySec(0);
        setup.GetPQConfig().SetCloseClientSessionWithEnabledRemotePreferredClusterDelaySec(2);
        const auto edgeActorID = setup.GetServer().GetRuntime()->AllocateEdgeActor();
        setup.GetFlatMsgBusPQClient().UpdateDC(setup.GetRemoteCluster(), false, false);

        grpc::ClientContext context;
        auto session = setup.GetPersQueueService()->StreamingWrite(&context);

        setup.GetFlatMsgBusPQClient().UpdateDC(setup.GetRemoteCluster(), false, true);

        setup.GetServer().GetRuntime()->Send(new IEventHandle(NPQ::NClusterTracker::MakeClusterTrackerID(), edgeActorID, new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe));
        log << TLOG_INFO << "Wait for cluster tracker event";
        auto clustersUpdate = setup.GetServer().GetRuntime()->GrabEdgeEvent<NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate>();
        TInstant now(TInstant::Now());
        setup.InitSession(session, GenerateSessionSetupWithPreferredCluster(setup.GetRemoteCluster()));

        AssertStreamingSessionAlive(session);

        log << TLOG_INFO << "Set small delay and wait for initialized session with remote preferred cluster to die";
        AssertStreamingSessionDead(session, Ydb::StatusIds::ABORTED, Ydb::PersQueue::ErrorCode::PREFERRED_CLUSTER_MISMATCHED);

        UNIT_ASSERT(TInstant::Now() - now > TDuration::MilliSeconds(1999));

    }

    Y_UNIT_TEST(SchemeOperationsTest) {
        NPersQueue::TTestServer server;
        server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });
        TString topic1 = "rt3.dc1--acc--topic1";
        TString topic3 = "rt3.dc1--acc--topic3";
        server.AnnoyingClient->CreateTopic(topic1, 1);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.AnnoyingClient->CreateConsumer("user");

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> StubP_;
        std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> TopicStubP_;

        {
            Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            StubP_ = Ydb::PersQueue::V1::PersQueueService::NewStub(Channel_);
            TopicStubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);
        }

        do {
            Ydb::Topic::CreateTopicRequest request;
            Ydb::Topic::CreateTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);

            grpc::ClientContext rcontext;
            rcontext.AddMetadata("x-ydb-auth-ticket", "user@" BUILTIN_ACL_DOMAIN);

            auto status = TopicStubP_->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            if (response.operation().status() == Ydb::StatusIds::UNAVAILABLE) {
                Sleep(TDuration::Seconds(1));
                continue;
            }
            Cerr << response.operation() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::UNAUTHORIZED);
            break;
        } while (true);

        {
            // local cluster
            Ydb::Topic::CreateTopicRequest request;
            Ydb::Topic::CreateTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);

            request.mutable_partitioning_settings()->set_min_active_partitions(2);
            request.mutable_retention_period()->set_seconds(TDuration::Days(1).Seconds());
            (*request.mutable_attributes())["_max_partition_storage_size"] = "1000";
            request.set_partition_write_speed_bytes_per_second(1000);
            request.set_partition_write_burst_bytes(1000);

            request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_RAW);
            request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_ZSTD);
            request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_CUSTOM);
            request.mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_CUSTOM + 5);

            auto consumer = request.add_consumers();
            consumer->set_name("first-consumer");
            consumer->set_important(false);
            consumer->mutable_read_from()->set_seconds(11223344);
            (*consumer->mutable_attributes())["_version"] = "5000";
            grpc::ClientContext rcontext;

            auto status = TopicStubP_->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
            server.AnnoyingClient->AddTopic(topic3);

        }


        auto driver = server.AnnoyingClient->GetDriver();

        auto client = NYdb::NScheme::TSchemeClient(*driver);
        auto lsRes = client.ListDirectory("/Root/PQ").GetValueSync();
        UNIT_ASSERT(lsRes.IsSuccess());
        for (auto& entry : lsRes.GetChildren()) {
            Cerr << "ENTRY: " << entry.Name << " type " << (int)entry.Type << "\n";
        }

        auto alter =[&TopicStubP_](const Ydb::Topic::AlterTopicRequest& request, Ydb::StatusIds::StatusCode statusCode, bool auth)
        {
            Ydb::Topic::AlterTopicResponse response;

            grpc::ClientContext rcontext;
            if (auth)
                rcontext.AddMetadata("x-ydb-auth-ticket", "user@" BUILTIN_ACL_DOMAIN);

            auto status = TopicStubP_->AlterTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::AlterTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), statusCode);
        };

        Ydb::Topic::AlterTopicRequest request;
        request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);

        request.mutable_set_retention_period()->set_seconds(TDuration::Days(2).Seconds());
        request.mutable_alter_partitioning_settings()->set_set_min_active_partitions(1);
        alter(request, Ydb::StatusIds::SCHEME_ERROR, true);
        alter(request, Ydb::StatusIds::BAD_REQUEST, false);
        request.mutable_alter_partitioning_settings()->set_set_min_active_partitions(3);
        request.set_set_retention_storage_mb(-2);
        alter(request, Ydb::StatusIds::BAD_REQUEST, false);
        request.set_set_retention_storage_mb(0);
        alter(request, Ydb::StatusIds::SUCCESS, false);
        auto rr = request.add_add_consumers();
        alter(request, Ydb::StatusIds::BAD_REQUEST, false);

        request.mutable_set_supported_codecs()->add_codecs(Ydb::Topic::CODEC_LZOP);
        request.mutable_set_supported_codecs()->add_codecs(Ydb::Topic::CODEC_CUSTOM + 5);

        request.set_set_partition_write_speed_bytes_per_second(123);
        (*request.mutable_alter_attributes())["_max_partition_storage_size"] = "234";
        rr->set_name("consumer");

        rr->mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_LZOP);
        rr->mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_CUSTOM + 5);

        rr->set_important(true);
        rr->mutable_read_from()->set_seconds(111);
        (*rr->mutable_attributes())["_version"] = "567";

        rr = request.add_add_consumers();
        rr->set_name("consumer2");
        rr->set_important(true);

        (*request.mutable_alter_attributes())["_allow_unauthenticated_read"] = "true";

        (*request.mutable_alter_attributes())["_partitions_per_tablet"] = "5";

        rr->mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_LZOP);

        alter(request, Ydb::StatusIds::BAD_REQUEST, false);

        rr->mutable_supported_codecs()->add_codecs(Ydb::Topic::CODEC_CUSTOM + 5);

        alter(request, Ydb::StatusIds::SUCCESS, false);

        request = Ydb::Topic::AlterTopicRequest{};
        request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);
        alter(request, Ydb::StatusIds::SUCCESS, false);

        request.add_drop_consumers("consumer2");
        auto ac = request.add_alter_consumers();
        ac->set_name("first-consumer");
        ac->set_set_important(false);
        alter(request, Ydb::StatusIds::SUCCESS, false);

        alter(request, Ydb::StatusIds::NOT_FOUND, false);

        request.clear_drop_consumers();
        (*ac->mutable_alter_attributes())["_version"] = "";
        alter(request, Ydb::StatusIds::SUCCESS, false);

        TString topic4 = "rt3.dc1--acc--topic4";
        server.AnnoyingClient->CreateTopic(topic4, 3); //ensure creation
        auto res = server.AnnoyingClient->DescribeTopic({topic3});
        Cerr << res.DebugString();
        TString resultDescribe = R"___(TopicInfo {
  Topic: "rt3.dc1--acc--topic3"
  NumPartitions: 3
  Config {
    PartitionConfig {
      MaxCountInPartition: 2147483647
      MaxSizeInPartition: 234
      LifetimeSeconds: 172800
      SourceIdLifetimeSeconds: 1382400
      WriteSpeedInBytesPerSecond: 123
      BurstSize: 1000
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      ExplicitChannelProfiles {
        PoolKind: "test"
      }
      SourceIdMaxCounts: 6000000
    }
    Version: 6
    LocalDC: true
    RequireAuthWrite: true
    RequireAuthRead: false
    Producer: "acc"
    Ident: "acc"
    Topic: "topic3"
    DC: "dc1"
    FormatVersion: 0
    Codecs {
      Ids: 2
      Ids: 10004
      Codecs: "lzop"
      Codecs: "CUSTOM"
    }
    TopicPath: "/Root/PQ/rt3.dc1--acc--topic3"
    YdbDatabasePath: "/Root"
    Consumers {
      Name: "first-consumer"
      ReadFromTimestampsMs: 11223344000
      FormatVersion: 0
      Codec {
      }
      ServiceType: "data-streams"
      Version: 0
    }
    Consumers {
      Name: "consumer"
      ReadFromTimestampsMs: 111000
      FormatVersion: 0
      Codec {
        Ids: 2
        Ids: 10004
        Codecs: "lzop"
        Codecs: "CUSTOM"
      }
      ServiceType: "data-streams"
      Version: 567
      Important: true
    }
  }
  ErrorCode: OK
}
)___";

        Cerr << ">>>>> " << res.DebugString() << Endl;

        UNIT_ASSERT_VALUES_EQUAL(res.DebugString(), resultDescribe);

        Cerr << "DESCRIBES:\n";
        {
            Ydb::Topic::DescribeTopicRequest request;
            Ydb::Topic::DescribeTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);
            grpc::ClientContext rcontext;
            rcontext.AddMetadata("x-ydb-auth-ticket", "user@" BUILTIN_ACL_DOMAIN);

            auto status = TopicStubP_->DescribeTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SCHEME_ERROR); // muts be Ydb::StatusIds::UNAUTHORIZED);
        }

        {
            Ydb::Topic::DescribeTopicRequest request;
            Ydb::Topic::DescribeTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--topic123");
            grpc::ClientContext rcontext;

            auto status = TopicStubP_->DescribeTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SCHEME_ERROR);
        }

        Ydb::Topic::DescribeTopicResult res1;

        {
            Ydb::Topic::DescribeTopicRequest request;
            Ydb::Topic::DescribeTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->DescribeTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);

            Cerr << response.DebugString() << "\n" << res.DebugString() << "\n";

            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
            res1 = res;
        }

        request = Ydb::Topic::AlterTopicRequest{};
        request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);
        alter(request, Ydb::StatusIds::SUCCESS, false);

        {
            Ydb::Topic::DescribeTopicRequest request;
            Ydb::Topic::DescribeTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);
            grpc::ClientContext rcontext;

            auto status = TopicStubP_->DescribeTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DescribeTopicResult descrRes;
            response.operation().result().UnpackTo(&descrRes);
            Cerr << response.DebugString() << "\n" << descrRes.DebugString() << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(descrRes.DebugString(), res1.DebugString());

            {
                NYdb::TDriverConfig driverCfg;
                driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort);
                std::shared_ptr<NYdb::TDriver> ydbDriver(new NYdb::TDriver(driverCfg));
                auto topicClient = NYdb::NTopic::TTopicClient(*ydbDriver);

                auto res = topicClient.DescribeTopic("/Root/PQ/" + topic3);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());

                auto res2 = NYdb::TProtoAccessor::GetProto(res.GetValue().GetTopicDescription());
                Cerr << res2 << "\n";

                UNIT_ASSERT_VALUES_EQUAL(descrRes.DebugString(), res2.DebugString());
                {
                    NYdb::NTopic::TCreateTopicSettings settings;
                    settings.PartitioningSettings(1,1)
                        .AppendSupportedCodecs((NYdb::NTopic::ECodec)10010)
                        .PartitionWriteSpeedBytesPerSecond(1024)
                        .AppendSupportedCodecs(NYdb::NTopic::ECodec::GZIP)
                        .AddAttribute("_partitions_per_tablet", "10")
                        .BeginAddConsumer("consumer").ReadFrom(TInstant::Seconds(112233))
                                                     .Important(true)
                                                     .AddAttribute("_version", "5")
                        .EndAddConsumer()
                        .AppendSupportedCodecs((NYdb::NTopic::ECodec)10011);

                    auto res = topicClient.CreateTopic("/Root/PQ/" + topic3 + "2", settings);
                    res.Wait();
                    Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                    UNIT_ASSERT(res.GetValue().IsSuccess());
                }

                {
                    NYdb::NTopic::TAlterTopicSettings settings;
                    settings.AlterPartitioningSettings(2,2)
                        .AppendSetSupportedCodecs((NYdb::NTopic::ECodec)10022)
                        .SetPartitionWriteSpeedBytesPerSecond(102400)
                        .SetRetentionPeriod(TDuration::Days(2))
                        .BeginAlterAttributes().Add("_partitions_per_tablet", "")
                                               .Drop("_abc_id")
                        .EndAlterAttributes()
                        .BeginAlterConsumer("consumer").SetReadFrom(TInstant::Seconds(1122))
                                                       .BeginAlterAttributes().Alter("_version", "5")
                                                       .EndAlterAttributes()
                        .EndAlterConsumer()
                        .AppendSetSupportedCodecs((NYdb::NTopic::ECodec)10020);

                    auto res = topicClient.AlterTopic("/Root/PQ/" + topic3 + "2", settings);
                    res.Wait();
                    Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                    UNIT_ASSERT(res.GetValue().IsSuccess());
                }

                res = topicClient.DescribeTopic("/Root/PQ/" + topic3 + "2");
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());
                res2 = NYdb::TProtoAccessor::GetProto(res.GetValue().GetTopicDescription());
                Cerr << "ANOTHER TOPIC: " << res2 << "\n";
                auto& description = res.GetValue().GetTopicDescription();
                UNIT_ASSERT_VALUES_EQUAL(description.GetTotalPartitionsCount(), 2);
                UNIT_ASSERT_VALUES_EQUAL(description.GetConsumers().size(), 1);
                TVector<NYdb::NTopic::ECodec> codecs = {(NYdb::NTopic::ECodec)10022, (NYdb::NTopic::ECodec)10020};
                UNIT_ASSERT_VALUES_EQUAL(description.GetSupportedCodecs(), codecs);
                UNIT_ASSERT_VALUES_EQUAL(description.GetEffectivePermissions().size(), 0);
            }

        }

        {
            Ydb::Topic::DropTopicRequest request;
            Ydb::Topic::DropTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);
            grpc::ClientContext rcontext;
            auto status = TopicStubP_->DropTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DropTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
            server.AnnoyingClient->RemoveTopic(topic3);
        }

        {
            Ydb::Topic::DropTopicRequest request;
            Ydb::Topic::DropTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic3);

            grpc::ClientContext rcontext;
            auto status = TopicStubP_->DropTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DropTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SCHEME_ERROR);
        }

        server.AnnoyingClient->CreateTopic("rt3.dc1--acc--topic5", 1); //ensure creation
        server.AnnoyingClient->DescribeTopic({topic3}, true);


        {
            NYdb::TDriverConfig driverCfg;
            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort);
            std::shared_ptr<NYdb::TDriver> ydbDriver(new NYdb::TDriver(driverCfg));
            auto pqClient = NYdb::NPersQueue::TPersQueueClient(*ydbDriver);

            auto res = pqClient.CreateTopic("/Root/PQ/rt3.dc1--acc2--topic2");
            res.Wait();
            Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
        }

        for (ui32 i = 0; i < 5; ++ i) {
            auto writer = CreateWriter(*driver, "acc/topic4", TStringBuilder() << "abacaba" << i);
            auto ev = writer->GetEvent(true);
            auto ct = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(&*ev);
            UNIT_ASSERT(ct);
            writer->Write(std::move(ct->ContinuationToken), "1234567890");
            UNIT_ASSERT(ev.Defined());
            while(true) {
                ev = writer->GetEvent(true);
                auto ack = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TAcksEvent>(&*ev);
                if (ack) {
                    break;
                }
            }
        }

        {
            NYdb::TDriverConfig driverCfg;
            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort);
            std::shared_ptr<NYdb::TDriver> ydbDriver(new NYdb::TDriver(driverCfg));
            auto topicClient = NYdb::NTopic::TTopicClient(*ydbDriver);

            auto res = topicClient.DescribeTopic("/Root/PQ/" + topic4, NYdb::NTopic::TDescribeTopicSettings{}.IncludeStats(true));
            res.Wait();
            Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
            UNIT_ASSERT(res.GetValue().IsSuccess());

            auto res2 = NYdb::TProtoAccessor::GetProto(res.GetValue().GetTopicDescription());
            Cerr << res2 << "\n";
            UNIT_ASSERT(res.GetValue().GetTopicDescription().GetPartitions().size() == 3);
            UNIT_ASSERT(res.GetValue().GetTopicDescription().GetPartitions()[0].GetPartitionStats());
            UNIT_ASSERT(res.GetValue().GetTopicDescription().GetPartitions()[0].GetPartitionStats()->GetEndOffset() > 0);
        }

        {
            Ydb::Topic::DescribeTopicRequest request;
            Ydb::Topic::DescribeTopicResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic4);
            request.set_include_stats(true);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->DescribeTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);

            Cerr << response.DebugString() << "\n" << res.DebugString() << "\n";

            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res.topic_stats().store_size_bytes(), 0);
            UNIT_ASSERT_GE(res.partitions(0).partition_stats().partition_offsets().end(), 1);
        }

        auto reader1 = CreateReader(
                *driver,
                NYdb::NPersQueue::TReadSessionSettings()
                    .AppendTopics(
                        NYdb::NPersQueue::TTopicReadSettings("acc/topic4")
                    )
                    .ConsumerName("shared/user")
                    .ReadOnlyOriginal(true)
            );
        int numLocks = 3;
        while (numLocks > 0) {
            auto msg = reader1->GetEvent(true, 1);
            UNIT_ASSERT(msg);

            Cerr << "===Got message: " << NYdb::NPersQueue::DebugString(*msg) << "\n";

            auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);
            UNIT_ASSERT(ev);
            --numLocks;
        }

        auto reader2 = CreateReader(
                *driver,
                NYdb::NPersQueue::TReadSessionSettings()
                    .AppendTopics(
                        NYdb::NPersQueue::TTopicReadSettings("acc/topic4")
                    )
                    .ConsumerName("shared/user")
                    .ReadOnlyOriginal(true)
            );

        numLocks = 1;
        while (numLocks > 0) {
            {
                auto msg = reader1->GetEvent(true, 1);
                UNIT_ASSERT(msg);
                Cerr << "===Got message: " << NYdb::NPersQueue::DebugString(*msg) << "\n";

                auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent>(&*msg);
                UNIT_ASSERT(ev);
                ev->Confirm();
            }
            {
                auto msg = reader2->GetEvent(true, 1);
                UNIT_ASSERT(msg);

                Cerr << "===Got message: " << NYdb::NPersQueue::DebugString(*msg) << "\n";

                auto ev = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*msg);
                UNIT_ASSERT(ev);
            }
            --numLocks;
        }

        {
            Ydb::Topic::DescribeConsumerRequest request;
            Ydb::Topic::DescribeConsumerResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic4);
            request.set_consumer("user");
            request.set_include_stats(true);
            grpc::ClientContext rcontext;

            auto status = TopicStubP_->DescribeConsumer(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DescribeConsumerResult res;
            response.operation().result().UnpackTo(&res);

            Cerr << "DESCRIBE CONSUMER RESULT:\n" << response << "\n" << res.DebugString() << "\n";

//            UNIT_ASSERT_GE(res.partitions(0).partition_stats().partition_offsets().end(), 1);
            //TODO: check here some stats from describe consumer
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT_VALUES_EQUAL(res.partitions_size(), 3);
            UNIT_ASSERT(res.partitions(0).partition_consumer_stats().read_session_id().size() > 0);
            UNIT_ASSERT(res.partitions(1).partition_consumer_stats().read_session_id().size() > 0);
            UNIT_ASSERT(res.partitions(2).partition_consumer_stats().read_session_id().size() > 0);

        }

        {
            Ydb::Topic::DescribeConsumerRequest request;
            Ydb::Topic::DescribeConsumerResponse response;
            request.set_path(TStringBuilder() << "/Root/PQ/" << topic4);
            request.set_consumer("not-consumer");
            request.set_include_stats(true);

            grpc::ClientContext rcontext;

            auto status = TopicStubP_->DescribeConsumer(&rcontext, request, &response);

            Cerr << response << "\n" << res << "\n";

            UNIT_ASSERT(status.ok());
            Ydb::Topic::DescribeConsumerResult res;
            response.operation().result().UnpackTo(&res);

            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SCHEME_ERROR);
        }
        {
            NYdb::TDriverConfig driverCfg;
            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort);
            std::shared_ptr<NYdb::TDriver> ydbDriver(new NYdb::TDriver(driverCfg));
            auto topicClient = NYdb::NTopic::TTopicClient(*ydbDriver);

            auto res = topicClient.DescribeConsumer("/Root/PQ/" + topic4, "user", NYdb::NTopic::TDescribeConsumerSettings{}.IncludeStats(true));
            res.Wait();
            Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
            UNIT_ASSERT(res.GetValue().IsSuccess());

            auto res2 = NYdb::TProtoAccessor::GetProto(res.GetValue().GetConsumerDescription());
            Cerr << res2 << "\n";
            UNIT_ASSERT(res.GetValue().GetConsumerDescription().GetPartitions().size() == 3);
            UNIT_ASSERT(res.GetValue().GetConsumerDescription().GetPartitions()[0].GetPartitionStats());
            UNIT_ASSERT(res.GetValue().GetConsumerDescription().GetPartitions()[0].GetPartitionStats()->GetEndOffset() > 0);
            UNIT_ASSERT(res.GetValue().GetConsumerDescription().GetPartitions()[0].GetPartitionConsumerStats());
        }
    }

    Y_UNIT_TEST(SchemeOperationFirstClassCitizen) {
        TServerSettings settings = PQSettings(0);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        NPersQueue::TTestServer server(settings);
        server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });

        TString topic1 = "/Root/PQ/topic1";
        server.AnnoyingClient->CreateTopicNoLegacy(topic1, 1);
        {
            NYdb::TDriverConfig driverCfg;
            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort);
            std::shared_ptr<NYdb::TDriver> ydbDriver(new NYdb::TDriver(driverCfg));
            auto topicClient = NYdb::NTopic::TTopicClient(*ydbDriver);

            auto res = topicClient.DescribeTopic(topic1);
            res.Wait();
            Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
            UNIT_ASSERT(res.GetValue().IsSuccess());

            auto res2 = NYdb::TProtoAccessor::GetProto(res.GetValue().GetTopicDescription());
            Cerr << res2 << "\n";
            {
                NYdb::NTopic::TAlterTopicSettings settings;
                settings.SetPartitionWriteSpeedBytesPerSecond(4_MB);
                settings.SetPartitionWriteBurstBytes(4_MB);

                auto res = topicClient.AlterTopic(topic1, settings);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());
            }

            {
                auto res = topicClient.DescribeTopic(topic1);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());
                UNIT_ASSERT_VALUES_EQUAL(res.GetValue().GetTopicDescription().GetPartitionWriteSpeedBytesPerSecond(), 4_MB);
                auto res2 = NYdb::TProtoAccessor::GetProto(res.GetValue().GetTopicDescription());
                Cerr << res2 << "\n";
            }

            {
                NYdb::NTopic::TAlterTopicSettings settings;
                settings.SetPartitionWriteSpeedBytesPerSecond(8_MB);
                settings.SetPartitionWriteBurstBytes(8_MB);

                auto res = topicClient.AlterTopic(topic1, settings);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());
            }

            {
                auto res = topicClient.DescribeTopic(topic1);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());
                UNIT_ASSERT_VALUES_EQUAL(res.GetValue().GetTopicDescription().GetPartitionWriteSpeedBytesPerSecond(), 8_MB);
                auto res2 = NYdb::TProtoAccessor::GetProto(res.GetValue().GetTopicDescription());
                Cerr << res2 << "\n";
            }
        }
    }


    Y_UNIT_TEST(SchemeOperationsCheckPropValues) {
        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });

        server.AnnoyingClient->CreateTopic("rt3.dc1--acc--topic1", 1);
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);
        server.AnnoyingClient->CreateConsumer("user");

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> StubP_;

        {
            Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            StubP_ = Ydb::PersQueue::V1::PersQueueService::NewStub(Channel_);
        }

        {
            // zero value is forbidden for: partitions_count
            CreateTopicRequest request;
            CreateTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--topic1");
            auto props = request.mutable_settings();
            props->set_partitions_count(0);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());

            grpc::ClientContext rcontext;
            auto status = StubP_->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
        {
            // zero value is forbidden for: retention_period_ms
            CreateTopicRequest request;
            CreateTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--topic1");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(0);

            grpc::ClientContext rcontext;
            auto status = StubP_->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
        {
            // zero value is allowed for: partition_storage_size, max_partition_write_speed, max_partition_write_burst
            CreateTopicRequest request;
            CreateTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--topic1");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            props->set_max_partition_storage_size(0);
            props->set_max_partition_write_speed(0);
            props->set_max_partition_write_burst(0);

            grpc::ClientContext rcontext;
            auto status = StubP_->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }
    }

    Y_UNIT_TEST(ReadRuleServiceType) {
        TServerSettings settings = PQSettings(0);
        {
            settings.PQConfig.AddClientServiceType()->SetName("MyGreatType");
            settings.PQConfig.AddClientServiceType()->SetName("AnotherType");
            settings.PQConfig.AddClientServiceType()->SetName("SecondType");
        }
        NPersQueue::TTestServer server(settings);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });

        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> pqStub;

        {
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            pqStub = Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
        }
        auto checkDescribe = [&](const TVector<std::pair<TString, TString>>& readRules) {
            DescribeTopicRequest request;
            DescribeTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            grpc::ClientContext rcontext;

            auto status = pqStub->DescribeTopic(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(res.settings().read_rules().size(), readRules.size());
            for (ui64 i = 0; i < readRules.size(); ++i) {
                const auto& rr = res.settings().read_rules(i);
                UNIT_ASSERT_EQUAL(rr.consumer_name(), readRules[i].first);
                UNIT_ASSERT_EQUAL(rr.service_type(), readRules[i].second);
            }
        };
        {
            CreateTopicRequest request;
            CreateTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer1");
            }
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer2");
                rr->set_service_type("MyGreatType");
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }
        checkDescribe({
            {"acc/consumer1", "data-streams"},
            {"acc/consumer2", "MyGreatType"}
        });
        {
            AlterTopicRequest request;
            AlterTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer1");
            }
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer2");
                rr->set_service_type("AnotherType");
            }
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer3");
                rr->set_service_type("SecondType");
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->AlterTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }
        checkDescribe({
            {"acc/consumer1", "data-streams"},
            {"acc/consumer2", "AnotherType"},
            {"acc/consumer3", "SecondType"}
        });

        {
            AlterTopicRequest request;
            AlterTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer1");
                rr->set_service_type("BadServiceType");
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->AlterTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
        checkDescribe({
            {"acc/consumer1", "data-streams"},
            {"acc/consumer2", "AnotherType"},
            {"acc/consumer3", "SecondType"}
        });
    }


    Y_UNIT_TEST(ReadRuleServiceTypeLimit) {
        TServerSettings settings = PQSettings(0);
        {
            auto type = settings.PQConfig.AddClientServiceType();
            type->SetName("MyGreatType");
            type->SetMaxReadRulesCountPerTopic(3);
        }
        NPersQueue::TTestServer server(settings);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });

        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> pqStub;

        {
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            pqStub = Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
        }
        {
            CreateTopicRequest request;
            CreateTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());

            grpc::ClientContext rcontext;
            auto status = pqStub->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }
        auto checkDescribe = [&](const TVector<std::pair<TString, TString>>& readRules) {
            DescribeTopicRequest request;
            DescribeTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            grpc::ClientContext rcontext;

            auto status = pqStub->DescribeTopic(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(res.settings().read_rules().size(), readRules.size());
            for (ui64 i = 0; i < readRules.size(); ++i) {
                const auto& rr = res.settings().read_rules(i);
                UNIT_ASSERT_EQUAL(rr.consumer_name(), readRules[i].first);
                UNIT_ASSERT_EQUAL(rr.service_type(), readRules[i].second);
            }
        };

        TVector<std::pair<TString, TString>> readRules;
        for (ui32 i = 0; i < 4; ++i) {
            AddReadRuleRequest request;
            AddReadRuleResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto rr = request.mutable_read_rule();
            rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
            rr->set_consumer_name(TStringBuilder() << "acc/new_user" << i);
            rr->set_service_type("MyGreatType");
            readRules.push_back({TStringBuilder() << "acc/new_user" << i, "MyGreatType"});

            grpc::ClientContext rcontext;
            auto status = pqStub->AddReadRule(&rcontext, request, &response);
            Cerr << response << "\n";
            if (i < 3) {
                UNIT_ASSERT(status.ok());
                checkDescribe(readRules);
            }
            if (i == 3) {
                UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
            }
        }
        {
            AddReadRuleRequest request;
            AddReadRuleResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto rr = request.mutable_read_rule();
            rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
            rr->set_consumer_name(TStringBuilder() << "acc/new_user0");
            rr->set_service_type("MyGreatType");

            grpc::ClientContext rcontext;
            auto status = pqStub->AddReadRule(&rcontext, request, &response);
            Cerr << response << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::ALREADY_EXISTS);
        }
        {
            AlterTopicRequest request;
            AlterTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            for(ui32 i = 0; i < 4; ++i) {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name(TStringBuilder() << "acc/new_user" << i);
                rr->set_service_type("MyGreatType");
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->AlterTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
    }


    Y_UNIT_TEST(ReadRuleDisallowDefaultServiceType) {
        TServerSettings settings = PQSettings(0);
        {
            settings.PQConfig.AddClientServiceType()->SetName("MyGreatType");
            settings.PQConfig.AddClientServiceType()->SetName("AnotherType");
            settings.PQConfig.AddClientServiceType()->SetName("SecondType");
            settings.PQConfig.SetDisallowDefaultClientServiceType(true);
        }
        NPersQueue::TTestServer server(settings);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });

        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> pqStub;

        {
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            pqStub = Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
        }
        auto checkDescribe = [&](const TVector<std::pair<TString, TString>>& readRules) {
            DescribeTopicRequest request;
            DescribeTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            grpc::ClientContext rcontext;

            auto status = pqStub->DescribeTopic(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(res.settings().read_rules().size(), readRules.size());
            for (ui64 i = 0; i < readRules.size(); ++i) {
                const auto& rr = res.settings().read_rules(i);
                UNIT_ASSERT_EQUAL(rr.consumer_name(), readRules[i].first);
                UNIT_ASSERT_EQUAL(rr.service_type(), readRules[i].second);
            }
        };
        {
            CreateTopicRequest request;
            CreateTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer1");
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
        {
            CreateTopicRequest request;
            CreateTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer1");
                rr->set_service_type("MyGreatType");
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->CreateTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        }
        checkDescribe({{"acc/consumer1", "MyGreatType"}});
        {
            AlterTopicRequest request;
            AlterTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer1");
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->AlterTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
        checkDescribe({{"acc/consumer1", "MyGreatType"}});

        {
            AlterTopicRequest request;
            AlterTopicResponse response;
            request.set_path("/Root/PQ/rt3.dc1--acc--some-topic");
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer1");
                rr->set_service_type("AnotherType");
            }
            {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name("acc/consumer2");
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->AlterTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::BAD_REQUEST);
        }
        checkDescribe({{"acc/consumer1", "MyGreatType"}});
    }

    Y_UNIT_TEST(ReadRuleServiceTypeMigration) {
        TServerSettings settings = PQSettings(0);
        {
            settings.PQConfig.MutableDefaultClientServiceType()->SetName("default_type");
            settings.PQConfig.AddClientServiceType()->SetName("MyGreatType");
            settings.PQConfig.AddClientServiceType()->SetName("AnotherType");
            settings.PQConfig.AddClientServiceType()->SetName("SecondType");
        }
        NPersQueue::TTestServer server(settings);

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });

        const ui32 topicsCount = 4;
        for (ui32 i = 1; i <= topicsCount; ++i) {
            TRequestCreatePQ createTopicRequest(TStringBuilder() << "rt3.dc1--topic_" << i, 1);
            createTopicRequest.ReadRules.clear();
            createTopicRequest.ReadRules.push_back("acc@user1");
            createTopicRequest.ReadRules.push_back("acc@user2");
            createTopicRequest.ReadRules.push_back("acc@user3");
            server.AnnoyingClient->CreateTopic(createTopicRequest);
        }

        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> pqStub;
        {
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            pqStub = Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
        }
        auto doAlter = [&](const TString& topic, const TVector<std::pair<TString, TString>>& readRules) {
            AlterTopicRequest request;
            AlterTopicResponse response;
            request.set_path(topic);
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            for (auto rrInfo : readRules) {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name(rrInfo.first);
                rr->set_service_type(rrInfo.second);
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->AlterTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);
        };


        auto checkDescribe = [&](const TString& topic, const TVector<std::pair<TString, TString>>& readRules) {
            Cerr << ">>>>> Check topic: " << topic << Endl;
            DescribeTopicRequest request;
            DescribeTopicResponse response;
            request.set_path(topic);
            grpc::ClientContext rcontext;

            auto status = pqStub->DescribeTopic(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << ">>>>> Response: " << response << Endl;
            Cerr << ">>>>> Result:" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            UNIT_ASSERT_VALUES_EQUAL(res.settings().read_rules().size(), readRules.size());
            for (ui64 i = 0; i < readRules.size(); ++i) {
                const auto& rr = res.settings().read_rules(i);
                UNIT_ASSERT_EQUAL(rr.consumer_name(), readRules[i].first);
                UNIT_ASSERT_EQUAL(rr.service_type(), readRules[i].second);
            }
        };
        checkDescribe(
            "/Root/PQ/rt3.dc1--topic_1",
            {
                {"acc/user1", "default_type"},
                {"acc/user2", "default_type"},
                {"acc/user3", "default_type"}
            }
        );
        {
            doAlter(
                "/Root/PQ/rt3.dc1--topic_2",
                {
                    {"acc/user1", ""},
                    {"acc/new_user", "MyGreatType"},
                    {"acc/user2", "default_type"},
                    {"acc/user3", "default_type"},
                    {"acc/user4", "AnotherType"}
                }
            );
            checkDescribe(
                "/Root/PQ/rt3.dc1--topic_2",
                {
                    {"acc/user1", "default_type"},
                    {"acc/new_user", "MyGreatType"},
                    {"acc/user2", "default_type"},
                    {"acc/user3", "default_type"},
                    {"acc/user4", "AnotherType"}
                }
            );
        }
        {
            AddReadRuleRequest request;
            AddReadRuleResponse response;
            request.set_path("/Root/PQ/rt3.dc1--topic_3");
            auto rr = request.mutable_read_rule();
            rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
            rr->set_consumer_name("acc/new_user");
            rr->set_service_type("MyGreatType");

            grpc::ClientContext rcontext;
            auto status = pqStub->AddReadRule(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            checkDescribe(
                "/Root/PQ/rt3.dc1--topic_3",
                {
                    {"acc/user1", "default_type"},
                    {"acc/user2", "default_type"},
                    {"acc/user3", "default_type"},
                    {"acc/new_user", "MyGreatType"}
                }
            );
        }

        {
            checkDescribe(
                "/Root/PQ/rt3.dc1--topic_4",
                {
                    {"acc/user1", "default_type"},
                    {"acc/user2", "default_type"},
                    {"acc/user3", "default_type"}
                }
            );

            RemoveReadRuleRequest request;
            RemoveReadRuleResponse response;
            request.set_path("/Root/PQ/rt3.dc1--topic_4");
            request.set_consumer_name("acc@user2");

            grpc::ClientContext rcontext;
            auto status = pqStub->RemoveReadRule(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), Ydb::StatusIds::SUCCESS);

            checkDescribe(
                "/Root/PQ/rt3.dc1--topic_4",
                {
                    {"acc/user1", "default_type"},
                    {"acc/user3", "default_type"}
                }
            );
        }
    }

    Y_UNIT_TEST(ReadRuleServiceTypeMigrationWithDisallowDefault) {
        TServerSettings settings = PQSettings(0);
        {
            settings.PQConfig.MutableDefaultClientServiceType()->SetName("default_type");
            settings.PQConfig.AddClientServiceType()->SetName("MyGreatType");
            settings.PQConfig.AddClientServiceType()->SetName("AnotherType");
            settings.PQConfig.AddClientServiceType()->SetName("SecondType");
            settings.PQConfig.SetDisallowDefaultClientServiceType(true);
        }
        NPersQueue::TTestServer server(settings);

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });

        const ui32 topicsCount = 4;
        for (ui32 i = 1; i <= topicsCount; ++i) {
            TRequestCreatePQ createTopicRequest(TStringBuilder() << "rt3.dc1--topic_" << i, 1);
            createTopicRequest.ReadRules.push_back("acc@user1");
            createTopicRequest.ReadRules.push_back("acc@user2");
            createTopicRequest.ReadRules.push_back("acc@user3");
            server.AnnoyingClient->CreateTopic(createTopicRequest);
        }

        std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> pqStub;
        {
            std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            pqStub = Ydb::PersQueue::V1::PersQueueService::NewStub(channel);
        }

        auto doAlter = [&](
            const TString& topic,
            const TVector<std::pair<TString, TString>>& readRules,
            Ydb::StatusIds::StatusCode statusCode = Ydb::StatusIds::SUCCESS
        ) {
            AlterTopicRequest request;
            AlterTopicResponse response;
            request.set_path(topic);
            auto props = request.mutable_settings();
            props->set_partitions_count(1);
            props->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            props->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            for (auto rrInfo : readRules) {
                auto rr = props->add_read_rules();
                rr->set_supported_format(Ydb::PersQueue::V1::TopicSettings::Format(1));
                rr->set_consumer_name(rrInfo.first);
                rr->set_service_type(rrInfo.second);
            }

            grpc::ClientContext rcontext;
            auto status = pqStub->AlterTopic(&rcontext, request, &response);

            UNIT_ASSERT(status.ok());
            CreateTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), statusCode);
        };

        auto checkDescribe = [&](
            const TString& topic,
            const TVector<std::pair<TString, TString>>& readRules,
            Ydb::StatusIds::StatusCode statusCode = Ydb::StatusIds::SUCCESS
        ) {
            DescribeTopicRequest request;
            DescribeTopicResponse response;
            request.set_path(topic);
            grpc::ClientContext rcontext;

            auto status = pqStub->DescribeTopic(&rcontext, request, &response);
            UNIT_ASSERT(status.ok());
            DescribeTopicResult res;
            response.operation().result().UnpackTo(&res);
            Cerr << response << "\n" << res << "\n";
            UNIT_ASSERT_VALUES_EQUAL(response.operation().status(), statusCode);
            if (statusCode == Ydb::StatusIds::SUCCESS) {
                UNIT_ASSERT_VALUES_EQUAL(res.settings().read_rules().size(), readRules.size());
                for (ui64 i = 0; i < readRules.size(); ++i) {
                    const auto& rr = res.settings().read_rules(i);
                    UNIT_ASSERT_EQUAL(rr.consumer_name(), readRules[i].first);
                    UNIT_ASSERT_EQUAL(rr.service_type(), readRules[i].second);
                }
            }
        };
        checkDescribe(
            "/Root/PQ/rt3.dc1--topic_1",
            {},
            Ydb::StatusIds::INTERNAL_ERROR
        );
        {
            doAlter(
                "/Root/PQ/rt3.dc1--topic_2",
                {
                    {"acc/new_user", "MyGreatType"},
                    {"acc/user2", "SecondType"},
                    {"acc/user3", "AnotherType"},
                    {"acc/user4", "AnotherType"}
                }
            );
            checkDescribe(
                "/Root/PQ/rt3.dc1--topic_2",
                {
                    {"acc/new_user", "MyGreatType"},
                    {"acc/user2", "SecondType"},
                    {"acc/user3", "AnotherType"},
                    {"acc/user4", "AnotherType"}
                }
            );
        }
    }


    void TestReadRuleServiceTypePasswordImpl(bool forcePassword)
    {
        TServerSettings settings = PQSettings(0);
        {
            settings.PQConfig.SetDisallowDefaultClientServiceType(false);
            settings.PQConfig.SetForceClientServiceTypePasswordCheck(forcePassword);
            settings.PQConfig.MutableDefaultClientServiceType()->SetName("default_type");
            settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
            auto type = settings.PQConfig.AddClientServiceType();
            type->SetName("MyGreatType");
            TString passwordHash = MD5::Data("password");
            passwordHash.to_lower();
            type->AddPasswordHashes(passwordHash);
        }

        NPersQueue::TTestServer server(settings);

        {
            NYdb::TDriverConfig driverCfg;
            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort);
            std::shared_ptr<NYdb::TDriver> ydbDriver(new NYdb::TDriver(driverCfg));
            auto topicClient = NYdb::NTopic::TTopicClient(*ydbDriver);

            {
                NYdb::NTopic::TCreateTopicSettings settings;

                NYdb::NTopic::TConsumerSettings<NYdb::NTopic::TCreateTopicSettings> consumerSettings(settings, "consumer");
                consumerSettings.AddAttribute("_service_type", "MyGreatType");
                if (!forcePassword)
                    consumerSettings.AddAttribute("_service_type_password", "aaa");

                settings.PartitioningSettings(1,1).AppendConsumers(consumerSettings);

                auto res = topicClient.CreateTopic("/Root/PQ/ttt", settings);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(!res.GetValue().IsSuccess());
            }
            {
                NYdb::NTopic::TCreateTopicSettings settings;
                settings.PartitioningSettings(1,1)
                    .BeginAddConsumer("consumer").AddAttribute("_service_type", "MyGreatType")
                                                 .AddAttribute("_service_type_password", "password")
                    .EndAddConsumer();
                auto res = topicClient.CreateTopic("/Root/PQ/ttt", settings);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());
            }

            {
                NYdb::NTopic::TAlterTopicSettings settings;

                NYdb::NTopic::TAlterConsumerSettings consumerSettings(settings, "consumer");

                if (!forcePassword) {
                    consumerSettings.BeginAlterAttributes().Add("_service_type_password", "aaa");
                }

                settings
                    .BeginAddConsumer("consumer2")
                    .EndAddConsumer()
                    .AppendAlterConsumers(consumerSettings);
                auto res = topicClient.AlterTopic("/Root/PQ/ttt", settings);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(!res.GetValue().IsSuccess());
            }
            {
                NYdb::NTopic::TAlterTopicSettings settings;
                settings
                    .BeginAddConsumer("consumer2")
                    .EndAddConsumer()
                    .BeginAlterConsumer("consumer").BeginAlterAttributes().Alter("_service_type_password", "password")
                                                   .EndAlterAttributes()
                    .EndAlterConsumer();
                auto res = topicClient.AlterTopic("/Root/PQ/ttt", settings);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());
            }

            {
                NYdb::NTopic::TAlterTopicSettings settings;
                settings.AppendDropConsumers("consumer");
                auto res = topicClient.AlterTopic("/Root/PQ/ttt", settings);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(res.GetValue().IsSuccess());
            }

            { // check that important consumer is forbidden
                NYdb::NTopic::TAlterTopicSettings settings;
                settings
                    .BeginAddConsumer("consumer2").Important(true)
                    .EndAddConsumer();
                auto res = topicClient.AlterTopic("/Root/PQ/ttt", settings);
                res.Wait();
                Cerr << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
                UNIT_ASSERT(!res.GetValue().IsSuccess());
            }
        }
    }
    Y_UNIT_TEST(TestReadRuleServiceTypePassword) {
        TestReadRuleServiceTypePasswordImpl(false);
        TestReadRuleServiceTypePasswordImpl(true);
    }

    void CreateTopicWithMeteringMode(bool meteringEnabled) {
        TServerSettings serverSettings = PQSettings(0);
        serverSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        serverSettings.PQConfig.MutableBillingMeteringConfig()->SetEnabled(meteringEnabled);
        NPersQueue::TTestServer server(serverSettings);

        using namespace NYdb::NTopic;
        auto client = TTopicClient(server.GetDriver());

        for (const auto mode : {EMeteringMode::RequestUnits, EMeteringMode::ReservedCapacity}) {
            const TString path = TStringBuilder() << "/Root/PQ/Topic" << mode;

            auto res = client.CreateTopic(path, TCreateTopicSettings()
                .MeteringMode(mode)
            ).ExtractValueSync();

            if (!meteringEnabled) {
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
                continue;
            }

            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            auto desc = client.DescribeTopic(path).ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetMeteringMode(), mode);
        }
    }

    Y_UNIT_TEST(CreateTopicWithMeteringMode) {
        CreateTopicWithMeteringMode(false);
        CreateTopicWithMeteringMode(true);
    }

    void SetMeteringMode(bool meteringEnabled) {
        TServerSettings serverSettings = PQSettings(0);
        serverSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        serverSettings.PQConfig.MutableBillingMeteringConfig()->SetEnabled(meteringEnabled);
        NPersQueue::TTestServer server(serverSettings);

        using namespace NYdb::NTopic;
        auto client = TTopicClient(server.GetDriver());

        {
            auto res = client.CreateTopic("/Root/PQ/ttt").ExtractValueSync();
            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        }

        for (const auto mode : {EMeteringMode::RequestUnits, EMeteringMode::ReservedCapacity}) {
            auto res = client.AlterTopic("/Root/PQ/ttt", TAlterTopicSettings()
                .SetMeteringMode(mode)
            ).ExtractValueSync();

            if (!meteringEnabled) {
                UNIT_ASSERT_VALUES_EQUAL(res.GetStatus(), NYdb::EStatus::PRECONDITION_FAILED);
                continue;
            }

            UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
            auto desc = client.DescribeTopic("/Root/PQ/ttt").ExtractValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetMeteringMode(), mode);
        }
    }

    Y_UNIT_TEST(SetMeteringMode) {
        SetMeteringMode(false);
        SetMeteringMode(true);
    }

    void DefaultMeteringMode(bool meteringEnabled) {
        TServerSettings serverSettings = PQSettings(0);
        serverSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        serverSettings.PQConfig.MutableBillingMeteringConfig()->SetEnabled(meteringEnabled);
        NPersQueue::TTestServer server(serverSettings);

        using namespace NYdb::NTopic;
        auto client = TTopicClient(server.GetDriver());

        auto res = client.CreateTopic("/Root/PQ/ttt").ExtractValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());

        auto desc = client.DescribeTopic("/Root/PQ/ttt").ExtractValueSync();
        UNIT_ASSERT_C(desc.IsSuccess(), desc.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(desc.GetTopicDescription().GetMeteringMode(), (meteringEnabled
            ? EMeteringMode::RequestUnits
            : EMeteringMode::Unspecified));
    }

    Y_UNIT_TEST(DefaultMeteringMode) {
        DefaultMeteringMode(false);
        DefaultMeteringMode(true);
    }

    Y_UNIT_TEST(TClusterTrackerTest) {
        APITestSetup setup{TEST_CASE_NAME};
        setup.GetPQConfig().SetClustersUpdateTimeoutSec(0);
        THashMap<TString, TPQTestClusterInfo> clusters = DEFAULT_CLUSTERS_LIST;

        auto runtime = setup.GetServer().GetRuntime();
        setup.GetFlatMsgBusPQClient().CheckClustersList(runtime, false, clusters);

        UNIT_ASSERT_EQUAL(clusters.count("dc1"), 1);
        UNIT_ASSERT_EQUAL(clusters.count("dc2"), 1);
        clusters["dc1"].Weight = 666;
        clusters["dc2"].Balancer = "newbalancer.net";
        setup.GetFlatMsgBusPQClient().InitDCs(clusters);
        setup.GetFlatMsgBusPQClient().CheckClustersList(runtime, true, clusters);
    }

    Y_UNIT_TEST(TestReadPartitionByGroupId) {
        NPersQueue::TTestServer server;

        ui32 partitionsCount = 100;
        TString topic = "topic1";
        TString topicFullName = "rt3.dc1--" + topic;

        server.AnnoyingClient->CreateTopic(topicFullName, partitionsCount);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY});

        auto driver = server.AnnoyingClient->GetDriver();

        for (ui32 partition = 30; partition < partitionsCount; ++partition) {
            auto reader = CreateReader(
                *driver,
                NYdb::NPersQueue::TReadSessionSettings()
                    .AppendTopics(
                        NYdb::NPersQueue::TTopicReadSettings(topic)
                            .AppendPartitionGroupIds(partition + 1)
                    )
                    .ConsumerName("shared/user")
                    .ReadOnlyOriginal(true)
            );

            TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader->GetEvent(true, 1);
            auto createStream = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*event);
            UNIT_ASSERT(createStream);
            TString stepDescription = TStringBuilder() << "create stream for partition=" << partition
                << " : " << createStream->DebugString();
            Cerr << stepDescription << Endl;
            UNIT_ASSERT_EQUAL_C(
                partition,
                createStream->GetPartitionStream()->GetPartitionId(),
                stepDescription

            );
        }
    }

    Y_UNIT_TEST(SrcIdCompatibility) {
        NPersQueue::TTestServer server{};
        auto runTest = [&] (
                const TString& topicToAdd, const TString& topicForHash, const TString& topicName,
                const TString& srcId, ui32 partId, ui64 accessTime = 0
        ) {
            TStringBuilder query;
            auto encoded = NPQ::NSourceIdEncoding::EncodeSrcId(
                    topicForHash, srcId,
                    NPQ::ESourceIdTableGeneration::SrcIdMeta2
            );
            Cerr << "===save partition with time: " << accessTime << Endl;

            if (accessTime == 0) {
                accessTime = TInstant::Now().MilliSeconds();
            }
            if (!topicToAdd.empty()) { // Empty means don't add anything
                query <<
                      "--!syntax_v1\n"
                      "UPSERT INTO `/Root/PQ/SourceIdMeta2` (Hash, Topic, SourceId, CreateTime, AccessTime, Partition) VALUES ("
                      << encoded.Hash << ", \"" << topicToAdd << "\", \"" << encoded.EscapedSourceId << "\", "
                      << TInstant::Now().MilliSeconds() << ", " << accessTime << ", " << partId << "); ";
                Cerr << "Run query:\n" << query << Endl;
                auto scResult = server.AnnoyingClient->RunYqlDataQuery(query);
                //UNIT_ASSERT(scResult.Defined());
            }

            auto driver = server.AnnoyingClient->GetDriver();
            auto writer = CreateWriter(*driver, topicName, srcId);
            auto ev = writer->GetEvent(true);
            auto ct = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent >(&*ev);
            UNIT_ASSERT(ct);
            writer->Write(std::move(ct->ContinuationToken), "1234567890");
            UNIT_ASSERT(ev.Defined());
            while(true) {
                ev = writer->GetEvent(true);
                auto ack = std::get_if<NYdb::NPersQueue::TWriteSessionEvent::TAcksEvent>(&*ev);
                if (ack) {
                    UNIT_ASSERT_VALUES_EQUAL(ack->Acks[0].Details->PartitionId, partId);
                    break;
                }

            }
        };

        TString legacyName = "rt3.dc1--account--topic100";
        TString shortLegacyName = "account--topic100";
        TString fullPath = "/Root/PQ/rt3.dc1--account--topic100";
        TString topicName = "account/topic100";
        TString srcId1 = "test-src-id-compat", srcId2 = "test-src-id-compat2";
        server.AnnoyingClient->CreateTopic(legacyName, 100);

        runTest(legacyName, shortLegacyName, topicName, srcId1, 5, 100);
        runTest(legacyName, shortLegacyName, topicName, srcId2, 6, 100);
        runTest("", "", topicName, srcId1, 5, 100);
        runTest("", "", topicName, srcId2, 6, 100);

        ui64 time = (TInstant::Now() + TDuration::Hours(4)).MilliSeconds();
        runTest(legacyName, shortLegacyName, topicName, srcId2, 7, time);
    }

    Y_UNIT_TEST(TestReadPartitionStatus) {
        NPersQueue::TTestServer server;

        TString topic = "topic1";
        TString topicFullName = "rt3.dc1--" + topic;

        server.AnnoyingClient->CreateTopic(topicFullName, 1);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY});

        auto driver = server.AnnoyingClient->GetDriver();

        auto reader = CreateReader(
            *driver,
            NYdb::NPersQueue::TReadSessionSettings()
                .AppendTopics(topic)
                .ConsumerName("shared/user")
                .ReadOnlyOriginal(true)
        );


        {
            TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader->GetEvent(true, 1);
            auto createStream = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*event);
            UNIT_ASSERT(createStream);
            Cerr << "Create stream event: " << createStream->DebugString() << Endl;
            createStream->GetPartitionStream()->RequestStatus();
        }
        {
            auto future = reader->WaitEvent();
            UNIT_ASSERT(future.Wait(TDuration::Seconds(10)));
            TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader->GetEvent(true, 1);
            auto partitionStatus = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TPartitionStreamStatusEvent>(&*event);
            UNIT_ASSERT(partitionStatus);
            Cerr << "partition status: " << partitionStatus->DebugString() << Endl;
        }
    }

    Y_UNIT_TEST(PartitionsMapping) {
        NPersQueue::TTestServer server;

        TString topic = "topic1";
        TString topicFullName = "rt3.dc1--" + topic;

        auto partsCount = 5u;
        server.AnnoyingClient->CreateTopic(topicFullName, partsCount);
        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY});

        auto driver = server.AnnoyingClient->GetDriver();

        NYdb::NPersQueue::TTopicReadSettings topicSettings =
            MakeTopicReadSettings(topic, {2, 4});
        NYdb::NPersQueue::TReadSessionSettings readerSettings =
            MakeReadSessionSettings(topicSettings,
                                    "shared/user",
                                    true);
        auto reader = CreateReader(*driver, readerSettings);

        THashSet<ui32> locksGot = {};
        while(locksGot.size() < 2) {
            TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader->GetEvent(true, 1);
            auto createStream = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*event);
            UNIT_ASSERT(createStream);
            Cerr << "Create stream event: " << createStream->DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(createStream->GetPartitionStream()->GetTopicPath(), topic);
            auto partId = createStream->GetPartitionStream()->GetPartitionId();
            UNIT_ASSERT(partId == 1 || partId == 3);
            UNIT_ASSERT(!locksGot.contains(partId));
            locksGot.insert(partId);
        }

        topicSettings =
            MakeTopicReadSettings(topic, {});
        readerSettings =
            MakeReadSessionSettings(topicSettings,
                                    "shared/user",
                                    true);
        auto reader2 = CreateReader(*driver, readerSettings);

        locksGot.clear();
        THashSet<ui32> releasesGot = {};
        while (locksGot.size() < 3) {
            TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader2->GetEvent(true, 1);
            auto createStream = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent>(&*event);
            UNIT_ASSERT(createStream);
            Cerr << "Create stream event: " << createStream->DebugString() << Endl;
            UNIT_ASSERT_VALUES_EQUAL(createStream->GetPartitionStream()->GetTopicPath(), topic);
            auto partId = createStream->GetPartitionStream()->GetPartitionId();
            if ((partId == 1) || (partId == 3)) {
                UNIT_ASSERT(!releasesGot.contains(partId));
                releasesGot.insert(partId);
            } else {
                UNIT_ASSERT(!locksGot.contains(partId));
                UNIT_ASSERT((partId == 0) || (partId == 2) || (partId == 4));
                locksGot.insert(partId);
            }
        }

        while (!releasesGot.empty()) {
            TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader->GetEvent(true, 1);
            auto release = std::get_if<NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent>(&*event);
            UNIT_ASSERT(release);
            UNIT_ASSERT_VALUES_EQUAL(release->GetPartitionStream()->GetTopicPath(), topic);
            auto partId = release->GetPartitionStream()->GetPartitionId();
            UNIT_ASSERT((partId == 1) || (partId == 3));
            releasesGot.erase(partId);
        }
    }

    THolder<NYdb::TDriver> SetupTestAndGetDriver(
            NPersQueue::TTestServer& server, const TString& topicName, ui64 partsCount = 1
    ) {
        NYdb::TDriverConfig driverCfg;
        driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort);
        auto driver = MakeHolder<NYdb::TDriver>(driverCfg);

        server.EnableLogs({ NKikimrServices::PQ_READ_PROXY, NKikimrServices::PQ_WRITE_PROXY});
        server.EnableLogs({ NKikimrServices::KQP_PROXY}, NActors::NLog::PRI_EMERG);

        server.AnnoyingClient->CreateTopic(topicName, partsCount);
        auto topicClient = NYdb::NTopic::TTopicClient(*driver);
        auto alterSettings = NYdb::NTopic::TAlterTopicSettings();
        alterSettings.BeginAddConsumer("debug");
        auto alterRes = topicClient.AlterTopic(TString("/Root/PQ/") + topicName, alterSettings).GetValueSync();
        UNIT_ASSERT(alterRes.IsSuccess());
        return std::move(driver);
    }

    Y_UNIT_TEST(MessageMetadata) {
        NPersQueue::TTestServer server;
        server.CleverServer->GetRuntime()->GetAppData().FeatureFlags.SetEnableTopicMessageMeta(true);
        TString topicFullName = "rt3.dc1--topic1";
        auto driver = SetupTestAndGetDriver(server, topicFullName);

        auto topicClient = NYdb::NTopic::TTopicClient(*driver);

        NYdb::NTopic::TWriteSessionSettings wSettings {topicFullName, "srcId", "srcId"};
        wSettings.DirectWriteToPartition(false);
        auto writer = topicClient.CreateSimpleBlockingWriteSession(wSettings);
        TVector<std::pair<TString, TString>> metadata = {{"key1", "val1"}, {"key2", "val2"}};
        {
            auto message = NYdb::NTopic::TWriteMessage{"Somedata"}.MessageMeta(metadata);
            writer->Write(std::move(message));
        }
        metadata = {{"key3", "val3"}};
        {
            auto message = NYdb::NTopic::TWriteMessage{"Somedata2"}.MessageMeta(metadata);
            writer->Write(std::move(message));
        }
        writer->Write("Somedata3");
        writer->Close();
        NYdb::NTopic::TReadSessionSettings rSettings;
        rSettings.ConsumerName("debug").AppendTopics({topicFullName});
        auto readSession = topicClient.CreateReadSession(rSettings);

        auto ev = readSession->GetEvent(true);
        UNIT_ASSERT(ev.Defined());
        auto spsEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*ev);
        UNIT_ASSERT(spsEv);
        spsEv->Confirm();
        ev = readSession->GetEvent(true);
        auto dataEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev);
        UNIT_ASSERT(dataEv);
        const auto& messages = dataEv->GetMessages();
        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);
        auto metadataSizeExpected = 2;
        for (const auto& msg : dataEv->GetMessages()) {
            auto meta = msg.GetMessageMeta();
            UNIT_ASSERT_VALUES_EQUAL(meta->Fields.size(), metadataSizeExpected);
            metadataSizeExpected--;
        }
    }

    Y_UNIT_TEST(DisableWrongSettings) {
        NPersQueue::TTestServer server;
        server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::BLACKBOX_VALIDATOR });
        server.EnableLogs({NKikimrServices::PERSQUEUE}, NActors::NLog::EPriority::PRI_INFO);
        TString topicFullName = "rt3.dc1--acc--topic1";
        auto driver = SetupTestAndGetDriver(server, topicFullName, 3);

        std::shared_ptr<grpc::Channel> Channel_;
        std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> TopicStubP_;
        {
            Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            TopicStubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);
        }

        {
            grpc::ClientContext rcontext1;
            auto writeStream1 = TopicStubP_->StreamWrite(&rcontext1);
            UNIT_ASSERT(writeStream1);
            Ydb::Topic::StreamWriteMessage::FromClient req;
            Ydb::Topic::StreamWriteMessage::FromServer resp;

            req.mutable_init_request()->set_path("acc/topic1");
            req.mutable_init_request()->set_message_group_id("some-group");
            if (!writeStream1->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(writeStream1->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.status() == Ydb::StatusIds::SUCCESS);
            UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamWriteMessage::FromServer::kInitResponse);

        }
        {
            grpc::ClientContext rcontext1;
            auto writeStream1 = TopicStubP_->StreamWrite(&rcontext1);
            UNIT_ASSERT(writeStream1);
            Ydb::Topic::StreamWriteMessage::FromClient req;
            Ydb::Topic::StreamWriteMessage::FromServer resp;

            req.mutable_init_request()->set_path("acc/topic1");
            req.mutable_init_request()->set_message_group_id("some-group");
            req.mutable_init_request()->set_producer_id("producer");
            if (!writeStream1->Write(req)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(writeStream1->Read(&resp));
            Cerr << "===Got response: " << resp.ShortDebugString() << Endl;
            UNIT_ASSERT(resp.status() == Ydb::StatusIds::BAD_REQUEST);
        }
    }

    Y_UNIT_TEST(DisableDeduplication) {
        NPersQueue::TTestServer server;
        TString topicFullName = "rt3.dc1--topic1";
        auto driver = SetupTestAndGetDriver(server, topicFullName, 3);

        auto topicClient = NYdb::NTopic::TTopicClient(*driver);
        NYdb::NTopic::TWriteSessionSettings wSettings;
        wSettings.Path(topicFullName).DeduplicationEnabled(false);
        wSettings.DirectWriteToPartition(false);

        TVector<std::shared_ptr<NYdb::NTopic::ISimpleBlockingWriteSession>> writers;
        for (auto i = 0u; i < 3; i++) {
            auto writer = topicClient.CreateSimpleBlockingWriteSession(wSettings);
            for (auto j = 0u; j < 3; j++) {
                writer->Write(TString("MyData") + ToString(i) + ToString(j));
            }
            writers.push_back(writer);
        }

        for (auto& w : writers) {
            w->Close();
        }

        NYdb::NTopic::TReadSessionSettings rSettings;
        rSettings.ConsumerName("debug").AppendTopics({topicFullName});
        auto readSession = topicClient.CreateReadSession(rSettings);

        THashSet<ui64> partitions;
        ui64 totalMessages = 0;
        Cerr << "Start reads\n";
        while(totalMessages < 9) {
            auto ev = readSession->GetEvent(true);
            UNIT_ASSERT(ev.Defined());

            auto spsEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*ev);
            if (spsEv) {
                spsEv->Confirm();
                Cerr << "Got start stream event\n";
                continue;
            }
            auto dataEv = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*ev);
            UNIT_ASSERT(dataEv);
            const auto& messages = dataEv->GetMessages();
            totalMessages += messages.size();
            Cerr << "Got data event with total " << messages.size() << " messages, current total messages: " << totalMessages << Endl;
            for (const auto& msg: dataEv->GetMessages()) {
                auto session = msg.GetPartitionSession();
                partitions.insert(session->GetPartitionId());
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 3);
    }

    Y_UNIT_TEST(LOGBROKER_7820) {
        //
        // 700 messages of 2000 characters are sent in the test
        //
        // the memory limit for the decompression task is ~512Kb. these are 263 messages of 2000 characters each
        //
        // the memory limit for the DataReceivedHandler handler is ~300Kb
        // it is expected to be called several times with blocks of no more than 154 messages
        //

        NPersQueue::TTestServer server;
        server.EnableLogs({ NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::PQ_READ_PROXY});
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1);

        auto driver = server.AnnoyingClient->GetDriver();

        NYdb::NPersQueue::TPersQueueClient persqueueClient(*driver);
        NYdb::NPersQueue::TReadSessionSettings settings;

        settings.ConsumerName("shared/user").AppendTopics(SHORT_TOPIC_NAME).ReadOriginal({"dc1"});
        settings.MaxMemoryUsageBytes(1_MB);
        settings.DisableClusterDiscovery(true);

        //
        // the handler stores the number of messages in each call
        //
        std::vector<size_t> dataSizes;

        auto handler = [&](NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& event){
            UNIT_ASSERT_LE(event.GetMessages().size(), 154);

            dataSizes.push_back(event.GetMessages().size());
        };
        settings.EventHandlers_.SimpleDataHandlers(std::move(handler), true);

        //
        // controlled decompressor
        //
        auto decompressor = CreateThreadPoolExecutorWrapper(1);
        settings.DecompressionExecutor(decompressor);

        //
        // blocks of ~300Kb will be transmitted for processing
        //
        settings.EventHandlers_.MaxMessagesBytes(300_KB);

        //
        // managed handler executor
        //
        auto executor = CreateThreadPoolExecutorWrapper(1);
        settings.EventHandlers_.HandlersExecutor(executor);

        auto session = persqueueClient.CreateReadSession(settings);
        auto counters = session->GetCounters();

        //
        // 700 messages of 2000 characters are transmitted
        //
        {
            auto writer = CreateSimpleWriter(*driver, SHORT_TOPIC_NAME, TStringBuilder() << "source" << 0, {}, TString("raw"));

            for (ui32 i = 1; i <= 700; ++i) {
                std::string message(2'000, 'x');

                bool res = writer->Write(message, i);
                UNIT_ASSERT(res);
            }

            auto res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        //
        // auxiliary functions for decompressor and handler control
        //
        auto WaitTasks = [&](auto f, size_t c) {
            while (f() < c) {
                Sleep(TDuration::Seconds(1));
            };
        };
        auto WaitPlannedTasks = [&](auto e, size_t count) {
            WaitTasks([&]() { return e->GetPlannedCount(); }, count);
        };
        auto WaitExecutedTasks = [&](auto e, size_t count) {
            WaitTasks([&]() { return e->GetExecutedCount(); }, count);
        };

        auto RunTasks = [&](auto e, const std::vector<size_t>& tasks) {
            size_t n = tasks.size();
            WaitPlannedTasks(e, n);
            size_t completed = e->GetExecutedCount();
            e->StartFuncs(tasks);
            WaitExecutedTasks(e, completed + n);
        };

        //
        // run executors
        //
        RunTasks(executor, {0}); // TCreatePartitionStreamEvent

        RunTasks(decompressor, {0});
        RunTasks(executor, {1, 2});

        RunTasks(decompressor, {1});
        RunTasks(executor, {3, 4});

        RunTasks(decompressor, {2});
        RunTasks(executor, {5, 6});

        //
        // all messages received
        //
        UNIT_ASSERT_VALUES_EQUAL(dataSizes.size(), 6);
        UNIT_ASSERT_VALUES_EQUAL(std::accumulate(dataSizes.begin(), dataSizes.end(), 0), 700);
        UNIT_ASSERT_VALUES_EQUAL(*std::max_element(dataSizes.begin(), dataSizes.end()), 154);

        UNIT_ASSERT_VALUES_EQUAL(counters->MessagesInflight->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightUncompressed->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightCompressed->Val(), 0);
        UNIT_ASSERT_VALUES_EQUAL(counters->BytesInflightTotal->Val(), 0);

        decompressor->RunAllTasks();
        executor->RunAllTasks();

        session->Close(TDuration::Seconds(10));

        driver->Stop();
    }

    Y_UNIT_TEST(ReadWithoutConsumer) {
        auto readToEndThenCommit = [] (NPersQueue::TTestServer& server, ui32 partitions, ui32 maxOffset, TString consumer, ui32 readByBytes) {
            std::shared_ptr<grpc::Channel> Channel_;
            std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> StubP_;

            Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            StubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);

            grpc::ClientContext rcontext;
            auto readStream = StubP_->StreamRead(&rcontext);
            UNIT_ASSERT(readStream);

            {
                Ydb::Topic::StreamReadMessage::FromClient  req;
                Ydb::Topic::StreamReadMessage::FromServer resp;

                auto topicReadSettings = req.mutable_init_request()->add_topics_read_settings();
                topicReadSettings->set_path(SHORT_TOPIC_NAME);
                for (ui32 i = 0; i < partitions; i++) {
                    topicReadSettings->add_partition_ids(i);
                }

                req.mutable_init_request()->set_consumer(consumer);

                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }

                UNIT_ASSERT(readStream->Read(&resp));
                UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
            }
            ui32 partitionsSigned = 0;

            while (partitionsSigned != partitions) {

                Ydb::Topic::StreamReadMessage::FromServer resp;
                UNIT_ASSERT(readStream->Read(&resp));
                UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest, resp);
                auto assignId = resp.start_partition_session_request().partition_session().partition_session_id();

                Ydb::Topic::StreamReadMessage::FromClient req;
                req.mutable_start_partition_session_response()->set_partition_session_id(assignId);
                req.mutable_start_partition_session_response()->set_read_offset(0);
                auto res = readStream->Write(req);
                UNIT_ASSERT(res);
                partitionsSigned++;
            }
            ui32 offset = 0;
            ui32 session = 0;
            while (offset != maxOffset) {
                Ydb::Topic::StreamReadMessage::FromClient  req;
                req.mutable_read_request()->set_bytes_size(readByBytes);
                readStream->Write(req);

                Ydb::Topic::StreamReadMessage::FromServer resp;
                UNIT_ASSERT(readStream->Read(&resp));
                UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);
                Cerr << "\n" << "Bytes readed: " << resp.read_response().bytes_size() << "\n";
                for (int j = 0; j < resp.read_response().partition_data_size(); j++) {
                    for (int k = 0; k < resp.read_response().partition_data(j).batches_size(); k++) {
                        for (int l = 0; l < resp.read_response().partition_data(j).batches(k).message_data_size(); l++) {
                            offset = resp.read_response().partition_data(j).batches(k).message_data(l).offset();
                            session = resp.read_response().partition_data(j).partition_session_id();
                            Cerr << "\n" << "Offset: " << offset << " from session " << session << "\n";
                        }
                    }
                }
            }

            //check commit failed
            Ydb::Topic::StreamReadMessage::FromClient commitRequest;
            Ydb::Topic::StreamReadMessage::FromServer commitResponse;

            auto commit = commitRequest.mutable_commit_offset_request()->add_commit_offsets();
            commit->set_partition_session_id(session);

            auto offsets = commit->add_offsets();
            offsets->set_start(0);
            offsets->set_end(maxOffset);

            if (!readStream->Write(commitRequest)) {
                ythrow yexception() << "write fail";
            }
            UNIT_ASSERT(readStream->Read(&commitResponse));
            UNIT_ASSERT_VALUES_EQUAL(commitResponse.status(), Ydb::StatusIds::BAD_REQUEST);
        };
        const ui32 partititonsCount = 5;
        const ui32 messagesCount = 10;

        NPersQueue::TTestServer server;
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, partititonsCount);
        TPQDataWriter writer("source", server);

        for(ui32 i = 0; i < messagesCount; i++) {
            writer.Write(SHORT_TOPIC_NAME, {"value" + std::to_string(i)}); //every Write call writes 4 messages. So, it messagesCount * 4 messages
        }

        std::thread thread1(readToEndThenCommit, std::ref(server), partititonsCount, 4 * messagesCount - 1, "", 400);
        std::thread thread2(readToEndThenCommit, std::ref(server), partititonsCount, 4 * messagesCount - 1, "", 400);
        thread1.join();
        thread2.join();
    }

    Y_UNIT_TEST(InflightLimit) {
        const auto writeSpeed = 10_KB;
        const auto readSpeed = writeSpeed * 2;
        const auto burst = readSpeed;
        const auto writeKbCount = 80_KB;

        auto readToEnd = [] (NPersQueue::TTestServer& server, ui32 partitions, ui32 maxOffset, TString consumer, ui32 readByBytes) {
            std::shared_ptr<grpc::Channel> Channel_;
            std::unique_ptr<Ydb::Topic::V1::TopicService::Stub> StubP_;

            Channel_ = grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials());
            StubP_ = Ydb::Topic::V1::TopicService::NewStub(Channel_);

            grpc::ClientContext rcontext;
            auto readStream = StubP_->StreamRead(&rcontext);
            UNIT_ASSERT(readStream);

            {
                Ydb::Topic::StreamReadMessage::FromClient  req;
                Ydb::Topic::StreamReadMessage::FromServer resp;

                auto topicReadSettings = req.mutable_init_request()->add_topics_read_settings();
                topicReadSettings->set_path(SHORT_TOPIC_NAME);
                for (ui32 i = 0; i < partitions; i++) {
                    topicReadSettings->add_partition_ids(i);
                }

                req.mutable_init_request()->set_consumer(consumer);

                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }

                UNIT_ASSERT(readStream->Read(&resp));
                UNIT_ASSERT(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kInitResponse);
            }
            ui32 partitionsSigned = 0;

            while (partitionsSigned != partitions) {

                Ydb::Topic::StreamReadMessage::FromServer resp;
                UNIT_ASSERT(readStream->Read(&resp));
                UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kStartPartitionSessionRequest, resp);
                auto assignId = resp.start_partition_session_request().partition_session().partition_session_id();

                Ydb::Topic::StreamReadMessage::FromClient req;
                req.mutable_start_partition_session_response()->set_partition_session_id(assignId);
                req.mutable_start_partition_session_response()->set_read_offset(0);
                auto res = readStream->Write(req);
                UNIT_ASSERT(res);
                partitionsSigned++;
            }
            ui32 offset = 0;
            ui32 session = 0;
            while (offset != maxOffset) {
                Ydb::Topic::StreamReadMessage::FromClient  req;
                req.mutable_read_request()->set_bytes_size(readByBytes);
                readStream->Write(req);

                Ydb::Topic::StreamReadMessage::FromServer resp;
                UNIT_ASSERT(readStream->Read(&resp));
                UNIT_ASSERT_C(resp.server_message_case() == Ydb::Topic::StreamReadMessage::FromServer::kReadResponse, resp);
                Cerr << "\n" << "Bytes readed: " << resp.read_response().bytes_size() << "\n";
                for (int j = 0; j < resp.read_response().partition_data_size(); j++) {
                    for (int k = 0; k < resp.read_response().partition_data(j).batches_size(); k++) {
                        for (int l = 0; l < resp.read_response().partition_data(j).batches(k).message_data_size(); l++) {
                            offset = resp.read_response().partition_data(j).batches(k).message_data(l).offset();
                            session = resp.read_response().partition_data(j).partition_session_id();
                            Cerr << "\n" << "Offset: " << offset << " from session " << session << "\n";
                        }
                    }
                }
            }
        };

        auto createServerAndWrite80kb = [] (ui32 inflightLimit) {
            TServerSettings settings = PQSettings(0);
            settings.PQConfig.SetMaxInflightReadRequestsPerPartition(inflightLimit);

            auto quotaSettings = settings.PQConfig.MutableQuotingConfig();
            quotaSettings->SetPartitionReadQuotaIsTwiceWriteQuota(true);
            quotaSettings->SetMaxParallelConsumersPerPartition(1);

            NPersQueue::TTestServer server(settings);

            server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 1, 8 + 1024 * 1024, 86400, writeSpeed);

            TPQDataWriter writer("source", server);
            TString s{writeKbCount/4, 'c'}; // writes 4 times
            writer.Write(SHORT_TOPIC_NAME, {s});
            return std::move(server);
        };

        auto serverWithNoInflightLimit = createServerAndWrite80kb(100);
        auto serverWithInflightLimit = createServerAndWrite80kb(1);


        const auto timeNeededToWaitQuotaAfterReadToEnd= (writeKbCount - burst)/readSpeed;

        readToEnd(serverWithNoInflightLimit, 1, 3, "", 1_MB);
        /*
        Here the reading quota is used. The two threads below will wait for the quota, and when execute in parallel,
        because inflight limit = 100
        */
        auto startTime = TInstant::Now();
        std::thread readParallel1(readToEnd, std::ref(serverWithNoInflightLimit), 1, 3, "", 1_MB);
        std::thread readParallel2(readToEnd, std::ref(serverWithNoInflightLimit), 1, 3, "", 1_MB);
        readParallel1.join();
        readParallel2.join();
        auto diff = (TInstant::Now() - startTime).Seconds();
        UNIT_ASSERT(diff <= (timeNeededToWaitQuotaAfterReadToEnd) + 1);

        readToEnd(serverWithInflightLimit, 1, 3, "", 1_MB);
        /*
        Here the reading quota is used. The two threads below will wait for the quota, and when execute consistently,
        because inflight limit = 1
        */
        startTime = TInstant::Now();
        std::thread readConsistently1(readToEnd, std::ref(serverWithInflightLimit), 1, 3, "", 1_MB);
        std::thread readConsistently2(readToEnd, std::ref(serverWithInflightLimit), 1, 3, "", 1_MB);
        readConsistently1.join();
        readConsistently2.join();
        diff = (TInstant::Now() - startTime).Seconds();
        UNIT_ASSERT(diff >= timeNeededToWaitQuotaAfterReadToEnd * 2);
    }

    Y_UNIT_TEST(TxCounters) {
        TServerSettings settings = PQSettings(0, 1);
        settings.PQConfig.SetTopicsAreFirstClassCitizen(true);
        settings.PQConfig.SetRoot("/Root");
        settings.PQConfig.SetDatabase("/Root");
        settings.SetEnableTopicServiceTx(true);
        NPersQueue::TTestServer server{settings, true};

        server.EnableLogs({ NKikimrServices::PERSQUEUE }, NActors::NLog::PRI_INFO);


        NYdb::TDriverConfig config;
        config.SetEndpoint(TStringBuilder() << "localhost:" + ToString(server.GrpcPort));
        config.SetDatabase("/Root");
        config.SetAuthToken("root@builtin");
        auto driver = NYdb::TDriver(config);

        NYdb::NTable::TTableClient client(driver);
        NYdb::NTopic::TTopicClient topicClient(driver);

        auto result = client.CreateSession().ExtractValueSync();
        auto tableSession = result.GetSession();
        auto txResult = tableSession.BeginTransaction().ExtractValueSync();
        auto tx = txResult.GetTransaction();

        TString topic = "topic";
        NYdb::NTopic::TCreateTopicSettings createSettings;
        createSettings.BeginConfigurePartitioningSettings()
              .MinActivePartitions(1)
              .MaxActivePartitions(1);

        auto status = topicClient.CreateTopic(topic, createSettings).GetValueSync();
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
        server.WaitInit(topic);

        NYdb::NTopic::TWriteSessionSettings options;
        options.Path(topic);
        options.ProducerId("123");
        auto writer = topicClient.CreateWriteSession(options);

        auto send = [&](ui64 dataSize, const TDuration& writeLag) {
            while (true) {
                auto msg = writer->GetEvent(true);
                UNIT_ASSERT(msg);
                auto ev = std::get_if<NYdb::NTopic::TWriteSessionEvent::TReadyToAcceptEvent>(&*msg);
                if (!ev)
                    continue;
                TString data(dataSize, 'a');
                NYdb::NTopic::TWriteMessage writeMsg{data};
                writeMsg.CreateTimestamp(TInstant::Now() - writeLag);
                writeMsg.Codec = NYdb::NTopic::ECodec::RAW;
                writeMsg.Tx(tx);
                writer->Write(std::move(ev->ContinuationToken), std::move(writeMsg));
                return;
            }
        };
        send(10, TDuration::MilliSeconds(1001));
        send(5 * 1024 + 1, TDuration::MilliSeconds(1101));
        send(5 * 1024 + 1, TDuration::MilliSeconds(1201));
        send(10241, TDuration::MilliSeconds(2505));
        writer->Close();

        auto commitResult = tx.Commit().ExtractValueSync();
        UNIT_ASSERT(commitResult.GetStatus() == NYdb::EStatus::SUCCESS);

        auto counters = server.GetRuntime()->GetAppData(0).Counters;
        auto serviceCounters = GetServiceCounters(counters, "datastreams", false);
        auto dbGroup = serviceCounters->GetSubgroup("database", "/Root")
                                ->GetSubgroup("cloud_id", "")
                                ->GetSubgroup("folder_id", "")
                                ->GetSubgroup("database_id", "")->GetSubgroup("topic", "topic");
        TStringStream countersStr;
        dbGroup->OutputHtml(countersStr);
        Cerr << "Counters: ================================ \n" << countersStr.Str() << Endl;
        auto checkSingleCounter = [&](const TString& name, ui64 expected) {
            auto counter = dbGroup->GetNamedCounter("name", name);
            UNIT_ASSERT(counter);
            UNIT_ASSERT_VALUES_EQUAL((ui64)counter->Val(), expected);
        };
        checkSingleCounter("api.grpc.topic.stream_write.bytes", 20796);
        checkSingleCounter("api.grpc.topic.stream_write.messages", 4);
        {
            auto group = dbGroup->GetSubgroup("name", "topic.write.lag_milliseconds");
            UNIT_ASSERT_VALUES_EQUAL((ui64)group->GetNamedCounter("bin", "2000")->Val(), 3);
            UNIT_ASSERT_VALUES_EQUAL((ui64)group->GetNamedCounter("bin", "5000")->Val(), 1);
        }
        {
            auto group = dbGroup->GetSubgroup("name", "topic.write.message_size_bytes");
            UNIT_ASSERT_VALUES_EQUAL((ui64)group->GetNamedCounter("bin", "1024")->Val(), 1);
            UNIT_ASSERT_VALUES_EQUAL((ui64)group->GetNamedCounter("bin", "10240")->Val(), 2);
            UNIT_ASSERT_VALUES_EQUAL((ui64)group->GetNamedCounter("bin", "20480")->Val(), 1);
        }
    }

}
}
