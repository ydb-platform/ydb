#pragma once
#include "pq_data_writer.h"
#include "test_utils.h"

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/core/testlib/test_pq_client.h>

#include <google/protobuf/message.h>
#include <library/cpp/logger/log.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/string/builder.h>

class APITestSetup {
private:
    using TStreamingWriteClientMessage = Ydb::PersQueue::V1::StreamingWriteClientMessage;
    using TStreamingWriteServerMessage = Ydb::PersQueue::V1::StreamingWriteServerMessage;
    TLog Log;
    NPersQueue::TTestServer server;
    std::shared_ptr<::grpc::Channel> channel;
    std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> service;
public:
    APITestSetup(const TString& testCaseName)
        : Log("cerr")
        , server(NPersQueue::TTestServer())
        , channel(grpc::CreateChannel("localhost:" + ToString(server.GrpcPort), grpc::InsecureChannelCredentials()))
        , service(Ydb::PersQueue::V1::PersQueueService::NewStub(channel))
    {
        Log.SetFormatter([testCaseName](ELogPriority priority, TStringBuf message) {
            return TStringBuilder() << TInstant::Now() << " :" << testCaseName << " " << priority << ": " << message << Endl;
        });
        server.CleverServer->GetRuntime()->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NActors::NLog::PRI_DEBUG);
        server.CleverServer->GetRuntime()->SetLogPriority(NKikimrServices::PQ_READ_PROXY, NActors::NLog::PRI_DEBUG);
        server.AnnoyingClient->CreateTopic("rt3.dc1--" + TString(GetTestTopic()), 1);
        NKikimr::NPersQueueTests::TPQDataWriter writer(GetTestMessageGroupId(), server);

        auto seed = TInstant::Now().MicroSeconds();
        // This makes failing randomized tests (for example with NUnitTest::RandomString(size, std::rand()) calls) reproducable
        Log << TLOG_INFO << "Random seed for debugging is " << seed;
        std::srand(seed);
    }

    TLog& GetLog() {
        return Log;
    }

    TString GetTestTopic() {
        return "topic1";
    }

    TString GetTestMessageGroupId() {
        return "test-message-group-id";
    }

    TString GetLocalCluster() {
        return "dc1";
    }

    TString GetRemoteCluster() {
        return "dc2";
    }

    TDuration GetLongDelay() {
        return TDuration::Max() - TDuration::Seconds(1) /* So delay does not get mixed up with infinity */;
    }

    NKikimr::Tests::TServer& GetServer() {
        return *server.CleverServer;
    }

    TString GetPQRoot() {
        return "/Root/PQ";
    }

    NKikimrPQ::TPQConfig& GetPQConfig() {
        return server.CleverServer->GetRuntime()->GetAppData().PQConfig;
    }

    NKikimr::NPersQueueTests::TFlatMsgBusPQClient& GetFlatMsgBusPQClient() {
        return *server.AnnoyingClient;
    }

    std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub>& GetPersQueueService() {
        return service;
    }

    std::pair<std::unique_ptr<grpc::ClientReaderWriter<TStreamingWriteClientMessage, TStreamingWriteServerMessage>>, std::unique_ptr<grpc::ClientContext>> InitWriteSession(
        const typename TStreamingWriteClientMessage::InitRequest& setup = TStreamingWriteClientMessage::InitRequest())
    {
        auto result = InitWriteSession(setup, false);
        return {std::move(result.Stream), std::move(result.Context)};
    }

    // Initializes session with default (possible overwriten by setup parameter) values and returns initialization response
    template<typename TClientMessage, typename TServerMessage>
    TServerMessage InitSession(std::unique_ptr<grpc::ClientReaderWriter<TClientMessage, TServerMessage>>& stream,
        const typename TClientMessage::InitRequest& setup = typename TClientMessage::InitRequest())
    {
        return InitSession(stream, setup, false);
    }

    using TClientContextPtr = std::unique_ptr<grpc::ClientContext>;
    using TStreamPtr = std::unique_ptr<grpc::ClientReaderWriter<TStreamingWriteClientMessage, TStreamingWriteServerMessage>>;

    struct TInitWriteSessionResult {
        TClientContextPtr Context;
        TStreamPtr Stream;
        TStreamingWriteServerMessage FirstMessage;
    };

    TInitWriteSessionResult InitWriteSession(const typename TStreamingWriteClientMessage::InitRequest& setup,
                                             bool mayBeAborted)
    {
        auto context = std::make_unique<grpc::ClientContext>();
        auto stream = service->StreamingWrite(context.get());
        auto message = InitSession(stream, setup, mayBeAborted);
        return {std::move(context), std::move(stream), std::move(message)};
    }

    template<typename TClientMessage, typename TServerMessage>
    TServerMessage InitSession(std::unique_ptr<grpc::ClientReaderWriter<TClientMessage, TServerMessage>>& stream,
                               const typename TClientMessage::InitRequest& setup,
                               bool mayBeAborted)
    {
        TClientMessage clientMessage;
        // TODO: Replace with MergeFrom?
        clientMessage.mutable_init_request()->CopyFrom(setup);
        if (setup.topic().empty()) {
            clientMessage.mutable_init_request()->set_topic(GetTestTopic());
        }
        if (setup.message_group_id().empty()) {
            clientMessage.mutable_init_request()->set_message_group_id(GetTestMessageGroupId());
        }
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);

        TServerMessage serverMessage;
        Log << TLOG_INFO << "Wait for \"init_response\"";
        AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
        Cerr << "Init response: " << serverMessage.ShortDebugString() << Endl;

        if ((serverMessage.status() != Ydb::StatusIds::ABORTED) || !mayBeAborted) {
            UNIT_ASSERT_C(serverMessage.server_message_case() == TServerMessage::kInitResponse, serverMessage);
            Log << TLOG_INFO << "Session ID is " << serverMessage.init_response().session_id().Quote();
        }

        return serverMessage;
    }
};
