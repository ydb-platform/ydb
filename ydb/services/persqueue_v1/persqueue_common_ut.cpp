#include "actors/read_session_actor.h"
#include <ydb/services/persqueue_v1/ut/pq_data_writer.h>
#include <ydb/services/persqueue_v1/ut/api_test_setup.h>
#include <ydb/services/persqueue_v1/ut/test_utils.h>
#include <ydb/services/persqueue_v1/ut/persqueue_test_fixture.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/persqueue/cluster_tracker.h>

#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/obfuscate/obfuscate.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/join.h>

#include <grpc++/client_context.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.h>


namespace NKikimr::NPersQueueTests {

using namespace Tests;
using namespace NKikimrClient;
using namespace Ydb::PersQueue;
using namespace Ydb::PersQueue::V1;
using namespace NThreading;
using namespace NNetClassifier;


#define MAKE_WRITE_STREAM(TOKEN)                                     \
    grpc::ClientContext context;                                     \
    context.AddMetadata(NYdb::YDB_AUTH_TICKET_HEADER, TOKEN);        \
    auto stream = server.ServiceStub->StreamingWrite(&context);      \


Y_UNIT_TEST_SUITE(TPersQueueCommonTest) {
    // Auth* tests are for both authentication and authorization
    Y_UNIT_TEST(Auth_CreateGrpcStreamWithInvalidTokenInInitialMetadata_SessionClosedWithUnauthenticatedError) {
        TPersQueueV1TestServer server;
        SET_LOCALS;

        runtime->GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);
        runtime->GetAppData().EnforceUserTokenRequirement = true;
        TVector<TString> invalidTokens = {TString(), "test_user", "test_user@invalid_domain"};

        for (const auto &invalidToken : invalidTokens) {
            Cerr << "Invalid token under test is '" << invalidToken << "'" << Endl;
            MAKE_WRITE_STREAM(invalidToken);

            // TODO: Message should be written to gRPC in order to get error. Fix gRPC data plane API code if our expectations are different.
            // Note that I check that initial metadata is sent during gRPC stream constructor.
            StreamingWriteClientMessage clientMessage;
            StreamingWriteServerMessage serverMessage;
            clientMessage.mutable_init_request()->set_topic(server.GetTopicPath());
            clientMessage.mutable_init_request()->set_message_group_id("test-group-id");
            AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);

            UNIT_ASSERT_C(!stream->Read(&serverMessage),
                          "Expect session close with no messages from the server but got message: "
                                  << serverMessage.DebugString());
            UNIT_ASSERT_EQUAL(grpc::StatusCode::UNAUTHENTICATED, stream->Finish().error_code());
        }
    }

    TString GenerateValidToken(int i = 0) {
        return "test_user_" + ToString(i) + "@" + BUILTIN_ACL_DOMAIN;
    }

    Y_UNIT_TEST(Auth_MultipleUpdateTokenRequestIterationsWithValidToken_GotUpdateTokenResponseForEachRequest) {
        TPersQueueV1TestServer server;

        const int iterations = 10;
        NACLib::TDiffACL acl;
        for (int i = 0; i != iterations; ++i) {
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, GenerateValidToken(i));
        }

        server.ModifyTopicACL(server.GetTopic(), acl);

        MAKE_WRITE_STREAM(GenerateValidToken(0));

        StreamingWriteClientMessage clientMessage;
        StreamingWriteServerMessage serverMessage;
        clientMessage.mutable_init_request()->set_topic(server.GetTopicPath());
        clientMessage.mutable_init_request()->set_message_group_id("test-group-id");
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);
        AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
        UNIT_ASSERT_C(serverMessage.server_message_case() == StreamingWriteServerMessage::kInitResponse,
                      serverMessage);


        for (int i = 1; i != iterations; ++i) {
            clientMessage.mutable_update_token_request()->set_token(GenerateValidToken(i));
            AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);
            AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
            UNIT_ASSERT_C(
                    serverMessage.server_message_case() == StreamingWriteServerMessage::kUpdateTokenResponse,
                    serverMessage);
        }
    }

    Y_UNIT_TEST(
            Auth_WriteSessionWithValidTokenAndACEAndThenRemoveACEAndSendWriteRequest_SessionClosedWithUnauthorizedErrorAfterSuccessfullWriteResponse
    ) {
        TPersQueueV1TestServer server;

        //setup.GetPQConfig().SetACLRetryTimeoutSec(0);
        NACLib::TDiffACL acl;
        const auto token = GenerateValidToken();
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, token);
        server.ModifyTopicACL(server.GetTopic(), acl);
        Cerr << "===Make write stream\n";

        MAKE_WRITE_STREAM(token);

        StreamingWriteClientMessage clientMessage;
        StreamingWriteServerMessage serverMessage;
        clientMessage.mutable_init_request()->set_topic(server.GetTopicPath());
        clientMessage.mutable_init_request()->set_message_group_id("test-group-id");
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);
        AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
        UNIT_ASSERT_C(serverMessage.server_message_case() == StreamingWriteServerMessage::kInitResponse,
                      serverMessage);

        acl.ClearAccess();
        Cerr << "===ModifyAcl\n";
        server.ModifyTopicACL(server.GetTopic(), acl);

        clientMessage = StreamingWriteClientMessage();
        auto *writeRequest = clientMessage.mutable_write_request();
        TString message = "x";
        writeRequest->add_sequence_numbers(1);
        writeRequest->add_message_sizes(message.size());
        writeRequest->add_created_at_ms(TInstant::Now().MilliSeconds());
        writeRequest->add_sent_at_ms(TInstant::Now().MilliSeconds());
        writeRequest->add_blocks_offsets(0);
        writeRequest->add_blocks_part_numbers(0);
        writeRequest->add_blocks_message_counts(1);
        writeRequest->add_blocks_uncompressed_sizes(message.size());
        writeRequest->add_blocks_headers(TString(1, '\0'));
        writeRequest->add_blocks_data(message);

        Cerr << "===Assert streaming op1\n";
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);
        Cerr << "===Assert streaming op2\n";
        AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
        UNIT_ASSERT_C(serverMessage.server_message_case() == StreamingWriteServerMessage::kBatchWriteResponse,
                      serverMessage);

        Cerr << "===Wait for session created with token with removed ACE to die";
        AssertStreamingSessionDead(stream, Ydb::StatusIds::UNAUTHORIZED,
                                   Ydb::PersQueue::ErrorCode::ACCESS_DENIED);
    }

    // TODO: Replace this test with a unit-test of TWriteSessionActor
    Y_UNIT_TEST(
            Auth_MultipleInflightWriteUpdateTokenRequestWithDifferentValidToken_SessionClosedWithOverloadedError
    ) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        const int iterations = 3;
        NACLib::TDiffACL acl;
        for (int i = 0; i != iterations; ++i) {
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, GenerateValidToken(i));
        }

        server.ModifyTopicACL(server.GetTopic(), acl);
        MAKE_WRITE_STREAM(GenerateValidToken(0));

        StreamingWriteClientMessage clientMessage;
        StreamingWriteServerMessage serverMessage;
        clientMessage.mutable_init_request()->set_topic(server.GetTopicPath());
        clientMessage.mutable_init_request()->set_message_group_id("test-group-id");
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);
        AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
        UNIT_ASSERT_C(serverMessage.server_message_case() == StreamingWriteServerMessage::kInitResponse,
                      serverMessage);

        // TWriteSessionActor uses GRpcRequestProxy for authentication. This will make next update token procedure stuck indefinetely
        auto noopActorID = TActorId();
        for (size_t i = 0; i != runtime->GetNodeCount(); ++i) {
            // Destroy GRpcRequestProxy
            runtime->RegisterService(NGRpcService::CreateGRpcRequestProxyId(), noopActorID);
        }

        clientMessage.mutable_update_token_request()->set_token(GenerateValidToken(1));
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);


        clientMessage.mutable_update_token_request()->set_token(GenerateValidToken(2));
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);


        AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
        UNIT_ASSERT_VALUES_EQUAL_C(Ydb::StatusIds::OVERLOADED, serverMessage.status(), serverMessage);
        UNIT_ASSERT_C(
                serverMessage.server_message_case() == StreamingWriteServerMessage::SERVER_MESSAGE_NOT_SET,
                serverMessage);
    }

    Y_UNIT_TEST(Auth_WriteUpdateTokenRequestWithInvalidToken_SessionClosedWithUnauthenticatedError) {
        TPersQueueV1TestServer server;
        SET_LOCALS;
        runtime->GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);
        const TString validToken = "test_user@" BUILTIN_ACL_DOMAIN;
        // TODO: Why test fails with 'BUILTIN_ACL_DOMAIN' as domain in invalid token?
        TVector<TString> invalidTokens = {TString(), "test_user", "test_user@invalid_domain"};
        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, validToken);
        server.ModifyTopicACL(server.GetTopic(), acl);

        for (const auto &invalidToken : invalidTokens) {
            Cerr << "Invalid token under test is '" << invalidToken << "'" << Endl;
            MAKE_WRITE_STREAM(validToken);

            StreamingWriteClientMessage clientMessage;
            StreamingWriteServerMessage serverMessage;
            clientMessage.mutable_init_request()->set_topic(server.GetTopicPath());
            clientMessage.mutable_init_request()->set_message_group_id("test-group-id");
            AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);
            AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
            UNIT_ASSERT_C(serverMessage.server_message_case() == StreamingWriteServerMessage::kInitResponse,
                          serverMessage);

            clientMessage.mutable_update_token_request()->set_token(invalidToken);
            AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);


            UNIT_ASSERT_C(!stream->Read(&serverMessage),
                          "Expect session close with no messages from the server but got message: "
                                  << serverMessage.DebugString());
            UNIT_ASSERT_EQUAL(grpc::StatusCode::UNAUTHENTICATED, stream->Finish().error_code());
        }
    }

    Y_UNIT_TEST(Auth_WriteUpdateTokenRequestWithValidTokenButWithoutACL_SessionClosedWithUnauthorizedError) {
        TPersQueueV1TestServer server;

        const TString validToken = "test_user@"
                                   BUILTIN_ACL_DOMAIN;
        const TString invalidToken = "test_user_2@"
                                     BUILTIN_ACL_DOMAIN;
        NACLib::TDiffACL acl;
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, validToken);

        server.ModifyTopicACL(server.GetTopic(), acl);

        MAKE_WRITE_STREAM(validToken);

        StreamingWriteClientMessage clientMessage;
        StreamingWriteServerMessage serverMessage;
        clientMessage.mutable_init_request()->set_topic(server.GetTopicPath());
        clientMessage.mutable_init_request()->set_message_group_id("test-message-group");
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);
        AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
        UNIT_ASSERT_C(serverMessage.server_message_case() == StreamingWriteServerMessage::kInitResponse,
                      serverMessage);


        clientMessage.mutable_update_token_request()->set_token(invalidToken);
        AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);


        AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
        UNIT_ASSERT_VALUES_EQUAL(Ydb::StatusIds::UNAUTHORIZED, serverMessage.status());
        UNIT_ASSERT_EQUAL_C(StreamingWriteServerMessage::SERVER_MESSAGE_NOT_SET,
                            serverMessage.server_message_case(), serverMessage);
    }

    void TestWriteWithRateLimiter(TPersQueueV1TestServerWithRateLimiter& server, const TDuration& minTime) {
        const std::vector<TString> differentTopicPathsTypes = {
                "account1/topic", // without folder
                "account2/folder/topic", // with folder
                "account3/folder1/folder2/topic", // complex
        };
        const TString data = TString("1234567890") * 120000; // 1200000 bytes
        for (const TString &topicPath : differentTopicPathsTypes) {
            server.CreateTopicWithQuota(topicPath, true, 10000000);
            auto driver = server.Server->AnnoyingClient->GetDriver();
            auto start = TInstant::Now();

            for (ui32 i = 0; i < 7; ++i) {
                auto writer = CreateSimpleWriter(*driver, server.TenantModeEnabled() ? "/Root/PQ/" + topicPath : topicPath, TStringBuilder() << "123" << i, {}, "raw");
                writer->Write(data);
                bool res = writer->Close(TDuration::Seconds(10));
                UNIT_ASSERT(res);
            }

            Cerr << "DURATION " << (TInstant::Now() - start) << "\n";
            UNIT_ASSERT(TInstant::Now() - start > minTime);
        }
    }

    Y_UNIT_TEST(TestWriteWithRateLimiterWithBlobsRateLimit) {
        TPersQueueV1TestServerWithRateLimiter server;
        server.InitAll(NKikimrPQ::TPQConfig::TQuotingConfig::WRITTEN_BLOB_SIZE);
        server.EnablePQLogs({NKikimrServices::PERSQUEUE}, NLog::EPriority::PRI_DEBUG);
        TestWriteWithRateLimiter(server, TDuration::MilliSeconds(5200));
    }

    Y_UNIT_TEST(TestWriteWithRateLimiterWithUserPayloadRateLimit) {
        TPersQueueV1TestServerWithRateLimiter server;
        server.InitAll(NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE);
        server.EnablePQLogs({NKikimrServices::PERSQUEUE}, NLog::EPriority::PRI_DEBUG);
        TestWriteWithRateLimiter(server, TDuration::MilliSeconds(2500));
    }

    void TestRateLimiterLimitsWrite(TPersQueueV1TestServerWithRateLimiter& server) {
        const TString topicPath = "account/topic";

        server.CreateTopicWithQuota(topicPath, true, 100.0);
        const TString data = TString("123") * 100; // 300 bytes // 3 seconds

        auto driver = server.Server->AnnoyingClient->GetDriver();

        // Warm up write
        {
            auto writer = CreateSimpleWriter(*driver, server.TenantModeEnabled() ? "/Root/PQ/" + topicPath : topicPath, "123", {}, "raw");

            writer->Write(data);
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        // will be removed

        const TInstant startWrite = TInstant::Now();


        {
            auto writer = CreateSimpleWriter(*driver, server.TenantModeEnabled() ? "/Root/PQ/" + topicPath : topicPath, "123", {}, "raw");

            writer->Write(data);
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        {
            auto writer = CreateSimpleWriter(*driver, server.TenantModeEnabled() ? "/Root/PQ/" + topicPath : topicPath, "123", {}, "raw");

            writer->Write(data);
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }


        const TInstant endWrite = TInstant::Now();
        // Check write time with quota
        const TDuration writeTime = endWrite - startWrite;
        // in new scheme cache rate limiting is turned off
        if (server.TenantModeEnabled()) {
            UNIT_ASSERT_GE_C(TDuration::Seconds(3), writeTime, "Write time: " << writeTime);
        } else {
            UNIT_ASSERT_GE_C(writeTime, TDuration::Seconds(3), "Write time: " << writeTime);
        }

    }

    Y_UNIT_TEST(TestLimiterLimitsWithBlobsRateLimit) {
        TPersQueueV1TestServerWithRateLimiter server;
        server.InitAll(NKikimrPQ::TPQConfig::TQuotingConfig::WRITTEN_BLOB_SIZE);
        server.EnablePQLogs({NKikimrServices::PERSQUEUE}, NLog::EPriority::PRI_DEBUG);
        TestRateLimiterLimitsWrite(server);
    }

    Y_UNIT_TEST(TestLimiterLimitsWithUserPayloadRateLimit) {
        TPersQueueV1TestServerWithRateLimiter server;
        server.InitAll(NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE);
        server.EnablePQLogs({NKikimrServices::PERSQUEUE}, NLog::EPriority::PRI_DEBUG);

        TestRateLimiterLimitsWrite(server);
    }

}

}
