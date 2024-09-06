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

#include <grpcpp/client_context.h>

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

class TCommonTests {
private:
    bool TenantModeEnabled;
public:
    TCommonTests(bool tenantModeEnabled)
        : TenantModeEnabled(tenantModeEnabled)
    {}

    TPersQueueV1TestServer CreateServer() {
        return TPersQueueV1TestServer({.TenantModeEnabled=TenantModeEnabled});
    }

    TPersQueueV1TestServerWithRateLimiter CreateServerWithRateLimiter() {
        return TPersQueueV1TestServerWithRateLimiter(TenantModeEnabled);
    }

    TString GenerateValidToken(int i = 0) {
        return "test_user_" + ToString(i) + "@" + BUILTIN_ACL_DOMAIN;
    }

    // Auth* tests are for both authentication and authorization
    void CreateGrpcStreamWithInvalidTokenInInitialMetadata() {
        TPersQueueV1TestServer server = CreateServer();
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

    void MultipleUpdateTokenRequestIterationsWithValidToken() {
        TPersQueueV1TestServer server = CreateServer();

        const int iterations = 10;
        TVector<std::pair<TString, TVector<TString>>> permissions;
        for (int i = 0; i != iterations; ++i) {
            permissions.push_back({GenerateValidToken(i), {"ydb.generic.write"}});
        }

        server.ModifyTopicACL(server.GetFullTopicPath(), permissions);

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

    void WriteSessionWithValidTokenAndACEAndThenRemoveACEAndSendWriteRequest() {
        TPersQueueV1TestServer server = CreateServer();

        const auto token = GenerateValidToken();

        server.ModifyTopicACL(server.GetFullTopicPath(), {{token, {"ydb.generic.write"}}});
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
        Cerr << "===ModifyAcl\n";

        server.ModifyTopicACL(server.GetFullTopicPath(), {{token, {}}});

        Cerr << "===Wait for session created with token with removed ACE to die";
        AssertStreamingSessionDead(stream, Ydb::StatusIds::UNAUTHORIZED,
                                   Ydb::PersQueue::ErrorCode::ACCESS_DENIED);
    }

    // TODO: Replace this test with a unit-test of TWriteSessionActor
    void MultipleInflightWriteUpdateTokenRequestWithDifferentValidToken() {
        TPersQueueV1TestServer server = CreateServer();
        SET_LOCALS;
        const int iterations = 3;
        TVector<std::pair<TString, TVector<TString>>> permissions;
        for (int i = 0; i != iterations; ++i) {
            permissions.push_back({GenerateValidToken(i), {"ydb.generic.write"}});
        }


        server.ModifyTopicACL(server.GetFullTopicPath(), permissions);
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

    void WriteUpdateTokenRequestWithInvalidToken() {
        TPersQueueV1TestServer server = CreateServer();
        SET_LOCALS;
        runtime->GetAppData().PQConfig.SetRequireCredentialsInNewProtocol(true);
        const TString validToken = "test_user@" BUILTIN_ACL_DOMAIN;
        // TODO: Why test fails with 'BUILTIN_ACL_DOMAIN' as domain in invalid token?
        TVector<TString> invalidTokens = {TString(), "test_user", "test_user@invalid_domain"};
        server.ModifyTopicACLAndWait(server.GetFullTopicPath(), {{validToken, {"ydb.generic.write"}}});

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

    void WriteUpdateTokenRequestWithValidTokenButWithoutACL() {
        TPersQueueV1TestServer server = CreateServer();

        const TString validToken = "test_user@"
                                   BUILTIN_ACL_DOMAIN;
        const TString invalidToken = "test_user_2@"
                                     BUILTIN_ACL_DOMAIN;

        server.ModifyTopicACL(server.GetFullTopicPath(), {{validToken, {"ydb.generic.write"}}});

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

    void TestWriteWithRateLimiter(NKikimrPQ::TPQConfig::TQuotingConfig::ELimitedEntity limitedEntity, const TDuration& minTime) {
        TPersQueueV1TestServerWithRateLimiter server = CreateServerWithRateLimiter();
        server.InitAll(limitedEntity);
        server.EnablePQLogs({NKikimrServices::PERSQUEUE}, NLog::EPriority::PRI_DEBUG);

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
            const TString fullTopicPath = server.TenantModeEnabled() ? "/Root/PQ/" + topicPath : topicPath;

            for (ui32 i = 0; i < 7; ++i) {
                auto writer = CreateSimpleWriter(*driver, fullTopicPath, TStringBuilder() << "123" << i, {}, "raw");
                writer->Write(data);
                bool res = writer->Close(TDuration::Seconds(10));
                UNIT_ASSERT(res);
            }

            Cerr << "DURATION " << (TInstant::Now() - start) << "\n";
            UNIT_ASSERT(TInstant::Now() - start > minTime);
        }
    }

    void TestRateLimiterLimitsWrite(NKikimrPQ::TPQConfig::TQuotingConfig::ELimitedEntity limitedEntity) {
        TPersQueueV1TestServerWithRateLimiter server = CreateServerWithRateLimiter();
        server.InitAll(limitedEntity);
        server.EnablePQLogs({NKikimrServices::PERSQUEUE}, NLog::EPriority::PRI_DEBUG);

        const TString topicPath = "account/topic";
        const TString fullTopicPath = server.TenantModeEnabled() ? "/Root/PQ/" + topicPath : topicPath;

        server.CreateTopicWithQuota(topicPath, true, 100.0);
        const TString data = TString("123") * 100; // 300 bytes // 3 seconds

        auto driver = server.Server->AnnoyingClient->GetDriver();

        // Warm up write
        {
            auto writer = CreateSimpleWriter(*driver, fullTopicPath, "123", {}, "raw");
            writer->Write(data);
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }

        // will be removed
        const TInstant startWrite = TInstant::Now();
        {
            auto writer = CreateSimpleWriter(*driver, fullTopicPath, "123", {}, "raw");
            writer->Write(data);
            bool res = writer->Close(TDuration::Seconds(10));
            UNIT_ASSERT(res);
        }
        {
            auto writer = CreateSimpleWriter(*driver, fullTopicPath, "123", {}, "raw");
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

    void WriteWithBlobsRateLimit() {
        //TestWriteWithRateLimiter(NKikimrPQ::TPQConfig::TQuotingConfig::WRITTEN_BLOB_SIZE, TDuration::MilliSeconds(5200));
    }

    void WriteWithUserPayloadRateLimit() {
        TestWriteWithRateLimiter(NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE, TDuration::MilliSeconds(2500));
    }

    void LimitsWithBlobsRateLimit() {
        TestRateLimiterLimitsWrite(NKikimrPQ::TPQConfig::TQuotingConfig::WRITTEN_BLOB_SIZE);
    }

    void LimitsWithUserPayloadRateLimit() {
        TestRateLimiterLimitsWrite(NKikimrPQ::TPQConfig::TQuotingConfig::USER_PAYLOAD_SIZE);
    }
}; // TCommonTests


#define COMMON_TEST(tenantMode, name, method) \
    Y_UNIT_TEST(name) {                       \
        TCommonTests(tenantMode).method();    \
    }

#define DECLARE_ALL_COMMON_TESTS(tenantMode)    \
Y_UNIT_TEST_SUITE(TPersQueueCommonTest) {       \
    COMMON_TEST(                                \
        tenantMode,                             \
        Auth_CreateGrpcStreamWithInvalidTokenInInitialMetadata_SessionClosedWithUnauthenticatedError,       \
        CreateGrpcStreamWithInvalidTokenInInitialMetadata                                                   \
    )                                                                                                       \
    COMMON_TEST(                                                                                            \
        tenantMode,                                                                                         \
        Auth_MultipleUpdateTokenRequestIterationsWithValidToken_GotUpdateTokenResponseForEachRequest,       \
        MultipleUpdateTokenRequestIterationsWithValidToken                                                  \
    )                                                                                                       \
    COMMON_TEST(                                                                                            \
        tenantMode,                                                                                         \
        Auth_WriteSessionWithValidTokenAndACEAndThenRemoveACEAndSendWriteRequest_SessionClosedWithUnauthorizedErrorAfterSuccessfullWriteResponse, \
        WriteSessionWithValidTokenAndACEAndThenRemoveACEAndSendWriteRequest                                 \
    )                                                                                                       \
    COMMON_TEST(                                                                                            \
        tenantMode,                                                                                         \
        Auth_MultipleInflightWriteUpdateTokenRequestWithDifferentValidToken_SessionClosedWithOverloadedError, \
        MultipleInflightWriteUpdateTokenRequestWithDifferentValidToken                                      \
    )                                                                                                       \
    COMMON_TEST(                                                                                            \
        tenantMode,                                                                                         \
        Auth_WriteUpdateTokenRequestWithInvalidToken_SessionClosedWithUnauthenticatedError,                 \
        WriteUpdateTokenRequestWithInvalidToken                                                             \
    )                                                                                                       \
    COMMON_TEST(                                                                                            \
        tenantMode,                                                                                         \
        Auth_WriteUpdateTokenRequestWithValidTokenButWithoutACL_SessionClosedWithUnauthorizedError,         \
        WriteUpdateTokenRequestWithValidTokenButWithoutACL                                                  \
    )                                                                                                       \
    COMMON_TEST(tenantMode, TestWriteWithRateLimiterWithBlobsRateLimit,WriteWithBlobsRateLimit)             \
    COMMON_TEST(tenantMode,TestWriteWithRateLimiterWithUserPayloadRateLimit,WriteWithUserPayloadRateLimit)  \
    COMMON_TEST(tenantMode, TestLimiterLimitsWithBlobsRateLimit,LimitsWithBlobsRateLimit)                   \
    COMMON_TEST(tenantMode, TestLimiterLimitsWithUserPayloadRateLimit, LimitsWithUserPayloadRateLimit)      \
} // Y_UNIT_TEST_SUITE
}
