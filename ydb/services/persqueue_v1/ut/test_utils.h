#pragma once
#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/core/testlib/test_pq_client.h>

#include <library/cpp/testing/unittest/registar.h>
#include <google/protobuf/message.h>
#include <contrib/libs/grpc/include/grpcpp/support/sync_stream.h>
#include <util/string/builder.h>
#include <util/system/type_name.h>

#define TEST_CASE_NAME (TypeName(*this).rfind("TTestCase") != TString::npos ? TypeName(*this).substr(TypeName(*this).rfind("TTestCase") + 9) : TypeName(*this))

static constexpr int DEBUG_LOG_LEVEL = 7;

using namespace NKikimr::NPersQueueTests;

template<typename TStream, typename TMessage = google::protobuf::Message>
void AssertSuccessfullStreamingOperation(bool ok, std::unique_ptr<TStream>& stream, TMessage* message = nullptr) {
    if (!ok) {
        auto status = stream->Finish();
        TStringBuilder errorMessage;
        errorMessage << "gRPC stream operation failed with error code " << static_cast<int>(status.error_code()) << " and error message '" << status.error_message() << "'.";
        if (message != nullptr) {
            errorMessage << " Last user message is " << message->DebugString();
        }
        UNIT_FAIL(errorMessage);
    }
}

template<typename TClientMessage, typename TServerMessage>
void AssertStreamingSessionAlive(std::unique_ptr<grpc::ClientReaderWriter<TClientMessage, TServerMessage>>& stream) {
    TClientMessage clientMessage;
    // TODO: Add 'ping_request' and 'ping_response' to write and read protocol for debugging?
    clientMessage.mutable_update_token_request();
    AssertSuccessfullStreamingOperation(stream->Write(clientMessage), stream, &clientMessage);
    TServerMessage serverMessage;
    AssertSuccessfullStreamingOperation(stream->Read(&serverMessage), stream);
    UNIT_ASSERT_EQUAL_C(TServerMessage::kUpdateTokenResponse, serverMessage.server_message_case(), serverMessage);
}

template<typename TClientMessage, typename TServerMessage>
void AssertStreamingSessionDead(std::unique_ptr<grpc::ClientReaderWriter<TClientMessage, TServerMessage>>& stream,
    const Ydb::StatusIds::StatusCode expectedStatus, const Ydb::PersQueue::ErrorCode::ErrorCode expectedErrorCode)
{
    TServerMessage serverMessage;
    auto res = stream->Read(&serverMessage);
    Cerr << serverMessage.DebugString() << "\n";
    AssertSuccessfullStreamingOperation(res, stream);
    UNIT_ASSERT_VALUES_EQUAL(expectedStatus, serverMessage.status());
    UNIT_ASSERT_LE(1, serverMessage.issues_size());
    // TODO: Why namespace duplicates enum name "ErrorCode::ErrorCode"?
    // TODO: Why "Ydb::PersQueue::ErrorCode::ErrorCode" doesn't work with streaming output like "Ydb::StatusIds::StatusCode" does?
    auto actualErrorCode = static_cast<Ydb::PersQueue::ErrorCode::ErrorCode>(serverMessage.issues(0).issue_code());
    UNIT_ASSERT_C(expectedErrorCode == actualErrorCode, serverMessage);
}

template<typename TClientMessage, typename TServerMessage>
void AssertStreamingSessionDead(std::unique_ptr<grpc::ClientReaderWriter<TClientMessage, TServerMessage>>& stream,
    const Ydb::StatusIds::StatusCode expectedStatus, const Ydb::PersQueue::ErrorCode::ErrorCode expectedErrorCode,
    const TServerMessage& firstMessage)
{
    auto ensureExpectedError = [&](const TServerMessage& serverMessage) {
        UNIT_ASSERT_LE(1, serverMessage.issues_size());
        // TODO: Why namespace duplicates enum name "ErrorCode::ErrorCode"?
        // TODO: Why "Ydb::PersQueue::ErrorCode::ErrorCode" doesn't work with streaming output like "Ydb::StatusIds::StatusCode" does?
        auto actualErrorCode = static_cast<Ydb::PersQueue::ErrorCode::ErrorCode>(serverMessage.issues(0).issue_code());
        UNIT_ASSERT_C(expectedErrorCode == actualErrorCode, serverMessage);
    };

    if (firstMessage.status() == expectedStatus) {
        ensureExpectedError(firstMessage);
        return;
    }

    TServerMessage serverMessage;
    auto res = stream->Read(&serverMessage);
    Cerr << serverMessage.DebugString() << "\n";
    AssertSuccessfullStreamingOperation(res, stream);
    UNIT_ASSERT_VALUES_EQUAL(expectedStatus, serverMessage.status());
    ensureExpectedError(serverMessage);
}
