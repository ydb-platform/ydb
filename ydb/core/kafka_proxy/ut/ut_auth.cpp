#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/base64/base64.h>

#include "kafka_test_client.h"
#include "test_server.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/library/login/sasl/scram.h>

#include <util/random/random.h>

using namespace NKafka;
using namespace NYdb;

Y_UNIT_TEST_SUITE(KafkaScramFirstMessage) {

    // Test successful first SCRAM message with valid user
    Y_UNIT_TEST(FirstMsgValidUser) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        auto apiVersionsMsg = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersionsMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        auto handshakeMsg = client.SaslHandshake("SCRAM-SHA-256");
        UNIT_ASSERT_VALUES_EQUAL(handshakeMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(handshakeMsg->Mechanisms.size(), 2u);

        TString randomBytes;
        randomBytes.reserve(16);
        for (size_t i = 0; i < 16; ++i) {
            randomBytes += static_cast<char>(RandomNumber<ui8>());
        }
        TString clientNonce = Base64Encode(randomBytes);

        auto response = client.SaslScramAuthenticateFirstMsg("ouruser", clientNonce);

        UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT(!response->ErrorMessage.has_value() || response->ErrorMessage->empty());
        UNIT_ASSERT(response->AuthBytes.has_value());
        UNIT_ASSERT_GT(response->AuthBytes->size(), 0);

        const auto& authBytes = response->AuthBytes.value();
        std::string serverFirstMessage(authBytes.data(), authBytes.size());
        NLogin::NSasl::TFirstServerMsg parsedServerMsg;
        auto parseResult = NLogin::NSasl::ParseFirstServerMsg(serverFirstMessage, parsedServerMsg);
        UNIT_ASSERT_C(parseResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse server first message");
        UNIT_ASSERT_GT(parsedServerMsg.IterationsCount, 0);
        UNIT_ASSERT(!parsedServerMsg.Salt.empty());
        UNIT_ASSERT(!parsedServerMsg.Nonce.empty());
    }

    // Test first SCRAM message with non-existent user
    Y_UNIT_TEST(FirstMsgNonExistentUser) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        auto apiVersionsMsg = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersionsMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        auto handshakeMsg = client.SaslHandshake("SCRAM-SHA-256");
        UNIT_ASSERT_VALUES_EQUAL(handshakeMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        TString randomBytes;
        randomBytes.reserve(16);
        for (size_t i = 0; i < 16; ++i) {
            randomBytes += static_cast<char>(RandomNumber<ui8>());
        }
        TString clientNonce = Base64Encode(randomBytes);

        auto response = client.SaslScramAuthenticateFirstMsg("nonexistentuser", clientNonce);

        UNIT_ASSERT_VALUES_UNEQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::SASL_AUTHENTICATION_FAILED));

        UNIT_ASSERT(response->ErrorMessage.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*response->ErrorMessage, "Authentication failure. ");

        UNIT_ASSERT(response->AuthBytes.has_value());
        std::string errorMessage(response->AuthBytes->data(), response->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        UNIT_ASSERT_C(parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse error message");
        UNIT_ASSERT_C(!errorMsg.Error.empty(), "Error field should not be empty");
        UNIT_ASSERT_STRING_CONTAINS(errorMsg.Error, "unknown-user");
    }

    // Test first SCRAM message with empty username
    Y_UNIT_TEST(FirstMsgEmptyUsername) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        auto apiVersionsMsg = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersionsMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        auto handshakeMsg = client.SaslHandshake("SCRAM-SHA-256");
        UNIT_ASSERT_VALUES_EQUAL(handshakeMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        TString randomBytes;
        randomBytes.reserve(16);
        for (size_t i = 0; i < 16; ++i) {
            randomBytes += static_cast<char>(RandomNumber<ui8>());
        }
        TString clientNonce = Base64Encode(randomBytes);

        auto response = client.SaslScramAuthenticateFirstMsg("", clientNonce);

        UNIT_ASSERT_VALUES_UNEQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::SASL_AUTHENTICATION_FAILED));

        UNIT_ASSERT(response->ErrorMessage.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*response->ErrorMessage, "Authentication failure. ");

        // Check error message
        UNIT_ASSERT(response->AuthBytes.has_value());
        std::string errorMessage(response->AuthBytes->data(), response->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        UNIT_ASSERT_C(parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse error message");
        UNIT_ASSERT_C(!errorMsg.Error.empty(), "Error field should not be empty");
        UNIT_ASSERT_VALUES_EQUAL(errorMsg.Error, "other-error");
    }

    // Test first SCRAM message with different nonce lengths
    Y_UNIT_TEST(FirstMsgDifferentNonceLengths) {
        TInsecureTestServer testServer("2");

        // Test with short nonce (8 bytes)
        {
            TKafkaTestClient client(testServer.Port);

            auto apiVersionsMsg = client.ApiVersions();
            UNIT_ASSERT_VALUES_EQUAL(apiVersionsMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            auto handshakeMsg = client.SaslHandshake("SCRAM-SHA-256");
            UNIT_ASSERT_VALUES_EQUAL(handshakeMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            TString randomBytes;
            randomBytes.reserve(8);
            for (size_t i = 0; i < 8; ++i) {
                randomBytes += static_cast<char>(RandomNumber<ui8>());
            }
            TString clientNonce = Base64Encode(randomBytes);

            auto response = client.SaslScramAuthenticateFirstMsg("ouruser", clientNonce);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT(!response->ErrorMessage.has_value() || response->ErrorMessage->empty());
            UNIT_ASSERT(response->AuthBytes.has_value());
        }

        // Test with long nonce (32 bytes)
        {
            TKafkaTestClient client(testServer.Port);

            auto apiVersionsMsg = client.ApiVersions();
            UNIT_ASSERT_VALUES_EQUAL(apiVersionsMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            auto handshakeMsg = client.SaslHandshake("SCRAM-SHA-256");
            UNIT_ASSERT_VALUES_EQUAL(handshakeMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

            TString randomBytes;
            randomBytes.reserve(32);
            for (size_t i = 0; i < 32; ++i) {
                randomBytes += static_cast<char>(RandomNumber<ui8>());
            }
            TString clientNonce = Base64Encode(randomBytes);

            auto response = client.SaslScramAuthenticateFirstMsg("ouruser", clientNonce);
            UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
            UNIT_ASSERT(!response->ErrorMessage.has_value() || response->ErrorMessage->empty());
            UNIT_ASSERT(response->AuthBytes.has_value());
        }
    }

    // Test first SCRAM message with non-printable characters in nonce (not base64-encoded)
    Y_UNIT_TEST(FirstMsgNonPrintableNonce) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        auto apiVersionsMsg = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersionsMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        auto handshakeMsg = client.SaslHandshake("SCRAM-SHA-256");
        UNIT_ASSERT_VALUES_EQUAL(handshakeMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        TString clientNonce;
        clientNonce.reserve(16);
        for (size_t i = 0; i < 16; ++i) {
            // Include control characters (0x00-0x1F)
            clientNonce += static_cast<char>(i % 32);
        }

        auto response = client.SaslScramAuthenticateFirstMsg("ouruser", clientNonce);

        UNIT_ASSERT_VALUES_UNEQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(response->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::SASL_AUTHENTICATION_FAILED));

        UNIT_ASSERT(response->ErrorMessage.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*response->ErrorMessage, "Authentication failure. ");

        UNIT_ASSERT(response->AuthBytes.has_value());
        std::string errorMessage(response->AuthBytes->data(), response->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        UNIT_ASSERT_C(parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse error message");
        UNIT_ASSERT_C(!errorMsg.Error.empty(), "Error field should not be empty");
    }
} // Y_UNIT_TEST_SUITE(KafkaScramFirstMessage)

Y_UNIT_TEST_SUITE(KafkaScramFinalMessage) {
    // Helper function to perform first message exchange and return parsed server response
    struct TFirstMessageResult {
        TString ServerNonce;
        TString Salt;
        ui32 IterationsCount;
        TString ClientFirstMessageBare;
        TString ServerFirstMessage;
        TString AuthMessage;
    };

    TFirstMessageResult PerformFirstMessage(TKafkaTestClient& client, const TString& userName, const TString& /* userPassword */) {
        auto apiVersionsMsg = client.ApiVersions();
        UNIT_ASSERT_VALUES_EQUAL(apiVersionsMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        auto handshakeMsg = client.SaslHandshake("SCRAM-SHA-256");
        UNIT_ASSERT_VALUES_EQUAL(handshakeMsg->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        TString randomBytes;
        randomBytes.reserve(16);
        for (size_t i = 0; i < 16; ++i) {
            randomBytes += static_cast<char>(RandomNumber<ui8>());
        }
        TString clientNonce = Base64Encode(randomBytes);

        TString clientFirstMessageBare = TStringBuilder() << "n=" << userName << ",r=" << clientNonce;

        auto response1 = client.SaslScramAuthenticateFirstMsg(userName, clientNonce);
        UNIT_ASSERT_VALUES_EQUAL(response1->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));

        UNIT_ASSERT(response1->AuthBytes.has_value());
        const auto& authBytes1 = response1->AuthBytes.value();
        std::string serverFirstMessage(authBytes1.data(), authBytes1.size());

        NLogin::NSasl::TFirstServerMsg parsedServerMsg;
        auto parseResult = NLogin::NSasl::ParseFirstServerMsg(serverFirstMessage, parsedServerMsg);
        UNIT_ASSERT_C(parseResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse server first message");

        // Decode salt from base64
        TString decodedSalt = Base64StrictDecode(parsedServerMsg.Salt);

        // Build client final message without proof
        TString gs2Header = "n,,";
        TString channelBinding = Base64Encode(gs2Header);
        TString clientFinalMessageWithoutProof = TStringBuilder() << "c=" << channelBinding << ",r=" << parsedServerMsg.Nonce;

        // Compute auth message
        TString authMessage = clientFirstMessageBare + "," + serverFirstMessage + "," + clientFinalMessageWithoutProof;

        TFirstMessageResult result;
        result.ServerNonce = TString(parsedServerMsg.Nonce);
        result.Salt = parsedServerMsg.Salt;
        result.IterationsCount = parsedServerMsg.IterationsCount;
        result.ClientFirstMessageBare = clientFirstMessageBare;
        result.ServerFirstMessage = TString(serverFirstMessage);
        result.AuthMessage = authMessage;

        return result;
    }

    // Test successful final SCRAM message with correct credentials
    Y_UNIT_TEST(FinalMsgCorrectCredentials) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        TString userName = "ouruser";
        TString userPassword = "ourUserPassword";

        auto firstMsgResult = PerformFirstMessage(client, userName, userPassword);

        TString decodedSalt = Base64StrictDecode(firstMsgResult.Salt);

        std::string clientProof;
        std::string errorText;
        bool success = NLogin::NSasl::ComputeClientProof(
            "SCRAM-SHA-256",
            std::string(userPassword.data(), userPassword.size()),
            std::string(decodedSalt.data(), decodedSalt.size()),
            firstMsgResult.IterationsCount,
            std::string(firstMsgResult.AuthMessage.data(), firstMsgResult.AuthMessage.size()),
            clientProof,
            errorText
        );
        UNIT_ASSERT_C(success, TString(errorText));

        TString encodedClientProof = Base64Encode(clientProof);

        auto response2 = client.SaslScramAuthenticateFinalMsg(firstMsgResult.ServerNonce, encodedClientProof);
        UNIT_ASSERT_VALUES_EQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT(!response2->ErrorMessage.has_value() || response2->ErrorMessage->empty());
        UNIT_ASSERT(response2->AuthBytes.has_value());

        const auto& authBytes2 = response2->AuthBytes.value();
        std::string serverFinalMessage(reinterpret_cast<const char*>(authBytes2.data()), authBytes2.size());

        NLogin::NSasl::TFinalServerMsg parsedFinalServerMsg;
        auto parseFinalResult = NLogin::NSasl::ParseFinalServerMsg(serverFinalMessage, parsedFinalServerMsg);
        UNIT_ASSERT_C(parseFinalResult == NLogin::NSasl::EParseMsgReturnCodes::Success,
                      "Failed to parse server final message: " + serverFinalMessage);

        UNIT_ASSERT_C(parsedFinalServerMsg.Error.empty(), "Server returned error: " + parsedFinalServerMsg.Error);

        UNIT_ASSERT_C(!parsedFinalServerMsg.ServerSignature.empty(), "Server signature is empty");

        std::string serverKey;
        std::string errorText2;
        bool success2 = NLogin::NSasl::ComputeServerKey(
            "SCRAM-SHA-256",
            std::string(userPassword.data(), userPassword.size()),
            std::string(decodedSalt.data(), decodedSalt.size()),
            firstMsgResult.IterationsCount,
            serverKey,
            errorText2
        );
        UNIT_ASSERT_C(success2, TString(errorText2));

        std::string expectedServerSignature;
        success2 = NLogin::NSasl::ComputeServerSignature(
            "SCRAM-SHA-256",
            serverKey,
            std::string(firstMsgResult.AuthMessage.data(), firstMsgResult.AuthMessage.size()),
            expectedServerSignature,
            errorText2
        );
        UNIT_ASSERT_C(success2, TString(errorText2));

        TString decodedServerSignature = Base64StrictDecode(parsedFinalServerMsg.ServerSignature);

        UNIT_ASSERT_VALUES_EQUAL_C(
            TString(expectedServerSignature.data(), expectedServerSignature.size()),
            decodedServerSignature,
            "Server signature verification failed"
        );
    }

    // Test final SCRAM message with incorrect client proof
    Y_UNIT_TEST(FinalMsgIncorrectProof) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        TString userName = "ouruser";
        TString userPassword = "ourUserPassword";

        auto firstMsgResult = PerformFirstMessage(client, userName, userPassword);

        TString randomBytes;
        randomBytes.reserve(32);
        for (size_t i = 0; i < 32; ++i) {
            randomBytes += static_cast<char>(RandomNumber<ui8>());
        }
        TString incorrectProof = Base64Encode(randomBytes);

        auto response2 = client.SaslScramAuthenticateFinalMsg(firstMsgResult.ServerNonce, incorrectProof);

        UNIT_ASSERT_VALUES_UNEQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::SASL_AUTHENTICATION_FAILED));

        UNIT_ASSERT(response2->ErrorMessage.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*response2->ErrorMessage, "Authentication failure. ");

        UNIT_ASSERT(response2->AuthBytes.has_value());
        std::string errorMessage(response2->AuthBytes->data(), response2->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        UNIT_ASSERT_C(parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse error message");
        UNIT_ASSERT_C(!errorMsg.Error.empty(), "Error field should not be empty");
        UNIT_ASSERT_STRING_CONTAINS(errorMsg.Error, "invalid-proof");
    }

    // Test final SCRAM message with wrong nonce
    Y_UNIT_TEST(FinalMsgWrongNonce) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        TString userName = "ouruser";
        TString userPassword = "ourUserPassword";

        auto firstMsgResult = PerformFirstMessage(client, userName, userPassword);

        TString decodedSalt = Base64StrictDecode(firstMsgResult.Salt);

        TString wrongNonce = "wrongNonce123456";
        TString gs2Header = "n,,";
        TString channelBinding = Base64Encode(gs2Header);
        TString clientFinalMessageWithoutProof = TStringBuilder() << "c=" << channelBinding << ",r=" << wrongNonce;

        TString authMessage = firstMsgResult.ClientFirstMessageBare + "," + firstMsgResult.ServerFirstMessage + "," + clientFinalMessageWithoutProof;

        std::string clientProof;
        std::string errorText;
        bool success = NLogin::NSasl::ComputeClientProof(
            "SCRAM-SHA-256",
            std::string(userPassword.data(), userPassword.size()),
            std::string(decodedSalt.data(), decodedSalt.size()),
            firstMsgResult.IterationsCount,
            std::string(authMessage.data(), authMessage.size()),
            clientProof,
            errorText
        );
        UNIT_ASSERT_C(success, TString(errorText));

        TString encodedClientProof = Base64Encode(clientProof);

        auto response2 = client.SaslScramAuthenticateFinalMsg(wrongNonce, encodedClientProof);

        UNIT_ASSERT_VALUES_UNEQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::SASL_AUTHENTICATION_FAILED));

        UNIT_ASSERT(response2->ErrorMessage.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*response2->ErrorMessage, "Authentication failure. ");

        UNIT_ASSERT(response2->AuthBytes.has_value());
        std::string errorMessage(response2->AuthBytes->data(), response2->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        UNIT_ASSERT_C(parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse error message");
        UNIT_ASSERT_C(!errorMsg.Error.empty(), "Error field should not be empty");
        UNIT_ASSERT_VALUES_EQUAL(errorMsg.Error, "other-error");
    }

    // Test final SCRAM message with empty proof
    Y_UNIT_TEST(FinalMsgEmptyProof) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        TString userName = "ouruser";
        TString userPassword = "ourUserPassword";

        auto firstMsgResult = PerformFirstMessage(client, userName, userPassword);

        auto response2 = client.SaslScramAuthenticateFinalMsg(firstMsgResult.ServerNonce, "");

        UNIT_ASSERT_VALUES_UNEQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::SASL_AUTHENTICATION_FAILED));

        UNIT_ASSERT(response2->ErrorMessage.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*response2->ErrorMessage, "Authentication failure. ");

        UNIT_ASSERT(response2->AuthBytes.has_value());
        std::string errorMessage(response2->AuthBytes->data(), response2->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        UNIT_ASSERT_C(parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse error message");
        UNIT_ASSERT_C(!errorMsg.Error.empty(), "Error field should not be empty");
        UNIT_ASSERT_VALUES_EQUAL(errorMsg.Error, "other-error");
    }

    // Test final SCRAM message with malformed proof (not base64)
    Y_UNIT_TEST(FinalMsgMalformedProof) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        TString userName = "ouruser";
        TString userPassword = "ourUserPassword";

        auto firstMsgResult = PerformFirstMessage(client, userName, userPassword);

        TString malformedProof = "this-is-not-valid-base64!!!";

        auto response2 = client.SaslScramAuthenticateFinalMsg(firstMsgResult.ServerNonce, malformedProof);

        // Server should return error for malformed proof
        UNIT_ASSERT_VALUES_UNEQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::SASL_AUTHENTICATION_FAILED));

        // Check ErrorMessage field
        UNIT_ASSERT(response2->ErrorMessage.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*response2->ErrorMessage, "Authentication failure. ");

        // Check error message
        UNIT_ASSERT(response2->AuthBytes.has_value());
        std::string errorMessage(response2->AuthBytes->data(), response2->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        UNIT_ASSERT_C(parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse error message");
        UNIT_ASSERT_C(!errorMsg.Error.empty(), "Error field should not be empty");
        UNIT_ASSERT_VALUES_EQUAL(errorMsg.Error, "invalid-encoding");
    }

    // Test final SCRAM message with proof computed using wrong password
    Y_UNIT_TEST(FinalMsgWrongPassword) {
        TInsecureTestServer testServer("2");
        TKafkaTestClient client(testServer.Port);

        TString userName = "ouruser";
        TString correctPassword = "ourUserPassword";
        TString wrongPassword = "wrongPassword123";

        auto firstMsgResult = PerformFirstMessage(client, userName, correctPassword);

        // Decode salt from base64
        TString decodedSalt = Base64StrictDecode(firstMsgResult.Salt);

        // Compute client proof with WRONG password
        std::string clientProof;
        std::string errorText;
        bool success = NLogin::NSasl::ComputeClientProof(
            "SCRAM-SHA-256",
            std::string(wrongPassword.data(), wrongPassword.size()),
            std::string(decodedSalt.data(), decodedSalt.size()),
            firstMsgResult.IterationsCount,
            std::string(firstMsgResult.AuthMessage.data(), firstMsgResult.AuthMessage.size()),
            clientProof,
            errorText
        );
        UNIT_ASSERT_C(success, TString(errorText));

        TString encodedClientProof = Base64Encode(clientProof);

        auto response2 = client.SaslScramAuthenticateFinalMsg(firstMsgResult.ServerNonce, encodedClientProof);

        UNIT_ASSERT_VALUES_UNEQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::NONE_ERROR));
        UNIT_ASSERT_VALUES_EQUAL(response2->ErrorCode, static_cast<TKafkaInt16>(EKafkaErrors::SASL_AUTHENTICATION_FAILED));

        UNIT_ASSERT(response2->ErrorMessage.has_value());
        UNIT_ASSERT_VALUES_EQUAL(*response2->ErrorMessage, "Authentication failure. ");

        UNIT_ASSERT(response2->AuthBytes.has_value());
        std::string errorMessage(response2->AuthBytes->data(), response2->AuthBytes->size());
        NLogin::NSasl::TFinalServerMsg errorMsg;
        auto parseErrorResult = NLogin::NSasl::ParseFinalServerMsg(errorMessage, errorMsg);
        UNIT_ASSERT_C(parseErrorResult == NLogin::NSasl::EParseMsgReturnCodes::Success, "Failed to parse error message");
        UNIT_ASSERT_C(!errorMsg.Error.empty(), "Error field should not be empty");
        UNIT_ASSERT_STRING_CONTAINS(errorMsg.Error, "invalid-proof");
    }
} // Y_UNIT_TEST_SUITE(KafkaScramFinalMessage)
