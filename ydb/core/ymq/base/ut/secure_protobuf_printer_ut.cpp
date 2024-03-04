#include <ydb/core/ymq/base/secure_protobuf_printer.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/protos/sqs.pb.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(SecureProtobufPrinterTest) {
    Y_UNIT_TEST(MessageBody) {
        {
            NKikimrClient::TSqsRequest msg;
            msg.MutableSendMessage()->SetMessageBody("trololo");
            UNIT_ASSERT_STRINGS_EQUAL(SecureShortUtf8DebugString(msg), "SendMessage { MessageBody: \"***\" }");
        }

        {
            NKikimrClient::TSqsResponse msg;
            msg.MutableReceiveMessage()->AddMessages()->SetData("trololo");
            UNIT_ASSERT_STRINGS_EQUAL(SecureShortUtf8DebugString(msg), "ReceiveMessage { Messages { Data: \"***\" } }");
        }
    }

    Y_UNIT_TEST(Tokens) {
        {
            NKikimrClient::TSqsRequest msg;
            msg.MutableGetQueueUrl()->MutableCredentials()->SetOAuthToken("123456789012345678901234567890");
            UNIT_ASSERT_STRINGS_EQUAL(SecureShortUtf8DebugString(msg), "GetQueueUrl { Credentials { OAuthToken: \"1234****7890 (F229119D)\" } }");
        }

        {
            NKikimrClient::TSqsRequest msg;
            msg.MutableGetQueueUrl()->MutableCredentials()->SetTvmTicket("short");
            UNIT_ASSERT_STRINGS_EQUAL(SecureShortUtf8DebugString(msg), "GetQueueUrl { Credentials { TvmTicket: \"**** (60C3567B)\" } }");
        }
    }
}

} // namespace NKikimr::NSQS
