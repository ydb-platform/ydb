#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/http_proxy/error/error.h>
#include <ydb/core/ymq/http/params.h>

using namespace NKikimr::NSQS;

Y_UNIT_TEST_SUITE(TParseParamsTests) {
    Y_UNIT_TEST(CreateUser) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("Action", "CreateUser");
        parser.Append("UserName", "test");
        parser.Append("Attribute.1.Name",  "DelaySeconds");
        parser.Append("Attribute.1.Value", "1");

        UNIT_ASSERT_EQUAL(params.Action, "CreateUser");
        UNIT_ASSERT_EQUAL(params.UserName, "test");

        UNIT_ASSERT_EQUAL(params.Attributes[1].GetName(), "DelaySeconds");
        UNIT_ASSERT_EQUAL(params.Attributes[1].GetValue(), "1");
    }

    Y_UNIT_TEST(ChangeMessageVisibilityBatchRequest) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("ChangeMessageVisibilityBatchRequestEntry.2.ReceiptHandle", "batch message 2");
        parser.Append("ChangeMessageVisibilityBatchRequestEntry.1.VisibilityTimeout", "10");
        parser.Append("Action", "ChangeMessageVisibilityBatch");
        parser.Append("ChangeMessageVisibilityBatchRequestEntry.2.VisibilityTimeout", "20");
        parser.Append("ChangeMessageVisibilityBatchRequestEntry.1.ReceiptHandle", "batch message 1");
        parser.Append("SendMessageBatchRequestEntry.2.Id", "Y");

        UNIT_ASSERT_EQUAL(params.Action, "ChangeMessageVisibilityBatch");
        UNIT_ASSERT_EQUAL(params.BatchEntries.size(), 2);
        UNIT_ASSERT_EQUAL(*params.BatchEntries[1].VisibilityTimeout, 10);
        UNIT_ASSERT_EQUAL(*params.BatchEntries[2].VisibilityTimeout, 20);
        UNIT_ASSERT_EQUAL(params.BatchEntries[1].ReceiptHandle, "batch message 1");
        UNIT_ASSERT_EQUAL(params.BatchEntries[2].ReceiptHandle, "batch message 2");
        UNIT_ASSERT_EQUAL(params.BatchEntries[2].Id, "Y");
    }

    Y_UNIT_TEST(DeleteMessageBatchRequest) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("DeleteMessageBatchRequestEntry.2.ReceiptHandle", "batch message 2");
        parser.Append("Action", "DeleteMessageBatch");
        parser.Append("DeleteMessageBatchRequestEntry.1.ReceiptHandle", "batch message 1");
        parser.Append("SendMessageBatchRequestEntry.2.Id", "Y");

        UNIT_ASSERT_EQUAL(params.Action, "DeleteMessageBatch");
        UNIT_ASSERT_EQUAL(params.BatchEntries.size(), 2);
        UNIT_ASSERT_EQUAL(params.BatchEntries[1].ReceiptHandle, "batch message 1");
        UNIT_ASSERT_EQUAL(params.BatchEntries[2].ReceiptHandle, "batch message 2");
        UNIT_ASSERT_EQUAL(params.BatchEntries[2].Id, "Y");
    }

    Y_UNIT_TEST(MessageBody) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("Action", "SendMessage");
        parser.Append("MessageBody", "test");
        parser.Append("Attribute.1.Name",  "DelaySeconds");
        parser.Append("Attribute.1.Value", "1");
        parser.Append("MessageAttribute.3.Name", "MyAttr");
        parser.Append("MessageAttribute.3.Value.StringValue", "test attr");
        parser.Append("MessageAttribute.3.Value.DataType", "string");
        parser.Append("MessageAttribute.5.Name", "MyBinaryAttr");
        parser.Append("MessageAttribute.5.Value.BinaryValue", "YmluYXJ5X2RhdGE=");
        parser.Append("MessageAttribute.5.Value.DataType", "Binary");

        UNIT_ASSERT_EQUAL(params.Action, "SendMessage");
        UNIT_ASSERT_EQUAL(params.MessageBody, "test");

        UNIT_ASSERT_EQUAL(params.Attributes[1].GetName(), "DelaySeconds");
        UNIT_ASSERT_EQUAL(params.Attributes[1].GetValue(), "1");

        UNIT_ASSERT_VALUES_EQUAL(params.MessageAttributes.size(), 2);
        UNIT_ASSERT_STRINGS_EQUAL(params.MessageAttributes[3].GetName(), "MyAttr");
        UNIT_ASSERT_STRINGS_EQUAL(params.MessageAttributes[3].GetStringValue(), "test attr");
        UNIT_ASSERT_STRINGS_EQUAL(params.MessageAttributes[3].GetDataType(), "string");
        UNIT_ASSERT_STRINGS_EQUAL(params.MessageAttributes[5].GetName(), "MyBinaryAttr");
        UNIT_ASSERT_STRINGS_EQUAL(params.MessageAttributes[5].GetBinaryValue(), "binary_data");
        UNIT_ASSERT_STRINGS_EQUAL(params.MessageAttributes[5].GetDataType(), "Binary");
    }

    Y_UNIT_TEST(SendMessageBatchRequest) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("SendMessageBatchRequestEntry.2.MessageBody", "batch message 2");
        parser.Append("SendMessageBatchRequestEntry.1.MessageGroupId", "1");
        parser.Append("SendMessageBatchRequestEntry.2.MessageGroupId", "2");
        parser.Append("SendMessageBatchRequestEntry.1.MessageBody", "batch message 1");
        parser.Append("Version", "2012-11-05");
        parser.Append("SendMessageBatchRequestEntry.2.MessageDeduplicationId", "b2");
        parser.Append("SendMessageBatchRequestEntry.1.MessageDeduplicationId", "b1");
        parser.Append("Action", "SendMessageBatch");
        parser.Append("SendMessageBatchRequestEntry.1.Id", "1");
        parser.Append("SendMessageBatchRequestEntry.2.Id", "Y");
        parser.Append("SendMessageBatchRequestEntry.2.MessageAttribute.1.Value.DataType", "string");

        UNIT_ASSERT_EQUAL(params.Action, "SendMessageBatch");
        UNIT_ASSERT_EQUAL(params.BatchEntries.size(), 2);
        UNIT_ASSERT_EQUAL(params.BatchEntries[1].Id, "1");
        UNIT_ASSERT_EQUAL(params.BatchEntries[2].Id, "Y");
        UNIT_ASSERT_EQUAL(params.BatchEntries[1].MessageBody, "batch message 1");
        UNIT_ASSERT_EQUAL(params.BatchEntries[2].MessageBody, "batch message 2");
        UNIT_ASSERT_EQUAL(params.BatchEntries[2].MessageAttributes[1].GetDataType(), "string");
    }

    Y_UNIT_TEST(DeleteQueueBatchRequest) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("DeleteQueueBatchRequestEntry.2.QueueUrl", "url2");
        parser.Append("DeleteQueueBatchRequestEntry.1.Id", "id1");

        UNIT_ASSERT_VALUES_EQUAL(params.BatchEntries.size(), 2);
        UNIT_ASSERT_STRINGS_EQUAL(*params.BatchEntries[1].Id, "id1");
        UNIT_ASSERT_STRINGS_EQUAL(*params.BatchEntries[2].QueueUrl, "url2");
    }

    Y_UNIT_TEST(PurgeQueueBatchRequest) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("PurgeQueueBatchRequestEntry.1.QueueUrl", "my/url");
        parser.Append("PurgeQueueBatchRequestEntry.1.Id", "id");

        UNIT_ASSERT_VALUES_EQUAL(params.BatchEntries.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(*params.BatchEntries[1].Id, "id");
        UNIT_ASSERT_STRINGS_EQUAL(*params.BatchEntries[1].QueueUrl, "my/url");
    }

    Y_UNIT_TEST(GetQueueAttributesBatchRequest) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("GetQueueAttributesBatchRequestEntry.2.QueueUrl", "url");
        parser.Append("GetQueueAttributesBatchRequestEntry.2.Id", "id");

        UNIT_ASSERT_VALUES_EQUAL(params.BatchEntries.size(), 1);
        UNIT_ASSERT_STRINGS_EQUAL(*params.BatchEntries[2].Id, "id");
        UNIT_ASSERT_STRINGS_EQUAL(*params.BatchEntries[2].QueueUrl, "url");
    }

    Y_UNIT_TEST(UnnumberedAttribute) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("Attribute.Name",  "DelaySeconds");
        parser.Append("Attribute.Value", "1");

        UNIT_ASSERT_EQUAL(params.Attributes.size(), 1);
        UNIT_ASSERT_EQUAL(params.Attributes[1].GetName(), "DelaySeconds");
        UNIT_ASSERT_EQUAL(params.Attributes[1].GetValue(), "1");
    }

    Y_UNIT_TEST(UnnumberedAttributeName) {
        TParameters params;
        TParametersParser parser(&params);
        parser.Append("AttributeName", "All");

        UNIT_ASSERT_EQUAL(params.AttributeNames.size(), 1);
        UNIT_ASSERT_EQUAL(params.AttributeNames[1], "All");
    }

    Y_UNIT_TEST(FailsOnInvalidDeduplicationId) {
        TParameters params;
        TParametersParser parser(&params);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("MessageDeduplicationId", "±"), TSQSException);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("MessageDeduplicationId", "very_big_0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999000000000011111111112222222222"), TSQSException);
    }

    Y_UNIT_TEST(FailsOnInvalidGroupId) {
        TParameters params;
        TParametersParser parser(&params);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("MessageGroupId", "§"), TSQSException);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("MessageGroupId", "very_big_0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999000000000011111111112222222222"), TSQSException);
    }

    Y_UNIT_TEST(FailsOnInvalidReceiveRequestAttemptId) {
        TParameters params;
        TParametersParser parser(&params);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("ReceiveRequestAttemptId", "§"), TSQSException);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("ReceiveRequestAttemptId", "very_big_0000000000111111111122222222223333333333444444444455555555556666666666777777777788888888889999999999000000000011111111112222222222"), TSQSException);
    }

    Y_UNIT_TEST(FailsOnInvalidMaxNumberOfMessages) {
        TParameters params;
        TParametersParser parser(&params);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("MaxNumberOfMessages", "-1"), TSQSException);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("MaxNumberOfMessages", "a"), TSQSException);
    }

    Y_UNIT_TEST(FailsOnInvalidWaitTime) {
        TParameters params;
        TParametersParser parser(&params);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("WaitTimeSeconds", "123456789012345678901234567890"), TSQSException); // too big for uint64
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("WaitTimeSeconds", "trololo"), TSQSException);
    }

    Y_UNIT_TEST(FailsOnInvalidDelaySeconds) {
        TParameters params;
        TParametersParser parser(&params);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("SendMessageBatchRequestEntry.2.DelaySeconds", "1.0"), TSQSException);
        UNIT_CHECK_GENERATED_EXCEPTION(parser.Append("DelaySeconds", "1+3"), TSQSException);
    }
}
