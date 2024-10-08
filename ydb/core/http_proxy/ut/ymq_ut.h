#pragma once

#include "library/cpp/json/writer/json_value.h"
#include "library/cpp/testing/unittest/registar.h"

#include <chrono>
#include <thread>

extern TString Name_;
extern bool ForceFork_;
extern TString FormAuthorizationStr(const TString& region);
extern NJson::TJsonValue CreateSqsGetQueueUrlRequest();
extern NJson::TJsonValue CreateSqsCreateQueueRequest();
extern struct THttpResult httpResult;

extern THttpResult SendHttpRequest(
        const TString& handler,
        const TString& target,
        NJson::TJsonValue value,
        const TString& authorizationStr,
        const TString& contentType = "application/json"
);


Y_UNIT_TEST_SUITE(TestYmqHttpProxy) {

    Y_UNIT_TEST_F(TestCreateQueue, THttpProxyTestMock) {
        CreateQueue({{"QueueName", "ExampleQueueName"}});
    }

    Y_UNIT_TEST_F(TestCreateQueueWithSameNameAndSameParams, THttpProxyTestMock) {
        auto req = NJson::TJsonMap{{"QueueName", "ExampleQueueName"}};
        CreateQueue(req);
        CreateQueue(req);
    }

    Y_UNIT_TEST_F(TestCreateQueueWithSameNameAndDifferentParams, THttpProxyTestMock) {
        auto req = NJson::TJsonMap{
            {"QueueName", "ExampleQueueName"},
            {"Attributes", NJson::TJsonMap{{"MessageRetentionPeriod", "60"}}}
        };
        CreateQueue(req);

        req["Attributes"]["MessageRetentionPeriod"] = "61";
        auto json = CreateQueue(req, 400);
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "ValidationError");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithBadQueueName, THttpProxyTestMock) {
        auto json = CreateQueue({
            {"QueueName", "B@d_queue_name"},
            {"Attributes", NJson::TJsonMap{{"MessageRetentionPeriod", "60"}}}
        }, 400);
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "InvalidParameterValue");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithEmptyName, THttpProxyTestMock) {
        auto json = CreateQueue({}, 400);
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "MissingParameter");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithWrongBody, THttpProxyTestMock) {
        auto json = CreateQueue({{"wrongField", "foobar"}}, 400);
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "InvalidArgumentException");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithWrongAttribute, THttpProxyTestMock) {
        auto json = CreateQueue({
            {"QueueName", "ExampleQueueName"},
            {"Attributes", NJson::TJsonMap{
                {"KmsMasterKeyId", "some-id"},
                {"KmsDataKeyReusePeriodSeconds", "60"},
                {"SqsManagedSseEnabled", "true"},
                {"Policy", "{}"}
            }}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "ValidationError");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithAllAttributes, THttpProxyTestMock) {
        auto json1 = CreateQueue({{"QueueName", "queue-1.fifo"}, {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}});
        auto attributes1 = GetQueueAttributes({
            {"QueueUrl", GetByPath<TString>(json1, "QueueUrl")},
            {"AttributeNames", NJson::TJsonArray{"QueueArn"}}
        });
        auto queueArn1 = GetByPath<TString>(attributes1, "Attributes.QueueArn");

        auto queueName = "ExampleQueueName.fifo";
        auto json = CreateQueue({
            {"QueueName", queueName},
            {"Attributes", NJson::TJsonMap{
                {"DelaySeconds", "60"},
                {"MaximumMessageSize", "1024"},
                {"MessageRetentionPeriod", "60"},
                {"ReceiveMessageWaitTimeSeconds", "10"},
                {"VisibilityTimeout", "3600"},

                {"RedrivePolicy", TStringBuilder() << "{\"deadLetterTargetArn\":\"" << queueArn1 << "\", \"maxReceiveCount\": 3}"},

                // 2024-10-07: RedriveAllowPolicy not supported yet.
                // {"RedriveAllowPolicy", TStringBuilder() << "{\"redrivePermission\":\"byQueue\", \"sourceQueueArns\": [\"" << queueArn2 << "\", \"" << queueArn3 << "\"]}"},

                // FIFO queue
                {"FifoQueue", "true"},
                {"ContentBasedDeduplication", "true"}

                // High throughput for FIFO queues not supported yet.
                // {"DeduplicationScope", "messageGroup"},
                // {"FifoThroughputLimit", "perMessageGroupId"}
            }}
        });
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith(queueName));
    }

    Y_UNIT_TEST_F(TestGetQueueUrlOfNotExistingQueue, THttpProxyTestMock) {
        auto req = CreateSqsGetQueueUrlRequest();
        req["QueueName"] = "not-existing-queue";
        auto res = SendHttpRequest("/Root", "AmazonSQS.GetQueueUrl", std::move(req), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "AWS.SimpleQueueService.NonExistentQueue");
        TString resultMessage = GetByPath<TString>(json, "message");
        UNIT_ASSERT_VALUES_EQUAL(resultMessage, "The specified queue doesn't exist.");
    }

    Y_UNIT_TEST_F(TestGetQueueUrlWithIAM, THttpProxyTestMock) {
        auto req = CreateSqsGetQueueUrlRequest();
        req["QueueName"] = "not-existing-queue";
        auto res = SendHttpRequest("/Root?folderId=XXX", "AmazonSQS.GetQueueUrl", std::move(req), "X-YaCloud-SubjectToken: Bearer proxy_sa@builtin");
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);

        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "AWS.SimpleQueueService.NonExistentQueue");
        TString resultMessage = GetByPath<TString>(json, "message");
        UNIT_ASSERT_VALUES_EQUAL(resultMessage, "The specified queue doesn't exist.");
    }

    Y_UNIT_TEST_F(TestSendMessage, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue sendMessageReq;
        sendMessageReq["QueueUrl"] = resultQueueUrl;
        auto body = "MessageBody-0";
        sendMessageReq["MessageBody"] = body;
        sendMessageReq["MessageDeduplicationId"] = "MessageDeduplicationId-0";
        sendMessageReq["MessageGroupId"] = "MessageGroupId-0";

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", std::move(sendMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(!GetByPath<TString>(json, "SequenceNumber").empty());
        UNIT_ASSERT(!GetByPath<TString>(json, "MD5OfMessageBody").empty());
        UNIT_ASSERT(!GetByPath<TString>(json, "MessageId").empty());
    }

    Y_UNIT_TEST_F(TestReceiveMessage, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", createQueueReq, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue sendMessageReq;
        sendMessageReq["QueueUrl"] = resultQueueUrl;
        auto body0 = "MessageBody-0";
        sendMessageReq["MessageBody"] = body0;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", sendMessageReq, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(!GetByPath<TString>(json, "MD5OfMessageBody").empty());
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue receiveMessageReq;
        receiveMessageReq["QueueUrl"] = resultQueueUrl;
        for (int i = 0; i < 20; ++i) {
            res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", receiveMessageReq, FormAuthorizationStr("ru-central1"));
            if (res.Body != "{}") {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body0);
    }

    Y_UNIT_TEST_F(TestReceiveMessageWithAttributes, THttpProxyTestMock) {
        // Test if we process AttributeNames, MessageSystemAttributeNames, MessageAttributeNames correctly.

        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", createQueueReq, FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        auto sendMessage = [this, resultQueueUrl](const TString& body) {
            NJson::TJsonValue sendMessageReq;
            sendMessageReq["QueueUrl"] = resultQueueUrl;
            sendMessageReq["MessageBody"] = body;

            auto res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", sendMessageReq, FormAuthorizationStr("ru-central1"));
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT(!GetByPath<TString>(json, "MD5OfMessageBody").empty());
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        };

        TString body = "MessageBody-0";
        sendMessage(body);

        auto receiveMessage = [this](NJson::TJsonValue request, const TString& expectedBody) -> NJson::TJsonValue {
            request["VisibilityTimeout"] = 0;  // Keep the message visible for next ReceiveMessage requests.
            THttpResult res;
            for (int i = 0; i < 20; ++i) {
                res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", request, FormAuthorizationStr("ru-central1"));
                if (res.Body != "{}") {
                    break;
                }
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            }

            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
            UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
            UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], expectedBody);
            return json;
        };

        {
            // Request SentTimestamp message system attribute using deprecated AttributeNames field.
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            receiveMessageReq["AttributeNames"] = NJson::TJsonArray{"SentTimestamp"};
            json = receiveMessage(receiveMessageReq, body);
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());
        }

        {
            // Request SentTimestamp message system attribute using MessageSystemAttributeNames field.
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            receiveMessageReq["MessageSystemAttributeNames"] = NJson::TJsonArray{"SentTimestamp"};
            json = receiveMessage(receiveMessageReq, body);
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());
        }

        {
            // Request All message system attributes using deprecated AttributeNames field.
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            receiveMessageReq["AttributeNames"] = NJson::TJsonArray{"All"};
            json = receiveMessage(receiveMessageReq, body);
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());
        }

        {
            // Request All message system attributes using MessageSystemAttributeNames field.
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            receiveMessageReq["MessageSystemAttributeNames"] = NJson::TJsonArray{"All"};
            json = receiveMessage(receiveMessageReq, body);
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());
        }
    }

    Y_UNIT_TEST_F(TestGetQueueAttributes, THttpProxyTestMock) {
        auto json1 = CreateQueue({{"QueueName", "queue-1.fifo"}, {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}});
        auto attributes1 = GetQueueAttributes({
            {"QueueUrl", GetByPath<TString>(json1, "QueueUrl")},
            {"AttributeNames", NJson::TJsonArray{"QueueArn"}}
        });
        auto queueArn1 = GetByPath<TString>(attributes1, "Attributes.QueueArn");

        auto queueName = "ExampleQueueName.fifo";
        auto json = CreateQueue({
            {"QueueName", queueName},
            {"Attributes", NJson::TJsonMap{
                {"DelaySeconds", "1"},
                {"FifoQueue", "true"},
                {"ContentBasedDeduplication", "true"},
                {"RedrivePolicy", TStringBuilder() << "{\"deadLetterTargetArn\":\"" << queueArn1 << "\", \"maxReceiveCount\": 3}"}
            }}
        });

        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith(queueName));

        GetQueueAttributes({{"wrong-field", "some-value"}}, 400);
        GetQueueAttributes({{"QueueUrl", "invalid-url"}}, 400);

        {
            auto json = GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
            });
            UNIT_ASSERT(json.GetMapSafe().empty());
        }

        {
            auto json = GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{}}
            });
            UNIT_ASSERT(json.GetMapSafe().empty());
        }

        {
            auto json = GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{"All"}}
            });
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["DelaySeconds"], "1");
            UNIT_ASSERT_GT(json["Attributes"].GetMapSafe().size(), 5);
        }

        {
            auto json = GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{"All", "DelaySeconds"}}
            });
            UNIT_ASSERT_GT(json["Attributes"].GetMapSafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["DelaySeconds"], "1");
        }

        {
            auto json = GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{"DelaySeconds"}}
            });
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"].GetMapSafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["DelaySeconds"], "1");
        }

        GetQueueAttributes({
            {"QueueUrl", resultQueueUrl},
            {"AttributeNames", NJson::TJsonArray{"UnknownAttribute"}}
        }, 400);

        GetQueueAttributes({
            {"QueueUrl", resultQueueUrl},
            {"AttributeNames", NJson::TJsonArray{"All", "UnknownAttribute"}}
        }, 400);

        GetQueueAttributes({
            {"QueueUrl", resultQueueUrl},
            {"AttributeNames", NJson::TJsonArray{"DelaySeconds", "UnknownAttribute"}}
        }, 400);

        {
            auto json = GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{
                    "ApproximateNumberOfMessages",
                    "ApproximateNumberOfMessagesDelayed",
                    "ApproximateNumberOfMessagesNotVisible",
                    "CreatedTimestamp",
                    "DelaySeconds",
                    "LastModifiedTimestamp",
                    "MaximumMessageSize",
                    "MessageRetentionPeriod",
                    "QueueArn",
                    "ReceiveMessageWaitTimeSeconds",
                    "VisibilityTimeout",

                    "RedrivePolicy",

                    "FifoQueue",
                    "ContentBasedDeduplication",
                }}
            });
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["DelaySeconds"], "1");
            UNIT_ASSERT_GT(json["Attributes"].GetMapSafe().size(), 5);
        }
    }

    Y_UNIT_TEST_F(TestListQueues, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue listQueuesReq;
        listQueuesReq["QueueNamePrefix"] = "Ex";
        res = SendHttpRequest("/Root", "AmazonSQS.ListQueues", std::move(listQueuesReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonArray result;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &result));
        UNIT_ASSERT_VALUES_EQUAL(result["QueueUrls"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(result["QueueUrls"][0], resultQueueUrl);
    }

    Y_UNIT_TEST_F(TestDeleteMessage, THttpProxyTestMock) {
        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        TString queueUrl = GetByPath<TString>(json, "QueueUrl");

        auto body = "MessageBody-0";
        SendMessage({{"QueueUrl", queueUrl}, {"MessageBody", body}});
        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});

        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.Empty());

        DeleteMessage({{"QueueUrl", queueUrl}, {"ReceiptHandle", receiptHandle}});

        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 1}});
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 0);
    }

    Y_UNIT_TEST_F(TestPurgeQueue, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue sendMessageReq;
        sendMessageReq["QueueUrl"] = resultQueueUrl;
        auto body = "MessageBody-0";
        sendMessageReq["MessageBody"] = body;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", std::move(sendMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue purgeQueueReq;
        purgeQueueReq["QueueUrl"] = resultQueueUrl;

        res = SendHttpRequest("/Root", "AmazonSQS.PurgeQueue", std::move(purgeQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue receiveMessageReq;
        receiveMessageReq["QueueUrl"] = resultQueueUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 0);
    }

    Y_UNIT_TEST_F(TestDeleteQueue, THttpProxyTestMock) {
        auto json = DeleteQueue({{"QueueUrl", "non-existent-queue"}}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "ValidationException");

        json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        TString queueUrl = GetByPath<TString>(json, "QueueUrl");

        DeleteQueue({{"QueueUrl", queueUrl}});

        auto getQueueUrlRequest = CreateSqsGetQueueUrlRequest();
        for (int i = 0; i < 61; ++i) {
            auto res = SendHttpRequest("/Root", "AmazonSQS.GetQueueUrl", getQueueUrlRequest, FormAuthorizationStr("ru-central1"));
            if (res.HttpCode == 200) {
                // The queue should be deleted within 60 seconds.
                std::this_thread::sleep_for(std::chrono::milliseconds(1000));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
                UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
                UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.NonExistentQueue");

                break;
            }
        }
    }

    Y_UNIT_TEST_F(TestSetQueueAttributes, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        NJson::TJsonValue attributes;
        attributes["DelaySeconds"] = "1";
        createQueueReq["Attributes"] = attributes;
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");

        NJson::TJsonValue setQueueAttributes;
        setQueueAttributes["QueueUrl"] = resultQueueUrl;
        attributes = {};
        attributes["DelaySeconds"] = "2";
        setQueueAttributes["Attributes"] = attributes;

        res = SendHttpRequest("/Root", "AmazonSQS.SetQueueAttributes", std::move(setQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue getQueueAttributes;
        getQueueAttributes["QueueUrl"] = resultQueueUrl;
        NJson::TJsonArray attributeNames = {"DelaySeconds"};
        getQueueAttributes["AttributeNames"] = attributeNames;

        res = SendHttpRequest("/Root", "AmazonSQS.GetQueueAttributes", std::move(getQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue resultJson;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &resultJson));
        UNIT_ASSERT_VALUES_EQUAL(resultJson["Attributes"]["DelaySeconds"], "2");
    }

    Y_UNIT_TEST_F(TestSendMessageBatch, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue message0;
        message0["Id"] = "Id-0";
        message0["MessageBody"] = "MessageBody-0";
        message0["MessageDeduplicationId"] = "MessageDeduplicationId-0";

        NJson::TJsonValue delaySeconds;
        delaySeconds["StringValue"] = "1";
        delaySeconds["DataType"] = "String";

        NJson::TJsonValue attributes;
        attributes["DelaySeconds"] = delaySeconds;

        message0["MessageAttributes"] = attributes;

        NJson::TJsonValue message1;
        message1["Id"] = "Id-1";
        message1["MessageBody"] = "MessageBody-1";
        message1["MessageDeduplicationId"] = "MessageDeduplicationId-1";

        NJson::TJsonArray entries = {message0, message1};

        NJson::TJsonValue sendMessageBatchReq;
        sendMessageBatchReq["QueueUrl"] = resultQueueUrl;
        sendMessageBatchReq["Entries"] = entries;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessageBatch", std::move(sendMessageBatchReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(json["Successful"].GetArray().size() == 2);
        auto succesful0 = json["Successful"][0];
        UNIT_ASSERT(succesful0["Id"] == "Id-0");
        UNIT_ASSERT(!GetByPath<TString>(succesful0, "MD5OfMessageAttributes").empty());
        UNIT_ASSERT(!GetByPath<TString>(succesful0, "MD5OfMessageBody").empty());
        UNIT_ASSERT(!GetByPath<TString>(succesful0, "MessageId").empty());

        NJson::TJsonValue receiveMessageReq;
        receiveMessageReq["QueueUrl"] = resultQueueUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
    }

    Y_UNIT_TEST_F(TestDeleteMessageBatch, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue message0;
        message0["Id"] = "Id-0";
        message0["MessageBody"] = "MessageBody-0";
        message0["MessageDeduplicationId"] = "MessageDeduplicationId-0";

        NJson::TJsonValue message1;
        message1["Id"] = "Id-1";
        message1["MessageBody"] = "MessageBody-1";
        message1["MessageDeduplicationId"] = "MessageDeduplicationId-1";

        NJson::TJsonArray entries = {message0, message1};

        NJson::TJsonValue sendMessageBatchReq;
        sendMessageBatchReq["QueueUrl"] = resultQueueUrl;
        sendMessageBatchReq["Entries"] = entries;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessageBatch", std::move(sendMessageBatchReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(json["Successful"].GetArray().size() == 2);

        TVector<NJson::TJsonValue> messages;
        for (int i = 0; i < 20; ++i) {
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
            if (res.Body != TString("{}")) {
                NJson::ReadJsonTree(res.Body, &json);
                if (json["Messages"].GetArray().size() == 2) {
                    messages.push_back(json["Messages"][0]);
                    messages.push_back(json["Messages"][1]);
                    break;
                }
                if (json["Messages"].GetArray().size() == 1) {
                    messages.push_back(json["Messages"][0]);
                    if (messages.size() == 2) {
                        break;
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

        auto receiptHandle0 = messages[0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle0.Empty());
        auto receiptHandle1 = messages[1]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle1.Empty());

        NJson::TJsonValue deleteMessageBatchReq;
        deleteMessageBatchReq["QueueUrl"] = resultQueueUrl;

        NJson::TJsonValue entry0;
        entry0["Id"] = "Id-0";
        entry0["ReceiptHandle"] = receiptHandle0;

        NJson::TJsonValue entry1;
        entry1["Id"] = "Id-1";
        entry1["ReceiptHandle"] = receiptHandle1;

        NJson::TJsonArray deleteEntries = {entry0, entry1};
        deleteMessageBatchReq["Entries"] = deleteEntries;

        res = SendHttpRequest("/Root", "AmazonSQS.DeleteMessageBatch", std::move(deleteMessageBatchReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][0]["Id"], "Id-0");
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][1]["Id"], "Id-1");

        NJson::TJsonValue receiveMessageReq;
        receiveMessageReq["QueueUrl"] = resultQueueUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 0);

    }

    Y_UNIT_TEST_F(TestListDeadLetterSourceQueues, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");

        auto createDlqReq = CreateSqsCreateQueueRequest();
        createQueueReq["QueueName"] = "DlqName";
        res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        TString dlqUrl = GetByPath<TString>(json, "QueueUrl");

        NJson::TJsonValue getQueueAttributes;
        getQueueAttributes["QueueUrl"] = dlqUrl;
        NJson::TJsonArray attributeNames = {"QueueArn"};
        getQueueAttributes["AttributeNames"] = attributeNames;
        res = SendHttpRequest("/Root", "AmazonSQS.GetQueueAttributes", std::move(getQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        TString dlqArn = GetByPath<TString>(json["Attributes"], "QueueArn");

        NJson::TJsonValue setQueueAttributes;
        setQueueAttributes["QueueUrl"] = resultQueueUrl;
        NJson::TJsonValue attributes = {};
        auto redrivePolicy = TStringBuilder()
            << "{\"deadLetterTargetArn\" : \"" << dlqArn << "\", \"maxReceiveCount\" : 100}";
        attributes["RedrivePolicy"] = redrivePolicy;
        setQueueAttributes["Attributes"] = attributes;

        res = SendHttpRequest("/Root", "AmazonSQS.SetQueueAttributes", std::move(setQueueAttributes), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        NJson::TJsonValue listDeadLetterSourceQueues;
        listDeadLetterSourceQueues["QueueUrl"] = dlqUrl;
        res = SendHttpRequest("/Root", "AmazonSQS.ListDeadLetterSourceQueues", std::move(listDeadLetterSourceQueues), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"][0], resultQueueUrl);
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibility, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");

        NJson::TJsonValue sendMessageReq;
        sendMessageReq["QueueUrl"] = resultQueueUrl;
        auto body = "MessageBody-0";
        sendMessageReq["MessageBody"] = body;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessage", std::move(sendMessageReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        for (int i = 0; i < 20; ++i) {
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
            if (res.Body != TString("{}")) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);

        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.Empty());

        NJson::TJsonValue changeMessageVisibility;
        changeMessageVisibility["QueueUrl"] = resultQueueUrl;
        changeMessageVisibility["ReceiptHandle"] = receiptHandle;
        changeMessageVisibility["VisibilityTimeout"] = 1;

        res = SendHttpRequest(
            "/Root",
            "AmazonSQS.ChangeMessageVisibility",
            std::move(changeMessageVisibility),
            FormAuthorizationStr("ru-central1")
        );

        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityBatch, THttpProxyTestMock) {
        auto createQueueReq = CreateSqsCreateQueueRequest();
        auto res = SendHttpRequest("/Root", "AmazonSQS.CreateQueue", std::move(createQueueReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        NJson::TJsonValue json;
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        TString resultQueueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT(resultQueueUrl.EndsWith("ExampleQueueName"));

        NJson::TJsonValue message0;
        message0["Id"] = "Id-0";
        message0["MessageBody"] = "MessageBody-0";
        message0["MessageDeduplicationId"] = "MessageDeduplicationId-0";

        NJson::TJsonValue message1;
        message1["Id"] = "Id-1";
        message1["MessageBody"] = "MessageBody-1";
        message1["MessageDeduplicationId"] = "MessageDeduplicationId-1";

        NJson::TJsonArray entries = {message0, message1};

        NJson::TJsonValue sendMessageBatchReq;
        sendMessageBatchReq["QueueUrl"] = resultQueueUrl;
        sendMessageBatchReq["Entries"] = entries;

        res = SendHttpRequest("/Root", "AmazonSQS.SendMessageBatch", std::move(sendMessageBatchReq), FormAuthorizationStr("ru-central1"));
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT(json["Successful"].GetArray().size() == 2);

        TVector<NJson::TJsonValue> messages;
        for (int i = 0; i < 20; ++i) {
            NJson::TJsonValue receiveMessageReq;
            receiveMessageReq["QueueUrl"] = resultQueueUrl;
            res = SendHttpRequest("/Root", "AmazonSQS.ReceiveMessage", std::move(receiveMessageReq), FormAuthorizationStr("ru-central1"));
            if (res.Body != TString("{}")) {
                NJson::ReadJsonTree(res.Body, &json);
                if (json["Messages"].GetArray().size() == 2) {
                    messages.push_back(json["Messages"][0]);
                    messages.push_back(json["Messages"][1]);
                    break;
                }
                if (json["Messages"].GetArray().size() == 1) {
                    messages.push_back(json["Messages"][0]);
                    if (messages.size() == 2) {
                        break;
                    }
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

        auto receiptHandle0 = messages[0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle0.Empty());
        auto receiptHandle1 = messages[1]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle1.Empty());


        NJson::TJsonValue changeMessageVisibilityBatchReq;
        changeMessageVisibilityBatchReq["QueueUrl"] = resultQueueUrl;

        NJson::TJsonValue entry0;
        entry0["Id"] = "Id-0";
        entry0["ReceiptHandle"] = receiptHandle0;
        entry0["VisibilityTimeout"] = 1;

        NJson::TJsonValue entry1;
        entry1["Id"] = "Id-1";
        entry1["ReceiptHandle"] = receiptHandle1;
        entry1["VisibilityTimeout"] = 2;

        NJson::TJsonArray changeVisibilityEntries = {entry0, entry1};
        changeMessageVisibilityBatchReq["Entries"] = changeVisibilityEntries;

        res = SendHttpRequest(
            "/Root", "AmazonSQS.ChangeMessageVisibilityBatch",
            std::move(changeMessageVisibilityBatchReq),
            FormAuthorizationStr("ru-central1")
        );
        UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 200);
        UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][0]["Id"], "Id-0");
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][1]["Id"], "Id-1");
    }
} // Y_UNIT_TEST_SUITE(TestYmqHttpProxy)
