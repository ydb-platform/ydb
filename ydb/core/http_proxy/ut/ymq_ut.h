#pragma once

#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/scheme/scheme.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <ydb/core/ymq/actor/metering.h>
#include <ydb/core/ymq/base/limits.h>

#include <chrono>
#include <future>
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

    Y_UNIT_TEST_F(TestCreateQueueWithTags, THttpProxyTestMock) {
        auto tags = NJson::TJsonMap{
            {"key1", "value1"},
            {"key2", "value2"},
        };
        auto json = CreateQueue({
            {"QueueName", "ExampleQueueName"},
            {"Tags", tags}
        });
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");
        json = ListQueueTags({{"QueueUrl", queueUrl}});
        UNIT_ASSERT(json["Tags"] == tags);

        // The next request asks to create a queue with the same name and the same set of tags.
        // We must return a URL to an existing queue.
        json = CreateQueue({
            {"QueueName", "ExampleQueueName"},
            {"Tags", tags}
        });
        UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetByPath<TString>(json, "QueueUrl"));

        // In the next requests we try to create a queue with the same name as before,
        // but with different sets of tags. All requests must be failed.

        CreateQueue({
            {"QueueName", "ExampleQueueName"},
            {"Tags", NJson::TJsonMap{
                {"key1", "value1"},
            }}
        }, 400);

        CreateQueue({
            {"QueueName", "ExampleQueueName"},
            {"Tags", NJson::TJsonMap{
                {"key1", "value1"},
                {"key2", "value0"},
            }}
        }, 400);

        CreateQueue({
            {"QueueName", "ExampleQueueName"},
            {"Tags", NJson::TJsonMap{
                {"key1", "value1"},
                {"key2", "value2"},
                {"key3", "value3"},
            }}
        }, 400);
    }

    Y_UNIT_TEST_F(TestGetQueueUrl, THttpProxyTestMock) {
        auto json = GetQueueUrl({}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "MissingParameter");

        json = CreateQueue({{"QueueName", ""}}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "MissingParameter");

        auto queueName = "ExampleQueueName";
        json = CreateQueue({{"QueueName", queueName}});

        auto queueUrl = GetByPath<TString>(json, "QueueUrl");
        json = GetQueueUrl({{"QueueName", queueName}});
        UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetByPath<TString>(json, "QueueUrl"));

        // We ignore QueueOwnerAWSAccountId parameter.
        json = GetQueueUrl({{"QueueName", queueName}, {"QueueOwnerAWSAccountId", "some-account-id"}});
        UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetByPath<TString>(json, "QueueUrl"));

        json = GetQueueUrl({{"QueueName", queueName}, {"WrongParameter", "some-value"}}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "InvalidArgumentException");
    }

    Y_UNIT_TEST_F(TestGetQueueUrlOfNotExistingQueue, THttpProxyTestMock) {
        auto json = GetQueueUrl({{"QueueName", "not-existing-queue"}}, 400);
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
        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        json = SendMessage({
            {"QueueUrl", queueUrl},
            {"MessageBody", "MessageBody-0"}
        });
        UNIT_ASSERT(!GetByPath<TString>(json, "SequenceNumber").empty());
        UNIT_ASSERT(!GetByPath<TString>(json, "MD5OfMessageBody").empty());
        UNIT_ASSERT(!GetByPath<TString>(json, "MessageId").empty());

        SendMessage({
            {"QueueUrl", queueUrl},
            {"MessageBody", "MessageBody-1"},
            {"DelaySeconds", 900}
        });
    }

    Y_UNIT_TEST_F(BillingRecordsForJsonApi, THttpProxyTestMockWithMetering) {
        auto meteringLogFilePath = KikimrServer->ServerSettings->AppConfig->GetSqsConfig().GetMeteringLogFilePath();

        // loadBillingRecords was copied from metering_ut.cpp.
        // Probably, that file is a better place for this test.
        auto loadBillingRecords = [](const TString& filepath) -> TVector<NSc::TValue> {
            TString data = TFileInput(filepath).ReadAll();
            auto rawRecords = SplitString(data, "\n");
            TVector<NSc::TValue> records;
            for (auto& record : rawRecords) {
                records.push_back(NSc::TValue::FromJson(record));
            }
            return records;
        };

        TVector<NSc::TValue> records;
        auto waitBillingRecords = [&]() {
            static size_t expectedCount = 0;
            expectedCount += 3;  // 2 traffic records, 1 request record
            while (records.size() != expectedCount) {
                Sleep(TDuration::Seconds(1));
                records = loadBillingRecords(meteringLogFilePath);
            }
        };
        auto queueTags = NJson::TJsonMap{
            {"k1", "v1"},
            {"k2", "v2"},
        };
        auto json = CreateQueue({
            {"QueueName", "ExampleQueueName"},
            {"Tags", queueTags}
        });
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");
        waitBillingRecords();

        SendMessage({
            {"QueueUrl", queueUrl},
            {"MessageBody", TString(1_KB, 'x')},  // 1 request unit
        });
        waitBillingRecords();

        json = ReceiveMessage({
            {"QueueUrl", queueUrl},
            {"WaitTimeSeconds", 20},
        });
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        waitBillingRecords();

        SendMessage({
            {"QueueUrl", queueUrl},
            {"MessageBody", TString(150_KB, 'x')},  // 3 request units
        });
        waitBillingRecords();

        json = ReceiveMessage({
            {"QueueUrl", queueUrl},
            {"WaitTimeSeconds", 20},
        });
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        waitBillingRecords();

        auto makeRecordTags = [](TVector<std::pair<TString, TString>> pairs) {
            NSc::TValue tags;
            tags.SetDict();
            for (auto const& [k, v] : pairs) {
                tags[k] = v;
            }
            return tags;
        };
        NSc::TValue queueTagsDict;
        queueTagsDict.SetDict();
        for (auto const& [k, v] : queueTags.GetMapSafe()) {
            queueTagsDict[k] = v.GetString();
        }
        auto makeRecord = [&makeRecordTags](
            TString type,
            TString resourceId,
            size_t quantity,
            TVector<std::pair<TString, TString>> tags,
            NSc::TValue queueTags = {}
        ) {
            return NKikimr::NSQS::CreateMeteringBillingRecord(
                "folder4",
                resourceId,
                type,
                "fqdn",
                TInstant::Now(),
                quantity,
                type == "ymq.traffic.v1" ? "byte" : "request",
                makeRecordTags(tags),
                queueTags
            );
        };

        TVector<NSc::TValue> expectedRecords{
            // CreateQueue
            makeRecord("ymq.traffic.v1", "", 0, {{"direction", "ingress"}, {"type", "inet"}}),
            makeRecord("ymq.traffic.v1", "", 0, {{"direction", "egress"}, {"type", "inet"}}),
            makeRecord("ymq.requests.v1", "", 1, {{"queue_type", "other"}}),

            // SendMessage 1 KB
            makeRecord("ymq.traffic.v1", "000000000000000101v0", 0, {{"direction", "ingress"}}, queueTagsDict),
            makeRecord("ymq.traffic.v1", "000000000000000101v0", 0, {{"direction", "egress"}}, queueTagsDict),
            makeRecord("ymq.requests.v1", "000000000000000101v0", 1, {{"queue_type", "std"}}, queueTagsDict),

            // ReceiveMessage 1 KB
            makeRecord("ymq.traffic.v1", "000000000000000101v0", 0, {{"direction", "ingress"}}, queueTagsDict),
            makeRecord("ymq.traffic.v1", "000000000000000101v0", 0, {{"direction", "egress"}}, queueTagsDict),
            makeRecord("ymq.requests.v1", "000000000000000101v0", 1, {{"queue_type", "std"}}, queueTagsDict),

            // SendMessage 150 KB
            makeRecord("ymq.traffic.v1", "000000000000000101v0", 0, {{"direction", "ingress"}}, queueTagsDict),
            makeRecord("ymq.traffic.v1", "000000000000000101v0", 0, {{"direction", "egress"}}, queueTagsDict),
            makeRecord("ymq.requests.v1", "000000000000000101v0", 3, {{"queue_type", "std"}}, queueTagsDict),

            // ReceiveMessage 150 KB
            makeRecord("ymq.traffic.v1", "000000000000000101v0", 0, {{"direction", "ingress"}}, queueTagsDict),
            makeRecord("ymq.traffic.v1", "000000000000000101v0", 0, {{"direction", "egress"}}, queueTagsDict),
            makeRecord("ymq.requests.v1", "000000000000000101v0", 3, {{"queue_type", "std"}}, queueTagsDict),
        };

        auto asExpected = [](NSc::TValue record, NSc::TValue expected) {
            return record["folder_id"] == expected["folder_id"] &&
                   record["resource_id"] == expected["resource_id"] &&
                   record["schema"] == expected["schema"] &&
                   record["usage"]["unit"] == expected["usage"]["unit"] &&
                   (record["schema"] != "ymq.requests.v1" || record["usage"]["quantity"] == expected["usage"]["quantity"]) &&
                   record["tags"]["direction"] == expected["tags"]["direction"] &&
                   record["tags"]["queue_type"] == expected["tags"]["queue_type"] &&
                   record["labels"]["k1"] == expected["labels"]["k1"] &&
                   record["labels"]["k2"] == expected["labels"]["k2"];
        };
        for (size_t i = 0; i < records.size(); ++i) {
            UNIT_ASSERT(asExpected(records[i], expectedRecords[i]));
        }
    }

    Y_UNIT_TEST_F(TestSendMessageEmptyQueueUrl, THttpProxyTestMockForSQS) {
        // We had a bug that crashed the server if QueueUrl was empty in a request.
        SendMessage({
            {"QueueUrl", ""},
            {"MessageBody", "MessageBody-0"}
        }, 400);
    }

    Y_UNIT_TEST_F(TestSendMessageFifoQueue, THttpProxyTestMock) {
        auto json = CreateQueue({
            {"QueueName", "ExampleQueueName.fifo"},
            {"Attributes", NJson::TJsonMap{
                {"FifoQueue", "true"}
            }}
        });
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        auto body = "MessageBody-0";
        json = SendMessage({
            {"QueueUrl", queueUrl},
            {"MessageBody", body},
            {"MessageDeduplicationId", "MessageDeduplicationId-0"},
            {"MessageGroupId", "MessageGroupId-0"}
        });
        UNIT_ASSERT(!GetByPath<TString>(json, "SequenceNumber").empty());
        UNIT_ASSERT(!GetByPath<TString>(json, "MD5OfMessageBody").empty());
        UNIT_ASSERT(!GetByPath<TString>(json, "MessageId").empty());
    }

    Y_UNIT_TEST_F(TestSendMessageWithAttributes, THttpProxyTestMock) {
        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        json = SendMessage({
            {"QueueUrl", queueUrl},
            {"MessageBody", "MessageBody-0"},
            {"MessageAttributes", NJson::TJsonMap{
                {"string-attr", NJson::TJsonMap{
                    {"DataType", "String"},
                    {"StringValue", "1"}
                }},
                {"number-attr", NJson::TJsonMap{
                    {"DataType", "Number"},
                    {"StringValue", "1"}
                }},
                {"binary-attr", NJson::TJsonMap{
                    {"DataType", "Binary"},
                    {"BinaryValue", Base64Encode("encoded-value")}
                }},
                {"custom-type-attr", NJson::TJsonMap{
                    {"DataType", "Number.float"},
                    {"StringValue", "2.7182818284"}
                }}
            }},

            // From Amazon SQS docs: "Currently, the only supported message system attribute is AWSTraceHeader".
            // We do not support the attribute, but need to check that it doesn't lead to fails.
            {"MessageSystemAttributes", NJson::TJsonMap{
                {"AWSTraceHeader", NJson::TJsonMap{
                    {"DataType", "String"},
                    {"StringValue", "Root=1-5759e988-bd862e3fe1be46a994272793;Sampled=1"}
                }}
            }}
        });

        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});

        auto attrs = json["Messages"][0]["MessageAttributes"];
        UNIT_ASSERT_VALUES_EQUAL(attrs["string-attr"]["StringValue"].GetString(), "1");
        UNIT_ASSERT_VALUES_EQUAL(attrs["number-attr"]["StringValue"].GetString(), "1");
        UNIT_ASSERT_VALUES_EQUAL(Base64Decode(attrs["binary-attr"]["BinaryValue"].GetString()), "encoded-value");
        UNIT_ASSERT_VALUES_EQUAL(attrs["custom-type-attr"]["StringValue"].GetString(), "2.7182818284");
    }

    Y_UNIT_TEST_F(TestReceiveMessage, THttpProxyTestMock) {
        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        auto body0 = "MessageBody-0";
        SendMessage({{"QueueUrl", queueUrl}, {"MessageBody", body0}});
        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});

        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body0);

        for (size_t i = 1; i <= 10; ++i) {
            auto body = TStringBuilder() << "MessageBody-" << i;
            SendMessage({{"QueueUrl", queueUrl}, {"MessageBody", body}});
        }

        WaitQueueAttributes(queueUrl, 10, {{"ApproximateNumberOfMessages", "11"}});

        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}, {"MaxNumberOfMessages", 10}});
        UNIT_ASSERT_GE(json["Messages"].GetArray().size(), 1);
    }

    Y_UNIT_TEST_F(TestReceiveMessageWithAttributes, THttpProxyTestMock) {
        // Test if we process AttributeNames, MessageSystemAttributeNames, MessageAttributeNames correctly.

        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        TString messageBody = "MessageBody-0";
        SendMessage({
            {"QueueUrl", queueUrl},
            {"MessageBody", messageBody},
            {"MessageAttributes", NJson::TJsonMap{
                {"SomeAttribute", NJson::TJsonMap{
                    {"StringValue", "1"},
                    {"DataType", "String"}
                }},
                {"AnotherAttribute", NJson::TJsonMap{
                    {"StringValue", "2"},
                    {"DataType", "String"}
                }}
            }}
        });

        auto receiveMessage = [&, this](NJson::TJsonMap request, ui32 expectedStatus = 200) -> NJson::TJsonMap {
            request["VisibilityTimeout"] = 0;  // Keep the message visible for next ReceiveMessage requests.
            request["QueueUrl"] = queueUrl;
            request["WaitTimeSeconds"] = 20;
            json = ReceiveMessage(request, expectedStatus);
            UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], messageBody);
            return json;
        };

        {
            // Test deprecated AttributeNames field.

            // Request SentTimestamp message system attribute using deprecated AttributeNames field.
            json = receiveMessage({{"AttributeNames", NJson::TJsonArray{"SentTimestamp"}}});
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());

            // Request All message system attributes using deprecated AttributeNames field.
            json = receiveMessage({{"AttributeNames", NJson::TJsonArray{"All"}}});
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());

            // Request message system attributes using AttributeNames field.
            json = receiveMessage({{"AttributeNames", NJson::TJsonArray{
                "SenderId", "SentTimestamp", "ApproximateReceiveCount", "ApproximateFirstReceiveTimestamp", "SequenceNumber"
                "MessageDeduplicationId", "MessageGroupId", "AWSTraceHeader", "DeadLetterQueueSourceArn"
            }}});
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());
        }

        {
            // Test MessageSystemAttributeNames field.

            // Request SentTimestamp.
            json = receiveMessage({{"MessageSystemAttributeNames", NJson::TJsonArray{"SentTimestamp"}}});
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());

            // Request All message system attributes.
            json = receiveMessage({{"MessageSystemAttributeNames", NJson::TJsonArray{"All"}}});
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());

            // Request message system attributes.
            json = receiveMessage({{"MessageSystemAttributeNames", NJson::TJsonArray{
                "SenderId", "SentTimestamp", "ApproximateReceiveCount", "ApproximateFirstReceiveTimestamp", "SequenceNumber"
                "MessageDeduplicationId", "MessageGroupId", "AWSTraceHeader", "DeadLetterQueueSourceArn"
            }}});
            UNIT_ASSERT(!json["Messages"][0]["Attributes"]["SentTimestamp"].GetString().empty());
        }

        {
            // Test MessageAttributeNames

            json = receiveMessage({{"MessageAttributeNames", NJson::TJsonArray{}}});
            auto attrs = json["Messages"][0]["MessageAttributes"];

            UNIT_ASSERT_VALUES_EQUAL(attrs.GetMapSafe().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(attrs["SomeAttribute"]["StringValue"].GetString(), "1");
            UNIT_ASSERT_VALUES_EQUAL(attrs["AnotherAttribute"]["StringValue"].GetString(), "2");

            json = receiveMessage({{"MessageAttributeNames", NJson::TJsonArray{"All"}}});
            UNIT_ASSERT_VALUES_EQUAL(attrs.GetMapSafe().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(attrs["SomeAttribute"]["StringValue"].GetString(), "1");
            UNIT_ASSERT_VALUES_EQUAL(attrs["AnotherAttribute"]["StringValue"].GetString(), "2");

            json = receiveMessage({{"MessageAttributeNames", NJson::TJsonArray{"SomeAttribute"}}});

            // We return all attributes, no matter what MessageAttributeNames are in the request. Should be fixed and uncommented:
            //     UNIT_ASSERT_VALUES_EQUAL(attrs.GetMapSafe().size(), 1);

            UNIT_ASSERT_VALUES_EQUAL(attrs["SomeAttribute"]["StringValue"].GetString(), "1");
        }
    }

    Y_UNIT_TEST_F(TestReceiveMessageWithAttemptId, THttpProxyTestMock) {
        auto json = CreateQueue({
            {"QueueName", "ExampleQueueName.fifo"},
            {"Attributes", NJson::TJsonMap{
                {"FifoQueue", "true"},
                {"VisibilityTimeout", "0"}
            }}
        });
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        SendMessage({
            {"QueueUrl", queueUrl},
            {"MessageBody", "message-body-0"},
            {"MessageGroupId", "message-group-0"},
            {"MessageDeduplicationId", "MessageDeduplicationId-0"}
        });

        auto json1 = ReceiveMessage({{"QueueUrl", queueUrl}, {"ReceiveRequestAttemptId", "attempt-0"}, {"VisibilityTimeout", 40000}});
        auto messageId = json1["Messages"][0]["MessageId"];
        auto json2 = ReceiveMessage({{"QueueUrl", queueUrl}, {"ReceiveRequestAttemptId", "attempt-0"}, {"VisibilityTimeout", 40000}});

        UNIT_ASSERT_VALUES_UNEQUAL(json1["Messages"][0]["ReceiptHandle"].GetStringSafe(), json2["Messages"][0]["ReceiptHandle"].GetStringSafe());
        UNIT_ASSERT_VALUES_EQUAL(messageId.GetStringSafe(), json2["Messages"][0]["MessageId"].GetStringSafe());

        // ReceiveMessage with ReceiveRequestAttemptId should reset VisibilityTimeout.
        // As we created the queue with VisibilityTimeout = 0, the message should immediately reappear in the queue.
        auto json3 = ReceiveMessage({{"QueueUrl", queueUrl}, {"ReceiveRequestAttemptId", "attempt-0"}});
        auto json4 = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 1}});
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
                    // "LastModifiedTimestamp",  // Not supported at this moment.
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
        auto json = ListQueues({});

        size_t numOfExampleQueues = 10;
        TVector<TString> queueUrls;
        for (size_t i = 0; i < numOfExampleQueues; ++i) {
            auto json = CreateQueue({{"QueueName", TStringBuilder() << "ExampleQueue-" << i}});
            queueUrls.push_back(GetByPath<TString>(json, "QueueUrl"));
        }

        json = CreateQueue({{"QueueName", "AnotherQueue"}});
        auto anotherQueueUrl = GetByPath<TString>(json, "QueueUrl");
        queueUrls.push_back(anotherQueueUrl);

        json = ListQueues({});
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), numOfExampleQueues + 1);

        json = ListQueues({{"QueueNamePrefix", ""}});
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), numOfExampleQueues + 1);

        json = ListQueues({{"QueueNamePrefix", "BadPrefix"}});
        UNIT_ASSERT(json["QueueUrls"].GetArray().empty());

        json = ListQueues({{"QueueNamePrefix", "Another"}});
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"][0], anotherQueueUrl);

        json = ListQueues({{"QueueNamePrefix", "Ex"}});
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), numOfExampleQueues);
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"][0], queueUrls[0]);

        // MaxResults and NextToken query parameters are currently ignored.
        json = ListQueues({{"MaxResults", 1}, {"NextToken", "unknown-next-token"}});
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), numOfExampleQueues + 1);
    }

    Y_UNIT_TEST_F(TestDeleteMessage, THttpProxyTestMock) {
        DeleteMessage({}, 400);
        DeleteMessage({{"QueueUrl", "wrong-queue-url"}}, 400);
        DeleteMessage({{"QueueUrl", 123}}, 400);

        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        TString queueUrl = GetByPath<TString>(json, "QueueUrl");

        DeleteMessage({{"QueueUrl", queueUrl}}, 400);
        DeleteMessage({{"QueueUrl", queueUrl}, {"ReceiptHandle", "unknown-receipt-handle"}}, 400);
        DeleteMessage({{"QueueUrl", queueUrl}, {"ReceiptHandle", ""}}, 400);
        DeleteMessage({{"QueueUrl", queueUrl}, {"ReceiptHandle", 123}}, 400);

        auto body = "MessageBody-0";
        SendMessage({{"QueueUrl", queueUrl}, {"MessageBody", body}});
        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});

        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.empty());

        DeleteMessage({{"QueueUrl", queueUrl}, {"ReceiptHandle", receiptHandle}, {"UnknownParameter", 123}}, 400);

        DeleteMessage({{"QueueUrl", queueUrl}, {"ReceiptHandle", receiptHandle}});

        WaitQueueAttributes(queueUrl, 10, {
            {"ApproximateNumberOfMessages", "0"},
            {"ApproximateNumberOfMessagesNotVisible", "0"},
        });

        DeleteMessage({{"QueueUrl", queueUrl}, {"ReceiptHandle", receiptHandle}});
    }

    Y_UNIT_TEST_F(TestPurgeQueue, THttpProxyTestMock) {
        auto json = PurgeQueue({{"QueueUrl", "unknown-queue-url"}}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "ValidationException");
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "message"), "Invalid queue url");

        json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        SendMessage({{"QueueUrl", queueUrl}, {"MessageBody", "MessageBody-0"}});
        SendMessage({{"QueueUrl", queueUrl}, {"MessageBody", "MessageBody-1"}});

        // All available messages in a queue (including in-flight messages) should be deleted.
        // Set VisibilityTimeout to large value to be sure the message is in-flight during the test.
        ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 1}, {"VisibilityTimeout", 43000}});  // ~12 hours

        WaitQueueAttributes(queueUrl, 10, {
            {"ApproximateNumberOfMessages", "2"},
            {"ApproximateNumberOfMessagesNotVisible", "1"},
        });

        PurgeQueue({{"QueueUrl", queueUrl}});

        WaitQueueAttributes(queueUrl, 10, {
            {"ApproximateNumberOfMessages", "0"},
            {"ApproximateNumberOfMessagesNotVisible", "0"},
        });
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
        auto json = CreateQueue({
            {"QueueName", "DLQ.fifo"},
            {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}
        });
        auto attributes1 = GetQueueAttributes({
            {"QueueUrl", GetByPath<TString>(json, "QueueUrl")},
            {"AttributeNames", NJson::TJsonArray{"QueueArn"}}
        });
        auto queueArn1 = GetByPath<TString>(attributes1, "Attributes.QueueArn");

        json = CreateQueue({
            {"QueueName", "ExampleQueueName.fifo"},
            {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}
        });
        TString queueUrl = GetByPath<TString>(json, "QueueUrl");

        auto attributes = NJson::TJsonMap{
            {"DelaySeconds", "2"},
            {"MaximumMessageSize", "12345"},
            {"MessageRetentionPeriod", "678"},
            {"ReceiveMessageWaitTimeSeconds", "9"},
            {"VisibilityTimeout", "1234"},

            {"RedrivePolicy", TStringBuilder() << "{\"deadLetterTargetArn\":\"" << queueArn1 << "\",\"maxReceiveCount\":3}"},

            // 2024-10-07: RedriveAllowPolicy not supported yet.
            // {"RedriveAllowPolicy", TStringBuilder() << "{\"redrivePermission\":\"byQueue\", \"sourceQueueArns\": [\"" << queueArn2 << "\", \"" << queueArn3 << "\"]}"},

            // High throughput for FIFO queues not supported yet.
            // {"DeduplicationScope", "messageGroup"},
            // {"FifoThroughputLimit", "perMessageGroupId"}
        };

        SetQueueAttributes({{"QueueUrl", queueUrl}, {"Attributes", attributes}});

        WaitQueueAttributes(queueUrl, 10, [&attributes](NJson::TJsonMap json) {
            for (auto& [k, v] : attributes.GetMapSafe()) {
                if (json["Attributes"][k].GetStringSafe() != v) {
                    return false;
                }
            }
            return true;
        });

        SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"DelaySeconds", "-1"}}}
        }, 400);

        SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"DelaySeconds", "901"}}}
        }, 400);

        WaitQueueAttributes(queueUrl, 10, [](NJson::TJsonMap json) {
            return json["Attributes"]["DelaySeconds"] == "2";
        });

        json = SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"UnknownAttribute", "value"}}}
        }, 400);
    }

    Y_UNIT_TEST_F(TestSendMessageBatch, THttpProxyTestMock) {
        auto json = CreateQueue({
            {"QueueName", "ExampleQueueName.fifo"},
            {"Attributes", NJson::TJsonMap{
                {"FifoQueue", "true"},
                {"ContentBasedDeduplication", "true"}
            }}
        });
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        json = SendMessageBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{
                    {"Id", "Id-0"},
                    {"MessageBody", "MessageBody-0"},
                    {"MessageGroupId", "MessageGroupId-0"},
                    {"MessageAttributes", NJson::TJsonMap{
                        {"SomeAttribute", NJson::TJsonMap{
                            {"DataType", "String"},
                            {"StringValue", "1"}
                        }}
                    }}
                },
                NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "MessageBody-1"}, {"MessageGroupId", "MessageGroupId-1"}},
                NJson::TJsonMap{{"Id", "Id-2"}, {"MessageBody", "MessageBody-2"}},
            }}
        });

        UNIT_ASSERT(json["Successful"].GetArray().size() == 2);
        auto succesful0 = json["Successful"][0];
        UNIT_ASSERT(succesful0["Id"] == "Id-0");
        UNIT_ASSERT(!GetByPath<TString>(succesful0, "MD5OfMessageAttributes").empty());
        UNIT_ASSERT(!GetByPath<TString>(succesful0, "MD5OfMessageBody").empty());
        UNIT_ASSERT(!GetByPath<TString>(succesful0, "MessageId").empty());

        UNIT_ASSERT(json["Successful"][1]["Id"] == "Id-1");

        UNIT_ASSERT(json["Failed"].GetArray().size() == 1);
    }

    Y_UNIT_TEST_F(TestDeleteMessageBatch, THttpProxyTestMock) {
        DeleteMessageBatch({}, 400);

        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        DeleteMessageBatch({
            {"QueueUrl", queueUrl}
        }, 400);

        DeleteMessageBatch({
            {"QueueUrl", queueUrl},
            {"Entries", {}}
        }, 400);

        DeleteMessageBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{}}
        }, 400);

        DeleteMessageBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonMap{}}
        }, 400);

        DeleteMessageBatch({
            {"QueueUrl", queueUrl},
            {"Entries", {""}}
        }, 400);

        json = SendMessageBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"MessageBody", "MessageBody-0"}, {"MessageDeduplicationId", "MessageDeduplicationId-0"}},
                NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "MessageBody-1"}, {"MessageDeduplicationId", "MessageDeduplicationId-1"}}
            }}
        });
        UNIT_ASSERT(json["Successful"].GetArray().size() == 2);

        TVector<NJson::TJsonValue> messages;
        for (int i = 0; i < 20; ++i) {
            auto json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});
            if (!json.GetMapSafe().empty()) {
                for (auto& m : json["Messages"].GetArray()) {
                    messages.push_back(m);
                }
            }
            if (messages.size() >= 2) {
                break;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

        auto receiptHandle0 = messages[0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle0.empty());
        auto receiptHandle1 = messages[1]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle1.empty());

        json = DeleteMessageBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"ReceiptHandle", receiptHandle0}},
                NJson::TJsonMap{{"Id", "Id-1"}, {"ReceiptHandle", receiptHandle1}}
            }}
        });

        UNIT_ASSERT_VALUES_EQUAL(json["Successful"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][0]["Id"], "Id-0");
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][1]["Id"], "Id-1");

        json = ReceiveMessage({{"QueueUrl", queueUrl}});

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
        ChangeMessageVisibility({}, 400);
        ChangeMessageVisibility({
            {"QueueUrl", "unknown-url"},
            {"ReceiptHandle", "unknown-receipt-handle"},
            {"VisibilityTimeout", 1}
        }, 400);

        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        ChangeMessageVisibility({
            {"QueueUrl", queueUrl},
            {"VisibilityTimeout", 1}
        }, 400);

        ChangeMessageVisibility({
            {"QueueUrl", queueUrl},
            {"ReceiptHandle", "unknown-receipt-handle"},
        }, 400);

        ChangeMessageVisibility({
            {"QueueUrl", queueUrl},
            {"ReceiptHandle", "unknown-receipt-handle"},
            {"VisibilityTimeout", 1}
        }, 400);

        auto body = "MessageBody-0";
        SendMessage({{"QueueUrl", queueUrl}, {"MessageBody", body}});

        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});
        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.empty());

        ChangeMessageVisibility({
            {"QueueUrl", queueUrl},
            {"ReceiptHandle", receiptHandle},
            {"VisibilityTimeout", 1}
        });

        WaitQueueAttributes(queueUrl, 10, {
            {"ApproximateNumberOfMessages", "1"},
            {"ApproximateNumberOfMessagesNotVisible", "0"},
        });

        ChangeMessageVisibility({
            {"QueueUrl", queueUrl},
            {"ReceiptHandle", receiptHandle},
            {"VisibilityTimeout", 1}
        }, 400);
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityBatch, THttpProxyTestMock) {
        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");

        json = SendMessageBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"MessageBody", "MessageBody-0"}, {"MessageDeduplicationId", "MessageDeduplicationId-0"}},
                NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "MessageBody-1"}, {"MessageDeduplicationId", "MessageDeduplicationId-1"}}
            }}
        });
        UNIT_ASSERT(json["Successful"].GetArray().size() == 2);

        TVector<NJson::TJsonValue> messages;
        for (int i = 0; i < 20; ++i) {
            auto json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});
            if (!json.GetMapSafe().empty()) {
                for (auto& m : json["Messages"].GetArray()) {
                    messages.push_back(m);
                }
            }
            if (messages.size() >= 2) {
                break;
            }
        }

        UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);

        auto receiptHandle0 = messages[0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle0.empty());
        auto receiptHandle1 = messages[1]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle1.empty());

        ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
        }, 400);

        ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
            {"Entries", ""}
        }, 400);

        json = ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{}}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(json["__type"].GetString(), "AWS.SimpleQueueService.EmptyBatchRequest");

        json = ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"ReceiptHandle", "CgNtZzEQAhojZmIzMGE1M2YtZjZkN2VjMTgtNTEwY2UwMGUtZTc2NjE2MWQg4uql3acyKAA"}, {"VisibilityTimeout", 1}}
            }}
        });


        ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"ReceiptHandle", 0}}
            }}
        }, 400);

        ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"ReceiptHandle", receiptHandle0}}
            }}
        });

        ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"VisibilityTimeout", 1}}
            }}
        });

        json = ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"ReceiptHandle", receiptHandle0}, {"VisibilityTimeout", 1}}
            }}
        });
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][0]["Id"], "Id-0");

        json = ChangeMessageVisibilityBatch({
            {"QueueUrl", queueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-0"}, {"ReceiptHandle", receiptHandle0}, {"VisibilityTimeout", 1}},
                NJson::TJsonMap{{"Id", "Id-1"}, {"ReceiptHandle", receiptHandle1}, {"VisibilityTimeout", 2}}
            }}
        });
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"].GetArray().size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][0]["Id"], "Id-0");
        UNIT_ASSERT_VALUES_EQUAL(json["Successful"][1]["Id"], "Id-1");
    }

    Y_UNIT_TEST_F(TestListQueueTags, THttpProxyTestMock) {
        auto queues = TVector{
            CreateQueue({{"QueueName", "ExampleQueueName"}}),
            CreateQueue({{"QueueName", "ExampleQueueName.fifo"}, {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}}),
        };
        for (const auto& q : queues) {
            auto queueUrl = GetByPath<TString>(q, "QueueUrl");
            auto response = ListQueueTags({{"QueueUrl", queueUrl}});
            UNIT_ASSERT_VALUES_EQUAL(response.GetMapSafe().size(), 0);
        }
    }

    Y_UNIT_TEST_F(TestTagQueue, THttpProxyTestMock) {
        using NJson::TJsonMap;
        using NJson::TJsonArray;
        auto queues = TVector{
            CreateQueue({{"QueueName", "ExampleQueueName"}}),
            CreateQueue({{"QueueName", "ExampleQueueName.fifo"}, {"Attributes", TJsonMap{{"FifoQueue", "true"}}}}),
        };
        for (const auto& q : queues) {
            auto queueUrl = GetByPath<TString>(q, "QueueUrl");

            {
                // Check that we can update a value of an existing tag.

                auto key = TString("key");

                TagQueue({{"QueueUrl", queueUrl}, {"Tags", TJsonMap{{key, "x"}}}});
                auto json = ListQueueTags({{"QueueUrl", queueUrl}});
                UNIT_ASSERT((json["Tags"] == TJsonMap{{key, "x"}}));

                TagQueue({{"QueueUrl", queueUrl}, {"Tags", TJsonMap{{key, "y"}}}});
                json = ListQueueTags({{"QueueUrl", queueUrl}});
                UNIT_ASSERT((json["Tags"] == TJsonMap{{key, "y"}}));

                UntagQueue({{"QueueUrl", queueUrl}, {"TagKeys", TJsonArray{key}}});
            }

            {
                // Multiple tags per query.

                auto setTags = TJsonMap{
                    {"key1", "value1"},
                    {"key2", "value2"},
                };
                TagQueue({{"QueueUrl", queueUrl}, {"Tags", setTags}});
                auto json = ListQueueTags({{"QueueUrl", queueUrl}});

                UNIT_ASSERT_VALUES_EQUAL(json.GetMapSafe().size(), 1);
                UNIT_ASSERT(json["Tags"] == setTags);
            }

            {
                // Existing tags should not be lost after the next query.

                TagQueue({{"QueueUrl", queueUrl}, {"Tags", TJsonMap{
                    {"key3", "value3"},
                }}});
                auto json = ListQueueTags({{"QueueUrl", queueUrl}});

                UNIT_ASSERT_VALUES_EQUAL(json.GetMapSafe().size(), 1);
                UNIT_ASSERT((json["Tags"] == TJsonMap{
                    {"key1", "value1"},
                    {"key2", "value2"},
                    {"key3", "value3"},
                }));
            }
        }
    }

    Y_UNIT_TEST_F(TestUntagQueue, THttpProxyTestMock) {
        auto queues = TVector{
            CreateQueue({{"QueueName", "ExampleQueueName"}}),
            CreateQueue({{"QueueName", "ExampleQueueName.fifo"}, {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}}),
        };
        for (const auto& q : queues) {
            auto queueUrl = GetByPath<TString>(q, "QueueUrl");

            UntagQueue({{"QueueUrl", queueUrl}, {"TagKeys", NJson::TJsonArray{"key0"}}});

            auto setTags = NJson::TJsonMap{
                {"key1", "value1"},
                {"key2", "value2"},
                {"key3", "value3"},
            };
            TagQueue({{"QueueUrl", queueUrl}, {"Tags", setTags}});

            auto json = ListQueueTags({{"QueueUrl", queueUrl}});
            UNIT_ASSERT(json["Tags"] == setTags);

            UntagQueue({{"QueueUrl", queueUrl}, {"TagKeys", NJson::TJsonArray{"key1"}}});
            json = ListQueueTags({{"QueueUrl", queueUrl}});
            UNIT_ASSERT((json["Tags"] == NJson::TJsonMap{
                {"key2", "value2"},
                {"key3", "value3"},
            }));

            UntagQueue({{"QueueUrl", queueUrl}, {"TagKeys", NJson::TJsonArray{"key1", "key2", "key3"}}});
            json = ListQueueTags({{"QueueUrl", queueUrl}});
            UNIT_ASSERT(json.GetMapSafe().empty());
        }
    }

    Y_UNIT_TEST_F(TestTagQueueMultipleQueriesInflight, THttpProxyTestMock) {
        // Without additional checks, a Tag/UntagQueue queries may overwrite
        // changes made by a different query run in parallel.
        // Current behavior: if there was a conflicting query, return 500 error.
        // This test either stops after an internal error, or completes successfully,
        // and the queue does not have any tags.

        auto queues = TVector{
            CreateQueue({{"QueueName", "ExampleQueueName"}}),
            CreateQueue({{"QueueName", "ExampleQueueName.fifo"}, {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}}),
        };
        for (const auto& q : queues) {
            auto queueUrl = GetByPath<TString>(q, "QueueUrl");

            std::atomic<bool> stop = false;
            {
                // Additional scope to wait for the async results before running ListQueueTags query.
                TVector<std::future<void>> asyncResults;
                for (size_t i = 0; i < NKikimr::NSQS::TLimits::MaxTagCount; ++i) {
                    asyncResults.emplace_back(std::async(std::launch::async, [&, i]() {
                        auto key = TStringBuilder() << "k" << i;
                        for (size_t j = 0; j < 20 && !stop; ++j) {
                            auto json = TagQueue({{"QueueUrl", queueUrl}, {"Tags", NJson::TJsonMap{{key, "v"}}}}, 0);
                            auto map = json.GetMapSafe();
                            if (!map.empty() && map["__type"] == "InternalFailure") {
                                stop = true;
                            }

                            json = UntagQueue({{"QueueUrl", queueUrl}, {"TagKeys", NJson::TJsonArray{key}}}, 0);
                            map = json.GetMapSafe();
                            if (!map.empty() && map["__type"] == "InternalFailure") {
                                stop = true;
                            }
                        }
                    }));
                }
            }

            auto json = ListQueueTags({{"QueueUrl", queueUrl}});
            UNIT_ASSERT(stop || json.GetMapSafe().empty());
        }
    }

} // Y_UNIT_TEST_SUITE(TestYmqHttpProxy)
