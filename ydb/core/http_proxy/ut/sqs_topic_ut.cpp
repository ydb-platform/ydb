
#include <ydb/core/http_proxy/ut/datastreams_fixture/datastreams_fixture.h>

#include <ydb/core/http_proxy/http_req.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/ymq/actor/metering.h>
#include <ydb/core/ymq/base/limits.h>

#include <ydb/library/testlib/service_mocks/access_service_mock.h>
#include <ydb/library/testlib/service_mocks/iam_token_service_mock.h>

#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/writer/json_value.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/string_utils/url/url.h>

#include <format>

using namespace NKikimr::NHttpProxy;
using namespace NKikimr::Tests;
using namespace NActors;


using TFixture = THttpProxyTestMockForSQSTopic;

namespace {

    using NYdb::TDriver;
    using NYdb::NTopic::TTopicClient;

    constexpr TStringBuf NON_EXISTING_QUEUE_URL = "/v1/5//Root/16/NonExistentQueue/16/ydb-sqs-consumer";

    TString GetPathFromFullQueueUrl(const TString& url) {
        auto [host, path] = NUrl::SplitUrlToHostAndPath(url);
        return ToString(path);
    }

    TString GetPathFromFullQueueUrl(const NJson::TJsonValue& url) {
        return GetPathFromFullQueueUrl(url.GetStringSafe());
    }

    TString GetPathFromQueueUrlMap(const NJson::TJsonMap& json) {
        TString url = GetByPath<TString>(json, "QueueUrl");
        return GetPathFromFullQueueUrl(url);
    }

    NYdb::TDriverConfig MakeDriverConfig(TFixture& fixture) {
        NYdb::TDriverConfig config;

        config.SetEndpoint("localhost:" + ToString(fixture.GRpcServerPort));
        config.SetEndpoint("localhost:" + ToString(fixture.KikimrGrpcPort));
        config.SetDatabase("/Root");
        config.SetAuthToken("root@builtin");
        config.SetLog(std::make_unique<TStreamLogBackend>(&Cerr));
        return config;
    }

    NYdb::TDriver MakeDriver(TFixture& fixture) {
        return TDriver(MakeDriverConfig(fixture));
    }

    bool CreateTopic(NYdb::TDriver& driver, const TString& topicName, NYdb::NTopic::TCreateTopicSettings& settings) {
        auto client = TTopicClient(driver);
        bool ok;
        {
            auto ct = client.CreateTopic(topicName, settings).GetValueSync();
            ct.Out(Cerr);
            Cerr << LabeledOutput(ct.IsSuccess(), ct.IsTransportError(), ct.GetEndpoint()) << Endl;
            ok = ct.IsSuccess();
        }
        {
            auto desc = client.DescribeTopic(topicName, NYdb::NTopic::TDescribeTopicSettings{}.IncludeLocation(true)).GetValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), topicName);
            auto description = desc.GetTopicDescription();
            desc.Out(Cerr);
            for (const auto& c : description.GetConsumers()) {
                Cerr << c.GetConsumerName() << Endl;
            }
        }
        return ok;
    }

    bool CreateTopic(NYdb::TDriver& driver, const TString& topicName, const TString& consumerName) {
        return CreateTopic(driver, topicName, NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer(consumerName)
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(20))
            .EndAddConsumer());

    }

    TMaybe<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent> GetNextDataMessage(const std::shared_ptr<NYdb::NTopic::IReadSession>& reader, TInstant deadline) {
        while (true) {
            reader->WaitEvent().Wait(deadline);
            std::optional<NYdb::NTopic::TReadSessionEvent::TEvent> event = reader->GetEvent(false);
            if (!event) {
                return {};
            }
            if (auto e = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                return *e;
            } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                e->Confirm();
            } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&*event)) {
                e->Confirm();
            } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&*event)) {
                e->Confirm();
                return {};
            } else if (std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&*event)) {
                return {};
            } else if (std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                return {};
            }
        }
    }

    TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> ReadMessagesSync(const std::shared_ptr<NYdb::NTopic::IReadSession>& reader, bool commit, size_t count, TDuration timeout) {
        TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> result;
        TInstant deadline = timeout.ToDeadLine();
        while (result.size() < count) {
            TInstant now = TInstant::Now();
            Cerr << (TStringBuilder() << "WAIT READ " << LabeledOutput(deadline - now) << "\n") << Flush;
            auto event = GetNextDataMessage(reader, deadline);
            if (!event) {
                break;
            }
            std::vector messages = event->GetMessages();
            for (NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message : messages) {
                Cerr << (TStringBuilder() << "READ " << LabeledOutput(message.GetOffset(), message.GetProducerId(), message.GetMessageGroupId(), message.GetSeqNo(), message.GetData().size()) << "\n") << Flush;
                result.push_back(std::move(message));
            }
            if (commit) {
                event->Commit();
            }
        }
        return result;
    }

    TVector<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage> ReadMessagesSync(NYdb::TDriver& driver, const TString& topicPath, bool commit, size_t count, TDuration timeout, TVector<ui32> partitionIds = {0}) {
        NYdb::NTopic::TTopicReadSettings topicSettings(topicPath);
        for (auto partition : partitionIds) {
            topicSettings.AppendPartitionIds(partition);
        }
        NYdb::NTopic::TTopicClient topicClient(driver);
        auto settings = NYdb::NTopic::TReadSessionSettings()
                            .AppendTopics(topicSettings)
                            .WithoutConsumer()
                            .Decompress(true);
        auto messages = ReadMessagesSync(topicClient.CreateReadSession(settings), commit, count, timeout);
        return messages;
    }
} // namespace

Y_UNIT_TEST_SUITE(TestSqsTopicHttpProxy) {

        Y_UNIT_TEST_F(TestGetQueueUrlEmpty, TFixture) {
            auto json = GetQueueUrl({}, 400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "MissingParameter");
        }

        Y_UNIT_TEST_F(TestGetQueueUrl, TFixture) {
            auto driver = MakeDriver(*this);
            const TString topicName = "ExampleQueueName";
            const TString consumer = "ydb-sqs-consumer";

            Y_ENSURE(CreateTopic(driver, topicName, consumer));

            const TString queueUrl = std::format("/v1/5//Root/{}/{}/{}/{}", topicName.size(), topicName.c_str(), consumer.size(), consumer.c_str());
            auto queueName = topicName;
            auto json = GetQueueUrl({{"QueueName", queueName}});
            UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));

            // ignore QueueOwnerAWSAccountId parameter.
            json = GetQueueUrl({{"QueueName", queueName}, {"QueueOwnerAWSAccountId", "some-account-id"}});
            UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));

            json = GetQueueUrl({{"QueueName", queueName}, {"WrongParameter", "some-value"}}, 400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "InvalidArgumentException");
        }

        Y_UNIT_TEST_F(TestGetQueueUrlOfNotExistingQueue, TFixture) {
            auto json = GetQueueUrl({{"QueueName", "not-existing-queue"}}, 400);
            TString resultType = GetByPath<TString>(json, "__type");
            UNIT_ASSERT_VALUES_EQUAL(resultType, "AWS.SimpleQueueService.NonExistentQueue");
            TString resultMessage = GetByPath<TString>(json, "message");
            UNIT_ASSERT_VALUES_EQUAL(resultMessage, "The specified queue doesn't exist");
        }

        Y_UNIT_TEST_F(TestGetQueueUrlWithConsumer, TFixture) {
            auto driver = MakeDriver(*this);
            const TString consumer = "user_consumer";
            const TString queueName = "ExampleQueueName";
            Y_ENSURE(CreateTopic(driver, queueName, consumer));
            const TString queueUrl = std::format("/v1/5//Root/{}/{}/{}/{}", queueName.size(), queueName.c_str(), consumer.size(), consumer.c_str());
            const TString requestQueueName = queueName + "@" + consumer;
            auto json = GetQueueUrl({
                {"QueueName", requestQueueName},
            });
            UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));

            const TString requestQueueNameWithWrongConsumer = queueName + "@" + "wrong_consumer";
            json = GetQueueUrl({
                {"QueueName", requestQueueNameWithWrongConsumer},
            }, 400);
            TString resultType = GetByPath<TString>(json, "__type");
            UNIT_ASSERT_VALUES_EQUAL(resultType, "AWS.SimpleQueueService.NonExistentQueue");
        }

        Y_UNIT_TEST_F(TestListQueues, TFixture) {
            NJson::TJsonMap json;
            json = ListQueues({});
            UNIT_ASSERT(json["QueueUrls"].GetArray().empty());

            const size_t numOfExampleQueues = 5;
            {
                auto driver = MakeDriver(*this);
                TVector<TString> queueUrls;
                for (size_t i = 0; i < numOfExampleQueues; ++i) {
                    Y_ENSURE(CreateTopic(driver, std::format("ExampleQueue-{}", i), "mlp-consumer"));
                }

                bool multiConsumerTopic = CreateTopic(driver, "AnotherQueue", NYdb::NTopic::TCreateTopicSettings()
                    .BeginAddConsumer("regular-consumer").EndAddConsumer()
                    .BeginAddSharedConsumer("ydb-sqs-consumer")
                        .KeepMessagesOrder(false)
                        .DefaultProcessingTimeout(TDuration::Seconds(20))
                    .EndAddConsumer()
                    .BeginAddSharedConsumer("ydb-sqs-consumer-2")
                        .KeepMessagesOrder(false)
                        .DefaultProcessingTimeout(TDuration::Seconds(20))
                    .EndAddConsumer());
                Y_ENSURE(multiConsumerTopic);

                bool regularTopic = CreateTopic(driver, "RegularTopic", NYdb::NTopic::TCreateTopicSettings()
                    .BeginAddConsumer("regular-consumer").EndAddConsumer());
                Y_ENSURE(regularTopic);
            }
            json = ListQueues({});
            UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), numOfExampleQueues + 2);

            json = ListQueues({{"QueueNamePrefix", ""}});
            UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), numOfExampleQueues + 2);

            json = ListQueues({{"QueueNamePrefix", "BadPrefix"}});
            UNIT_ASSERT(json["QueueUrls"].GetArray().empty());

            json = ListQueues({{"QueueNamePrefix", "Another"}});
            UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(GetPathFromFullQueueUrl(json["QueueUrls"][0]), "/v1/5//Root/12/AnotherQueue/16/ydb-sqs-consumer");
            UNIT_ASSERT_VALUES_EQUAL(GetPathFromFullQueueUrl(json["QueueUrls"][1]), "/v1/5//Root/12/AnotherQueue/18/ydb-sqs-consumer-2");

            json = ListQueues({{"QueueNamePrefix", "Ex"}});
            UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), numOfExampleQueues);
            UNIT_ASSERT_VALUES_EQUAL(GetPathFromFullQueueUrl(json["QueueUrls"][0]), "/v1/5//Root/14/ExampleQueue-0/12/mlp-consumer");

            {
                json = ListQueues({{"MaxResults", 1}});
                UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), 1);
                UNIT_ASSERT(json.Has("NextToken"));
                TString queueUrl1 = GetPathFromFullQueueUrl(json["QueueUrls"][0]);
                json = ListQueues({{"MaxResults", 1}, {"NextToken", json["NextToken"].GetString()}});
                UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), 1);
                UNIT_ASSERT(json.Has("NextToken"));
                TString queueUrl2 = GetPathFromFullQueueUrl(json["QueueUrls"][0]);
                UNIT_ASSERT_VALUES_UNEQUAL(queueUrl1, queueUrl2);
            }
        }

        struct TSqsTopicPaths {
            TString Database = "/Root";
            TString TopicName = "topic1";
            TString TopicPath = Database + "/" + TopicName;
            TString ConsumerName = "consumer";
            TString QueueUrl = std::format("/v1/{}/{}/{}/{}/{}/{}", Database.size(), Database.c_str(), TopicName.size(), TopicName.c_str(), ConsumerName.size(), ConsumerName.c_str());
        };


        Y_UNIT_TEST_F(TestSendMessage, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;

            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            auto json0 = SendMessage({
                {"QueueUrl", path.QueueUrl},
                {"MessageBody", "MessageBody-0"},
            });
            UNIT_ASSERT(!GetByPath<TString>(json0, "SequenceNumber").empty());
            UNIT_ASSERT(!GetByPath<TString>(json0, "MD5OfMessageBody").empty());
            UNIT_ASSERT(!GetByPath<TString>(json0, "MessageId").empty());

            auto json1 = SendMessage({
                {"QueueUrl", path.QueueUrl},
                {"MessageBody", ""},
                {"DelaySeconds", 900},
            });
            UNIT_ASSERT(!GetByPath<TString>(json1, "SequenceNumber").empty());
            UNIT_ASSERT(!GetByPath<TString>(json1, "MD5OfMessageBody").empty());
            UNIT_ASSERT(!GetByPath<TString>(json1, "MessageId").empty());
            constexpr TStringBuf emptyStringHash = "d41d8cd98f00b204e9800998ecf8427e";
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json1, "MD5OfMessageBody"), emptyStringHash);
            UNIT_ASSERT_VALUES_UNEQUAL(GetByPath<TString>(json0, "MessageId"), GetByPath<TString>(json1, "MessageId"));

            const auto messages = ReadMessagesSync(driver, path.TopicPath, false, 2, TDuration::Minutes(5));
            UNIT_ASSERT_VALUES_EQUAL(messages.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "MessageBody-0");
            UNIT_ASSERT_VALUES_EQUAL(messages[1].GetData(), "");
        }

        Y_UNIT_TEST_F(TestSendMessageBadQueueUrl, TFixture) {
            auto json0 = SendMessage({
                {"QueueUrl", ""},
                {"MessageBody", "MessageBody-0"}
            }, 400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json0, "__type"), "MissingParameter");

            auto json1 = SendMessage({
                {"QueueUrl", NON_EXISTING_QUEUE_URL},
                {"MessageBody", "MessageBody-0"}
            }, 400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json1, "__type"), "AWS.SimpleQueueService.NonExistentQueue");
        }

        Y_UNIT_TEST_F(TestSendMessageTooBig, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;

            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);
            auto json0 = SendMessage({
                {"QueueUrl", path.QueueUrl},
                {"MessageBody", TString(2_MB, 'x')},
            },  400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json0, "__type"), "InvalidParameterValue");
        }

        Y_UNIT_TEST_F(TestSendMessageBatch, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;

            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            auto json = SendMessageBatch({
                {"QueueUrl", path.QueueUrl},
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
                    NJson::TJsonMap{{"Id", "Id-2"}, {"MessageBody", TString(2_MB, 'a')}},
                    NJson::TJsonMap{{"Id", "Id-3"}, {"MessageBody", "MessageBody-3"}},

                }}
            });

            Cerr << (TStringBuilder() << "json = " << WriteJson(json, true, true) << '\n');
            UNIT_ASSERT_VALUES_EQUAL(json["Successful"].GetArray().size(), 3);
            auto succesful0 = json["Successful"][0];
            UNIT_ASSERT_VALUES_EQUAL(succesful0["Id"], "Id-0");
            UNIT_ASSERT(!GetByPath<TString>(succesful0, "MD5OfMessageAttributes").empty());
            UNIT_ASSERT(!GetByPath<TString>(succesful0, "MD5OfMessageBody").empty());
            UNIT_ASSERT(!GetByPath<TString>(succesful0, "MessageId").empty());

            UNIT_ASSERT_VALUES_EQUAL(json["Successful"][1]["Id"], "Id-1");
            UNIT_ASSERT_VALUES_EQUAL(json["Successful"][2]["Id"], "Id-3");

            UNIT_ASSERT_VALUES_EQUAL(json["Failed"].GetArray().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["Failed"][0]["Id"], "Id-2");


            const auto messages = ReadMessagesSync(driver, path.TopicPath, false, 3, TDuration::Minutes(5));
            UNIT_ASSERT_VALUES_EQUAL(messages.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(messages[0].GetData(), "MessageBody-0");
            UNIT_ASSERT_VALUES_EQUAL(messages[1].GetData(), "MessageBody-1");
            UNIT_ASSERT_VALUES_EQUAL(messages[2].GetData(), "MessageBody-3");
        }


        Y_UNIT_TEST_F(TestSendMessageBatchEmpty, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;

            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            auto json = SendMessageBatch({
                {"QueueUrl", path.QueueUrl},
                {"Entries", NJson::TJsonArray{
                }}
            }, 400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.EmptyBatchRequest");
        }

        Y_UNIT_TEST_F(TestSendMessageBatchLong, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;
            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            auto json = SendMessageBatch({
                {"QueueUrl", path.QueueUrl},
                {"Entries", NJson::TJsonArray{
                    NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "MessageBody-1"}, {"MessageGroupId", "MessageGroupId-1"}},
                    NJson::TJsonMap{{"Id", "Id-2"}, {"MessageBody", "MessageBody-2"}, {"MessageGroupId", "MessageGroupId-2"}},
                    NJson::TJsonMap{{"Id", "Id-3"}, {"MessageBody", "MessageBody-3"}, {"MessageGroupId", "MessageGroupId-3"}},
                    NJson::TJsonMap{{"Id", "Id-4"}, {"MessageBody", "MessageBody-4"}, {"MessageGroupId", "MessageGroupId-4"}},
                    NJson::TJsonMap{{"Id", "Id-5"}, {"MessageBody", "MessageBody-5"}, {"MessageGroupId", "MessageGroupId-5"}},
                    NJson::TJsonMap{{"Id", "Id-6"}, {"MessageBody", "MessageBody-6"}, {"MessageGroupId", "MessageGroupId-6"}},
                    NJson::TJsonMap{{"Id", "Id-7"}, {"MessageBody", "MessageBody-7"}, {"MessageGroupId", "MessageGroupId-7"}},
                    NJson::TJsonMap{{"Id", "Id-8"}, {"MessageBody", "MessageBody-8"}, {"MessageGroupId", "MessageGroupId-8"}},
                    NJson::TJsonMap{{"Id", "Id-9"}, {"MessageBody", "MessageBody-9"}, {"MessageGroupId", "MessageGroupId-9"}},
                    NJson::TJsonMap{{"Id", "Id-a"}, {"MessageBody", "MessageBody-a"}, {"MessageGroupId", "MessageGroupId-a"}},
                    NJson::TJsonMap{{"Id", "Id-b"}, {"MessageBody", "MessageBody-b"}, {"MessageGroupId", "MessageGroupId-b"}},
                }
            }}, 400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.TooManyEntriesInBatchRequest");
        }


        static constexpr std::initializer_list<std::pair<TStringBuf, TStringBuf>> CommonReceiveMessageAttributes{
            {"MD5OfMessageBody", "MD5OfBody"},
            {"MessageId", "MessageId"},
        };

        static void CompareCommonSendAndReceivedAttrubutes(const NJson::TJsonValue& jsonSend, const NJson::TJsonValue& jsonReceived, TStringBuf caseName = {}) {
            for (const auto& [keyS, keyR] : CommonReceiveMessageAttributes) {
                UNIT_ASSERT_VALUES_EQUAL_C(GetByPath<TString>(jsonSend, keyS), GetByPath<TString>(jsonReceived, keyR), LabeledOutput(keyS, caseName));
            }
        }

        Y_UNIT_TEST_F(TestReceiveMessageEmpty, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;
            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            auto jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 1}});
            UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"].GetArray().size(), 0);
        }

        Y_UNIT_TEST_F(TestReceiveMessageInvalidQueueUrl, TFixture) {
            auto jsonReceived = ReceiveMessage({{"QueueUrl", "/invalid/queue/url/"}, {"WaitTimeSeconds", 1}}, 400);
            TString resultType = GetByPath<TString>(jsonReceived, "__type");
        }

        Y_UNIT_TEST_F(TestReceiveMessageNonExistingQueue, TFixture) {
            auto jsonReceived = ReceiveMessage({{"QueueUrl", NON_EXISTING_QUEUE_URL}, {"WaitTimeSeconds", 1}}, 400);
            TString resultType = GetByPath<TString>(jsonReceived, "__type");
            UNIT_ASSERT_VALUES_EQUAL(resultType, "AWS.SimpleQueueService.NonExistentQueue");
        }

        Y_UNIT_TEST_F(TestReceiveMessageInvalidSize, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;
            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            for (int num : {-10, 0, 50, Max<int>(), Min<int>()}) {
                auto jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 1}, {"MaxNumberOfMessages", num}}, 400);
                TString resultType = GetByPath<TString>(jsonReceived, "__type");
                UNIT_ASSERT_VALUES_EQUAL_C(resultType, "InvalidParameterValue", LabeledOutput(num)) ;
            }
        }

         Y_UNIT_TEST_F(TestReceiveMessage, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;
            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            auto jsonSend = SendMessage({
                {"QueueUrl", path.QueueUrl},
                {"MessageBody", "MessageBody-0"},
            });

            auto jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});
            UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"].GetArraySafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"][0]["Body"], "MessageBody-0");
            CompareCommonSendAndReceivedAttrubutes(jsonSend, jsonReceived["Messages"][0]);
            // Second call during visibility timeout
            jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 1}});
            UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"].GetArray().size(), 0);
        }

        Y_UNIT_TEST_F(TestReceiveMessageReturnToQueue, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;
            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            auto jsonSend = SendMessage({
                {"QueueUrl", path.QueueUrl},
                {"MessageBody", "MessageBody-0"},
            });

            auto jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}, {"VisibilityTimeout", 1}});
            UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"].GetArraySafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"][0]["Body"], "MessageBody-0");
            CompareCommonSendAndReceivedAttrubutes(jsonSend, jsonReceived["Messages"][0]);

            do {
                Sleep(TDuration::MilliSeconds(350));
                // Second call after visibility timeout
                jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});
                Cerr << (TStringBuilder() << "jsonReceived = " << WriteJson(jsonReceived, true, true) << '\n');
            } while (jsonReceived["Messages"].GetArray().size() == 0);

            UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"].GetArraySafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"][0]["Body"], "MessageBody-0");
            CompareCommonSendAndReceivedAttrubutes(jsonSend, jsonReceived["Messages"][0]);
        }

        Y_UNIT_TEST_F(TestReceiveMessageGroup, TFixture) {
            auto driver = MakeDriver(*this);
            const TSqsTopicPaths path;
            bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
            UNIT_ASSERT(a);

            THashSet<TString> messages;
            const int n = 300;

            for (int i = 0; i < n; i += 10) {
                NJson::TJsonMap req{
                    {"QueueUrl", path.QueueUrl},
                    {"Entries", NJson::TJsonArray{}},
                };
                for (int j = i; j < i + 10; j++) {
                    TString body = TStringBuilder() << "MessageBody-" << j;
                    req["Entries"].AppendValue(
                        NJson::TJsonMap{
                            {"Id", std::format("Id-{}", j)},
                            {"MessageBody", body},
                        }
                    );
                    messages.insert(body);
                }
                auto json = SendMessageBatch(req);
            };
            UNIT_ASSERT_VALUES_EQUAL(messages.size(), n);
            TMap<size_t, size_t> batchSizesHistogram;
            THashSet<TString> receipts;
            size_t iteration = 0;
            while (!messages.empty()) {
                ++iteration;
                size_t maxNumberOfMessages = 1 + iteration % 9;
                auto jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 5}, {"MaxNumberOfMessages", maxNumberOfMessages}});
                for (const auto& message : jsonReceived["Messages"].GetArray()) {
                    const TString body = message["Body"].GetString();

                    size_t cnt = messages.erase(body);
                    UNIT_ASSERT_VALUES_EQUAL_C(cnt, 1, LabeledOutput(body, maxNumberOfMessages, iteration, messages.size()));

                    auto [_, unique] = receipts.insert(message["ReceiptHandle"].GetStringSafe());
                    UNIT_ASSERT_C(unique,  LabeledOutput(body, maxNumberOfMessages, iteration, messages.size()));
                }
                size_t batchSize = jsonReceived["Messages"].GetArray().size();
                batchSizesHistogram[batchSize]++;
                UNIT_ASSERT_LE_C(batchSize, maxNumberOfMessages, LabeledOutput(maxNumberOfMessages, iteration, messages.size()));
            }

            Cerr << "batchSizesHistogram (" << batchSizesHistogram.size() << "):\n";
            for (const auto& [size, cnt] : batchSizesHistogram) {
                Cerr << "    " << size << ": " << cnt << "\n";
            }

            UNIT_ASSERT(!batchSizesHistogram.empty());
            UNIT_ASSERT(batchSizesHistogram.size() > 1 || batchSizesHistogram.begin()->first > 1);

            size_t total = 0;
            for (const auto& [size, cnt] : batchSizesHistogram) {
                total += size * cnt ;
            }
            UNIT_ASSERT_VALUES_EQUAL(total, n);
        }


    Y_UNIT_TEST_F(TestDeleteMessageInvalid, TFixture) {

        DeleteMessage({}, 400);
        DeleteMessage({{"QueueUrl", "wrong-queue-url"}}, 400);
        DeleteMessage({{"QueueUrl", "/v1/5//Root/16/ExampleQueueName/13/user_consumer"}}, 400);

        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        DeleteMessage({{"QueueUrl", path.QueueUrl}}, 400);
        DeleteMessage({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", "unknown-receipt-handle"}}, 400);
        DeleteMessage({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", ""}}, 400);
        DeleteMessage({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", 123}}, 400);
    }

    Y_UNIT_TEST_F(TestDeleteMessage, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);
        TString body = "MessageBody-0";
        SendMessage({{"QueueUrl", path.QueueUrl}, {"MessageBody", body}});
        auto json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});

        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.empty());

        DeleteMessage({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", receiptHandle}});
    }

    Y_UNIT_TEST_F(TestDeleteMessageIdempotence, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);
        TString body = "MessageBody-0";
        SendMessage({{"QueueUrl", path.QueueUrl}, {"MessageBody", body}});
        auto json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}});

        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.empty());

        DeleteMessage({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", receiptHandle}});
        if (!"X-Fail") { // TODO MLP commit idempotence
            DeleteMessage({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", receiptHandle}});
        }
    }

    Y_UNIT_TEST_F(TestDeleteMessageBatch, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        auto json = SendMessageBatch({
            {"QueueUrl", path.QueueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "MessageBody-1"}, {"MessageGroupId", "MessageGroupId-1"}},
                NJson::TJsonMap{{"Id", "Id-2"}, {"MessageBody", "MessageBody-2"}},
                NJson::TJsonMap{{"Id", "Id-3"}, {"MessageBody", "MessageBody-3"}},
            }}
        });
        const size_t n = 3;

        THashMap<TString, TString> receiptHandles;
        while (receiptHandles.size() < n) {
            auto jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 5}, {"MaxNumberOfMessages", 10}});
            for (const auto& message : jsonReceived["Messages"].GetArray()) {
                const TString body = message["Body"].GetString();
                const TString receiptHandle = message["ReceiptHandle"].GetString();
                UNIT_ASSERT(receiptHandles.try_emplace(body, receiptHandle).second);
            }
        }

        NJson::TJsonArray entries{
            NJson::TJsonMap{{"Id", "delete-invalid"}, {"ReceiptHandle", "invalid"}},
        };

        THashSet<TString> ids;
        for (const auto& [body, receiptHandle] : receiptHandles) {
            TString id = "delete-id-" + ToString(ids.size());
            entries.AppendValue(NJson::TJsonMap{{"Id", id}, {"ReceiptHandle", receiptHandle}});
            ids.insert(id);
        }

        auto deleteJson = DeleteMessageBatch({
            {"QueueUrl", path.QueueUrl},
            {"Entries", entries},
        });

        UNIT_ASSERT_VALUES_EQUAL(deleteJson["Successful"].GetArray().size(), n);
        for (const auto& message : deleteJson["Successful"].GetArray()) {
            TString id = message["Id"].GetString();
            UNIT_ASSERT_C(ids.contains(id), LabeledOutput(id, ids.size()));
            ids.erase(id);
        }

        UNIT_ASSERT_VALUES_EQUAL(deleteJson["Failed"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(deleteJson["Failed"][0]["Id"], "delete-invalid");
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityInvalid, TFixture) {
        // Missing parameters
        ChangeMessageVisibility({}, 400);
        ChangeMessageVisibility({{"QueueUrl", "wrong-queue-url"}}, 400);
        ChangeMessageVisibility({{"QueueUrl", "/v1/5//Root/16/ExampleQueueName/13/user_consumer"}}, 400);

        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        // Missing ReceiptHandle
        ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}}, 400);
        // Missing VisibilityTimeout
        ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", "some-receipt"}}, 400);
        // Invalid ReceiptHandle
        ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", "unknown-receipt-handle"}, {"VisibilityTimeout", 30}}, 400);
        ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", ""}, {"VisibilityTimeout", 30}}, 400);
        ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", 123}, {"VisibilityTimeout", 30}}, 400);
        // Invalid VisibilityTimeout (negative)
        ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", "some-receipt"}, {"VisibilityTimeout", -1}}, 400);
        // Invalid VisibilityTimeout (too large, > 43200 seconds = 12 hours)
        ChangeMessageVisibility({{"QueueUrl", path.QueueUrl}, {"ReceiptHandle", "some-receipt"}, {"VisibilityTimeout", 43201}}, 400);
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityBasic, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        TString body = "MessageBody-0";
        SendMessage({{"QueueUrl", path.QueueUrl}, {"MessageBody", body}});

        // Receive message with short visibility timeout
        auto json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}, {"VisibilityTimeout", 60}});
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.empty());

        // Extend visibility timeout
        auto changeJson = ChangeMessageVisibility({
            {"QueueUrl", path.QueueUrl},
            {"ReceiptHandle", receiptHandle},
            {"VisibilityTimeout", 120}
        });
        // Successful response should be empty (no error)
        UNIT_ASSERT(!changeJson.Has("__type"));
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityZeroTimeout, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        TString body = "MessageBody-0";
        SendMessage({{"QueueUrl", path.QueueUrl}, {"MessageBody", body}});

        // Receive message with visibility timeout
        auto json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}, {"VisibilityTimeout", 60}});
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.empty());

        // Set visibility timeout to 0 (make message immediately available)
        ChangeMessageVisibility({
            {"QueueUrl", path.QueueUrl},
            {"ReceiptHandle", receiptHandle},
            {"VisibilityTimeout", 0}
        });

        // Message should be immediately available again
        do {
            Sleep(TDuration::MilliSeconds(350));
            json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 5}});
        } while (json["Messages"].GetArray().size() == 0);

        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"][0]["Body"], body);
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityExtendTimeout, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        TString body = "MessageBody-0";
        SendMessage({{"QueueUrl", path.QueueUrl}, {"MessageBody", body}});

        // Receive message with short visibility timeout
        auto json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 20}, {"VisibilityTimeout", 2}});
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 1);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.empty());

        // Extend visibility timeout before it expires
        ChangeMessageVisibility({
            {"QueueUrl", path.QueueUrl},
            {"ReceiptHandle", receiptHandle},
            {"VisibilityTimeout", 60}
        });

        // Wait for original timeout to pass
        Sleep(TDuration::Seconds(3));

        // Message should still be invisible (extended timeout)
        json = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 1}});
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 0);
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityBatchInvalid, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        // Empty batch
        auto json = ChangeMessageVisibilityBatch({
            {"QueueUrl", path.QueueUrl},
            {"Entries", NJson::TJsonArray{}}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.EmptyBatchRequest");

        // Too many entries (> 10)
        NJson::TJsonArray tooManyEntries;
        for (int i = 0; i < 11; ++i) {
            tooManyEntries.AppendValue(NJson::TJsonMap{
                {"Id", std::format("Id-{}", i)},
                {"ReceiptHandle", std::format("receipt-{}", i)},
                {"VisibilityTimeout", 30}
            });
        }
        json = ChangeMessageVisibilityBatch({
            {"QueueUrl", path.QueueUrl},
            {"Entries", tooManyEntries}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.TooManyEntriesInBatchRequest");
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityBatch, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        // Send multiple messages
        auto json = SendMessageBatch({
            {"QueueUrl", path.QueueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "MessageBody-1"}},
                NJson::TJsonMap{{"Id", "Id-2"}, {"MessageBody", "MessageBody-2"}},
                NJson::TJsonMap{{"Id", "Id-3"}, {"MessageBody", "MessageBody-3"}},
            }}
        });
        const size_t n = 3;

        // Receive all messages
        THashMap<TString, TString> receiptHandles;
        while (receiptHandles.size() < n) {
            auto jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 5}, {"MaxNumberOfMessages", 10}, {"VisibilityTimeout", 60}});
            for (const auto& message : jsonReceived["Messages"].GetArray()) {
                const TString body = message["Body"].GetString();
                const TString receiptHandle = message["ReceiptHandle"].GetString();
                UNIT_ASSERT(receiptHandles.try_emplace(body, receiptHandle).second);
            }
        }

        // Build batch request with one invalid entry
        NJson::TJsonArray entries{
            NJson::TJsonMap{{"Id", "change-invalid"}, {"ReceiptHandle", "invalid"}, {"VisibilityTimeout", 30}},
        };

        THashSet<TString> ids;
        for (const auto& [body, receiptHandle] : receiptHandles) {
            TString id = "change-id-" + ToString(ids.size());
            entries.AppendValue(NJson::TJsonMap{
                {"Id", id},
                {"ReceiptHandle", receiptHandle},
                {"VisibilityTimeout", 120}
            });
            ids.insert(id);
        }

        auto changeJson = ChangeMessageVisibilityBatch({
            {"QueueUrl", path.QueueUrl},
            {"Entries", entries},
        });

        Cerr << (TStringBuilder() << "changeJson = " << WriteJson(changeJson, true, true) << '\n');

        UNIT_ASSERT_VALUES_EQUAL(changeJson["Successful"].GetArray().size(), n);
        for (const auto& message : changeJson["Successful"].GetArray()) {
            TString id = message["Id"].GetString();
            UNIT_ASSERT_C(ids.contains(id), LabeledOutput(id, ids.size()));
            ids.erase(id);
        }

        UNIT_ASSERT_VALUES_EQUAL(changeJson["Failed"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(changeJson["Failed"][0]["Id"], "change-invalid");
    }

    Y_UNIT_TEST_F(TestChangeMessageVisibilityBatchMixedTimeouts, TFixture) {
        auto driver = MakeDriver(*this);
        const TSqsTopicPaths path;
        bool a = CreateTopic(driver, path.TopicName, path.ConsumerName);
        UNIT_ASSERT(a);

        // Send multiple messages
        SendMessageBatch({
            {"QueueUrl", path.QueueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "Id-1"}, {"MessageBody", "MessageBody-1"}},
                NJson::TJsonMap{{"Id", "Id-2"}, {"MessageBody", "MessageBody-2"}},
            }}
        });

        // Receive all messages
        TVector<std::pair<TString, TString>> messages; // body -> receiptHandle
        while (messages.size() < 2) {
            auto jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 5}, {"MaxNumberOfMessages", 10}, {"VisibilityTimeout", 60}});
            for (const auto& message : jsonReceived["Messages"].GetArray()) {
                messages.emplace_back(message["Body"].GetString(), message["ReceiptHandle"].GetString());
            }
        }

        // Change visibility with different timeouts for each message
        auto changeJson = ChangeMessageVisibilityBatch({
            {"QueueUrl", path.QueueUrl},
            {"Entries", NJson::TJsonArray{
                NJson::TJsonMap{{"Id", "change-1"}, {"ReceiptHandle", messages[0].second}, {"VisibilityTimeout", 0}},  // Make immediately visible
                NJson::TJsonMap{{"Id", "change-2"}, {"ReceiptHandle", messages[1].second}, {"VisibilityTimeout", 300}}, // Extend timeout
            }},
        });

        UNIT_ASSERT_VALUES_EQUAL(changeJson["Successful"].GetArray().size(), 2);

        // First message should be available again (timeout = 0)
        NJson::TJsonValue jsonReceived;
        do {
            Sleep(TDuration::MilliSeconds(350));
            jsonReceived = ReceiveMessage({{"QueueUrl", path.QueueUrl}, {"WaitTimeSeconds", 5}});
        } while (jsonReceived["Messages"].GetArray().size() == 0);

        UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"].GetArray().size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(jsonReceived["Messages"][0]["Body"], messages[0].first);
    }


    struct TGetQueueAttributesParams {
        bool Fifo = false;
        bool Dlq = false;
        int SharedConsumers = 1;
        TDuration RetentionPeriod = TDuration::Zero();
    };

    void TestGetQueueAttributesImpl(TFixture& fixture, const TGetQueueAttributesParams& params) {
        const TString database = "/Root";
        const TString topicName = "ExampleQueueName";
        const TString queueName = TString::Join(topicName, params.Fifo ? ".fifo" : "");
        auto driver = MakeDriver(fixture);
        auto consumerName = [](int i) { return std::format("ydb-sqs-consumer-{}", i); };
        auto queueUrlForConsumer = [&](int i) { return std::format("/v1/{}/{}/{}/{}/{}/{}", database.size(), database.c_str(), topicName.size(), topicName.c_str(), consumerName(i).size(), consumerName(i).c_str()); };
        const TDuration retentionPeriod = TDuration::Hours(10);
        {
            NYdb::NTopic::TCreateTopicSettings settings;
            settings.RetentionPeriod(retentionPeriod);
            settings.BeginAddConsumer("regular-consumer").EndAddConsumer();
            for (int i = 0; i < params.SharedConsumers; ++i) {
                auto& consumer = settings.BeginAddSharedConsumer(consumerName(i));
                consumer.KeepMessagesOrder(params.Fifo);
                consumer.DefaultProcessingTimeout(TDuration::Seconds(25));
                if (params.Dlq) {
                    auto&& dlqSettings = consumer.BeginDeadLetterPolicy();
                    dlqSettings.Enable();
                    dlqSettings.BeginCondition().MaxProcessingAttempts(117).EndCondition();
                    dlqSettings.MoveAction("DeadLetterQueue");
                    dlqSettings.EndDeadLetterPolicy();
                }
                if (params.RetentionPeriod != TDuration::Zero()) {
                    consumer.AvailabilityPeriod(params.RetentionPeriod);
                }
                consumer.EndAddConsumer();

            }
            Y_ENSURE(CreateTopic(driver, topicName, settings));
        }

        {
            const TString wrongConsumerQueueUrl = queueUrlForConsumer(params.SharedConsumers + 1);
            auto json = fixture.GetQueueAttributes({
                {"QueueUrl", wrongConsumerQueueUrl},
                {"AttributeNames", NJson::TJsonArray{"All"}},
            }, 400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.NonExistentQueue");
        }
        if (params.SharedConsumers == 0) {
            return;
        }

        const TString resultQueueUrl = queueUrlForConsumer(0);

        fixture.GetQueueAttributes({{"wrong-field", "some-value"}}, 400);
        fixture.GetQueueAttributes({{"QueueUrl", "invalid-url"}}, 400);

        auto checkDlq = [&params](const NJson::TJsonValue& json, bool expectRedrivePolicyIfHasDlq = true, std::source_location loc = std::source_location::current()) {
            const TString subcase = std::format("line={}", loc.line());
            if (expectRedrivePolicyIfHasDlq && params.Dlq) {
                UNIT_ASSERT_C(json["Attributes"].Has("RedrivePolicy"), subcase);
                TString redrivePolicyJson = json["Attributes"]["RedrivePolicy"].GetString();
                NJson::TJsonValue redrivePolicy;
                UNIT_ASSERT_C(ReadJsonTree(redrivePolicyJson, &redrivePolicy), subcase);
                UNIT_ASSERT_VALUES_EQUAL_C(redrivePolicy["maxReceiveCount"].GetInteger(), 117, subcase);
                UNIT_ASSERT_VALUES_EQUAL_C(redrivePolicy["deadLetterTargetArn"].GetString(), "yrn:yc:ymq:ru-central1::/v1/5//Root/15/DeadLetterQueue/18/ydb-sqs-consumer-0", subcase);
            } else {
                TString redrivePolicyJson = json["Attributes"]["RedrivePolicy"].GetStringSafe("null");
                NJson::TJsonValue redrivePolicy;
                UNIT_ASSERT_C(ReadJsonTree(redrivePolicyJson, &redrivePolicy), subcase);
                UNIT_ASSERT_C(redrivePolicy.IsNull() || redrivePolicy.IsMap() && redrivePolicy.GetMap().empty(), subcase);
            }
        };
        auto checkFifo = [&params](const NJson::TJsonValue& json, bool expectFifoAttr = true, std::source_location loc = std::source_location::current()) {
            const TString subcase = std::format("line={}", loc.line());
            if (!expectFifoAttr) {
                UNIT_ASSERT_C(!json["Attributes"]["FifoQueue"].IsDefined(), subcase);
                return;
            } else if (params.Fifo) {
                UNIT_ASSERT_VALUES_EQUAL_C(json["Attributes"]["FifoQueue"].GetStringSafe(), "true", subcase);
            } else {
                UNIT_ASSERT_VALUES_EQUAL_C(json["Attributes"]["FifoQueue"].GetStringSafe("false"), "false", subcase);
            }
        };

        {
            auto json = fixture.GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
            });
            UNIT_ASSERT(json.GetMapSafe().empty());
        }

        {
            auto json = fixture.GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{}}
            });
            UNIT_ASSERT(json.GetMapSafe().empty());
         }

        {
            auto json = fixture.GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{"All"}}
            });
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["DelaySeconds"], "0");
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["VisibilityTimeout"], "25");
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["MessageRetentionPeriod"], ToString(Max(retentionPeriod, params.RetentionPeriod).Seconds()));
            UNIT_ASSERT_GT(json["Attributes"].GetMapSafe().size(), 5);
            checkFifo(json);
            checkDlq(json);
        }

        {
            auto json = fixture.GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{"All", "VisibilityTimeout"}}
            });
            UNIT_ASSERT_GT(json["Attributes"].GetMapSafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["VisibilityTimeout"], "25");
            checkFifo(json);
            checkDlq(json);
        }

        {
            auto json = fixture.GetQueueAttributes({
                {"QueueUrl", resultQueueUrl},
                {"AttributeNames", NJson::TJsonArray{"VisibilityTimeout"}}
            });
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"].GetMapSafe().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["VisibilityTimeout"], "25");
            checkFifo(json, false);
            checkDlq(json, false);
        }

        fixture.GetQueueAttributes({
            {"QueueUrl", resultQueueUrl},
            {"AttributeNames", NJson::TJsonArray{"UnknownAttribute"}}
        }, 400);

        fixture.GetQueueAttributes({
            {"QueueUrl", resultQueueUrl},
            {"AttributeNames", NJson::TJsonArray{"All", "UnknownAttribute"}}
        }, 400);

        fixture.GetQueueAttributes({
            {"QueueUrl", resultQueueUrl},
            {"AttributeNames", NJson::TJsonArray{"DelaySeconds", "UnknownAttribute"}}
        }, 400);

        {
            auto json = fixture.GetQueueAttributes({
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
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["VisibilityTimeout"], "25");
            UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["MessageRetentionPeriod"], ToString(Max(retentionPeriod, params.RetentionPeriod).Seconds()));
            UNIT_ASSERT_GT(json["Attributes"].GetMapSafe().size(), 5);
            checkFifo(json);
            checkDlq(json);
        }
    }

    Y_UNIT_TEST_F(TestGetQueueAttributesStd, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = false, .Dlq = false, .SharedConsumers = 1});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesFifo, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = true, .Dlq = false, .SharedConsumers = 1});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesStdDlq, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = false, .Dlq = true, .SharedConsumers = 1});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesFifoDlq, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = true, .Dlq = true, .SharedConsumers = 1});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesStd3Consumers, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = false, .Dlq = false, .SharedConsumers = 3});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesFifo3Consumers, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = true, .Dlq = false, .SharedConsumers = 3});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesStdDlq3Consumers, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = false, .Dlq = true, .SharedConsumers = 3});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesFifoDlq3Consumers, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = true, .Dlq = true, .SharedConsumers = 3});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesStd0Consumers, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = false, .Dlq = false, .SharedConsumers = 0});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesFifo0Consumers, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = true, .Dlq = false, .SharedConsumers = 0});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesStdDlq0Consumers, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = false, .Dlq = true, .SharedConsumers = 0});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesFifoDlq0Consumers, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = true, .Dlq = true, .SharedConsumers = 0});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesStdWithConsumersRetentionExtended, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = false, .SharedConsumers = 1, .RetentionPeriod = TDuration::Hours(50)});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesFifoWithConsumersRetentionExtended, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = true, .SharedConsumers = 1, .RetentionPeriod = TDuration::Hours(50)});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesStdWithConsumersRetentionShrinked, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = false, .SharedConsumers = 1, .RetentionPeriod = TDuration::Hours(1)});
    }
    Y_UNIT_TEST_F(TestGetQueueAttributesFifoWithConsumersRetentionShrinked, TFixture) {
        TestGetQueueAttributesImpl(*this, TGetQueueAttributesParams{.Fifo = true, .SharedConsumers = 1, .RetentionPeriod = TDuration::Hours(1)});
    }

    Y_UNIT_TEST_F(TestCreateQueue, TFixture) {
        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        UNIT_ASSERT(!GetByPath<TString>(json, "QueueUrl").empty());
        TString queueUrl = GetPathFromQueueUrlMap(json);
        UNIT_ASSERT(queueUrl.Contains("ExampleQueueName"));
        UNIT_ASSERT(queueUrl.Contains("ydb-sqs-consumer"));
    }

    Y_UNIT_TEST_F(TestCreateQueueWithCustomConsumer, TFixture) {
        auto json = CreateQueue({{"QueueName", "ExampleQueueName@custom-consumer"}});
        UNIT_ASSERT(!GetByPath<TString>(json, "QueueUrl").empty());
        TString queueUrl = GetPathFromQueueUrlMap(json);
        UNIT_ASSERT(queueUrl.Contains("ExampleQueueName"));
        UNIT_ASSERT(queueUrl.Contains("custom-consumer"));
    }

    Y_UNIT_TEST_F(TestCreateQueueWithSameNameAndSameParams, TFixture) {
        auto req = NJson::TJsonMap{{"QueueName", "ExampleQueueName"}};
        auto json1 = CreateQueue(req);
        auto json2 = CreateQueue(req);
        UNIT_ASSERT_VALUES_EQUAL(GetPathFromQueueUrlMap(json1), GetPathFromQueueUrlMap(json2));
    }

    Y_UNIT_TEST_F(TestCreateQueueWithSameNameAndDifferentParams, TFixture) {
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

    Y_UNIT_TEST_F(TestCreateQueueWithBadQueueName, TFixture) {
        auto json = CreateQueue({
            {"QueueName", "B/d_queue_name"},
            {"Attributes", NJson::TJsonMap{{"MessageRetentionPeriod", "60"}}}
        }, 400);
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "InvalidParameterValue");
    }

    Y_UNIT_TEST_F(TestCreateQueueWithEmptyName, TFixture) {
        auto json = CreateQueue({}, 400);
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "MissingParameter");
    }

    Y_UNIT_TEST_F(TestCreateQueueFifo, TFixture) {
        auto json = CreateQueue({
            {"QueueName", "ExampleQueue.fifo"},
            {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}
        });
        TString queueUrl = GetPathFromQueueUrlMap(json);
        UNIT_ASSERT(queueUrl.Contains("ExampleQueue.fifo"));
    }

    Y_UNIT_TEST_F(TestCreateQueueWithAttributes, TFixture) {
        auto json = CreateQueue({
            {"QueueName", "ExampleQueueName"},
            {"Attributes", NJson::TJsonMap{
                {"VisibilityTimeout", "30"},
                {"MessageRetentionPeriod", "3600"},
                {"MaximumMessageSize", "262144"}
            }}
        });
        UNIT_ASSERT(!GetByPath<TString>(json, "QueueUrl").empty());
    }

    Y_UNIT_TEST_F(TestCreateQueueExistingTopicWithSharedConsumer, TFixture) {
        // Create topic with shared consumer via Topic API
        auto driver = MakeDriver(*this);
        const TString topicName = "ExistingTopic";
        const TString consumerName = "ydb-sqs-consumer";

        Y_ENSURE(CreateTopic(driver, topicName, NYdb::NTopic::TCreateTopicSettings()
            .BeginAddSharedConsumer(consumerName)
                .KeepMessagesOrder(false)
                .DefaultProcessingTimeout(TDuration::Seconds(30))
            .EndAddConsumer()));

        // Try to create queue with same topic and consumer - should succeed
        auto json = CreateQueue({{"QueueName", topicName}});
        UNIT_ASSERT(!GetByPath<TString>(json, "QueueUrl").empty());
        TString queueUrl = GetPathFromQueueUrlMap(json);
        UNIT_ASSERT(queueUrl.Contains(topicName));
        UNIT_ASSERT(queueUrl.Contains(consumerName));
    }

    Y_UNIT_TEST_F(TestCreateQueueExistingTopicWithStreamingConsumer, TFixture) {
        // Create topic with regular (streaming) consumer via Topic API
        auto driver = MakeDriver(*this);
        const TString topicName = "ExistingTopicStreaming";
        const TString consumerName = "regular-consumer";

        Y_ENSURE(CreateTopic(driver, topicName, NYdb::NTopic::TCreateTopicSettings()
            .BeginAddConsumer(consumerName)  // Regular consumer, not shared
            .EndAddConsumer()));

        // Try to create queue with same topic and consumer - should fail
        auto json = CreateQueue({{"QueueName", topicName + "@" + consumerName}}, 400);
        TString resultType = GetByPath<TString>(json, "__type");
        UNIT_ASSERT_VALUES_EQUAL(resultType, "InvalidParameterValue");
    }

    Y_UNIT_TEST_F(TestCreateQueueExistingTopicNoConsumer, TFixture) {
        auto driver = MakeDriver(*this);
        const TString topicName = "ExistingTopicNoConsumer";
        const TString consumerName = "ydb-sqs-consumer";

        Y_ENSURE(CreateTopic(driver, topicName, NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(TDuration::Hours(24))
            .BeginAddConsumer("other-consumer")
            .EndAddConsumer()));

        auto json = CreateQueue({{"QueueName", topicName}});
        UNIT_ASSERT(!GetByPath<TString>(json, "QueueUrl").empty());
        TString queueUrl = GetPathFromQueueUrlMap(json);
        UNIT_ASSERT(queueUrl.Contains(topicName));
        UNIT_ASSERT(queueUrl.Contains(consumerName));
    }

    Y_UNIT_TEST_F(TestCreateQueueExistingTopicNoConsumerWithExtendedRetention, TFixture) {
        auto driver = MakeDriver(*this);
        const TString topicName = "ExistingTopicNoConsumerRetention";
        const TString consumerName = "ydb-sqs-consumer";
        const TDuration topicRetention = TDuration::Hours(24);
        const TDuration queueRetention = TDuration::Hours(48);

        Y_ENSURE(CreateTopic(driver, topicName, NYdb::NTopic::TCreateTopicSettings()
            .RetentionPeriod(topicRetention)
            .BeginAddConsumer("other-consumer")
            .EndAddConsumer()));

        auto json = CreateQueue({
            {"QueueName", topicName},
            {"Attributes", NJson::TJsonMap{{"MessageRetentionPeriod", ToString(queueRetention.Seconds())}}}
        });
        UNIT_ASSERT(!GetByPath<TString>(json, "QueueUrl").empty());
        TString queueUrl = GetPathFromQueueUrlMap(json);
        UNIT_ASSERT(queueUrl.Contains(topicName));
        UNIT_ASSERT(queueUrl.Contains(consumerName));

        auto attrJson = GetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"AttributeNames", NJson::TJsonArray{"MessageRetentionPeriod"}}
        });
        UNIT_ASSERT_VALUES_EQUAL(attrJson["Attributes"]["MessageRetentionPeriod"], ToString(queueRetention.Seconds()));
    }

    Y_UNIT_TEST_F(TestDeleteQueueInvalid, TFixture) {
        auto json = DeleteQueue({{"QueueUrl", "InvalidExistentQueue"}}, 400);
    }

    Y_UNIT_TEST_F(TestDeleteQueueNonExisting, TFixture) {
        auto json = DeleteQueue({{"QueueUrl", NON_EXISTING_QUEUE_URL}}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.NonExistentQueue");
    }

    Y_UNIT_TEST_F(TestDeleteQueue, TFixture) {
        auto json = CreateQueue({{"QueueName", "ExampleQueueName"}});
        TString queueUrl = GetByPath<TString>(json, "QueueUrl");

        DeleteQueue({{"QueueUrl", queueUrl}});

        auto getQueueUrlRequest = CreateSqsGetQueueUrlRequest();
        for (const TInstant deadline = TDuration::Seconds(60).ToDeadLine(); TInstant::Now() <= deadline; ) {
            auto res = SendHttpRequest("/Root", "AmazonSQS.GetQueueUrl", getQueueUrlRequest, FormAuthorizationStr("ru-central1"));
            if (res.HttpCode == 200) {
                // The queue should be deleted within 60 seconds.
                Sleep(TDuration::MilliSeconds(250));
            } else {
                UNIT_ASSERT_VALUES_EQUAL(res.HttpCode, 400);
                UNIT_ASSERT(NJson::ReadJsonTree(res.Body, &json));
                UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.NonExistentQueue");
                return;
            }
        }
        UNIT_FAIL("Queue was not deleted");
    }

    Y_UNIT_TEST_F(TestDeleteQueueMultiConsumer, TFixture) {
        const int nConsumers = 10;
        auto driver = MakeDriver(*this);

        const TString queueName = "ExampleQueueName";
        auto consumerName = [](int i) { return "user_consumer" + ToString(i); };
        auto queueUrl = [&](int i) {
            return std::format("/v1/5//Root/{}/{}/{}/{}", queueName.size(), queueName.c_str(), consumerName(i).size(), consumerName(i).c_str());
        };
        {
            NYdb::NTopic::TCreateTopicSettings settings;
            for (int i = 0; i < nConsumers; ++i) {
                settings.BeginAddSharedConsumer(consumerName(i)).KeepMessagesOrder(false).DefaultProcessingTimeout(TDuration::Seconds(20)).EndAddConsumer();
            }
            Y_ENSURE(CreateTopic(driver, queueName, settings));
        }
        auto client = TTopicClient(driver);
        for (int consumerRemains = nConsumers; consumerRemains >= 0; --consumerRemains) {
            auto desc = client.DescribeTopic(queueName, NYdb::NTopic::TDescribeTopicSettings{}.IncludeLocation(true)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(desc.IsSuccess(), consumerRemains > 0, consumerRemains);
            auto description = desc.GetTopicDescription();
            desc.Out(Cerr);
            for (const auto& c : description.GetConsumers()) {
                Cerr << c.GetConsumerName() << Endl;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(consumerRemains, description.GetConsumers().size(), consumerRemains);

            for (int i = 0; i < nConsumers; ++i) {
                const TString requestQueueName = queueName + "@" + consumerName(i);
                const int code = (i < consumerRemains) ? 200 : 400;
                auto json = GetQueueUrl({{"QueueName", requestQueueName},}, code);
            }

            if (consumerRemains > 0) {
                DeleteQueue({{"QueueUrl", queueUrl(consumerRemains - 1)}});
            }
        }
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesBasic, TFixture) {
        auto json = CreateQueue({
            {"QueueName", "TestSetAttrsQueue"},
            {"Attributes", NJson::TJsonMap{{"VisibilityTimeout", "30"}}}
        });
        TString queueUrl = GetPathFromQueueUrlMap(json);

        SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"VisibilityTimeout", "60"}}}
        });

        auto attrJson = GetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"AttributeNames", NJson::TJsonArray{"VisibilityTimeout"}}
        });
        UNIT_ASSERT_VALUES_EQUAL(attrJson["Attributes"]["VisibilityTimeout"], "60");
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesMultiple, TFixture) {
        auto json = CreateQueue({
            {"QueueName", "TestSetAttrsMultiQueue"},
            {"Attributes", NJson::TJsonMap{
                {"VisibilityTimeout", "30"},
                {"DelaySeconds", "0"},
                {"ReceiveMessageWaitTimeSeconds", "0"}
            }}
        });
        TString queueUrl = GetPathFromQueueUrlMap(json);

        SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{
                {"VisibilityTimeout", "120"},
                {"DelaySeconds", "5"},
                {"ReceiveMessageWaitTimeSeconds", "10"}
            }}
        });

        auto attrJson = GetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"AttributeNames", NJson::TJsonArray{"All"}}
        });
        UNIT_ASSERT_VALUES_EQUAL(attrJson["Attributes"]["VisibilityTimeout"], "120");
        UNIT_ASSERT_VALUES_EQUAL(attrJson["Attributes"]["DelaySeconds"], "5");
        UNIT_ASSERT_VALUES_EQUAL(attrJson["Attributes"]["ReceiveMessageWaitTimeSeconds"], "10");
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesFifoImmutable, TFixture) {
        auto json = CreateQueue({
            {"QueueName", "TestFifoImmutable.fifo"},
            {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}
        });
        TString queueUrl = GetPathFromQueueUrlMap(json);

        auto errorJson = SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"FifoQueue", "false"}}}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(errorJson, "__type"), "InvalidParameterValue");
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesNonExistentQueue, TFixture) {
        auto errorJson = SetQueueAttributes({
            {"QueueUrl", NON_EXISTING_QUEUE_URL},
            {"Attributes", NJson::TJsonMap{{"VisibilityTimeout", "60"}}}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(errorJson, "__type"), "AWS.SimpleQueueService.NonExistentQueue");
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesNonExistentConsumer, TFixture) {
        auto driver = MakeDriver(*this);
        const TString topicName = "SetAttrsTopicNoConsumer";

        Y_ENSURE(CreateTopic(driver, topicName, NYdb::NTopic::TCreateTopicSettings()
            .BeginAddConsumer("other-consumer")
            .EndAddConsumer()));

        auto errorJson = SetQueueAttributes({
            {"QueueUrl", std::format("/v1/5//Root/{}/{}/16/ydb-sqs-consumer", topicName.size(), topicName.c_str())},
            {"Attributes", NJson::TJsonMap{{"VisibilityTimeout", "60"}}}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(errorJson, "__type"), "AWS.SimpleQueueService.NonExistentQueue");
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesInvalidValues, TFixture) {
        auto json = CreateQueue({
            {"QueueName", "TestSetAttrsInvalid"},
            {"Attributes", NJson::TJsonMap{{"VisibilityTimeout", "30"}}}
        });
        TString queueUrl = GetPathFromQueueUrlMap(json);

        auto errorJson = SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"VisibilityTimeout", "-1"}}}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(errorJson, "__type"), "InvalidParameterValue");

        errorJson = SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"DelaySeconds", "901"}}}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(errorJson, "__type"), "InvalidParameterValue");
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesUnknownAttribute, TFixture) {
        auto json = CreateQueue({{"QueueName", "TestSetAttrsUnknown"}});
        TString queueUrl = GetPathFromQueueUrlMap(json);

        auto errorJson = SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"UnknownAttribute", "value"}}}
        }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(errorJson, "__type"), "InvalidParameterValue");
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesRedrivePolicy, TFixture) {
        auto dlqJson = CreateQueue({
            {"QueueName", "SetAttrsDLQ.fifo"},
            {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}
        });
        TString dlqUrl = GetPathFromQueueUrlMap(dlqJson);

        auto dlqAttrJson = GetQueueAttributes({
            {"QueueUrl", dlqUrl},
            {"AttributeNames", NJson::TJsonArray{"QueueArn"}}
        });
        TString dlqArn = GetByPath<TString>(dlqAttrJson, "Attributes.QueueArn");

        auto json = CreateQueue({
            {"QueueName", "SetAttrsMain.fifo"},
            {"Attributes", NJson::TJsonMap{{"FifoQueue", "true"}}}
        });
        TString queueUrl = GetPathFromQueueUrlMap(json);

        TString redrivePolicy = TStringBuilder() << R"({"deadLetterTargetArn":")" << dlqArn << R"(","maxReceiveCount":5})";
        SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"RedrivePolicy", redrivePolicy}}}
        });

        auto attrJson = GetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"AttributeNames", NJson::TJsonArray{"RedrivePolicy"}}
        });
        UNIT_ASSERT(attrJson["Attributes"].Has("RedrivePolicy"));
        TString resultPolicy = attrJson["Attributes"]["RedrivePolicy"].GetString();
        NJson::TJsonValue policyJson;
        UNIT_ASSERT(NJson::ReadJsonTree(resultPolicy, &policyJson));
        UNIT_ASSERT_VALUES_EQUAL(policyJson["maxReceiveCount"].GetInteger(), 5);
    }

    Y_UNIT_TEST_F(TestSetQueueAttributesRetentionPeriod, TFixture) {
        auto json = CreateQueue({
            {"QueueName", "TestSetAttrsRetention"},
            {"Attributes", NJson::TJsonMap{{"MessageRetentionPeriod", "3600"}}}
        });
        TString queueUrl = GetPathFromQueueUrlMap(json);

        SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"MessageRetentionPeriod", "7200"}}}
        });

        auto attrJson = GetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"AttributeNames", NJson::TJsonArray{"MessageRetentionPeriod"}}
        });
        UNIT_ASSERT_VALUES_EQUAL(attrJson["Attributes"]["MessageRetentionPeriod"], "7200");
    }

    Y_UNIT_TEST_F(TestPurgeQueueInvalid, TFixture) {
        auto json = PurgeQueue({{"QueueUrl", "invlid-queue-url"}}, 400);
    }

    Y_UNIT_TEST_F(TestPurgeQueueNonExisting, TFixture) {
        auto json = PurgeQueue({{"QueueUrl", NON_EXISTING_QUEUE_URL}}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.NonExistentQueue");
    }

    Y_UNIT_TEST_F(TestPurgeQueue, TFixture) {
        auto driver = MakeDriver(*this);
        auto client = TTopicClient(driver);

        const int nMessages = 5;
        const TString queueName = "queueName";
        const TString consumer = "consumer-3";
        Y_ENSURE(CreateTopic(driver, queueName, consumer));

        auto describeAndCountUncommited = [&]() {
            auto desc = client.DescribeConsumer(queueName, consumer, NYdb::NTopic::TDescribeConsumerSettings{}.IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_C(desc.IsSuccess(), queueName);
            auto description = desc.GetConsumerDescription();
            desc.Out(Cerr);
            ui64 offsetDiff = 0;
            for (const NYdb::NTopic::TPartitionInfo& part : description.GetPartitions()) {
                UNIT_ASSERT_C(part.GetPartitionStats().has_value(), part.GetPartitionId());
                UNIT_ASSERT_C(part.GetPartitionConsumerStats().has_value(), part.GetPartitionId());
                offsetDiff += part.GetPartitionStats().value().GetEndOffset() -
                              part.GetPartitionConsumerStats().value().GetCommittedOffset();
            }
            return offsetDiff;
        };
        UNIT_ASSERT_VALUES_EQUAL(describeAndCountUncommited(), 0);

        auto json = CreateQueue({{"QueueName", queueName + "@" + consumer}});
        auto queueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT_VALUES_EQUAL(describeAndCountUncommited(), 0);

        for (int i = 0; i < nMessages; ++i) {
            SendMessage({{"QueueUrl", queueUrl}, {"MessageBody", "MessageBody-" + ToString(i)}});
        }

        // All available messages in a queue (including in-flight messages) should be deleted.
        // Set VisibilityTimeout to large value to be sure the message is in-flight during the test.
        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 1}, {"MaxNumberOfMessages", 3}, {"VisibilityTimeout", 43000}}); // ~12 hours
        UNIT_ASSERT_VALUES_UNEQUAL(json["Messages"].GetArray().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(describeAndCountUncommited(), nMessages);
        PurgeQueue({{"QueueUrl", queueUrl}});

        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 1}, {"VisibilityTimeout", 30}});
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArray().size(), 0);
        UNIT_ASSERT_VALUES_EQUAL(describeAndCountUncommited(), 0);
    }

} // Y_UNIT_TEST_SUITE(TestSqsTopicHttpProxy)
