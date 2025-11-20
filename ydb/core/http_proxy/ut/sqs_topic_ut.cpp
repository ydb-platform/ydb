
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

    TString GetPathFromQueueUrlMap(const NJson::TJsonMap& json) {
        TString url = GetByPath<TString>(json, "QueueUrl");
        auto [host, path] = NUrl::SplitUrlToHostAndPath(url);
        return ToString(path);
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
            const TString queueUrl = "/v1/5//Root/16/ExampleQueueName/16/ydb-sqs-consumer";
            auto queueName = "ExampleQueueName";
            auto json = GetQueueUrl({{"QueueName", queueName}});
            UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));

            // ignore QueueOwnerAWSAccountId parameter.
            json = GetQueueUrl({{"QueueName", queueName}, {"QueueOwnerAWSAccountId", "some-account-id"}});
            UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));

            json = GetQueueUrl({{"QueueName", queueName}, {"WrongParameter", "some-value"}}, 400);
            UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "InvalidArgumentException");
        }

        Y_UNIT_TEST_F(TestGetQueueUrlOfNotExistingQueue, TFixture) {
            if ("X-Fail") {
                return;
            }
            auto json = GetQueueUrl({{"QueueName", "not-existing-queue"}}, 400);
            TString resultType = GetByPath<TString>(json, "__type");
            UNIT_ASSERT_VALUES_EQUAL(resultType, "AWS.SimpleQueueService.NonExistentQueue");
            TString resultMessage = GetByPath<TString>(json, "message");
            UNIT_ASSERT_VALUES_EQUAL(resultMessage, "The specified queue doesn't exist.");
        }

        Y_UNIT_TEST_F(TestGetQueueUrlWithConsumer, TFixture) {
            const TString consumer = "user_consumer";
            const TString queueName = "ExampleQueueName";
            const TString queueUrl = "/v1/5//Root/16/ExampleQueueName/13/user_consumer";
            const TString requestQueueName = queueName + "@" + consumer;
            auto json = GetQueueUrl({
                {"QueueName", requestQueueName},
            });
            UNIT_ASSERT_VALUES_EQUAL(queueUrl, GetPathFromQueueUrlMap(json));
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
                {"QueueUrl", "/v1/5//Root/16/ExampleQueueName/13/user_consumer"},
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


    } // Y_UNIT_TEST_SUITE(TestYmqHttpProxy)
