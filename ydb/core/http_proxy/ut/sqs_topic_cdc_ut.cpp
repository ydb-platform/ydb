
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


inline namespace NTest {
    class TNoAuthFixture: public THttpProxyTestMockForSQSTopic {
    public:
        void SetUp(NUnitTest::TTestContext& ctx) override {
            THttpProxyTestMockForSQSTopic::SetUp(ctx);
            DisableAuthorization();
        }
    };

    class TWithEnforceUserTokenRequirementFixture: public THttpProxyTestMock {
    public:
        void SetUp(NUnitTest::TTestContext&) override {
            InitAll(TInitParameters{
                .EnableSqsTopic = true,
                .EnforceUserTokenRequirement = true,
            });
        }
    };


    class TNoAuthWithEnforceUserTokenRequirementFixture: public TWithEnforceUserTokenRequirementFixture {
    public:
        void SetUp(NUnitTest::TTestContext& ctx) override {
            TWithEnforceUserTokenRequirementFixture::SetUp(ctx);
            DisableAuthorization();
        }
    };

    using NYdb::TDriver;
    using NYdb::NTopic::TTopicClient;

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

    NYdb::TDriverConfig MakeDriverConfig(std::derived_from<THttpProxyTestMock> auto& fixture) {
        NYdb::TDriverConfig config;
        config.SetEndpoint("localhost:" + ToString(fixture.KikimrGrpcPort));
        config.SetDatabase("/Root");
        config.SetAuthToken("root@builtin");
        config.SetLog(std::make_unique<TStreamLogBackend>(&Cerr));
        return config;
    }

    NYdb::TDriver MakeDriver(std::derived_from<THttpProxyTestMock> auto& fixture) {
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

    void ExecuteQuery(NYdb::NTable::TSession& session, const TString& query) {
        const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }
} // namespace

Y_UNIT_TEST_SUITE(TestSqsTopicHttpProxyCdc) {


    TString PrepareChangefeedAndGetQueueUrl(TFixture& fixture) {
        auto driver = MakeDriver(fixture);
        NYdb::NTable::TTableClient tableClient(driver, NYdb::NTable::TClientSettings().UseQueryCache(false));
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        ExecuteQuery(session, R"-(
            --!syntax_v1
            CREATE TABLE `/Root/table1` (
                id Uint64,
                value Text,
                PRIMARY KEY (id)
            );
        )-");
        ExecuteQuery(session, R"-(
            --!syntax_v1
            ALTER TABLE `/Root/table1` ADD CHANGEFEED feed WITH (FORMAT = 'JSON', MODE = 'UPDATES');
        )-");

        const auto insertResult = session.ExecuteDataQuery(R"-(
            --!syntax_v1
            INSERT INTO `/Root/table1` (id, value) VALUES (1001u, "foobar");
        )-",
             NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), NYdb::EStatus::SUCCESS, insertResult.GetIssues().ToString());

        {
            NYdb::NTopic::TTopicClient client{driver};
            NYdb::NTopic::TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAddSharedConsumer()
                .ConsumerName("mlp")
                .EndAddConsumer();
            auto f = client.AlterTopic("/Root/table1/feed", alterSettings);
            f.Wait();

            auto v = f.GetValueSync();
            UNIT_ASSERT_C(v.IsSuccess(), "Error: " << v);
        }
        auto json = fixture.GetQueueUrl({{"QueueName", "table1/feed@mlp"}});
        const TString queueUrl = GetByPath<TString>(json, "QueueUrl");
        UNIT_ASSERT_STRING_CONTAINS(queueUrl, "table1");
        return queueUrl;
    }

    Y_UNIT_TEST_F(TestListQueues, TFixture) {
        const TString queueUrl = PrepareChangefeedAndGetQueueUrl(*this);

        NJson::TJsonMap json;
        json = ListQueues({});
        UNIT_ASSERT_VALUES_EQUAL(json["QueueUrls"].GetArray().size(), 1);
        const TString listQueueUrl = json["QueueUrls"][0].GetString();
        UNIT_ASSERT_VALUES_EQUAL(queueUrl, listQueueUrl);
    }

    Y_UNIT_TEST_F(TestReceiveMessage, TFixture) {
        const TString queueUrl = PrepareChangefeedAndGetQueueUrl(*this);
        NJson::TJsonMap json;
        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArraySafe().size(), 1);
        UNIT_ASSERT_STRING_CONTAINS(json["Messages"][0]["Body"].GetString(), "foobar");
    }

    Y_UNIT_TEST_F(TestDeleteMessage, TFixture) {
        const TString queueUrl = PrepareChangefeedAndGetQueueUrl(*this);
        NJson::TJsonMap json;
        json = ReceiveMessage({{"QueueUrl", queueUrl}, {"WaitTimeSeconds", 20}});
        UNIT_ASSERT_VALUES_EQUAL(json["Messages"].GetArraySafe().size(), 1);

        auto receiptHandle = json["Messages"][0]["ReceiptHandle"].GetString();
        UNIT_ASSERT(!receiptHandle.empty());
        DeleteMessage({{"QueueUrl", queueUrl}, {"ReceiptHandle", receiptHandle}});
    }

    Y_UNIT_TEST_F(TestDeleteQueue, TFixture) {
        const TString queueUrl = PrepareChangefeedAndGetQueueUrl(*this);
        auto json = DeleteQueue({{"QueueUrl", queueUrl}}, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.UnsupportedOperation");
    }

    Y_UNIT_TEST_F(TestCreateQueue, TFixture) {
        const TString queueUrl = PrepareChangefeedAndGetQueueUrl(*this);
        auto json = CreateQueue({{"QueueName", "table1/feed"}}, 400);
    }

    Y_UNIT_TEST_F(TestSendMessage, TFixture) {
        const TString queueUrl = PrepareChangefeedAndGetQueueUrl(*this);
        NJson::TJsonMap json;
        json = SendMessage({
                               {"QueueUrl", queueUrl},
                               {"MessageBody", "MessageBody-0"},
                           }, 400);
        UNIT_ASSERT_VALUES_EQUAL(GetByPath<TString>(json, "__type"), "AWS.SimpleQueueService.UnsupportedOperation");
    }

    Y_UNIT_TEST_F(TestGetQueueAttributes, TFixture) {
        const TString queueUrl = PrepareChangefeedAndGetQueueUrl(*this);
        auto json = GetQueueAttributes({{"QueueUrl", queueUrl},
                                        {"AttributeNames", NJson::TJsonArray{"All"}}});
        UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["DelaySeconds"], "0");
        UNIT_ASSERT_GT(json["Attributes"].GetMapSafe().size(), 5);
    }

    Y_UNIT_TEST_F(TestSetQueueAttributes, TFixture) {
        const TString queueUrl = PrepareChangefeedAndGetQueueUrl(*this);
        SetQueueAttributes({
            {"QueueUrl", queueUrl},
            {"Attributes", NJson::TJsonMap{{"VisibilityTimeout", "61"}}}
        });
        auto json = GetQueueAttributes({{"QueueUrl", queueUrl},
                                        {"AttributeNames", NJson::TJsonArray{"VisibilityTimeout"}}});
        UNIT_ASSERT_VALUES_EQUAL(json["Attributes"]["VisibilityTimeout"], "61");
        UNIT_ASSERT_GE(json["Attributes"].GetMapSafe().size(), 1);
    }

} // Y_UNIT_TEST_SUITE(TestSqsTopicHttpProxyCdc)
