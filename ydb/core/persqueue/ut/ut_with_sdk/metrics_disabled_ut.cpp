#include <ydb/library/testlib/helpers.h>
#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/include/client.h>
#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>

#include <chrono>
#include <map>

namespace NKikimr::NPersQueueTests {
namespace {

using namespace NYdb::NTopic;

const TString TOPIC = "/Root/metrics-disabled";
const TString CONSUMER = "reader";
const TString METRICS_DISABLED = "_metrics_disabled";
using TAttributes = std::map<TString, TString>;

class TMetricsDisabledEnv {
public:
    NYdb::NTopic::NTests::TTopicSdkTestSetup Setup;
    NYdb::TDriver Driver;
    TTopicClient Client;
    NYdb::NPersQueue::TPersQueueClient PqClient;
    std::unique_ptr<Ydb::PersQueue::V1::PersQueueService::Stub> Stub;

    TMetricsDisabledEnv()
        : Setup("MetricsDisabled", NYdb::NTopic::NTests::TTopicSdkTestSetup::MakeServerSettings(), false)
        , Driver(Setup.MakeDriverConfig())
        , Client(Driver)
        , PqClient(Driver)
        , Stub(Ydb::PersQueue::V1::PersQueueService::NewStub(grpc::CreateChannel(
            Setup.GetEndpoint(), grpc::InsecureChannelCredentials())))
    {
    }

    void InitContext(grpc::ClientContext& context) {
        context.AddMetadata("x-ydb-database", Setup.GetDatabase());
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(30));
    }

    static void AssertStatus(const Ydb::Operations::Operation& operation, Ydb::StatusIds::StatusCode expected) {
        UNIT_ASSERT_C(operation.ready(), operation.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL_C(operation.status(), expected, operation.ShortDebugString());
    }

    void Create(bool pqv1, const TAttributes& attributes = {},
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
    {
        if (pqv1) {
            Ydb::PersQueue::V1::CreateTopicRequest request;
            request.set_path(TOPIC);
            auto* settings = request.mutable_settings();
            settings->set_partitions_count(1);
            settings->set_retention_period_ms(TDuration::Days(1).MilliSeconds());
            settings->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            settings->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
            auto* rule = settings->add_read_rules();
            rule->set_consumer_name(CONSUMER);
            rule->set_supported_format(Ydb::PersQueue::V1::TopicSettings::FORMAT_BASE);
            rule->add_supported_codecs(Ydb::PersQueue::V1::CODEC_RAW);
            for (const auto& [name, value] : attributes) {
                (*settings->mutable_attributes())[name] = value;
            }
            grpc::ClientContext context;
            InitContext(context);
            Ydb::PersQueue::V1::CreateTopicResponse response;
            UNIT_ASSERT(Stub->CreateTopic(&context, request, &response).ok());
            AssertStatus(response.operation(), expected);
        } else {
            auto settings = TCreateTopicSettings().PartitioningSettings(1, 1)
                .BeginAddConsumer(CONSUMER).EndAddConsumer();
            for (const auto& [name, value] : attributes) {
                settings.AddAttribute(name, value);
            }
            const auto result = Client.CreateTopic(TOPIC, settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), static_cast<NYdb::EStatus>(expected), result.GetIssues().ToString());
        }
    }

    Ydb::PersQueue::V1::TopicSettings DescribePq() {
        Ydb::PersQueue::V1::DescribeTopicRequest request;
        request.set_path(TOPIC);
        grpc::ClientContext context;
        InitContext(context);
        Ydb::PersQueue::V1::DescribeTopicResponse response;
        UNIT_ASSERT(Stub->DescribeTopic(&context, request, &response).ok());
        AssertStatus(response.operation(), Ydb::StatusIds::SUCCESS);
        Ydb::PersQueue::V1::DescribeTopicResult result;
        UNIT_ASSERT(response.operation().result().UnpackTo(&result));
        return result.settings();
    }

    void Alter(bool pqv1, const TAttributes& attributes,
        Ydb::StatusIds::StatusCode expected = Ydb::StatusIds::SUCCESS)
    {
        if (pqv1) {
            Ydb::PersQueue::V1::AlterTopicRequest request;
            request.set_path(TOPIC);
            *request.mutable_settings() = DescribePq();
            auto& attrs = *request.mutable_settings()->mutable_attributes();
            attrs.erase(METRICS_DISABLED);
            for (const auto& [name, value] : attributes) {
                attrs[name] = value;
            }
            grpc::ClientContext context;
            InitContext(context);
            Ydb::PersQueue::V1::AlterTopicResponse response;
            UNIT_ASSERT(Stub->AlterTopic(&context, request, &response).ok());
            AssertStatus(response.operation(), expected);
        } else {
            auto settings = TAlterTopicSettings();
            for (const auto& [name, value] : attributes) {
                settings.BeginAlterAttributes().Alter(name, value).EndAlterAttributes();
            }
            const auto result = Client.AlterTopic(TOPIC, settings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), static_cast<NYdb::EStatus>(expected), result.GetIssues().ToString());
        }
    }

    void AssertAttribute(const TString& value = {}) {
        const auto pqSettings = DescribePq();
        const auto describe = Client.DescribeTopic(TOPIC).GetValueSync();
        AssertSuccess(describe);
        const auto& attributes = describe.GetTopicDescription().GetAttributes();
        const auto pqIt = pqSettings.attributes().find(METRICS_DISABLED);
        const auto it = attributes.find(METRICS_DISABLED);
        if (value.empty()) {
            UNIT_ASSERT(pqIt == pqSettings.attributes().end());
            UNIT_ASSERT(it == attributes.end());
        } else {
            UNIT_ASSERT(pqIt != pqSettings.attributes().end());
            UNIT_ASSERT_VALUES_EQUAL(pqIt->second, value);
            UNIT_ASSERT(it != attributes.end());
            UNIT_ASSERT_VALUES_EQUAL(it->second, value);
        }
    }

    static void AssertSuccess(const NYdb::TStatus& status) {
        UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TTopicMetricsDisabled) {
    Y_UNIT_TEST_TWIN(CreateWithMetricsDisabled, pqv1) {
        for (const auto* value : {"false", "true"}) {
            TMetricsDisabledEnv env;
            env.Create(pqv1, {{METRICS_DISABLED, value}});
            env.AssertAttribute(value);
        }
    }

    Y_UNIT_TEST_TWIN(AlterMetricsDisabled, pqv1) {
        TMetricsDisabledEnv env;
        env.Create(pqv1);
        env.AssertAttribute();
        env.Alter(pqv1, {});
        env.AssertAttribute();
        for (const auto* value : {"true", "false", "true"}) {
            env.Alter(pqv1, {{METRICS_DISABLED, value}});
            env.AssertAttribute(value);

            env.Alter(pqv1, {});
            env.AssertAttribute(value);

            const auto describe = env.PqClient.DescribeTopic(TOPIC).GetValueSync();
            env.AssertSuccess(describe);
            env.AssertSuccess(env.PqClient.AlterTopic(TOPIC,
                NYdb::NPersQueue::TAlterTopicSettings().SetSettings(describe.TopicSettings())).GetValueSync());
            env.AssertAttribute(value);

            env.AssertSuccess(env.Client.AlterTopic(TOPIC,
                TAlterTopicSettings().SetRetentionPeriod(TDuration::Hours(12))).GetValueSync());
            env.AssertAttribute(value);
        }
    }

    Y_UNIT_TEST_TWIN(RejectInvalidMetricsDisabled, pqv1) {
        TMetricsDisabledEnv env;
        for (const auto* value : {"", "1", "0", "TRUE", "False", "unknown"}) {
            env.Create(pqv1, {{METRICS_DISABLED, value}}, Ydb::StatusIds::BAD_REQUEST);
        }
        env.Create(pqv1, {{METRICS_DISABLED, "true"}});
        for (const auto* value : {"", "1", "0", "TRUE", "False", "unknown"}) {
            env.Alter(pqv1, {{METRICS_DISABLED, value}}, Ydb::StatusIds::BAD_REQUEST);
            env.AssertAttribute("true");
        }
    }
}

} // namespace NKikimr::NPersQueueTests
