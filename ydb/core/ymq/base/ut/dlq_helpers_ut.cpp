#include <ydb/core/ymq/base/dlq_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NSQS {

Y_UNIT_TEST_SUITE(RedrivePolicy) {
    Y_UNIT_TEST(RedrivePolicyValidationTest) {
        NKikimrConfig::TSqsConfig config;
        {
            // basic sanity
            TRedrivePolicy basicPolicy = TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:mymegadlq","maxReceiveCount":3})__", config);
            UNIT_ASSERT(basicPolicy.IsValid());
            UNIT_ASSERT(basicPolicy.TargetArn && *basicPolicy.TargetArn == "yrn:ya:sqs:ru-central1:radix:mymegadlq");
            UNIT_ASSERT(basicPolicy.TargetQueueName && *basicPolicy.TargetQueueName == "mymegadlq");
            UNIT_ASSERT(basicPolicy.MaxReceiveCount && *basicPolicy.MaxReceiveCount == 3);
        }
        {
            // count as str
            TRedrivePolicy basicPolicy = TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:ultradlq","maxReceiveCount":"42"})__", config);
            UNIT_ASSERT(basicPolicy.IsValid());
            UNIT_ASSERT(basicPolicy.TargetArn && *basicPolicy.TargetArn == "yrn:ya:sqs:ru-central1:radix:ultradlq");
            UNIT_ASSERT(basicPolicy.TargetQueueName && *basicPolicy.TargetQueueName == "ultradlq");
            UNIT_ASSERT(basicPolicy.MaxReceiveCount && *basicPolicy.MaxReceiveCount == 42);
        }

        UNIT_ASSERT(TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:mymegadlq","maxReceiveCount":3})__", config).IsValid());
        // maxReceiveCount should be from 1 to 1000
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:mymegadlq","maxReceiveCount":0})__", config).IsValid());
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:mymegadlq","maxReceiveCount":1001})__", config).IsValid());
        // maxReceiveCount should be integer
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:mymegadlq","maxReceiveCount":"omglol"})__", config).IsValid());
        // target arn shouldn't be empty
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"","maxReceiveCount":3})__", config).IsValid());
        // both fields are expected
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":""})__", config).IsValid());
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"maxReceiveCount":3})__", config).IsValid());
        // no cheats!
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":":","maxReceiveCount":"8"})__", config).IsValid());
        {
            // empty value is okay
            TRedrivePolicy clearPolicy = TRedrivePolicy::FromJson("", config);
            UNIT_ASSERT(clearPolicy.IsValid());
            UNIT_ASSERT(clearPolicy.TargetArn && clearPolicy.TargetArn->empty());
            UNIT_ASSERT(clearPolicy.TargetQueueName && clearPolicy.TargetQueueName->empty());
            UNIT_ASSERT(clearPolicy.MaxReceiveCount && *clearPolicy.MaxReceiveCount == 0);
        }
    }

    Y_UNIT_TEST(RedrivePolicyToJsonTest) {
        NKikimrConfig::TSqsConfig config;
        const TString json = R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:mymegadlq","maxReceiveCount":3})__";
        UNIT_ASSERT_EQUAL(TRedrivePolicy::FromJson(json, config).ToJson(), json);
    }

    Y_UNIT_TEST(RedrivePolicyArnValidationTest) {
        NKikimrConfig::TSqsConfig config;
        UNIT_ASSERT(TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:mymegadlq","maxReceiveCount":3})__", config).IsValid());
        // empty queue name
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1:radix:","maxReceiveCount":3})__", config).IsValid());
        // empty account
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central1::mymegadlq","maxReceiveCount":3})__", config).IsValid());
        // bad region
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:ya:sqs:ru-central42:radix:mymegadlq","maxReceiveCount":3})__", config).IsValid());
        // bad prefix
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrrrr:ru-central1:radix:mymegadlq","maxReceiveCount":3})__", config).IsValid());
        // broken prefix
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrr:ya:sqs:ru-central1:radix:mymegadlq","maxReceiveCount":3})__", config).IsValid());

        config.SetYandexCloudMode(true); // check cloud prefix
        UNIT_ASSERT(TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yrn:yc:ymq:ru-central1:radix:mymegadlq","maxReceiveCount":3})__", config).IsValid());
        UNIT_ASSERT(!TRedrivePolicy::FromJson(R"__({"deadLetterTargetArn":"yarrr:yc:ymq:ru-central1:radix:mymegadlq","maxReceiveCount":3})__", config).IsValid());
    }
}

} // namespace NKikimr::NSQS
