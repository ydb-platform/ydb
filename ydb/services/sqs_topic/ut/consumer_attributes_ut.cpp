#include <ydb/services/sqs_topic/consumer_attributes.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/testlib/actor_helpers.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NSqsTopic;

Y_UNIT_TEST_SUITE(SqsTopicConsumerAttributes) {
    Y_UNIT_TEST_TWIN(CompareDeadLetterQueuePaths, enableRelativePaths) {
        NKikimr::TActorSystemStub actorSystem;
        actorSystem.AppData.FeatureFlags.SetEnableRelativePaths(enableRelativePaths);
        NKikimrPQ::TPQTabletConfig config;
        config.SetYdbDatabasePath("/Root");
        NKikimrPQ::TPQTabletConfig::TConsumer consumer;
        V1::TQueueAttributes attributes;
        attributes.DeadLetterQueue = "dlq";

        consumer.SetDeadLetterQueue("dlq");
        UNIT_ASSERT(V1::CompareWithExistingQueueAttributes(config, consumer, attributes).has_value());
        consumer.SetDeadLetterQueue("/Root/dlq");
        UNIT_ASSERT_VALUES_EQUAL(V1::CompareWithExistingQueueAttributes(config, consumer, attributes).has_value(), enableRelativePaths);
        consumer.SetDeadLetterQueue("Root/dlq");
        UNIT_ASSERT_VALUES_EQUAL(V1::CompareWithExistingQueueAttributes(config, consumer, attributes).has_value(), enableRelativePaths);
        consumer.SetDeadLetterQueue("/Other/dlq");
        UNIT_ASSERT(!V1::CompareWithExistingQueueAttributes(config, consumer, attributes).has_value());
    }
}
