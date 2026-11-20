#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/public/api/protos/ydb_topic.pb.h>
#include <ydb/library/testlib/helpers.h>
#include <ydb/core/testlib/actor_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr;

Y_UNIT_TEST_SUITE(TResolveConsumerServiceTypeTest) {

Y_UNIT_TEST(UsesExplicitServiceType) {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    consumer.SetServiceType("MyGreatType");

    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.MutableDefaultClientServiceType()->SetName("default_type");
    pqConfig.SetDisallowDefaultClientServiceType(true);

    TString serviceType;
    TString error;
    UNIT_ASSERT(ResolveConsumerServiceType(consumer, pqConfig, true, serviceType, error));
    UNIT_ASSERT_VALUES_EQUAL(serviceType, "MyGreatType");
    UNIT_ASSERT(error.empty());
}

Y_UNIT_TEST(FillsDefaultWhenAllowed) {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;

    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.MutableDefaultClientServiceType()->SetName("default_type");
    pqConfig.SetDisallowDefaultClientServiceType(false);

    TString serviceType;
    TString error;
    UNIT_ASSERT(ResolveConsumerServiceType(consumer, pqConfig, true, serviceType, error));
    UNIT_ASSERT_VALUES_EQUAL(serviceType, "default_type");
    UNIT_ASSERT(error.empty());
}

Y_UNIT_TEST(FailsWhenDisallowDefaultAndMissing) {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;

    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.MutableDefaultClientServiceType()->SetName("default_type");
    pqConfig.SetDisallowDefaultClientServiceType(true);

    TString serviceType;
    TString error;
    UNIT_ASSERT(!ResolveConsumerServiceType(consumer, pqConfig, true, serviceType, error));
    UNIT_ASSERT_STRING_CONTAINS(error, "service type must be set for all read rules");
}

Y_UNIT_TEST(SkipsCheckWhenDisabled) {
    NKikimrPQ::TPQTabletConfig::TConsumer consumer;

    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.MutableDefaultClientServiceType()->SetName("default_type");
    pqConfig.SetDisallowDefaultClientServiceType(true);

    TString serviceType;
    TString error;
    UNIT_ASSERT(ResolveConsumerServiceType(consumer, pqConfig, false, serviceType, error));
    UNIT_ASSERT_VALUES_EQUAL(serviceType, "");
    UNIT_ASSERT(error.empty());
}

} // Y_UNIT_TEST_SUITE(TResolveConsumerServiceTypeTest)

Y_UNIT_TEST_SUITE(TTopicDescriptionTest) {

Y_UNIT_TEST_TWIN(ReplicationConsumerAttributeUsesPublicName, firstClass) {
    TActorSystemStub actorSystem;
    actorSystem.AppData.PQConfig.SetTopicsAreFirstClassCitizen(firstClass);

    NKikimrSchemeOp::TPersQueueGroupDescription description;
    auto* config = description.MutablePQTabletConfig();
    auto* replication = config->AddConsumers();
    replication->SetName(firstClass ? "account/replication" : "account@replication");
    replication->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_REPLICATION);
    auto* reader = config->AddConsumers();
    reader->SetName("reader");
    reader->SetType(NKikimrPQ::TPQTabletConfig::CONSUMER_TYPE_STREAMING);

    Ydb::Topic::DescribeTopicResult result;
    Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS;
    TString error;
    UNIT_ASSERT_C(FillTopicDescription(result, description, {}, Nothing(), status, error), error);
    const auto it = result.attributes().find("_ydb_replication_consumer");
    UNIT_ASSERT_C(it != result.attributes().end(), result.DebugString());
    UNIT_ASSERT_VALUES_EQUAL(it->second, "account/replication");
    UNIT_ASSERT_VALUES_EQUAL(result.consumers_size(), 2);
    UNIT_ASSERT_VALUES_EQUAL(it->second, result.consumers(0).name());
}

} // Y_UNIT_TEST_SUITE(TTopicDescriptionTest)
