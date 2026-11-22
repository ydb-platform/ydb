#include <ydb/core/ydb_convert/topic_description.h>
#include <ydb/core/protos/pqconfig.pb.h>

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
