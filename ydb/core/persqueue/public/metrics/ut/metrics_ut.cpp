#include <ydb/core/persqueue/public/metrics/metrics.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NPQ;

namespace {

struct TCase {
    const char* Name;
    const char* Federation;
};

// Federation applies ConvertOld: '@' becomes '/', and a name without '@' gets the "shared/" prefix.
// First-class topics keep the name unchanged.
constexpr TCase Cases[] = {
    {"ydb_sqs_consumer", "shared/ydb_sqs_consumer"},
    {"client", "shared/client"},
    {"account@dir--topic", "account/dir--topic"},
    {"crowdplatform@ycs-tasks@sqs@push2yt@push2yt_consumer", "crowdplatform/ycs-tasks/sqs/push2yt/push2yt_consumer"},
    {"crowdplatform/ycs-tasks/sqs/push2yt/push2yt_consumer", "shared/crowdplatform/ycs-tasks/sqs/push2yt/push2yt_consumer"},
    {"shared/ydb_sqs_consumer", "shared/shared/ydb_sqs_consumer"},
    {"a@b/c", "a/b/c"},
    {"", "shared/"},
};

NKikimrPQ::TPQConfig PqConfig(bool topicsAreFirstClassCitizen) {
    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.SetTopicsAreFirstClassCitizen(topicsAreFirstClassCitizen);
    return pqConfig;
}

void Expect(TStringBuf name, TStringBuf expected, const NKikimrPQ::TPQConfig& pqConfig) {
    UNIT_ASSERT_VALUES_EQUAL_C(MetricsConsumerName(TString(name), pqConfig), expected, name);

    NKikimrPQ::TPQTabletConfig::TConsumer consumer;
    consumer.SetName(TString(name));
    UNIT_ASSERT_VALUES_EQUAL_C(MetricsConsumerName(consumer, pqConfig), expected, name);
}

} // namespace

Y_UNIT_TEST_SUITE(MetricsConsumerName) {

Y_UNIT_TEST(Federation) {
    const auto pqConfig = PqConfig(false);
    for (const auto& testCase : Cases) {
        Expect(testCase.Name, testCase.Federation, pqConfig);
    }
}

Y_UNIT_TEST(FirstClassCitizenKeepsName) {
    const auto pqConfig = PqConfig(true);
    for (const auto& testCase : Cases) {
        Expect(testCase.Name, testCase.Name, pqConfig);
    }
}

} // Y_UNIT_TEST_SUITE(MetricsConsumerName)
