#include <ydb/core/persqueue/public/metrics.h>

#include <ydb/core/protos/pqconfig.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace {

NKikimrPQ::TPQConfig MakeConfig(bool topicsAreFirstClassCitizen) {
    NKikimrPQ::TPQConfig config;
    config.SetTopicsAreFirstClassCitizen(topicsAreFirstClassCitizen);
    return config;
}

} // namespace

Y_UNIT_TEST_SUITE(MetricsConsumerName) {
    Y_UNIT_TEST(FirstClassCitizenKeepsBothForms) {
        const auto config = MakeConfig(true);
        const TString configName = "crowdplatform@ycs-tasks@sqs@push2yt@push2yt_consumer";
        const TString pathForm = "crowdplatform/ycs-tasks/sqs/push2yt/push2yt_consumer";

        UNIT_ASSERT_VALUES_EQUAL(NKikimr::NPQ::MetricsConsumerName(configName, config), configName);
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::NPQ::MetricsConsumerName(pathForm, config), pathForm);
    }

    Y_UNIT_TEST(FederationConvertsConfigNameAndPathFormOnce) {
        const auto config = MakeConfig(false);
        const TString configName = "crowdplatform@ycs-tasks@sqs@push2yt@push2yt_consumer";
        const TString pathForm = "crowdplatform/ycs-tasks/sqs/push2yt/push2yt_consumer";

        UNIT_ASSERT_VALUES_EQUAL(NKikimr::NPQ::MetricsConsumerName(configName, config), pathForm);
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::NPQ::MetricsConsumerName(pathForm, config), pathForm);
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::NPQ::MetricsConsumerName("account@dir--topic", config), "account/dir--topic");
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::NPQ::MetricsConsumerName("ydb_sqs_consumer", config), "shared/ydb_sqs_consumer");
        UNIT_ASSERT_VALUES_EQUAL(NKikimr::NPQ::MetricsConsumerName("shared/ydb_sqs_consumer", config), "shared/ydb_sqs_consumer");
    }
}
