#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NPersQueue;

Y_UNIT_TEST_SUITE(TopicParserHelpers) {

Y_UNIT_TEST(MakeConsumerPath) {
    UNIT_ASSERT_VALUES_EQUAL(MakeConsumerPath("user"), "shared/user");
    UNIT_ASSERT_VALUES_EQUAL(MakeConsumerPath("account@dir@consumer"), "account/dir/consumer");
}

Y_UNIT_TEST(StripLeadSlashAndNormalizeFullPath) {
    UNIT_ASSERT_VALUES_EQUAL(StripLeadSlash("/a/b"), "a/b");
    UNIT_ASSERT_VALUES_EQUAL(StripLeadSlash("a/b"), "a/b");
    UNIT_ASSERT_VALUES_EQUAL(NormalizeFullPath("a/b"), "/a/b");
    UNIT_ASSERT_VALUES_EQUAL(NormalizeFullPath("/a/b"), "/a/b");
    UNIT_ASSERT_VALUES_EQUAL(NormalizeFullPath(""), "");
}

Y_UNIT_TEST(GetFullTopicPath) {
    UNIT_ASSERT_VALUES_EQUAL(GetFullTopicPath(TMaybe<TString>("/Root"), "topic"), "/Root/topic");
    UNIT_ASSERT_VALUES_EQUAL(GetFullTopicPath(TMaybe<TString>("/Root"), "/Root/topic"), "/Root/topic");
}

Y_UNIT_TEST(ConvertConsumerNameFccKeepsAsIs) {
    NKikimrPQ::TPQConfig pqConfig;
    pqConfig.SetTopicsAreFirstClassCitizen(true);
    UNIT_ASSERT_VALUES_EQUAL(ConvertNewConsumerName("user", pqConfig), "user");
    UNIT_ASSERT_VALUES_EQUAL(ConvertOldConsumerName("user", pqConfig), "user");
}

} // Y_UNIT_TEST_SUITE(TopicParserHelpers)
