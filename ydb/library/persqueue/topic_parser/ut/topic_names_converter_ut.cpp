#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NPersQueue;
namespace NTests {

void BasicLegacyModeChecks(TTopicNameConverter* converter) {
    UNIT_ASSERT(converter->IsValid());
    UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), converter->GetFullLegacyPath());
    UNIT_ASSERT_VALUES_EQUAL(converter->GetClientsideName(), converter->GetFullLegacyName());
}

void BasicPathModeChecks(TTopicNameConverter* converter, const TString& fullPath) {
    UNIT_ASSERT_C(converter->IsValid(), converter->GetReason());
    UNIT_ASSERT_VALUES_EQUAL(converter->GetClientsideName(), converter->GetModernName());
    UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), fullPath);
    UNIT_ASSERT_VALUES_EQUAL(converter->GetModernPath(), fullPath);
}

Y_UNIT_TEST_SUITE(TopicNamesConverterTest) {
    Y_UNIT_TEST(LegacyModeWithMinimalName) {
        TTopicNamesConverterFactory factory(false, "/Root/PQ");

        auto converter = factory.MakeTopicNameConverter("topic1", "dc1", "");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetShortLegacyName(), "topic1");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.dc1--topic1");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyPath(), "Root/PQ/rt3.dc1--topic1");
        BasicLegacyModeChecks(converter.get());

        converter = factory.MakeTopicNameConverter("topic2", "dc1", "");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetShortLegacyName(), "topic2");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.dc1--topic2");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyPath(), "Root/PQ/rt3.dc1--topic2");
        BasicLegacyModeChecks(converter.get());
    }

    Y_UNIT_TEST(LegacyModeWithReset) {
        TTopicNamesConverterFactory factory(false, "/Root/PQ");
        auto converter = factory.MakeTopicNameConverter("topic1", "dc1", "");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.dc1--topic1");
        BasicLegacyModeChecks(converter.get());

        converter = factory.MakeTopicNameConverter("topic1", "dc2", "");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.dc2--topic1");
        BasicLegacyModeChecks(converter.get());

    }

    Y_UNIT_TEST(LegacyModeWithShortNameAndDc) {
        TTopicNamesConverterFactory factory(false, "/Root/PQ");
        auto converter = factory.MakeTopicNameConverter("rt3.dc2--topic2", "", "");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetShortLegacyName(), "topic2");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.dc2--topic2");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyPath(), "Root/PQ/rt3.dc2--topic2");
        BasicLegacyModeChecks(converter.get());
    }

    Y_UNIT_TEST(LegacyModeWithNormalNameAndRootDB) {
        TTopicNamesConverterFactory factory(false, "/Root/PQ");
        auto converter = factory.MakeTopicNameConverter("account--topic1", "dc2", "/Root");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetShortLegacyName(), "account--topic1");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.dc2--account--topic1");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyPath(), "Root/PQ/rt3.dc2--account--topic1");
        BasicLegacyModeChecks(converter.get());

        converter = factory.MakeTopicNameConverter("/Root/PQ/rt3.dc3--account3--topic3", "", "");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetShortLegacyName(), "account3--topic3");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.dc3--account3--topic3");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyPath(), "Root/PQ/rt3.dc3--account3--topic3");
        BasicLegacyModeChecks(converter.get());
    }

    Y_UNIT_TEST(LegacyModeWithFullNameAndRootDB) {
        TTopicNamesConverterFactory factory(false, "/Root/PQ");
        auto converter = factory.MakeTopicNameConverter("rt3.sas--accountX@dir--topicX", "", "/Root");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetShortLegacyName(), "accountX@dir--topicX");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.sas--accountX@dir--topicX");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyPath(), "Root/PQ/rt3.sas--accountX@dir--topicX");
        BasicLegacyModeChecks(converter.get());
    }

    Y_UNIT_TEST(PathModeWithConversionToLegacy) {
        TTopicNamesConverterFactory factory(false, "/Root/PQ");
        auto converter = factory.MakeTopicNameConverter("account1/topic1", "dc", "/Root");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetShortLegacyName(), "account1--topic1");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyName(), "rt3.dc--account1--topic1");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullLegacyPath(), "Root/PQ/rt3.dc--account1--topic1");
        BasicLegacyModeChecks(converter.get());
    }

    Y_UNIT_TEST(LegacyModeWithErrors) {
        TTopicNamesConverterFactory factory(false, "/Root/PQ");
        //UNIT_ASSERT(!factory.MakeTopicNameConverter("topic", "", "")->IsValid());
        //UNIT_ASSERT(!factory.MakeTopicNameConverter("topic", "", "/database")->IsValid());
        UNIT_ASSERT(!factory.MakeTopicNameConverter("rt3.man--account2--topic", "man", "")->IsValid());
        UNIT_ASSERT(!factory.MakeTopicNameConverter("rt3.man--account2--topic", "man", "")->IsValid());
        UNIT_ASSERT(!factory.MakeTopicNameConverter("rt3.man--account2--topic", "man", "")->IsValid());

    }

    Y_UNIT_TEST(PathWithNoDc) {
        TTopicNamesConverterFactory factory(false, "/Root/PQ");
        auto converter = factory.MakeTopicNameConverter("account/topic1", "", "");
        UNIT_ASSERT(converter->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(converter->GetModernName(), "account/topic1");
    }

    Y_UNIT_TEST(FstClassModeWithShortTopic) {
        TTopicNamesConverterFactory factory(true, "");
        {
            auto converter = factory.MakeTopicNameConverter("topic1", "", "/database");
            UNIT_ASSERT_VALUES_EQUAL(converter->GetClientsideName(), "topic1");
            BasicPathModeChecks(converter.get(), "database/topic1");
        }
        {
            auto converter = factory.MakeTopicNameConverter("topic2", "", "database2");
            UNIT_ASSERT_VALUES_EQUAL(converter->GetClientsideName(), "topic2");
            BasicPathModeChecks(converter.get(), "database2/topic2");
        }
        {
            auto converter = factory.MakeTopicNameConverter("topic3", "", "database3/");
            BasicPathModeChecks(converter.get(), "database3/topic3");
            UNIT_ASSERT_VALUES_EQUAL(converter->GetClientsideName(), "topic3");
        }
    }
    Y_UNIT_TEST(FstClassModeWithLegacyNames) {
        {
            TTopicNamesConverterFactory factory(true, "Root/PQ");
            auto converter = factory.MakeTopicNameConverter("/Root/PQ/rt3.dc1--topic3", "", "/Root");
            UNIT_ASSERT(!converter->IsValid());
            converter = factory.MakeTopicNameConverter("/Root/PQ/rt3.dc1--topic3", "", "");
            UNIT_ASSERT(!converter->IsValid());
        }
        {
            TTopicNamesConverterFactory factory(true, "");
            auto converter = factory.MakeTopicNameConverter("/Root/PQ/rt3.dc1--topic3", "", "/Root");
            UNIT_ASSERT(!converter->IsValid());
        }
    }

    Y_UNIT_TEST(FstClassModeWithRoot) {
        TVector<TTopicNamesConverterFactory> factories{
                {true, ""},
                {true, "/Root/PQ"}
        };
        for (auto& factory: factories) {
            {
                auto converter = factory.MakeTopicNameConverter("account/topic1", "", "/Root");
                BasicPathModeChecks(converter.get(), "Root/account/topic1");
                UNIT_ASSERT_VALUES_EQUAL(converter->GetClientsideName(), "account/topic1");
            }
            {
                auto converter = factory.MakeTopicNameConverter("/Root/account2/topic2", "", "/Root");
                BasicPathModeChecks(converter.get(), "Root/account2/topic2");
                UNIT_ASSERT_VALUES_EQUAL(converter->GetClientsideName(), "account2/topic2");
            }
        }
    }

    Y_UNIT_TEST(FstClassModeWithLongTopic) {
        TTopicNamesConverterFactory factory(true, "");

        auto converter = factory.MakeTopicNameConverter("dir4/topic4", "", "/database4");
        BasicPathModeChecks(converter.get(), "database4/dir4/topic4");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetClientsideName(), "dir4/topic4");

    }

    Y_UNIT_TEST(FstClassModeWithBadTopics) {
        TTopicNamesConverterFactory factory(true, "/Root/PQ");
        UNIT_ASSERT(!factory.MakeTopicNameConverter("topic1", "", "")->IsValid());
    }

}

} // NTests