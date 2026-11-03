#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/library/persqueue/topic_parser/describe_retry.h>
#include <library/cpp/testing/unittest/registar.h>

#include <memory>

#include <util/generic/maybe.h>

namespace NPersQueue::NTests {

class TConverterTestWrapper {
public:
    TConverterTestWrapper(bool firstClass, const TString& pqRoot, const TString& localDc)
        : Factory(firstClass, pqRoot, localDc)
    {}

    void SetConverter(const TString& topic, const TString& dc, const TString& database) {
        DiscoveryConverter = Factory.MakeDiscoveryConverter(topic, {}, dc, database);
    }

    void SetConverter(NKikimrPQ::TPQTabletConfig& pqTabletConfig) {
        TopicConverter = Factory.MakeTopicConverter(pqTabletConfig);
    }

    void BasicLegacyModeChecks() {
        UNIT_ASSERT(DiscoveryConverter->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(
                DiscoveryConverter->GetPrimaryPath(),
                TString(TStringBuilder() << "/Root/PQ/" << DiscoveryConverter->FullLegacyName)
        );
        UNIT_ASSERT(!DiscoveryConverter->FullLegacyName.empty());
        UNIT_ASSERT(!DiscoveryConverter->ShortLegacyName.empty());
        //UNIT_ASSERT_VALUES_EQUAL(DiscoveryConverter->GetInternalName(), DiscoveryConverter->GetPrimaryPath());
    }

    void BasicFirstClassChecks() {
        UNIT_ASSERT_C(DiscoveryConverter->IsValid(), DiscoveryConverter->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(
                DiscoveryConverter->GetPrimaryPath(),
                DiscoveryConverter->FullModernPath
        );

        //UNIT_ASSERT_VALUES_EQUAL(DiscoveryConverter->GetInternalName(), DiscoveryConverter->GetPrimaryPath());
    }

    void SetDatabase(const TString& database) {
        DiscoveryConverter->SetDatabase(database);
    }

    TString GetAccount() {
        return *DiscoveryConverter->GetAccount_();
    }

#define WRAPPER_METHOD(NAME)             \
    TString Get##NAME() const {           \
        return DiscoveryConverter->NAME; \
    }

    WRAPPER_METHOD(ShortLegacyName);
    WRAPPER_METHOD(FullLegacyName);
    WRAPPER_METHOD(Dc);

    TString GetAccount() const {
        return  DiscoveryConverter->Account_.GetOrElse("");
    }

#undef WRAPPER_METHOD

    TTopicNamesConverterFactory Factory;
    TDiscoveryConverterPtr DiscoveryConverter;
    TTopicConverterPtr TopicConverter;

};

Y_UNIT_TEST_SUITE(DiscoveryConverterTest) {
    Y_UNIT_TEST(FullLegacyNames) {

        TConverterTestWrapper wrapper(false, "/Root/PQ", TString("dc1"));
        wrapper.SetConverter("rt3.dc1--account--topic", "", "");
        UNIT_ASSERT_C(wrapper.DiscoveryConverter->IsValid(), wrapper.DiscoveryConverter->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetShortLegacyName(), "account--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetFullLegacyName(), "rt3.dc1--account--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetDc(), "dc1");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc1--account--topic");

        wrapper.SetConverter("account--topic", "", "");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetFullLegacyName(), "rt3.dc1--account--topic");

        wrapper.SetConverter("account--topic", "dc2", "");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetFullLegacyName(), "rt3.dc2--account--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetDc(), "dc2");
    }

    Y_UNIT_TEST(FullLegacyPath) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", TString("dc1"));
        wrapper.SetConverter("/Root/PQ/rt3.dc1--account--topic", "", "/Root");
        //UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetInternalName(), "/Root/PQ/rt3.dc1--account--topic");
    }

    Y_UNIT_TEST(MinimalName) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", TString("dc1"));
        wrapper.SetConverter("rt3.dc1--topic", "", "");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetShortLegacyName(), "topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetFullLegacyName(), "rt3.dc1--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc1--topic");

        wrapper.SetConverter("topic", "", "");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetFullLegacyName(), "rt3.dc1--topic");

        wrapper.SetConverter("topic", "dc2", "");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetFullLegacyName(), "rt3.dc2--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc2--topic");
    }

    Y_UNIT_TEST(FullLegacyNamesWithRootDatabase) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", TString("dc1"));
        wrapper.SetConverter("rt3.dc1--account--topic", "", "/Root");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc1--account--topic");
    }

    Y_UNIT_TEST(WithLogbrokerPath) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", TString("dc1"));
        wrapper.SetConverter("account/topic", "", "");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc1--account--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetAccount(), "account");

        wrapper.SetConverter("account/topic", "dc2", "/Root");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc2--account--topic");
    }

    Y_UNIT_TEST(AccountDatabase) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", TString("dc1"));
        wrapper.SetConverter("account/topic", "", "");
        UNIT_ASSERT_C(wrapper.DiscoveryConverter->IsValid(), wrapper.DiscoveryConverter->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetAccount(), "account");

        wrapper.SetDatabase("/database");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetSecondaryPath(""), "/database/topic");

        wrapper.SetConverter("account2/topic2", "", "");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetSecondaryPath("database2"), "/database2/topic2");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetAccount(), "account2");

        wrapper.SetConverter("rt3.dc1--account3--topic3", "", "");

        wrapper.SetConverter("account/dir1/dir2/topic", "dc3", "");
//        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetSecondaryPath("database3"), "database3/dir1/dir2/topic");
//        UNIT_ASSERT_VALUES_EQUAL(wrapper.GetClientsideName(), "rt3.dc3--account@dir1@dir2--topic");

    }
    Y_UNIT_TEST(CmWay) {
        auto converterFactory = NPersQueue::TTopicNamesConverterFactory(
                false, "/Root/PQ", "dc1", false
        );
        auto converter = converterFactory.MakeDiscoveryConverter(
                "account/account-topic", {}, "dc1", "account"
        );
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullModernName(), "account-topic-mirrored-from-dc1");

        converter = converterFactory.MakeDiscoveryConverter(
                "account/topic", {}, "dc1", "account"
        );
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullModernName(), "topic-mirrored-from-dc1");

        converterFactory = NPersQueue::TTopicNamesConverterFactory(
                false, "/Root/PQ", "dc1", true
        );
        converter = converterFactory.MakeDiscoveryConverter(
                "account/topic", {}, "dc1", "account"
        );
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullModernName(), "topic");

        converter = converterFactory.MakeDiscoveryConverter(
                "account/account-topic", {}, "dc1", "account"
        );
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullModernName(), "account-topic");

        converter = converterFactory.MakeDiscoveryConverter(
                "account/account/account", {}, "dc1", "account"
        );
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullModernName(), "account/account");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetSecondaryPath("account"), "/account/account/account");

        converter = converterFactory.MakeDiscoveryConverter(
                "account/", {}, "dc1", "account"
        );
        UNIT_ASSERT(!converter->IsValid());
        UNIT_ASSERT(!converter->GetReason().Contains("Internal error"));

    }
    Y_UNIT_TEST(DiscoveryConverter) {
        auto converterFactory = NPersQueue::TTopicNamesConverterFactory(
                false, "/Root/PQ", "dc1", {}
        );
        TVector<TString> badNames = {"account//", "account/", "/"};
        for (const auto& name : badNames) {
            auto converter = converterFactory.MakeDiscoveryConverter(
                    name, {}, "dc1", ""
            );
            UNIT_ASSERT_C(!converter->IsValid(), name);
            UNIT_ASSERT(!converter->GetReason().Contains("Internal error"));
        }
    }
    Y_UNIT_TEST(EmptyModern) {
        auto converterFactory = NPersQueue::TTopicNamesConverterFactory(
                false, "/Root/PQ", "dc1", {}
        );
        auto converter = converterFactory.MakeDiscoveryConverter(
                "account/", {}, "dc1", "account"
        );
        UNIT_ASSERT_VALUES_EQUAL(converter->IsValid(), false);

        converter = converterFactory.MakeDiscoveryConverter(
                "/account", {}, "dc1", ""
        );
        UNIT_ASSERT(converter->IsValid());
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullModernName(), "account");

    }

    Y_UNIT_TEST(FirstClass) {
        TConverterTestWrapper wrapper(true, "", "");
        wrapper.SetConverter("account/stream", "", "/database");
        wrapper.BasicFirstClassChecks();
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/database/account/stream");

        wrapper.SetConverter("/somedb/account/stream", "", "/somedb");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/somedb/account/stream");

        wrapper.SetConverter("/somedb2/account/stream", "", "");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.DiscoveryConverter->GetPrimaryPath(), "/somedb2/account/stream");
    }
}

Y_UNIT_TEST_SUITE(TopicNameConverterTest) {
    Y_UNIT_TEST(LegacyStyle) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", "dc1");

        NKikimrPQ::TPQTabletConfig pqConfig;
        pqConfig.SetTopicName("rt3.dc1--account--topic");
        pqConfig.SetTopicPath("/Root/PQ/rt3.dc1--account--topic");
        pqConfig.SetFederationAccount("account");
        pqConfig.SetLocalDC(true);
        pqConfig.SetYdbDatabasePath("");
        wrapper.SetConverter(pqConfig);

        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetFederationPath(), "account/topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetCluster(), "dc1");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetTopicForSrcId(), "rt3.dc1--account--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetTopicForSrcIdHash(), "account--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetClientsideName(), "rt3.dc1--account--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetModernName(), "topic");

        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc1--account--topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetFederationPath(), "account/topic");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetInternalName(), "rt3.dc1--account--topic");
    }

    Y_UNIT_TEST(LegacyStyleDoubleName) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", "dc1");

        NKikimrPQ::TPQTabletConfig pqConfig;
        pqConfig.SetTopicName("rt3.dc1--account@account--account");
        pqConfig.SetTopicPath("/Root/PQ/rt3.dc1--account@account--account");
        pqConfig.SetFederationAccount("account");
        pqConfig.SetLocalDC(true);
        pqConfig.SetYdbDatabasePath("");
        wrapper.SetConverter(pqConfig);

        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetFederationPath(), "account/account/account");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetCluster(), "dc1");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetTopicForSrcId(), "rt3.dc1--account@account--account");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetTopicForSrcIdHash(), "account@account--account");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetClientsideName(), "rt3.dc1--account@account--account");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetModernName(), "account/account");

        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc1--account@account--account");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetInternalName(), "rt3.dc1--account@account--account");
    }



    Y_UNIT_TEST(Paths) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", "dc1");
        {
            NKikimrPQ::TPQTabletConfig pqConfig;
            pqConfig.SetTopicName("rt3.dc1--account@path--topic");
            pqConfig.SetTopicPath("/lb/account-database/path/topic");
            pqConfig.SetFederationAccount("account");
            pqConfig.SetDC("dc1");
            pqConfig.SetLocalDC(true);

            pqConfig.SetYdbDatabasePath("/lb/account-database");

            wrapper.SetConverter(pqConfig);

            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetPrimaryPath(), "/lb/account-database/path/topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetSecondaryPath(), "/Root/PQ/rt3.dc1--account@path--topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetModernName(), "path/topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetClientsideName(), "rt3.dc1--account@path--topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetFederationPath(), "account/path/topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetInternalName(), "rt3.dc1--account@path--topic");

        }
        {
            NKikimrPQ::TPQTabletConfig pqConfig;
            pqConfig.SetTopicName("rt3.dc2--account@path--topic");
            pqConfig.SetLocalDC(false);
            pqConfig.SetTopicPath("/lb/account-database/path/topic-mirrored-from-dc2");
            pqConfig.SetFederationAccount("account");
            pqConfig.SetYdbDatabasePath("/lb/account-database");

            wrapper.SetConverter(pqConfig);

            UNIT_ASSERT_C(wrapper.TopicConverter->IsValid(), wrapper.TopicConverter->GetReason());
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetPrimaryPath(), "/lb/account-database/path/topic-mirrored-from-dc2");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetSecondaryPath(), "/Root/PQ/rt3.dc2--account@path--topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetModernName(), "path/topic-mirrored-from-dc2");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetClientsideName(), "rt3.dc2--account@path--topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetFederationPath(), "account/path/topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetInternalName(), "rt3.dc2--account@path--topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetTopicForSrcId(), "rt3.dc2--account@path--topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetTopicForSrcIdHash(), "account@path--topic");
        }

    }
    Y_UNIT_TEST(NoTopicName) {
        TConverterTestWrapper wrapper(false, "/Root/PQ", "dc1");
        {
            NKikimrPQ::TPQTabletConfig pqConfig;
            pqConfig.SetTopic("topic");
            pqConfig.SetTopicPath("/Root/PQ/rt3.dc1--account@path--topic");
            pqConfig.SetFederationAccount("account");
            pqConfig.SetDC("dc1");
            pqConfig.SetLocalDC(true);

            wrapper.SetConverter(pqConfig);

            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetPrimaryPath(), "/Root/PQ/rt3.dc1--account@path--topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetModernName(), "path/topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetClientsideName(), "rt3.dc1--account@path--topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetFederationPath(), "account/path/topic");
            UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetInternalName(), "rt3.dc1--account@path--topic");

        }
    }

    Y_UNIT_TEST(FirstClass) {
        TConverterTestWrapper wrapper(true, "", "");

        NKikimrPQ::TPQTabletConfig pqConfig;
        pqConfig.SetTopicName("my-stream");
        pqConfig.SetTopicPath("/lb/database/my-stream");
        pqConfig.SetYdbDatabasePath("/lb/database");
        wrapper.SetConverter(pqConfig);

        //UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetFederationPath(), "my-stream");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetTopicForSrcId(), "lb/database/my-stream");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetTopicForSrcIdHash(), "lb/database/my-stream");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetModernName(), "my-stream");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetFederationPath(), "my-stream");
        UNIT_ASSERT_VALUES_EQUAL(wrapper.TopicConverter->GetInternalName(), "/lb/database/my-stream");
    }
    Y_UNIT_TEST(PathFromDiscoveryConverter) {
        auto converterFactory = NPersQueue::TTopicNamesConverterFactory(false, "/Root/PQ", TString(), false);
        auto converter = converterFactory.MakeDiscoveryConverter("topic1", false, "other", "database1");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetFullModernName(), "topic1-mirrored-from-other");
    }
}

Y_UNIT_TEST_SUITE(TopicNameConverterForCPTest) {
    Y_UNIT_TEST(CorrectLegacyTopics) {
        auto converter = TTopicNameConverter::ForFederation("/Root/PQ", "", "rt3.sas--account--topic", "/Root/PQ", "", true);
        UNIT_ASSERT_C(converter->IsValid(), converter->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(converter->GetAccount(), "account");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetCluster(), "sas");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetLegacyProducer(), "account");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetLegacyLogtype(), "topic");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), "/Root/PQ/rt3.sas--account--topic");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetInternalName(), "rt3.sas--account--topic");

        converter = TTopicNameConverter::ForFederation("/Root/PQ", "", "rt3.sas--account@dir--topic", "/Root/PQ", "/Root", false);
        UNIT_ASSERT_C(converter->IsValid(), converter->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(converter->GetAccount(), "account");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetCluster(), "sas");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetLegacyProducer(), "account@dir");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetLegacyLogtype(), "topic");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), "/Root/PQ/rt3.sas--account@dir--topic");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetInternalName(), "rt3.sas--account@dir--topic");
    }

    Y_UNIT_TEST(BadLegacyTopics) {
        auto converter = TTopicNameConverter::ForFederation("", "", "rt3.sas--account--topic", "/Root/PQ", "", true);
        UNIT_ASSERT(!converter->IsValid());
        converter = TTopicNameConverter::ForFederation("/Root/PQ2", "", "rt3.sas--account--topic", "/Root/PQ", "", true);
        UNIT_ASSERT(!converter->IsValid());
        converter = TTopicNameConverter::ForFederation("/Root/PQ", "", "account/topic", "/Root/PQ", "", true);
        UNIT_ASSERT(!converter->IsValid());
    }

    Y_UNIT_TEST(CorrectModernTopics) {
        auto converter = TTopicNameConverter::ForFederation(
                "/Root/PQ", "", "topic", "/LbCommunal/account", "/LbCommunal/account", true, "sas", "account"
        );
        UNIT_ASSERT_C(converter->IsValid(), converter->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(converter->GetAccount(), "account");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetCluster(), "sas");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetLegacyProducer(), "account");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetLegacyLogtype(), "topic");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), "/LbCommunal/account/topic");

        converter = TTopicNameConverter::ForFederation(
                "/Root/PQ", "", "topic-mirrored-from-sas", "/LbCommunal/account/dir", "/LbCommunal/account", false, "", "account"
        );
        UNIT_ASSERT_C(converter->IsValid(), converter->GetReason());
        UNIT_ASSERT_VALUES_EQUAL(converter->GetAccount(), "account");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetCluster(), "sas");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetLegacyProducer(), "account@dir");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetLegacyLogtype(), "topic");
        UNIT_ASSERT_VALUES_EQUAL(converter->GetPrimaryPath(), "/LbCommunal/account/dir/topic-mirrored-from-sas");
    }

    Y_UNIT_TEST(BadModernTopics) {
        auto converter = TTopicNameConverter::ForFederation(
                "", "", "topic", "/LbCommunal/account", "/LbCommunal/account", false, "sas", "account"
        );
        UNIT_ASSERT(!converter->IsValid());

        converter = TTopicNameConverter::ForFederation(
                "", "", "topic", "/LbCommunal/account/.topic", "/LbCommunal/account", false, "sas", "account"
        );
        UNIT_ASSERT(!converter->IsValid());

        converter = TTopicNameConverter::ForFederation(
                "", "", "mirrored-from-sas", "/LbCommunal/account/.topic", "/LbCommunal/account", false, "sas", "account"
        );
        UNIT_ASSERT(!converter->IsValid());

        // this is valid, corresponding check relaxed
        converter = TTopicNameConverter::ForFederation(
                "", "", "topic-mirrored-from-sas", "/LbCommunal/account", "/LbCommunal/account", true, "sas", "account"
        );
        UNIT_ASSERT(converter->IsValid());

        converter = TTopicNameConverter::ForFederation(
                "", "", "topic-mirrored-from-", "/LbCommunal/account", "/LbCommunal/account", true, "sas", "account"
        );
        UNIT_ASSERT(!converter->IsValid());

    }
}

struct TRetryTopic {
    TMaybe<TString> Account;
    bool AllowSecondary = true;
    TVector<TString> Databases;

    const TMaybe<TString>& GetAccount_() const {
        return Account;
    }

    void SetDatabase(const TString& database) {
        Databases.push_back(database);
    }

    TMaybe<TString> GetSecondaryPath(const TString&) const {
        if (!AllowSecondary || Databases.empty()) {
            return {};
        }
        return TMaybe<TString>("/secondary");
    }
};

using TRetryTopics = TVector<std::shared_ptr<TRetryTopic>>;

std::shared_ptr<TRetryTopic> RetryTopic(TMaybe<TString> account, bool allowSecondary = true) {
    auto topic = std::make_shared<TRetryTopic>();
    topic->Account = std::move(account);
    topic->AllowSecondary = allowSecondary;
    return topic;
}

TVector<ui64> SelectRetry(const TString& dbRoot, TRetryTopics& topics, const TVector<bool>& unknown) {
    return NKikimr::NMsgBusProxy::NPqMetaCacheV2::SelectSecondTryTopics(
        dbRoot, topics, [&](size_t index) { return unknown[index]; });
}

Y_UNIT_TEST_SUITE(SecondTryTopicSelection) {
    Y_UNIT_TEST(SkippedEntryDoesNotStealNextTopic) {
        TRetryTopics topics{
            RetryTopic(Nothing()),
            RetryTopic(TString("acc")),
        };
        const auto selected = SelectRetry("/lb", topics, {true, true});

        UNIT_ASSERT_VALUES_EQUAL(selected.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(selected[0], 1);
        UNIT_ASSERT(topics[0]->Databases.empty());
        UNIT_ASSERT_VALUES_EQUAL(topics[1]->Databases.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(topics[1]->Databases[0], "/lb/acc");
    }

    Y_UNIT_TEST(MiddleSkipKeepsLaterIndex) {
        TRetryTopics topics{
            RetryTopic(TString("a")),
            RetryTopic(Nothing()),
            RetryTopic(TString("c")),
        };
        const auto selected = SelectRetry("/lb", topics, {true, true, true});

        UNIT_ASSERT_VALUES_EQUAL(selected.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(selected[0], 0);
        UNIT_ASSERT_VALUES_EQUAL(selected[1], 2);
        UNIT_ASSERT(topics[1]->Databases.empty());
        UNIT_ASSERT_VALUES_EQUAL(topics[0]->Databases[0], "/lb/a");
        UNIT_ASSERT_VALUES_EQUAL(topics[2]->Databases[0], "/lb/c");
    }

    Y_UNIT_TEST(KnownPathAndEmptyRootAreNotRetried) {
        TRetryTopics topics{RetryTopic(TString("acc"))};
        UNIT_ASSERT(SelectRetry("/lb", topics, {false}).empty());
        UNIT_ASSERT(topics[0]->Databases.empty());

        UNIT_ASSERT(SelectRetry("", topics, {true}).empty());
        UNIT_ASSERT(topics[0]->Databases.empty());
    }

    Y_UNIT_TEST(MissingSecondaryPathDoesNotShiftIndex) {
        TRetryTopics topics{
            RetryTopic(TString("a"), false),
            RetryTopic(TString("b")),
        };
        const auto selected = SelectRetry("/lb", topics, {true, true});

        UNIT_ASSERT_VALUES_EQUAL(selected.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(selected[0], 1);
        UNIT_ASSERT_VALUES_EQUAL(topics[1]->Databases[0], "/lb/b");
    }

    Y_UNIT_TEST(RealConverterSkipDoesNotStealNextTopic) {
        TTopicNamesConverterFactory firstClass(true, "", "");
        TTopicNamesConverterFactory federation(false, "/Root/PQ", "dc1");
        TVector<TDiscoveryConverterPtr> topics{
            firstClass.MakeDiscoveryConverter("/database/stream", {}, "", "/database"),
            federation.MakeDiscoveryConverter("acc/topic", {}, "dc1", ""),
        };
        UNIT_ASSERT(topics[0]->IsValid());
        UNIT_ASSERT(topics[1]->IsValid());

        const auto selected = NKikimr::NMsgBusProxy::NPqMetaCacheV2::SelectSecondTryTopics(
            "/lb", topics, [](size_t) { return true; });

        UNIT_ASSERT_VALUES_EQUAL(selected.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(selected[0], 1);
        const auto& secondary = topics[1]->GetSecondaryPath("");
        UNIT_ASSERT(secondary.Defined());
        UNIT_ASSERT(secondary->StartsWith("/lb/acc"));
    }
}
} // NTests
