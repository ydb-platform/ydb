#include <ydb/core/kafka_proxy/actors/actors.h>
#include <ydb/core/kafka_proxy/actors/kafka_read_session_utils.h>
#include <ydb/core/kafka_proxy/kafka_constants.h>
#include <ydb/core/kafka_proxy/kafka_consumer_protocol.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <initializer_list>
#include <utility>

namespace NKafka {
    namespace {

        struct TFixture {
            NKikimrConfig::TKafkaProxyConfig Config;
            TContext Context{Config};

            TFixture(std::initializer_list<std::pair<const char*, const char*>> rules = {
                         {"^/alias(/|$)", "/Root\\1"}, {"^/Root(/|$)", "/Decoy\\1"}},
                     const TString& database = "/alias")
            {
                NKikimrConfig::TPathRewriteConfig config;
                for (const auto& [pattern, replacement] : rules) {
                    auto* rule = config.AddRules();
                    rule->SetPattern(pattern);
                    rule->SetReplacement(replacement);
                }
                Context.LogicalDatabasePath = database;
                Context.DatabasePath = "/Root";
                Context.PathContext = std::make_shared<NKikimr::NPathAliasing::TPathContext>(
                    NKikimr::NPathAliasing::TPathNormalizer(config), database);
            }

            std::shared_ptr<const TResolvedKafkaTopics> Resolve(const TApiMessage& request) const {
                std::shared_ptr<const TResolvedKafkaTopics> result;
                const TString error = ResolveKafkaRequestPaths(request, Context, result);
                UNIT_ASSERT_C(error.empty(), error);
                return result;
            }
        };

        template <class TRequest>
        void CheckNamedTopics() {
            TFixture fixture;
            auto request = std::make_shared<TRequest>();
            request->Topics.emplace_back().Name = "/alias/One";
            request->Topics.emplace_back().Name = "Two";
            const auto paths = fixture.Resolve(*request);
            UNIT_ASSERT_VALUES_EQUAL(paths->at("/alias/One"), "/Root/One");
            UNIT_ASSERT_VALUES_EQUAL(paths->at("Two"), "/Root/Two");
            UNIT_ASSERT_VALUES_EQUAL(*request->Topics[0].Name, "/alias/One");
            UNIT_ASSERT_VALUES_EQUAL(*request->Topics[1].Name, "Two");
            TMessagePtr<TRequest> message({}, request, paths);
            UNIT_ASSERT_VALUES_EQUAL(message.GetTopicPath("/Root", "/alias/One"), "/Root/One");
            UNIT_ASSERT_VALUES_EQUAL(message.GetTopicPath("/Root", "Two"), "/Root/Two");
            auto clone = message.template Cast<TApiMessage>();
            UNIT_ASSERT(clone.GetResolvedTopics() == paths);
            UNIT_ASSERT_VALUES_EQUAL(clone.GetTopicPath("/Root", "Two"), "/Root/Two");
        }

        template <class TRequest>
        void CheckTransactionalOwnerGrammar() {
            for (ui32 mode = 0; mode < 3; ++mode) {
                TFixture fixture(mode == 0
                                     ? std::initializer_list<std::pair<const char*, const char*>>{}
                                 : mode == 1
                                     ? std::initializer_list<std::pair<const char*, const char*>>{{"^/Never$", "/Unused"}}
                                     : std::initializer_list<std::pair<const char*, const char*>>{
                                           {"^(.*)$", "\\1"}, {"^(.*)$", "/Wrong"}}, "/Root");
                for (const TString& path : {TString("/Topic"), TString("/RootSibling/T"),
                                            TString("Topic"), TString("RootSibling/T"), TString("//Root//T/")}) {
                    auto request = std::make_shared<TRequest>();
                    request->Topics.emplace_back().Name = path;
                    TMessagePtr<TRequest> message({}, request, fixture.Resolve(*request));
                    const auto expected = NPersQueue::GetFullTopicPath(TString("/Root"), path);
                    const auto actual = NPersQueue::GetFullTopicPath(TString("/Root"), message.GetTopicPathOrOriginal(path));
                    UNIT_ASSERT_VALUES_EQUAL(actual, expected);
                    UNIT_ASSERT_VALUES_EQUAL(*request->Topics[0].Name, path);
                    if (path == "/Topic") {
                        UNIT_ASSERT_VALUES_EQUAL(actual, "/Topic");
                    }
                }
            }
        }

        template <class TRequest>
        void CheckTransactionalOwnerRewrite() {
            TFixture fixture({{"^/alias/Table$", "/Root/Actual"}, {"^/shortcut$", "/Root/Actual"},
                              {"^/alias$", "/Root"},
                              {"^/Root/Actual$", "/Root/Wrong"}});
            auto request = std::make_shared<TRequest>();
            request->Topics.emplace_back().Name = "Table";
            request->Topics.emplace_back().Name = "/shortcut";
            TMessagePtr<TRequest> message({}, request, fixture.Resolve(*request));
            for (const TString& path : {TString("Table"), TString("/shortcut")}) {
                UNIT_ASSERT_VALUES_EQUAL(message.GetTopicPathOrOriginal(path), "/Root/Actual");
                UNIT_ASSERT_VALUES_EQUAL(NPersQueue::GetFullTopicPath(TString("/Root"),
                                                                      message.GetTopicPathOrOriginal(path)), "/Root/Actual");
            }
            UNIT_ASSERT_VALUES_EQUAL(*request->Topics[0].Name, "Table");
            UNIT_ASSERT_VALUES_EQUAL(*request->Topics[1].Name, "/shortcut");
        }

    } // namespace

    Y_UNIT_TEST_SUITE(KafkaPathAliasing) {
        Y_UNIT_TEST(TransactionalOwnersKeepTheirOriginalFullTopicGrammar) {
            CheckTransactionalOwnerGrammar<TAddPartitionsToTxnRequestData>();
            CheckTransactionalOwnerGrammar<TTxnOffsetCommitRequestData>();
        }

        Y_UNIT_TEST(TransactionalOwnersResolveLogicalCompleteOperandsOnce) {
            CheckTransactionalOwnerRewrite<TAddPartitionsToTxnRequestData>();
            CheckTransactionalOwnerRewrite<TTxnOffsetCommitRequestData>();
        }

        Y_UNIT_TEST(MetadataRetainsItsDifferentLeadingSlashGrammar) {
            TFixture fixture({{"^/Never$", "/Unused"}}, "/Root");
            auto request = std::make_shared<TMetadataRequestData>();
            request->Topics.emplace_back().Name = "/Topic";
            TMessagePtr<TMetadataRequestData> message({}, request, fixture.Resolve(*request));
            UNIT_ASSERT_VALUES_EQUAL(message.GetTopicPath("/Root", "/Topic"), "/Root/Topic");
            UNIT_ASSERT_VALUES_EQUAL(*request->Topics[0].Name, "/Topic");
        }

        Y_UNIT_TEST(DisabledRulesDoNotAllocateMetadataOrMutateNames) {
            TFixture fixture({});
            TMetadataRequestData request;
            request.Topics.emplace_back().Name = "//alias//Topic";
            UNIT_ASSERT(!fixture.Resolve(request));
            UNIT_ASSERT_VALUES_EQUAL(*request.Topics[0].Name, "//alias//Topic");
        }

        Y_UNIT_TEST(EveryNamedTopicArrayUsesOneSharedResolution) {
            CheckNamedTopics<TMetadataRequestData>();
            CheckNamedTopics<TListOffsetsRequestData>();
            CheckNamedTopics<TOffsetCommitRequestData>();
            CheckNamedTopics<TOffsetFetchRequestData>();
            CheckNamedTopics<TCreateTopicsRequestData>();
            CheckNamedTopics<TCreatePartitionsRequestData>();
            CheckNamedTopics<TAddPartitionsToTxnRequestData>();
            CheckNamedTopics<TTxnOffsetCommitRequestData>();
        }

        Y_UNIT_TEST(ProduceFetchAndNestedOffsetGroupsKeepCorrelationNames) {
            TFixture fixture;
            TProduceRequestData produce;
            produce.TransactionalId = "/alias/transaction-id";
            produce.TopicData.emplace_back().Name = "/alias/Topic";
            UNIT_ASSERT_VALUES_EQUAL(fixture.Resolve(produce)->at("/alias/Topic"), "/Root/Topic");
            UNIT_ASSERT_VALUES_EQUAL(*produce.TransactionalId, "/alias/transaction-id");
            UNIT_ASSERT_VALUES_EQUAL(*produce.TopicData[0].Name, "/alias/Topic");
            TFetchRequestData fetch;
            fetch.Topics.emplace_back().Topic = "/alias/Topic";
            UNIT_ASSERT_VALUES_EQUAL(fixture.Resolve(fetch)->at("/alias/Topic"), "/Root/Topic");
            UNIT_ASSERT_VALUES_EQUAL(*fetch.Topics[0].Topic, "/alias/Topic");
            TOffsetFetchRequestData offsets;
            auto& group = offsets.Groups.emplace_back();
            group.GroupId = "/alias/group-id";
            group.Topics.emplace_back().Name = "/alias/Topic";
            UNIT_ASSERT_VALUES_EQUAL(fixture.Resolve(offsets)->at("/alias/Topic"), "/Root/Topic");
            UNIT_ASSERT_VALUES_EQUAL(*group.GroupId, "/alias/group-id");
            UNIT_ASSERT_VALUES_EQUAL(*group.Topics[0].Name, "/alias/Topic");
        }

        Y_UNIT_TEST(OnlyTopicConfigurationResourceNamesAreSchemaOperands) {
            TFixture fixture;
            TDescribeConfigsRequestData describe;
            auto& topic = describe.Resources.emplace_back();
            topic.ResourceType = TOPIC_RESOURCE_TYPE;
            topic.ResourceName = "/alias/Topic";
            auto& broker = describe.Resources.emplace_back();
            broker.ResourceType = 4; // Kafka broker configuration identifier, not a topic.
            broker.ResourceName = "/alias/broker-id";
            const auto paths = fixture.Resolve(describe);
            UNIT_ASSERT_VALUES_EQUAL(paths->size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(paths->at("/alias/Topic"), "/Root/Topic");
            UNIT_ASSERT_VALUES_EQUAL(*describe.Resources[1].ResourceName, "/alias/broker-id");
            TAlterConfigsRequestData alter;
            auto& resource = alter.Resources.emplace_back();
            resource.ResourceType = TOPIC_RESOURCE_TYPE;
            resource.ResourceName = "/alias/Topic";
            auto& config = resource.Configs.emplace_back();
            config.Name = "/alias/config-name";
            config.Value = "/alias/config-value";
            UNIT_ASSERT_VALUES_EQUAL(fixture.Resolve(alter)->at("/alias/Topic"), "/Root/Topic");
            UNIT_ASSERT_VALUES_EQUAL(*config.Name, "/alias/config-name");
            UNIT_ASSERT_VALUES_EQUAL(*config.Value, "/alias/config-value");
        }

        Y_UNIT_TEST(JoinGroupSubscriptionBlobAndLocalIdentifiersAreNotModified) {
            TFixture fixture;
            fixture.Context.ReadSession.PendingBalancingMode = EBalancingMode::Server;
            constexpr TKafkaVersion version = 3;
            TConsumerProtocolSubscription subscription;
            subscription.Topics = {"/alias/One", "Two"};
            TKafkaWriteBuffer buffer(subscription.Size(version) + sizeof(version));
            TKafkaWritable writable(buffer);
            writable << version;
            subscription.Write(writable, version);
            const auto& bytes = buffer.GetFrontBuffer();
            const TString original(bytes.data(), bytes.size());
            TJoinGroupRequestData request;
            request.GroupId = "/alias/group-id";
            request.MemberId = "/alias/member-id";
            request.ProtocolType = SUPPORTED_JOIN_GROUP_PROTOCOL;
            auto& protocol = request.Protocols.emplace_back();
            protocol.Name = ASSIGN_STRATEGY_ROUNDROBIN;
            protocol.Metadata = TKafkaRawBytes(bytes.data(), bytes.size());
            const auto paths = fixture.Resolve(request);
            UNIT_ASSERT_VALUES_EQUAL(paths->at("/alias/One"), "/Root/One");
            UNIT_ASSERT_VALUES_EQUAL(paths->at("Two"), "/Root/Two");
            UNIT_ASSERT_VALUES_EQUAL(TString(bytes.data(), bytes.size()), original);
            const auto parsed = GetSubscriptions(request);
            UNIT_ASSERT(parsed);
            UNIT_ASSERT_VALUES_EQUAL(*parsed->Topics[0], "/alias/One");
            UNIT_ASSERT_VALUES_EQUAL(*parsed->Topics[1], "Two");
            UNIT_ASSERT_VALUES_EQUAL(*request.GroupId, "/alias/group-id");
            UNIT_ASSERT_VALUES_EQUAL(*request.MemberId, "/alias/member-id");
            fixture.Context.ReadSession.PendingBalancingMode = EBalancingMode::Native;
            UNIT_ASSERT(!fixture.Resolve(request));
            UNIT_ASSERT_VALUES_EQUAL(TString(bytes.data(), bytes.size()), original);
        }

        Y_UNIT_TEST(AbsoluteAliasWinsBeforeLegacyJoinAndIdentityStopsFallback) {
            TFixture fixture({{"^/alias$", "/Root"}, {"^/shortcut(/|$)", "/Root\\1"},
                              {"^/alias/legacy(/|$)", "/Root/legacy\\1"}});
            TMetadataRequestData request;
            request.Topics.emplace_back().Name = "/shortcut/Topic";
            request.Topics.emplace_back().Name = "/legacy/Topic";
            const auto paths = fixture.Resolve(request);
            UNIT_ASSERT_VALUES_EQUAL(paths->at("/shortcut/Topic"), "/Root/Topic");
            UNIT_ASSERT_VALUES_EQUAL(paths->at("/legacy/Topic"), "/Root/legacy/Topic");
            TFixture identity({{"^/alias$", "/Root"}, {"^/shortcut/Topic$", "/shortcut/Topic"},
                               {"^/alias/shortcut/Topic$", "/Root/Topic"}});
            request.Topics.resize(1);
            std::shared_ptr<const TResolvedKafkaTopics> rejected;
            UNIT_ASSERT(!ResolveKafkaRequestPaths(request, identity.Context, rejected).empty());
            UNIT_ASSERT(!rejected);
        }

        Y_UNIT_TEST(InvalidTargetPublishesNoPartialResolutionMap) {
            TFixture fixture({{"^/alias(/|$)", "/Root\\1"}, {"^/broken$", "relative"}});
            TMetadataRequestData request;
            request.Topics.emplace_back().Name = "/alias/valid";
            request.Topics.emplace_back().Name = "/broken";
            std::shared_ptr<const TResolvedKafkaTopics> paths;
            UNIT_ASSERT(!ResolveKafkaRequestPaths(request, fixture.Context, paths).empty());
            UNIT_ASSERT(!paths);
            UNIT_ASSERT_VALUES_EQUAL(*request.Topics[0].Name, "/alias/valid");
            UNIT_ASSERT_VALUES_EQUAL(*request.Topics[1].Name, "/broken");
        }

        Y_UNIT_TEST(ValidateOnlyRetainsExistingOwnersWithoutInterpretingPaths) {
            TFixture fixture({{"^/alias/Topic$", "invalid-relative-target"}});
            TCreateTopicsRequestData create;
            create.ValidateOnly = true;
            create.Topics.emplace_back().Name = "/alias/Topic";
            UNIT_ASSERT(!fixture.Resolve(create));
            UNIT_ASSERT_VALUES_EQUAL(*create.Topics[0].Name, "/alias/Topic");
            TCreatePartitionsRequestData partitions;
            partitions.ValidateOnly = true;
            partitions.Topics.emplace_back().Name = "/alias/Topic";
            UNIT_ASSERT(!fixture.Resolve(partitions));
            UNIT_ASSERT_VALUES_EQUAL(*partitions.Topics[0].Name, "/alias/Topic");
            TAlterConfigsRequestData alter;
            alter.ValidateOnly = true;
            auto& resource = alter.Resources.emplace_back();
            resource.ResourceType = TOPIC_RESOURCE_TYPE;
            resource.ResourceName = "/alias/Topic";
            UNIT_ASSERT(!fixture.Resolve(alter));
            UNIT_ASSERT_VALUES_EQUAL(*resource.ResourceName, "/alias/Topic");
        }

        Y_UNIT_TEST(ConfiguredPhysicalDatabaseIsNotAnAliasOperand) {
            TFixture fixture({{"^/Root$", "/Decoy"}});
            fixture.Context.DatabasePath = "/Root";
            fixture.Context.LogicalDatabasePath = "/Root";
            NKikimrConfig::TPathRewriteConfig config;
            config.AddRules()->SetPattern("^/Root$");
            config.MutableRules(0)->SetReplacement("/Decoy");
            fixture.Context.PathContext = std::make_shared<NKikimr::NPathAliasing::TPathContext>(
                NKikimr::NPathAliasing::TPathNormalizer(config), Nothing());
            TMetadataRequestData request;
            request.Topics.emplace_back().Name = "Topic";
            UNIT_ASSERT_VALUES_EQUAL(fixture.Resolve(request)->at("Topic"), "/Root/Topic");
            UNIT_ASSERT_VALUES_EQUAL(fixture.Context.DatabasePath, "/Root");
            TContext copy(fixture.Context);
            UNIT_ASSERT(copy.PathContext == fixture.Context.PathContext);
            UNIT_ASSERT_VALUES_EQUAL(copy.LogicalDatabasePath, "/Root");
        }
    } // Y_UNIT_TEST_SUITE(KafkaPathAliasing)

} // namespace NKafka
