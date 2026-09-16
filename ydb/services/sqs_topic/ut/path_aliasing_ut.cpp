#include <ydb/services/sqs_topic/actor.h>
#include <ydb/services/sqs_topic/consumer_attributes.h>
#include <ydb/services/sqs_topic/queue_url/arn.h>
#include <ydb/services/persqueue_v1/actors/schema/common/grpc_proxy_actor.h>
#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/rpc_calls_topic.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/testlib/helpers.h>

#include <library/cpp/testing/unittest/registar.h>
#include <initializer_list>
#include <utility>

namespace NKikimr::NSqsTopic::V1 {
    namespace {

        using TRpc = NGRpcService::TEvCreateTopicRequest;
        using TRequest = NRpcService::TLocalRpcCtx<TRpc, std::function<void(const TRpc::TResponse&)>>;

        struct TObservation {
            bool Accepted = false;
            TString Path;
            TString Database;
            ui32 Replies = 0;
            Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
        };

        struct TEvDone: NActors::TEventLocal<TEvDone, NActors::TEvents::ES_PRIVATE + 7423> {};

        class TQueueUrlProbe: public TGrpcActorBase<TQueueUrlProbe, TRpc> {
            using TBase = TGrpcActorBase<TQueueUrlProbe, TRpc>;

        public:
            TQueueUrlProbe(NGRpcService::IRequestOpCtx* request, NActors::TActorId edge,
                           std::shared_ptr<TObservation> observation)
                : TBase(request, "/Root/a/source")
                , Edge(edge)
                , Observation(std::move(observation))
            {
            }

            void Bootstrap(const NActors::TActorContext& ctx) {
                Observation->Accepted = ResolveQueueUrlPath(Observation->Path, Observation->Database);
                if (Observation->Accepted) {
                    this->Request_->ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
                }
                ctx.Send(Edge, new TEvDone);
                if (Observation->Accepted) {
                    this->Die(ctx);
                }
            }

            ui64 GetRUCost() override {
                return 0;
            }

        private:
            const NActors::TActorId Edge;
            const std::shared_ptr<TObservation> Observation;
        };

        class TFederationDlqProbe: public NGRpcProxy::V1::TGrpcProxyActor<TFederationDlqProbe, TRpc> {
            using TBase = NGRpcProxy::V1::TGrpcProxyActor<TFederationDlqProbe, TRpc>;

        public:
            TFederationDlqProbe(NGRpcService::IRequestOpCtx* request, NActors::TActorId edge,
                                std::shared_ptr<TObservation> observation)
                : TBase(request)
                , Edge(edge)
                , Observation(std::move(observation))
            {
            }

            void Bootstrap(const NActors::TActorContext& ctx) {
                Observation->Accepted = ResolveDeadLetterQueue(Observation->Path);
                if (Observation->Accepted) {
                    this->Request_->ReplyWithYdbStatus(Ydb::StatusIds::SUCCESS);
                }
                ctx.Send(Edge, new TEvDone);
                if (Observation->Accepted) {
                    this->Die(ctx);
                }
            }

        private:
            const NActors::TActorId Edge;
            const std::shared_ptr<TObservation> Observation;
        };

        class TFixture {
        public:
            TFixture(std::initializer_list<std::pair<const char*, const char*>> rules) {
                Runtime.Initialize({new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr),
                                    nullptr, nullptr,
                                    {},
                                    {}});
                NKikimrConfig::TPathRewriteConfig config;
                for (const auto& [pattern, replacement] : rules) {
                    auto* rule = config.AddRules();
                    rule->SetPattern(pattern);
                    rule->SetReplacement(replacement);
                }
                auto& app = Runtime.GetAppData();
                app.PathNormalizer = std::make_shared<NPathAliasing::TPathNormalizer>(config);
                app.PQConfig.SetTopicsAreFirstClassCitizen(false);
                app.PQConfig.SetRoot("/Root/PQ");
                app.PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root");
            }

            THolder<TRequest> Request(const TString& database, const std::shared_ptr<TObservation>& observation) {
                auto request = MakeHolder<TRequest>(TRpc::TRequest{}, [observation](const TRpc::TResponse& response) {
                    ++observation->Replies;
                    observation->Status = response.operation().status();
                }, database, Nothing(), Nothing(), false);
                request->SetPathRewriteSettings(NGRpcService::TPathRewriteSettings::UserInput());
                UNIT_ASSERT(request->InitializePathRewriteContext(Runtime.GetAppData()).empty());
                return request;
            }

            template <class TProbe>
            void Run(const TString& database, const std::shared_ptr<TObservation>& observation) {
                const auto edge = Runtime.AllocateEdgeActor();
                auto request = Request(database, observation);
                Runtime.Register(new TProbe(request.Release(), edge, observation));
                const auto done = Runtime.GrabEdgeEvent<TEvDone>(edge, TDuration::Seconds(5));
                UNIT_ASSERT(done);
                UNIT_ASSERT_VALUES_EQUAL(observation->Replies, 1);
            }

            TTestActorRuntime Runtime;
        };

        google::protobuf::Map<TString, TString> RedrivePolicy(const TString& database, const TString& topic) {
            const TString arn = MakeQueueArn(false, "region", "account", TRichQueueUrl{.Database = database, .TopicPath = topic, .Consumer = "consumer", .Fifo = false});
            google::protobuf::Map<TString, TString> attributes;
            attributes["RedrivePolicy"] = TStringBuilder()
                                          << "{\"deadLetterTargetArn\":\"" << arn << "\",\"maxReceiveCount\":3}";
            return attributes;
        }

    } // namespace

    Y_UNIT_TEST_SUITE(DeadLetterQueuePathAliasing) {
        Y_UNIT_TEST(UnrelatedRuleNeverRebasesCompleteCrossDatabaseArn) {
            for (auto usage : {EConsumerAttributeUsageTarget::Create, EConsumerAttributeUsageTarget::Alter}) {
                TFixture fixture({{"^/never", "/unused"}});
                auto request = fixture.Request("/Root/a", std::make_shared<TObservation>());
                for (const TString& database : {TString("/Root/b"), TString("/Root/ab")}) {
                    const auto attributes = RedrivePolicy(database, "topic");
                    UNIT_ASSERT(!ParseQueueAttributes(attributes, "source", "consumer", "/Root/a", usage).has_value());
                    UNIT_ASSERT(!ParseQueueAttributes(attributes, "source", "consumer", "/Root/a", usage, request.Get()).has_value());
                }
            }
        }

        Y_UNIT_TEST(ExplicitArnAliasIntoEffectiveDatabaseIsAppliedOnce) {
            TFixture fixture({{"^/Root/b/topic$", "/Root/a/dlq"}, {"^/Root/a/dlq$", "/Root/a/decoy"}});
            auto request = fixture.Request("/Root/a", std::make_shared<TObservation>());
            const auto attributes = RedrivePolicy("/Root/b", "topic");
            const TString raw = attributes.at("RedrivePolicy");
            const auto result = ParseQueueAttributes(attributes, "source", "consumer", "/Root/a",
                                                     EConsumerAttributeUsageTarget::Create, request.Get());
            UNIT_ASSERT_C(result.has_value(), result.has_value() ? TString() : TString(result.error()));
            UNIT_ASSERT_VALUES_EQUAL(result->DeadLetterQueue.GetRef(), "dlq");
            UNIT_ASSERT_VALUES_EQUAL(result->MaxReceiveCount.GetRef(), 3);
            UNIT_ASSERT_VALUES_EQUAL(attributes.at("RedrivePolicy"), raw);
        }

        Y_UNIT_TEST(UnrelatedRulesPreserveCreateQueueIdempotenceForStoredRelativeDlq) {
            TFixture fixture({{"^/never", "/unused"}});
            auto request = fixture.Request("/Root/a", std::make_shared<TObservation>());
            const auto attributes = RedrivePolicy("/Root/a", "dlq");
            const auto parsed = ParseQueueAttributes(attributes, "source", "consumer", "/Root/a",
                                                     EConsumerAttributeUsageTarget::Create, request.Get());
            UNIT_ASSERT(parsed.has_value());
            NKikimrPQ::TPQTabletConfig existingConfig;
            NKikimrPQ::TPQTabletConfig::TConsumer existingConsumer;
            existingConsumer.SetDeadLetterQueue("dlq");
            existingConsumer.SetMaxProcessingAttempts(3);
            const auto equivalent = CompareWithExistingQueueAttributes(existingConfig, existingConsumer, *parsed);
            UNIT_ASSERT_C(equivalent.has_value(), equivalent.has_value() ? TString() : TString(equivalent.error()));
            UNIT_ASSERT_VALUES_EQUAL(parsed->DeadLetterQueue.GetRef(), "dlq");
        }

        Y_UNIT_TEST_TWIN(UnchangedArnKeepsLegacyDatabaseAndTopicSpelling, IdentityRule) {
            TFixture fixture(IdentityRule
                                 ? std::initializer_list<std::pair<const char*, const char*>>{{"^/Root/a/dlq$", "/Root/a/dlq"}}
                                 : std::initializer_list<std::pair<const char*, const char*>>{{"^/never", "/unused"}});
            const TString database = "//Root//a/";
            auto request = fixture.Request(database, std::make_shared<TObservation>());
            const auto matching = ParseQueueAttributes(RedrivePolicy(database, "dlq//"), "source", "consumer", database,
                                                       EConsumerAttributeUsageTarget::Create, request.Get());
            UNIT_ASSERT(matching.has_value());
            UNIT_ASSERT_VALUES_EQUAL(matching->DeadLetterQueue.GetRef(), "dlq//");
            const auto attributes = RedrivePolicy("/Root/a", "dlq");
            const auto oldMismatch = ParseQueueAttributes(attributes, "source", "consumer", database,
                                                          EConsumerAttributeUsageTarget::Create);
            const auto mismatch = ParseQueueAttributes(attributes, "source", "consumer", database,
                                                       EConsumerAttributeUsageTarget::Create, request.Get());
            UNIT_ASSERT(!oldMismatch.has_value());
            UNIT_ASSERT(!mismatch.has_value());
            UNIT_ASSERT_VALUES_EQUAL(mismatch.error(), oldMismatch.error());
        }

        Y_UNIT_TEST_TWIN(UnchangedFederationDlqRetainsLegacyRootResolution, IdentityRule) {
            TFixture fixture(IdentityRule
                                 ? std::initializer_list<std::pair<const char*, const char*>>{{"^/Root/LbCommunal/account/topic$", "/Root/LbCommunal/account/topic"}}
                                 : std::initializer_list<std::pair<const char*, const char*>>{{"^/never", "/unused"}});
            fixture.Runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/LbCommunal");
            auto observed = std::make_shared<TObservation>();
            observed->Path = "account/topic";
            fixture.Run<TFederationDlqProbe>("/Root/PQ", observed);
            UNIT_ASSERT(observed->Accepted);
            UNIT_ASSERT_VALUES_EQUAL(observed->Path, "account/topic");
        }

        Y_UNIT_TEST(IdentityArnRuleOutsideDatabaseCannotFallThrough) {
            TFixture fixture({{"^/Root/b/topic$", "/Root/b/topic"}, {"^/Root/b/topic$", "/Root/a/dlq"}});
            auto request = fixture.Request("/Root/a", std::make_shared<TObservation>());
            UNIT_ASSERT(!ParseQueueAttributes(RedrivePolicy("/Root/b", "topic"), "source", "consumer", "/Root/a",
                                              EConsumerAttributeUsageTarget::Alter, request.Get())
                             .has_value());
        }

        Y_UNIT_TEST(CompleteQueueUrlMissRejectsWithoutMutatingUrlComponents) {
            TFixture fixture({{"^/never", "/unused"}});
            auto observed = std::make_shared<TObservation>();
            observed->Path = "/Root/b/topic";
            observed->Database = "/Root/b";
            fixture.Run<TQueueUrlProbe>("/Root/a", observed);
            UNIT_ASSERT(!observed->Accepted);
            UNIT_ASSERT_VALUES_EQUAL(observed->Path, "/Root/b/topic");
            UNIT_ASSERT_VALUES_EQUAL(observed->Database, "/Root/b");
        }

        Y_UNIT_TEST_TWIN(UnchangedQueueUrlRetainsRawDatabaseSpelling, IdentityRule) {
            TFixture fixture(IdentityRule
                                 ? std::initializer_list<std::pair<const char*, const char*>>{{"^/Root/a/dlq$", "/Root/a/dlq"}}
                                 : std::initializer_list<std::pair<const char*, const char*>>{{"^/never", "/unused"}});
            auto observed = std::make_shared<TObservation>();
            observed->Path = "/Root/a/dlq";
            observed->Database = "//Root//a/";
            fixture.Run<TQueueUrlProbe>("/Root/a", observed);
            UNIT_ASSERT(observed->Accepted);
            UNIT_ASSERT_VALUES_EQUAL(observed->Path, "/Root/a/dlq");
            UNIT_ASSERT_VALUES_EQUAL(observed->Database, "//Root//a/");
        }

        Y_UNIT_TEST(CompleteQueueUrlAliasPublishesOneResolvedIdentity) {
            TFixture fixture({{"^/Root/b/topic$", "/Root/a/dlq"}, {"^/Root/a/dlq$", "/Root/a/decoy"}});
            auto observed = std::make_shared<TObservation>();
            observed->Path = "/Root/b/topic";
            observed->Database = "/Root/b";
            fixture.Run<TQueueUrlProbe>("/Root/a", observed);
            UNIT_ASSERT(observed->Accepted);
            UNIT_ASSERT_VALUES_EQUAL(observed->Path, "/Root/a/dlq");
            UNIT_ASSERT_VALUES_EQUAL(observed->Database, "/Root/a");
        }

        Y_UNIT_TEST(FederationDlqRejectsOutOfDatabaseResolvedPathBeforeForwarding) {
            TFixture fixture({{"^/Root/account/dlq$", "/Other/dlq"}});
            auto observed = std::make_shared<TObservation>();
            observed->Path = "dlq";
            fixture.Run<TFederationDlqProbe>("/Root/account", observed);
            UNIT_ASSERT(!observed->Accepted);
            UNIT_ASSERT_VALUES_EQUAL(observed->Status, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(observed->Path, "dlq");
        }

        Y_UNIT_TEST(FederationDlqRejectsTargetThatDownstreamWouldReinterpret) {
            TFixture fixture({{"^/Root/LbCommunal/account/topic$", "/Root/PQ/account/target"}});
            fixture.Runtime.GetAppData().PQConfig.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Root/LbCommunal");
            auto observed = std::make_shared<TObservation>();
            observed->Path = "account/topic";
            fixture.Run<TFederationDlqProbe>("/Root/PQ", observed);
            UNIT_ASSERT(!observed->Accepted);
            UNIT_ASSERT_VALUES_EQUAL(observed->Status, Ydb::StatusIds::BAD_REQUEST);
            UNIT_ASSERT_VALUES_EQUAL(observed->Path, "account/topic");
        }

        Y_UNIT_TEST(FederationDlqAllowsContainedAliasAndDoesNotChain) {
            TFixture fixture({{"^/Root/account/dlq$", "/Root/account/target"},
                              {"^/Root/account/target$", "/Other/decoy"}});
            auto observed = std::make_shared<TObservation>();
            observed->Path = "dlq";
            fixture.Run<TFederationDlqProbe>("/Root/account", observed);
            UNIT_ASSERT(observed->Accepted);
            UNIT_ASSERT_VALUES_EQUAL(observed->Path, "/Root/account/target");
        }
    } // Y_UNIT_TEST_SUITE(DeadLetterQueuePathAliasing)

} // namespace NKikimr::NSqsTopic::V1
