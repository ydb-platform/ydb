#include "msgbus_server_pq_metacache.h"

#include <ydb/core/base/path.h>
#include <ydb/core/path_aliasing/context/path_context.h>
#include <ydb/core/testlib/tablet_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#include <initializer_list>
#include <utility>

namespace NKikimr::NMsgBusProxy {
    namespace {

        using TEvCache = NPqMetaCacheV2::TEvPqNewMetaCache;
        using TNavigate = NSchemeCache::TSchemeCacheNavigate;

        struct TFixture {
            TTestBasicRuntime Runtime;
            TActorId Schema;
            TActorId Client;
            TActorId Cache;
            std::shared_ptr<const NPathAliasing::TPathContext> Context;
            NPersQueue::TDiscoveryConverterPtr Converter;

            explicit TFixture(std::initializer_list<std::pair<const char*, const char*>> rules) {
                SetupTabletServices(Runtime);
                auto& config = Runtime.GetAppData().PQConfig;
                config.SetEnabled(true);
                config.SetTopicsAreFirstClassCitizen(false);
                config.SetRoot("/Root/PQ");
                config.MutablePQDiscoveryConfig()->SetUseLbAccountAlias(true);
                config.MutablePQDiscoveryConfig()->SetLbUserDatabaseRoot("/Legacy");
                config.MutablePQDiscoveryConfig()->SetLBFrontEnabled(false);
                Schema = Runtime.AllocateEdgeActor();
                Client = Runtime.AllocateEdgeActor();
                Cache = Runtime.Register(NPqMetaCacheV2::CreatePQMetaCache(Schema));
                NKikimrConfig::TPathRewriteConfig aliases;
                for (const auto& [pattern, replacement] : rules) {
                    auto* rule = aliases.AddRules();
                    rule->SetPattern(pattern);
                    rule->SetReplacement(replacement);
                }
                Context = std::make_shared<NPathAliasing::TPathContext>(NPathAliasing::TPathNormalizer(aliases), Nothing());
                NPersQueue::TTopicNamesConverterFactory factory(false, "/Root/PQ", "dc1");
                Converter = factory.MakeDiscoveryConverter("rt3.dc1--account--topic", Nothing());
                UNIT_ASSERT_C(Converter->IsValid(), Converter->GetReason());
                UNIT_ASSERT_VALUES_EQUAL(Converter->GetPrimaryPath(), "/Root/PQ/rt3.dc1--account--topic");
            }

            void Start(bool logical = true) {
                auto request = MakeHolder<TEvCache::TEvDescribeTopicsRequest>(
                    TVector<NPersQueue::TDiscoveryConverterPtr>{Converter}, false);
                if (logical) {
                    request->PathContext = Context;
                }
                // Preserve mailbox order: Bootstrap must precede the request.
                Runtime.Send(new IEventHandle(Cache, Client, request.Release()), 0, true);
            }

            TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr Navigate(size_t expectedSize = 1) {
                auto request = Runtime.GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>(Schema, TDuration::Seconds(5));
                UNIT_ASSERT(request);
                UNIT_ASSERT_VALUES_EQUAL(request->Get()->Request->ResultSet.size(), expectedSize);
                return request;
            }

            void ReplyMissing(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& request) {
                auto result = request->Get()->Request.Release();
                for (auto& entry : result->ResultSet) {
                    entry.Status = TNavigate::EStatus::PathErrorUnknown;
                }
                Runtime.Send(new IEventHandle(request->Sender, Schema,
                                              new TEvTxProxySchemeCache::TEvNavigateKeySetResult(result)));
            }

            TEvCache::TEvDescribeTopicsResponse::TPtr Response() {
                auto response = Runtime.GrabEdgeEvent<TEvCache::TEvDescribeTopicsResponse>(Client, TDuration::Seconds(5));
                UNIT_ASSERT(response);
                return response;
            }
        };

    } // namespace

    Y_UNIT_TEST_SUITE(LegacyPqSchemaPathAliasing) {
        Y_UNIT_TEST(PrimaryFullCandidateRewritesOnceAndRetainsWireTopic) {
            TFixture fixture({{"^/Root/PQ/", "/Physical/PQ/"}, {"^/Physical/", "/Decoy/"}});
            fixture.Start();
            auto primary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(primary->Get()->Request->ResultSet[0].Path),
                                     "/Physical/PQ/rt3.dc1--account--topic");
            fixture.ReplyMissing(primary);
            // A matched primary is terminal, even if its target does not exist.
            auto response = fixture.Response();
            UNIT_ASSERT(response->Get()->PathRewriteError.empty());
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->TopicsRequested.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->TopicsRequested[0]->GetOriginalTopic(), "rt3.dc1--account--topic");
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->RewrittenTopics.size(), 1);
            UNIT_ASSERT(response->Get()->RewrittenTopics.contains(fixture.Converter->GetOriginalPath()));
        }

        Y_UNIT_TEST(PrimaryMissRewritesSecondaryCompleteCandidateAndItsDatabase) {
            TFixture fixture({{"^/Legacy(/|$)", "/Physical\\1"}, {"^/Physical(/|$)", "/Decoy\\1"}});
            fixture.Start();
            auto primary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(primary->Get()->Request->ResultSet[0].Path),
                                     "/Root/PQ/rt3.dc1--account--topic");
            fixture.ReplyMissing(primary);
            auto secondary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(secondary->Get()->Request->ResultSet[0].Path), "/Physical/account/topic");
            UNIT_ASSERT_VALUES_EQUAL(secondary->Get()->Request->DatabaseName, "/Physical/account");
            fixture.ReplyMissing(secondary);
            auto response = fixture.Response();
            UNIT_ASSERT(response->Get()->PathRewriteError.empty());
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->TopicsRequested[0]->GetOriginalTopic(), "rt3.dc1--account--topic");
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->RewrittenTopics.size(), 1);
            UNIT_ASSERT(response->Get()->RewrittenTopics.contains(fixture.Converter->GetOriginalPath()));
        }

        Y_UNIT_TEST(IdentityPrimaryMatchDoesNotFallThroughToSecondaryAlias) {
            TFixture fixture({{"^/Root/PQ/", "/Root/PQ/"}, {"^/Legacy(/|$)", "/Physical\\1"}});
            fixture.Start();
            auto primary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(primary->Get()->Request->ResultSet[0].Path),
                                     "/Root/PQ/rt3.dc1--account--topic");
            fixture.ReplyMissing(primary);
            auto response = fixture.Response();
            UNIT_ASSERT(response->Get()->PathRewriteError.empty());
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->Result->ResultSet[0].Status, TNavigate::EStatus::PathErrorUnknown);
            UNIT_ASSERT(response->Get()->RewrittenTopics.empty());
        }

        Y_UNIT_TEST(InvalidTargetReturnsOrdinaryErrorWithoutNavigation) {
            TFixture fixture({{"^/Root/PQ/", "relative/"}});
            fixture.Start();
            auto response = fixture.Response();
            UNIT_ASSERT(!response->Get()->PathRewriteError.empty());
            UNIT_ASSERT(response->Get()->Result->ResultSet.empty());
        }

        Y_UNIT_TEST(InternalPhysicalRequestsDoNotInferUserProvenance) {
            TFixture fixture({{"^/Root/PQ/", "/Physical/PQ/"}, {"^/Legacy(/|$)", "/Physical\\1"}});
            fixture.Start(false);
            auto primary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(primary->Get()->Request->ResultSet[0].Path),
                                     "/Root/PQ/rt3.dc1--account--topic");
            fixture.ReplyMissing(primary);
            auto secondary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(secondary->Get()->Request->ResultSet[0].Path), "/Legacy/account/topic");
            fixture.ReplyMissing(secondary);
            auto response = fixture.Response();
            UNIT_ASSERT(response->Get()->RewrittenTopics.empty());
        }

        Y_UNIT_TEST(UnmatchedAndDatabaseOnlyRulesDoNotMarkResourceRewritten) {
            for (const char* pattern : {"^/Never$", "^/Legacy/account$"}) {
                TFixture fixture({{pattern, "/Physical/account"}});
                fixture.Start();
                auto primary = fixture.Navigate();
                fixture.ReplyMissing(primary);
                auto secondary = fixture.Navigate();
                UNIT_ASSERT_VALUES_EQUAL(CanonizePath(secondary->Get()->Request->ResultSet[0].Path),
                                         "/Legacy/account/topic");
                fixture.ReplyMissing(secondary);
                const auto response = fixture.Response();
                UNIT_ASSERT(response->Get()->PathRewriteError.empty());
                UNIT_ASSERT(response->Get()->RewrittenTopics.empty());
            }
        }

        Y_UNIT_TEST(SecondaryIdentityDoesNotCreateACorrelationOverride) {
            TFixture fixture({{"^/Legacy/account/topic$", "/Legacy/account/topic"},
                              {"^/Legacy(/|$)", "/Wrong\\1"}});
            fixture.Start();
            auto primary = fixture.Navigate();
            fixture.ReplyMissing(primary);
            auto secondary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(secondary->Get()->Request->ResultSet[0].Path),
                                     "/Legacy/account/topic");
            fixture.ReplyMissing(secondary);
            const auto response = fixture.Response();
            UNIT_ASSERT(response->Get()->PathRewriteError.empty());
            UNIT_ASSERT(response->Get()->RewrittenTopics.empty());
        }

        Y_UNIT_TEST(MixedTopicsKeepPerResourceRewriteMarkersAcrossFallback) {
            TFixture fixture({{"^/Root/PQ/rt3.dc1--account--topic$", "/Physical/renamed"}});
            NPersQueue::TTopicNamesConverterFactory factory(false, "/Root/PQ", "dc1");
            auto unchanged = factory.MakeDiscoveryConverter("rt3.dc1--account--other", Nothing());
            UNIT_ASSERT(unchanged->IsValid());
            auto request = MakeHolder<TEvCache::TEvDescribeTopicsRequest>(
                TVector<NPersQueue::TDiscoveryConverterPtr>{fixture.Converter, unchanged}, false);
            request->PathContext = fixture.Context;
            fixture.Runtime.Send(new IEventHandle(fixture.Cache, fixture.Client, request.Release()), 0, true);
            auto primary = fixture.Navigate(2);
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(primary->Get()->Request->ResultSet[0].Path), "/Physical/renamed");
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(primary->Get()->Request->ResultSet[1].Path),
                                     "/Root/PQ/rt3.dc1--account--other");
            fixture.ReplyMissing(primary);
            auto secondary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(secondary->Get()->Request->ResultSet[0].Path),
                                     "/Legacy/account/other");
            fixture.ReplyMissing(secondary);
            const auto response = fixture.Response();
            UNIT_ASSERT(response->Get()->PathRewriteError.empty());
            UNIT_ASSERT_VALUES_EQUAL(response->Get()->RewrittenTopics.size(), 1);
            UNIT_ASSERT(response->Get()->RewrittenTopics.contains(fixture.Converter->GetOriginalPath()));
            UNIT_ASSERT(!response->Get()->RewrittenTopics.contains(unchanged->GetOriginalPath()));
        }

        Y_UNIT_TEST(ResolvedCdcChildDoesNotAcquireANewRewriteMarker) {
            TFixture fixture({{"^/Root/PQ/", "/Wrong/"}});
            auto request = MakeHolder<TEvCache::TEvDescribeTopicsRequest>(
                TVector<NPersQueue::TDiscoveryConverterPtr>{fixture.Converter}, false);
            request->PathContext = fixture.Context;
            request->ResolvedTopics.insert(fixture.Converter->GetOriginalPath());
            fixture.Runtime.Send(new IEventHandle(fixture.Cache, fixture.Client, request.Release()), 0, true);
            auto primary = fixture.Navigate();
            UNIT_ASSERT_VALUES_EQUAL(CanonizePath(primary->Get()->Request->ResultSet[0].Path),
                                     "/Root/PQ/rt3.dc1--account--topic");
            fixture.ReplyMissing(primary);
            const auto response = fixture.Response();
            UNIT_ASSERT(response->Get()->PathRewriteError.empty());
            UNIT_ASSERT(response->Get()->RewrittenTopics.empty());
        }
    } // Y_UNIT_TEST_SUITE(LegacyPqSchemaPathAliasing)

} // namespace NKikimr::NMsgBusProxy
