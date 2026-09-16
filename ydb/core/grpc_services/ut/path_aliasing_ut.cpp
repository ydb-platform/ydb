#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/core/grpc_services/rpc_common/rpc_common.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <initializer_list>
#include <utility>

namespace NKikimr::NGRpcService {
    namespace {

        struct TRpc {
            using TRequest = Ydb::Scheme::MakeDirectoryRequest;
            using TResponse = Ydb::Scheme::MakeDirectoryResponse;
            static constexpr bool IsOp = true;
        };

        class TFixture {
        public:
            TFixture(const TString& database, std::initializer_list<std::pair<const char*, const char*>> rules)
                : Request(TRpc::TRequest{}, [](const TRpc::TResponse&) {}, database, Nothing(), Nothing(), false)
            {
                NKikimrConfig::TPathRewriteConfig config;
                for (const auto& [pattern, replacement] : rules) {
                    auto* rule = config.AddRules();
                    rule->SetPattern(pattern);
                    rule->SetReplacement(replacement);
                }
                App.PathNormalizer = std::make_shared<NPathAliasing::TPathNormalizer>(config);
                Request.SetPathRewriteSettings(TPathRewriteSettings::UserInput());
                UNIT_ASSERT(Request.InitializePathRewriteContext(App).empty());
            }

            void Expect(const TString& input, const TString& expected, NPathAliasing::EPathRewriteOutcome outcome) {
                const auto result = ResolveTopicSchemaPath(Request, input);
                UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
                UNIT_ASSERT_VALUES_EQUAL(result->Path, expected);
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(outcome));
            }

        public:
            TAppData App{0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr};
            NRpcService::TLocalRpcCtx<TRpc, std::function<void(const TRpc::TResponse&)>> Request;
        };

    } // namespace

    Y_UNIT_TEST_SUITE(RootSchemaPathAliasingAdapter) {
        Y_UNIT_TEST(SingleComponentAbsoluteAliasResolvesBeforeContainment) {
            TFixture fixture("/Root", {{"^/object$", "/Root/Table"},
                                       {"^/Root/Table$", "/Root/Wrong"}});
            const auto path = SplitRootSchemaPath(fixture.Request, "/object", true);
            UNIT_ASSERT_VALUES_EQUAL(path.first, "/Root");
            UNIT_ASSERT_VALUES_EQUAL(path.second, "Table");
        }

        Y_UNIT_TEST(SingleComponentAliasStillValidatesTargetAndDatabase) {
            for (const char* target : {"/Other/Table", "/RootSibling/Table", "/Root", "relative/Table", ""}) {
                TFixture fixture("/Root", {{"^/object$", target}});
                UNIT_ASSERT_EXCEPTION(SplitRootSchemaPath(fixture.Request, "/object", true), yexception);
            }
        }

        Y_UNIT_TEST(UnchangedSingleComponentOperandsKeepLegacyRejection) {
            TFixture disabled("/Root", {});
            TFixture unrelated("/Root", {{"^/Never$", "/Root/Table"}});
            TFixture identity("/Root", {{"^/object$", "/object"},
                                        {"^/object$", "/Root/Table"}});
            for (TFixture* fixture : {&disabled, &unrelated, &identity}) {
                for (const char* path : {"/object", "object", "", "/"}) {
                    UNIT_ASSERT_EXCEPTION(SplitPath(fixture->Request.GetDatabaseName(), path), yexception);
                    UNIT_ASSERT_EXCEPTION(SplitRootSchemaPath(fixture->Request, path, true), yexception);
                }
            }
        }

        Y_UNIT_TEST(AbsoluteAliasDoesNotInventRelativeOrEmptyInputSyntax) {
            TFixture fixture("/Root", {{"^/object$", "/Root/Table"},
                                       {"^/$", "/Root/Table"}});
            UNIT_ASSERT_EXCEPTION(SplitRootSchemaPath(fixture.Request, "object", true), yexception);
            UNIT_ASSERT_EXCEPTION(SplitRootSchemaPath(fixture.Request, "", true), yexception);
        }

        Y_UNIT_TEST(UnmatchedLegacyRootSpellingsKeepOwnersSplit) {
            TFixture fixture("/Root", {{"^/Never$", "/Root/Table"}});
            for (const char* path : {"/Root/Table", "Root/Table", "//Root//Table/"}) {
                const auto baseline = SplitPath(fixture.Request.GetDatabaseName(), path);
                const auto aliased = SplitRootSchemaPath(fixture.Request, path, true);
                UNIT_ASSERT_VALUES_EQUAL(aliased.first, baseline.first);
                UNIT_ASSERT_VALUES_EQUAL(aliased.second, baseline.second);
            }
        }
    } // Y_UNIT_TEST_SUITE(RootSchemaPathAliasingAdapter)

    Y_UNIT_TEST_SUITE(TopicPathAliasingAdapter) {
        using NPathAliasing::EPathRewriteOutcome;

        Y_UNIT_TEST(FstClassRepeatedSlashCandidateMatchesCanonicalRuleOnce) {
            TFixture fixture("/Root", {{"^/Root/dir/Topic$", "/Root/Actual"},
                                       {"^/Root/Actual$", "/Root/Wrong"}});
            const auto result = ResolveFstClassTopicSchemaPath(fixture.Request, "dir//Topic");
            UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
            UNIT_ASSERT_VALUES_EQUAL(result->Path, "/Root/Actual");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(EPathRewriteOutcome::Rewritten));
        }

        Y_UNIT_TEST(FstClassUnchangedRulesPreserveRepeatedSlashOwnerSpelling) {
            for (bool identity : {false, true}) {
                TFixture fixture("/Root//Sub", identity
                                                   ? std::initializer_list<std::pair<const char*, const char*>>{
                                                         {"^/Root/Sub/dir/Topic$", "/Root/Sub/dir/Topic"},
                                                         {"^/Root/Sub/dir/Topic$", "/Root/Sub/Wrong"}}
                                                   : std::initializer_list<std::pair<const char*, const char*>>{{"^/Never$", "/Unused"}});
                const auto result = ResolveFstClassTopicSchemaPath(fixture.Request, "dir//Topic");
                UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
                UNIT_ASSERT_VALUES_EQUAL(result->Path, "/Root//Sub/dir//Topic");
                UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(
                                                                                identity ? EPathRewriteOutcome::Identity : EPathRewriteOutcome::NoMatch));
            }
        }

        Y_UNIT_TEST(FstClassChangedTargetRejectsDownstreamDatabaseReinterpretation) {
            TFixture fixture("/Root//Sub", {{"^/alias/Topic$", "/Root/Sub/Actual"}});
            const auto result = ResolveFstClassTopicSchemaPath(fixture.Request, "/alias/Topic");
            // The legacy converter would prepend raw /Root//Sub to the
            // canonical replacement and silently select a different object.
            UNIT_ASSERT(result.IsFail());
        }

        Y_UNIT_TEST(FullTopicOwnerRetainsOptionalDatabaseAndAbsolutePathGrammar) {
            for (const TString& database : {TString(), TString("/Root")}) {
                for (ui32 mode = 0; mode < 3; ++mode) {
                    TFixture fixture(database, mode == 0
                                                   ? std::initializer_list<std::pair<const char*, const char*>>{}
                                               : mode == 1
                                                   ? std::initializer_list<std::pair<const char*, const char*>>{
                                                         {"^/Never(/|$)", "/Unused\\1"}}
                                                   : std::initializer_list<std::pair<const char*, const char*>>{{"^(.*)$", "\\1"}, {"^(.*)$", "/Wrong"}});
                    for (const TString& path : {TString("/Topic"), TString("/RootSibling/T"),
                                                TString("Topic"), TString("RootSibling/T"),
                                                TString("/Root//T/")}) {
                        const auto expected = NPersQueue::GetFullTopicPath(fixture.Request.GetLogicalDatabaseName(), path);
                        const auto result = ResolveFullTopicSchemaPath(fixture.Request, path);
                        UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
                        UNIT_ASSERT_VALUES_EQUAL(NPersQueue::GetFullTopicPath(
                                                     fixture.Request.GetDatabaseName(), result->Path), expected);
                        UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(
                                                                                        mode == 2 ? EPathRewriteOutcome::Identity : EPathRewriteOutcome::NoMatch));
                    }
                }
            }
        }

        Y_UNIT_TEST(FullTopicOwnerMatchesLogicalCompleteOperandOnce) {
            TFixture fixture("/alias", {{"^/alias/Table$", "/Root/Actual"},
                                        {"^/alias$", "/Root"},
                                        {"^/Root/Actual$", "/Root/Wrong"}});
            const auto result = ResolveFullTopicSchemaPath(fixture.Request, "Table");
            UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
            UNIT_ASSERT_VALUES_EQUAL(result->Path, "/Root/Actual");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(EPathRewriteOutcome::Rewritten));
        }

        Y_UNIT_TEST(FstClassWriterMissingDatabaseKeepsConfiguredOwnerDefault) {
            TFixture fixture("", {{"^/Root/Topic$", "/Root/Actual"},
                                  {"^/Root$", "/WrongConfiguredDatabase"},
                                  {"^/Root/Actual$", "/Root/Wrong"}});
            UNIT_ASSERT(!fixture.Request.GetLogicalDatabaseName().Defined());
            UNIT_ASSERT(!fixture.Request.GetDatabaseName().Defined());
            const auto result = ResolveFstClassTopicSchemaPath(fixture.Request, "Topic", "/Root");
            UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
            UNIT_ASSERT_VALUES_EQUAL(result->Path, "/Root/Actual");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(EPathRewriteOutcome::Rewritten));
            // Other owners retain their own default instead of inheriting the
            // writer's /Root merely because rewriting is configured.
            const auto withoutDefault = ResolveFstClassTopicSchemaPath(fixture.Request, "Topic");
            UNIT_ASSERT_C(withoutDefault.IsSuccess(), withoutDefault.IsFail() ? withoutDefault.GetErrorMessage() : TString());
            UNIT_ASSERT_VALUES_EQUAL(withoutDefault->Path, "/Topic");
        }

        Y_UNIT_TEST(DisabledRulesLeaveLegacyParsingToOwner) {
            TFixture fixture("/Root", {});
            for (const TString& path : {TString(), TString("Topic"), TString("/Topic"), TString("//Root//Topic/")}) {
                fixture.Expect(path, path, EPathRewriteOutcome::NoMatch);
            }
        }

        Y_UNIT_TEST(UnrelatedRulesRetainLeadingSlashRelativeInterpretation) {
            TFixture fixture("/Root", {{"^/elsewhere(/|$)", "/target\\1"}});
            fixture.Expect("/Topic", "/Root/Topic", EPathRewriteOutcome::NoMatch);
            fixture.Expect("Topic", "/Root/Topic", EPathRewriteOutcome::NoMatch);
            fixture.Expect("/Root/Topic", "/Root/Topic", EPathRewriteOutcome::NoMatch);
            fixture.Expect("", "", EPathRewriteOutcome::NoMatch);
        }

        Y_UNIT_TEST(ExplicitAbsoluteAliasWinsBeforeLegacyDatabaseJoin) {
            TFixture fixture("/Root", {{"^/alias(/|$)", "/Root\\1"}});
            fixture.Expect("/alias/Topic", "/Root/Topic", EPathRewriteOutcome::Rewritten);
        }

        Y_UNIT_TEST(OnlyAMissTriesTheLegacyLogicalCandidate) {
            TFixture fixture("/Root", {{"^/Root/legacy(/|$)", "/Root/Actual\\1"}});
            fixture.Expect("/legacy/Topic", "/Root/Actual/Topic", EPathRewriteOutcome::Rewritten);
        }

        Y_UNIT_TEST(IdentityMatchDoesNotFallThroughToLegacyAlias) {
            TFixture fixture("/Root", {
                                          {"^/alias(/|$)", "/alias\\1"},
                                          {"^/Root/alias(/|$)", "/Root/Actual\\1"},
                                      });
            const auto result = ResolveTopicSchemaPath(fixture.Request, "/alias/Topic");
            UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
            UNIT_ASSERT_VALUES_EQUAL(result->Path, "/Root/alias/Topic");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(EPathRewriteOutcome::Identity));
        }

        Y_UNIT_TEST(RelativeTopicUsesOriginalDatabaseNamespace) {
            TFixture fixture("/alias", {
                                           {"^/alias/Topic$", "/Root/Actual"},
                                           {"^/alias$", "/Root"},
                                       });
            fixture.Expect("Topic", "/Root/Actual", EPathRewriteOutcome::Rewritten);
            UNIT_ASSERT_VALUES_EQUAL(*fixture.Request.GetDatabaseName(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(*fixture.Request.GetLogicalDatabaseName(), "/alias");
        }

        Y_UNIT_TEST(PhysicalTargetIsNotNormalizedASecondTime) {
            TFixture fixture("/Root", {
                                          {"^/alias(/|$)", "/Root\\1"},
                                          {"^/Root/Topic$", "/Root/Decoy"},
                                      });
            fixture.Expect("/alias/Topic", "/Root/Topic", EPathRewriteOutcome::Rewritten);
            fixture.Expect("/alias/Topic", "/Root/Topic", EPathRewriteOutcome::Rewritten);
            auto settings = fixture.Request.GetPathRewriteSettings();
            settings.Resources = EPathInputOrigin::Resolved;
            fixture.Request.SetPathRewriteSettings(settings);
            fixture.Expect("/Root/Topic", "/Root/Topic", EPathRewriteOutcome::NoMatch);
        }

        Y_UNIT_TEST(ChangedTargetMustRemainInEffectiveDatabase) {
            for (const char* replacement : {"/Other/Topic", "/RootSibling/Topic", "relative/Topic"}) {
                TFixture fixture("/Root", {{"^/alias/Topic$", replacement}});
                const auto result = ResolveTopicSchemaPath(fixture.Request, "/alias/Topic");
                UNIT_ASSERT(result.IsFail());
                UNIT_ASSERT(!result.GetErrorMessage().empty());
            }
        }

        Y_UNIT_TEST(FstClassOwnerKeepsItsLegacyGrammarOnMissAndIdentity) {
            for (bool identity : {false, true}) {
                TFixture fixture("/Root", identity
                                              ? std::initializer_list<std::pair<const char*, const char*>>{
                                                    {"^/Root(/|$)", "/Root\\1"}, {"^/Root(/|$)", "/Wrong\\1"}}
                                              : std::initializer_list<std::pair<const char*, const char*>>{{"^/Never(/|$)", "/Unused\\1"}});
                for (const char* path : {"RootSibling/Topic", "/RootSibling/Topic"}) {
                    const auto result = ResolveFstClassTopicSchemaPath(fixture.Request, path);
                    UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
                    UNIT_ASSERT_VALUES_EQUAL(result->Path, "/Root/Sibling/Topic");
                    UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(
                                                                                    identity ? EPathRewriteOutcome::Identity : EPathRewriteOutcome::NoMatch));
                }
            }
        }

        Y_UNIT_TEST(FstClassExplicitAliasIsMatchedBeforeItsLegacyCandidate) {
            TFixture fixture("/Root", {{"^/outside/Topic$", "/Root/Actual"},
                                       {"^/Root/Actual$", "/Root/Wrong"}});
            const auto result = ResolveFstClassTopicSchemaPath(fixture.Request, "/outside/Topic");
            UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
            UNIT_ASSERT_VALUES_EQUAL(result->Path, "/Root/Actual");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(EPathRewriteOutcome::Rewritten));
        }

        Y_UNIT_TEST(FstClassIdentityPreventsFallbackToAnotherAlias) {
            TFixture fixture("/Root", {{"^/outside/Topic$", "/outside/Topic"},
                                       {"^/Root/outside/Topic$", "/Root/Actual"}});
            const auto result = ResolveFstClassTopicSchemaPath(fixture.Request, "/outside/Topic");
            UNIT_ASSERT_C(result.IsSuccess(), result.IsFail() ? result.GetErrorMessage() : TString());
            UNIT_ASSERT_VALUES_EQUAL(result->Path, "/Root/outside/Topic");
            UNIT_ASSERT_VALUES_EQUAL(static_cast<int>(result->Outcome), static_cast<int>(EPathRewriteOutcome::Identity));
        }
    } // Y_UNIT_TEST_SUITE(TopicPathAliasingAdapter)

} // namespace NKikimr::NGRpcService
