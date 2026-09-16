#include <ydb/core/client/server/path_aliasing/path_aliasing.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/flat_scheme_op.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/protos/yql_translation_settings.pb.h>

#include <library/cpp/testing/unittest/registar.h>

#include <initializer_list>
#include <utility>

namespace NKikimr::NMsgBusProxy {
    namespace {

        using namespace NKikimrSchemeOp;

        NPathAliasing::TPathContext Context(std::initializer_list<std::pair<const char*, const char*>> rules = {
                                                {"^/alias(/|$)", "/Root\\1"}, {"^/Root(/|$)", "/Decoy\\1"}})
        {
            NKikimrConfig::TPathRewriteConfig config;
            for (const auto& [pattern, replacement] : rules) {
                auto* rule = config.AddRules();
                rule->SetPattern(pattern);
                rule->SetReplacement(replacement);
            }
            return {NPathAliasing::TPathNormalizer(config), TString("/alias")};
        }

        void Normalize(TModifyScheme& scheme, const NPathAliasing::TPathContext& context = Context()) {
            const auto status = NormalizeMessageBusSchemaPaths(scheme, context);
            UNIT_ASSERT_C(status.IsSuccess(), status.GetErrorMessage());
        }

    } // namespace

    Y_UNIT_TEST_SUITE(MessageBusSchemaPathAliasing) {
        Y_UNIT_TEST(NativeColumnTableTtlStorageReferencesRewriteExactlyOnce) {
            for (bool alter : {false, true}) {
                TModifyScheme scheme;
                scheme.SetOperationType(alter ? ESchemeOpAlterColumnTable : ESchemeOpCreateColumnTable);
                scheme.SetWorkingDir("/alias/store");
                TColumnDataLifeCycle::TTtl* ttl;
                if (alter) {
                    scheme.MutableAlterColumnTable()->SetName("Table");
                    ttl = scheme.MutableAlterColumnTable()->MutableAlterTtlSettings()->MutableEnabled();
                } else {
                    scheme.MutableCreateColumnTable()->SetName("Table");
                    ttl = scheme.MutableCreateColumnTable()->MutableTtlSettings()->MutableEnabled();
                }
                ttl->SetColumnName("/alias/local-column-name");
                for (ui32 i = 0; i < 2; ++i) {
                    auto* tier = ttl->AddTiers();
                    tier->SetApplyAfterSeconds(360 * (i + 1));
                    tier->MutableEvictToExternalStorage()->SetStorage(i == 0 ? "/alias/Cold" : "/alias/Warm");
                }
                auto* deletion = ttl->AddTiers();
                deletion->SetApplyAfterSeconds(1080);
                deletion->MutableDelete();
                Normalize(scheme);
                UNIT_ASSERT_VALUES_EQUAL(scheme.GetWorkingDir(), "/Root/store");
                UNIT_ASSERT_VALUES_EQUAL(ttl->GetColumnName(), "/alias/local-column-name");
                UNIT_ASSERT_VALUES_EQUAL(ttl->TiersSize(), 3);
                UNIT_ASSERT_VALUES_EQUAL(ttl->GetTiers(0).GetEvictToExternalStorage().GetStorage(), "/Root/Cold");
                UNIT_ASSERT_VALUES_EQUAL(ttl->GetTiers(1).GetEvictToExternalStorage().GetStorage(), "/Root/Warm");
                for (ui32 i = 0; i < 3; ++i) {
                    UNIT_ASSERT_VALUES_EQUAL(ttl->GetTiers(i).GetApplyAfterSeconds(), 360 * (i + 1));
                }
                UNIT_ASSERT(ttl->GetTiers(2).HasDelete());
                UNIT_ASSERT(!ttl->GetTiers(2).HasEvictToExternalStorage());
            }
        }

        Y_UNIT_TEST(UnchangedNativeColumnTableTtlPreservesWireBytesAndPresence) {
            const auto disabled = Context({});
            const auto unrelated = Context({{"^/Never$", "/Unused"}});
            const auto identity = Context({{"^/alias(/|$)", "/alias\\1"}, {"^/alias(/|$)", "/Wrong\\1"}});
            for (const auto* context : {&disabled, &unrelated, &identity}) {
                for (bool alter : {false, true}) {
                    TModifyScheme scheme;
                    scheme.SetOperationType(alter ? ESchemeOpAlterColumnTable : ESchemeOpCreateColumnTable);
                    scheme.SetWorkingDir("//alias//store/");
                    const TString absent = scheme.SerializeAsString();
                    Normalize(scheme, *context);
                    UNIT_ASSERT_VALUES_EQUAL(scheme.SerializeAsString(), absent);
                    TColumnDataLifeCycle::TTtl* ttl;
                    if (alter) {
                        scheme.MutableAlterColumnTable()->SetName("Table");
                        ttl = scheme.MutableAlterColumnTable()->MutableAlterTtlSettings()->MutableEnabled();
                    } else {
                        scheme.MutableCreateColumnTable()->SetName("Table");
                        ttl = scheme.MutableCreateColumnTable()->MutableTtlSettings()->MutableEnabled();
                    }
                    ttl->SetColumnName("timestamp");
                    ttl->AddTiers()->MutableEvictToExternalStorage(); // Storage remains absent.
                    auto* populated = ttl->AddTiers();
                    populated->SetApplyAfterSeconds(360);
                    populated->MutableEvictToExternalStorage()->SetStorage("//alias//Cold/");
                    ttl->AddTiers()->MutableDelete();
                    const TString before = scheme.SerializeAsString();
                    Normalize(scheme, *context);
                    UNIT_ASSERT_VALUES_EQUAL(scheme.SerializeAsString(), before);
                }
            }
        }

        Y_UNIT_TEST(NativeStreamingQueryNamesRewriteWithoutTouchingSqlOrProperties) {
            for (auto operation : {ESchemeOpCreateStreamingQuery, ESchemeOpAlterStreamingQuery}) {
                for (const TString& sql : {TString(), TString("INSERT INTO `/alias/Output` SELECT * FROM `/alias/Input`")}) {
                    TModifyScheme scheme;
                    scheme.SetOperationType(operation);
                    scheme.SetWorkingDir("/alias/nested/folder");
                    auto* description = scheme.MutableCreateStreamingQuery();
                    description->SetName("Query");
                    auto* properties = description->MutableProperties();
                    auto& values = *properties->MutableProperties();
                    values["query_text"] = sql;
                    values["__query_text_revision"] = "9";
                    values["resource_pool"] = "/alias/not-a-schema-path";
                    const TString before = properties->SerializeAsString();
                    Normalize(scheme);
                    UNIT_ASSERT_VALUES_EQUAL(scheme.GetWorkingDir(), "/Root/nested/folder");
                    UNIT_ASSERT_VALUES_EQUAL(description->GetName(), "Query");
                    UNIT_ASSERT_VALUES_EQUAL(properties->SerializeAsString(), before);
                }
            }
        }

        Y_UNIT_TEST(NativeViewNameRewritesWithoutTouchingSqlOrCapturedContext) {
            TModifyScheme scheme;
            scheme.SetOperationType(ESchemeOpCreateView);
            scheme.SetWorkingDir("/alias/nested/folder");
            auto* view = scheme.MutableCreateView();
            view->SetName("View");
            view->SetQueryText("SELECT * FROM `/alias/Table` WHERE Value = '/alias/literal'");
            view->MutableCapturedContext()->SetPathPrefix("/alias/sql-prefix");
            view->MutableCapturedContext()->AddPragmas("TablePathPrefix=\"/alias/pragma\"");
            const TString sql = view->GetQueryText();
            const TString captured = view->GetCapturedContext().SerializeAsString();
            Normalize(scheme);
            UNIT_ASSERT_VALUES_EQUAL(scheme.GetWorkingDir(), "/Root/nested/folder");
            UNIT_ASSERT_VALUES_EQUAL(view->GetName(), "View");
            UNIT_ASSERT_VALUES_EQUAL(view->GetQueryText(), sql);
            UNIT_ASSERT_VALUES_EQUAL(view->GetCapturedContext().SerializeAsString(), captured);
        }

        Y_UNIT_TEST(DisabledAndUnrelatedRulesPreserveWireBytesAndPresence) {
            const auto disabled = Context({});
            const auto unrelated = Context({{"^/unrelated(/|$)", "/unused\\1"}});
            for (const auto* context : {&disabled, &unrelated}) {
                for (auto operation : {ESchemeOpCreateTable, ESchemeOpCreateIndexedTable,
                                       ESchemeOpMoveTable, ESchemeOpMoveIndex, ESchemeOpCreateExternalTable,
                                       ESchemeOpCreateReplication, ESchemeOpCreateTransfer, ESchemeOpCreateBackupCollection})
                {
                    TModifyScheme absent;
                    absent.SetOperationType(operation);
                    absent.SetWorkingDir("//Root//legacy/");
                    const TString before = absent.SerializeAsString();
                    Normalize(absent, *context);
                    UNIT_ASSERT_VALUES_EQUAL(absent.SerializeAsString(), before);
                }
                TModifyScheme populated;
                populated.SetOperationType(ESchemeOpCreateTable);
                populated.SetWorkingDir("//Root//legacy/");
                auto* table = populated.MutableCreateTable();
                table->SetName("child//Table");
                table->SetCopyFromTable("//Root//source/");
                table->AddColumns()->SetDefaultFromSequence("/Root/sequence");
                const TString before = populated.SerializeAsString();
                Normalize(populated, *context);
                UNIT_ASSERT_VALUES_EQUAL(populated.SerializeAsString(), before);
            }
        }

        Y_UNIT_TEST(PrimaryObjectIsComposedBeforeExactlyOneRewrite) {
            const auto context = Context({{"^/alias/child/Table$", "/Root/renamed/Result"},
                                          {"^/Root(/|$)", "/Decoy\\1"}});
            TModifyScheme scheme;
            scheme.SetOperationType(ESchemeOpCreateTable);
            scheme.SetWorkingDir("/alias");
            scheme.MutableCreateTable()->SetName("child/Table");
            Normalize(scheme, context);
            UNIT_ASSERT_VALUES_EQUAL(scheme.GetWorkingDir(), "/Root/renamed");
            UNIT_ASSERT_VALUES_EQUAL(scheme.GetCreateTable().GetName(), "Result");
        }

        Y_UNIT_TEST(IdentityPrimaryRulePreservesOriginalSpellingAndStops) {
            const auto context = Context({{"^/alias/child/Table$", "/alias/child/Table"},
                                          {"^/alias", "/Root"}});
            TModifyScheme scheme;
            scheme.SetOperationType(ESchemeOpCreateTable);
            scheme.SetWorkingDir("//alias//");
            scheme.MutableCreateTable()->SetName("child//Table");
            const TString before = scheme.SerializeAsString();
            Normalize(scheme, context);
            UNIT_ASSERT_VALUES_EQUAL(scheme.SerializeAsString(), before);
        }

        Y_UNIT_TEST(RepeatedCopyAndMoveOperandsAreIndependent) {
            TModifyScheme copies;
            copies.SetOperationType(ESchemeOpCreateConsistentCopyTables);
            for (const TString& name : {TString("one"), TString("two")}) {
                auto* copy = copies.MutableCreateConsistentCopyTables()->AddCopyTableDescriptions();
                copy->SetSrcPath("/alias/source/" + name);
                copy->SetDstPath("/alias/target/" + name);
            }
            Normalize(copies);
            for (const auto& copy : copies.GetCreateConsistentCopyTables().GetCopyTableDescriptions()) {
                UNIT_ASSERT(copy.GetSrcPath().StartsWith("/Root/source/"));
                UNIT_ASSERT(copy.GetDstPath().StartsWith("/Root/target/"));
            }
            TModifyScheme move;
            move.SetOperationType(ESchemeOpMoveTable);
            move.MutableMoveTable()->SetSrcPath("/alias/old");
            move.MutableMoveTable()->SetDstPath("/alias/new");
            Normalize(move);
            UNIT_ASSERT_VALUES_EQUAL(move.GetMoveTable().GetSrcPath(), "/Root/old");
            UNIT_ASSERT_VALUES_EQUAL(move.GetMoveTable().GetDstPath(), "/Root/new");
        }

        Y_UNIT_TEST(TableSecondaryPathsKeepOriginalLogicalParent) {
            TModifyScheme scheme;
            scheme.SetOperationType(ESchemeOpCreateTable);
            scheme.SetWorkingDir("/alias/directory");
            auto* table = scheme.MutableCreateTable();
            table->SetName("Table");
            table->SetCopyFromTable("/alias/source");
            table->AddColumns()->SetDefaultFromSequence("existing");
            table->AddColumns()->SetDefaultFromSequence("declared");
            table->AddSequences()->SetName("declared");
            table->MutableTTLSettings()->MutableEnabled()->AddTiers()->MutableEvictToExternalStorage()->SetStorage("/alias/storage");
            Normalize(scheme);
            UNIT_ASSERT_VALUES_EQUAL(scheme.GetWorkingDir(), "/Root/directory");
            UNIT_ASSERT_VALUES_EQUAL(table->GetCopyFromTable(), "/Root/source");
            UNIT_ASSERT_VALUES_EQUAL(table->GetColumns(0).GetDefaultFromSequence(), "/Root/directory/existing");
            UNIT_ASSERT_VALUES_EQUAL(table->GetColumns(1).GetDefaultFromSequence(), "declared");
            UNIT_ASSERT_VALUES_EQUAL(table->GetSequences(0).GetName(), "declared");
            UNIT_ASSERT_VALUES_EQUAL(table->GetTTLSettings().GetEnabled().GetTiers(0).GetEvictToExternalStorage().GetStorage(), "/Root/storage");
        }

        Y_UNIT_TEST(IndexedTableDeclaredSequencesAreLocalNames) {
            TModifyScheme indexed;
            indexed.SetOperationType(ESchemeOpCreateIndexedTable);
            indexed.SetWorkingDir("/alias");
            auto* creation = indexed.MutableCreateIndexedTable();
            creation->MutableTableDescription()->SetName("indexed");
            creation->MutableTableDescription()->AddColumns()->SetDefaultFromSequence("declared");
            creation->AddSequenceDescription()->SetName("declared");
            Normalize(indexed);
            UNIT_ASSERT_VALUES_EQUAL(creation->GetTableDescription().GetColumns(0).GetDefaultFromSequence(), "declared");
            UNIT_ASSERT_VALUES_EQUAL(creation->GetSequenceDescription(0).GetName(), "declared");
        }

        Y_UNIT_TEST(BackupMembersAndNamespacePrefixAreIndependentCompletePaths) {
            TModifyScheme scheme;
            scheme.SetOperationType(ESchemeOpCreateBackupCollection);
            scheme.SetWorkingDir("/alias");
            auto* backup = scheme.MutableCreateBackupCollection();
            backup->SetName("collection");
            backup->SetPrefix("/alias/prefix");
            backup->MutableExplicitEntryList()->AddEntries()->SetPath("/alias/one");
            backup->MutableExplicitEntryList()->AddEntries()->SetPath("/alias/two");
            backup->MutableExplicitEntryList()->AddEntries()->SetPath("legacy-relative");
            Normalize(scheme);
            UNIT_ASSERT_VALUES_EQUAL(backup->GetPrefix(), "/Root/prefix");
            UNIT_ASSERT_VALUES_EQUAL(backup->GetExplicitEntryList().GetEntries(0).GetPath(), "/Root/one");
            UNIT_ASSERT_VALUES_EQUAL(backup->GetExplicitEntryList().GetEntries(1).GetPath(), "/Root/two");
            UNIT_ASSERT_VALUES_EQUAL(backup->GetExplicitEntryList().GetEntries(2).GetPath(), "legacy-relative");
        }

        Y_UNIT_TEST(ExternalPayloadAndLocationsAreNotSchemaPaths) {
            TModifyScheme scheme;
            scheme.SetOperationType(ESchemeOpCreateExternalTable);
            scheme.SetWorkingDir("/alias");
            auto* table = scheme.MutableCreateExternalTable();
            table->SetName("external");
            table->SetDataSourcePath("/alias/source");
            table->SetLocation("/alias/object/key");
            table->SetContent("/alias/data-is-not-a-path");
            table->AddColumns()->SetName("/alias/column-name");
            Normalize(scheme);
            UNIT_ASSERT_VALUES_EQUAL(table->GetDataSourcePath(), "/Root/source");
            UNIT_ASSERT_VALUES_EQUAL(table->GetLocation(), "/alias/object/key");
            UNIT_ASSERT_VALUES_EQUAL(table->GetContent(), "/alias/data-is-not-a-path");
            UNIT_ASSERT_VALUES_EQUAL(table->GetColumns(0).GetName(), "/alias/column-name");
        }

        Y_UNIT_TEST(ReplicationOnlyRewritesLocalDestinationPaths) {
            TModifyScheme scheme;
            scheme.SetOperationType(ESchemeOpCreateReplication);
            scheme.SetWorkingDir("/alias");
            scheme.MutableReplication()->SetName("replication");
            auto* config = scheme.MutableReplication()->MutableConfig();
            config->MutableSrcConnectionParams()->SetEndpoint("/alias/endpoint");
            config->MutableSrcConnectionParams()->SetDatabase("/alias/remote-db");
            auto* target = config->MutableSpecific()->AddTargets();
            target->SetSrcPath("/alias/remote-table");
            target->SetSrcStreamName("/alias/stream-name");
            target->SetDstPath("/alias/local-table");
            Normalize(scheme);
            UNIT_ASSERT_VALUES_EQUAL(config->GetSrcConnectionParams().GetEndpoint(), "/alias/endpoint");
            UNIT_ASSERT_VALUES_EQUAL(config->GetSrcConnectionParams().GetDatabase(), "/alias/remote-db");
            UNIT_ASSERT_VALUES_EQUAL(target->GetSrcPath(), "/alias/remote-table");
            UNIT_ASSERT_VALUES_EQUAL(target->GetSrcStreamName(), "/alias/stream-name");
            UNIT_ASSERT_VALUES_EQUAL(target->GetDstPath(), "/Root/local-table");
        }

        Y_UNIT_TEST(AclAndLocalIndexNamesRemainOpaque) {
            TModifyScheme acl;
            acl.SetOperationType(ESchemeOpModifyACL);
            acl.SetWorkingDir("/alias");
            acl.MutableModifyACL()->SetName("object");
            acl.MutableModifyACL()->SetDiffACL("/alias/principal-and-acl-bytes");
            Normalize(acl);
            UNIT_ASSERT_VALUES_EQUAL(acl.GetWorkingDir(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(acl.GetModifyACL().GetDiffACL(), "/alias/principal-and-acl-bytes");
            TModifyScheme index;
            index.SetOperationType(ESchemeOpMoveIndex);
            index.MutableMoveIndex()->SetTablePath("/alias/Table");
            index.MutableMoveIndex()->SetSrcPath("/alias/local-index-name");
            index.MutableMoveIndex()->SetDstPath("/alias/renamed-index-name");
            Normalize(index);
            UNIT_ASSERT_VALUES_EQUAL(index.GetMoveIndex().GetTablePath(), "/Root/Table");
            UNIT_ASSERT_VALUES_EQUAL(index.GetMoveIndex().GetSrcPath(), "/alias/local-index-name");
            UNIT_ASSERT_VALUES_EQUAL(index.GetMoveIndex().GetDstPath(), "/alias/renamed-index-name");
        }

        Y_UNIT_TEST(RootCandidateIsNotConfusedWithMissingOperand) {
            const auto context = Context({{"^/$", "/Root"}});
            TModifyScheme scheme;
            scheme.SetOperationType(ESchemeOpMoveTable);
            scheme.MutableMoveTable()->SetSrcPath("///");
            scheme.MutableMoveTable()->SetDstPath("");
            Normalize(scheme, context);
            UNIT_ASSERT_VALUES_EQUAL(scheme.GetMoveTable().GetSrcPath(), "/Root");
            UNIT_ASSERT_VALUES_EQUAL(scheme.GetMoveTable().GetDstPath(), "");
        }

        Y_UNIT_TEST(InvalidChangedTargetsFailWithoutThrowing) {
            for (const char* replacement : {"", "relative", "/Root/../escape", "/Root/./object", "/"}) {
                const auto context = Context({{"^/alias/object$", replacement}});
                TModifyScheme scheme;
                scheme.SetOperationType(ESchemeOpMkDir);
                scheme.SetWorkingDir("/alias");
                scheme.MutableMkDir()->SetName("object");
                UNIT_ASSERT(NormalizeMessageBusSchemaPaths(scheme, context).IsFail());
            }
        }
    } // Y_UNIT_TEST_SUITE(MessageBusSchemaPathAliasing)

} // namespace NKikimr::NMsgBusProxy
