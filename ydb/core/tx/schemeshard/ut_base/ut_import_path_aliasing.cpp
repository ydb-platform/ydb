#include <ydb/core/path_aliasing/path_normalizer.h>
#include <ydb/core/tx/schemeshard/schemeshard_info_types.h>
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>

#include <initializer_list>
#include <utility>

namespace NKikimr::NSchemeShard {
    namespace {

        using namespace NSchemeShardUT_Private;

        struct TFixture {
            TSchemeShard* SchemeShard = nullptr;
            TTestBasicRuntime Runtime;
            THolder<TTestEnv> Env;
            std::shared_ptr<const NPathAliasing::TPathNormalizer> Normalizer;

            TFixture(std::initializer_list<std::pair<const char*, const char*>> rules = {
                         {"^/alias(/|$)", "/MyRoot\\1"}, {"^/MyRoot(/|$)", "/MyRoot/Decoy\\1"}})
            {
                auto factory = [this](const TActorId& tablet, TTabletStorageInfo* info) {
                    SchemeShard = new TSchemeShard(tablet, info);
                    return SchemeShard;
                };
                Env = MakeHolder<TTestEnv>(Runtime, TTestEnvOptions{}, factory);
                NKikimrConfig::TPathRewriteConfig config;
                for (const auto& [pattern, replacement] : rules) {
                    auto* rule = config.AddRules();
                    rule->SetPattern(pattern);
                    rule->SetReplacement(replacement);
                }
                Normalizer = std::make_shared<NPathAliasing::TPathNormalizer>(config);
                Runtime.RunCall([&] {
                    AppData()->PathNormalizer = Normalizer;
                    return true;
                });
            }

            TImportInfo::TPtr Import(const TString& settingsText = {}, const TString& mapping = R"({
        "exportedObjects": {"dir/Table": {"exportPrefix": "/alias/remote-object-key"}}
    })") {
                Ydb::Import::ImportFromS3Settings settings;
                UNIT_ASSERT(NProtoBuf::TextFormat::ParseFromString(settingsText, &settings));
                const auto domain = TPath::Resolve("/MyRoot", SchemeShard).Base()->PathId;
                TImportInfo::TPtr import = new TImportInfo(42, "uid_42", TImportInfo::EKind::S3, settings, domain, "localhost");
                import->LogicalDatabase = "/alias";
                import->PathRewriteFingerprint = Normalizer->GetFingerprint();
                import->State = TImportInfo::EState::DownloadExportMetadata;
                for (const auto& item : settings.items()) {
                    auto& target = import->Items.emplace_back(item.destination_path());
                    target.SrcPath = item.source_path();
                    target.SrcPrefix = item.source_prefix();
                }
                import->SchemaMapping.ConstructInPlace();
                TString error;
                UNIT_ASSERT_C(import->SchemaMapping->Deserialize(mapping, error), error);
                return import;
            }

            TImportInfo::TFillItemsFromSchemaMappingResult Fill(const TImportInfo::TPtr& import) {
                return Runtime.RunCall([&] { return import->FillItemsFromSchemaMapping(SchemeShard); });
            }

            void Success(const TImportInfo::TPtr& import) {
                const auto result = Fill(import);
                UNIT_ASSERT_C(result.Success, result.ErrorMessage);
            }
        };

    } // namespace

    Y_UNIT_TEST_SUITE(ImportPathAliasing) {
        Y_UNIT_TEST(DefaultDestinationUsesOriginalLogicalDatabaseOnce) {
            TFixture fixture;
            const auto import = fixture.Import();
            fixture.Success(import);
            UNIT_ASSERT_VALUES_EQUAL(import->Items.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].DstPathName, "/MyRoot/dir/Table");
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].SrcPath, "dir/Table");
            // Schema mapping strips a leading slash; path aliases must not rewrite remote keys.
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].SrcPrefix, "alias/remote-object-key");
        }

        Y_UNIT_TEST(CommonRootRelativeItemAndManifestSuffixComposeBeforeMatch) {
            TFixture fixture({{"^/alias/common/renamed/Table$", "/MyRoot/ExactTarget"},
                              {"^/MyRoot(/|$)", "/MyRoot/Decoy\\1"}});
            const auto import = fixture.Import(R"(
            destination_path: "/alias/common"
            items { source_path: "dir" destination_path: "renamed" }
        )");
            fixture.Success(import);
            UNIT_ASSERT_VALUES_EQUAL(import->Items.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].DstPathName, "/MyRoot/ExactTarget");
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].SrcPrefix, "alias/remote-object-key");
        }

        Y_UNIT_TEST(ExplicitAbsoluteItemDoesNotJoinCommonDestination) {
            TFixture fixture({{"^/shortcut(/|$)", "/MyRoot\\1"},
                              {"^/MyRoot(/|$)", "/MyRoot/Decoy\\1"}});
            const auto import = fixture.Import(R"(
            destination_path: "/alias/ignored"
            items { source_path: "dir/Table" destination_path: "/shortcut/Explicit" }
        )");
            fixture.Success(import);
            UNIT_ASSERT_VALUES_EQUAL(import->Items.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].DstPathName, "/MyRoot/Explicit");
        }

        Y_UNIT_TEST(DuplicatePhysicalDestinationsFailBeforePublishingItems) {
            TFixture fixture({{"^/alias/(one|two)$", "/MyRoot/Same"}});
            const auto import = fixture.Import(R"(
            items { source_path: "first" destination_path: "/alias/one" }
            items { source_path: "second" destination_path: "/alias/two" }
        )", R"({"exportedObjects": {
            "first": {"exportPrefix": "remote-one"}, "second": {"exportPrefix": "remote-two"}
        }})");
            const auto result = fixture.Fill(import);
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.ErrorMessage, "Duplicate resolved import destination");
            UNIT_ASSERT_VALUES_EQUAL(import->Items.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].DstPathName, "/alias/one");
            UNIT_ASSERT_VALUES_EQUAL(import->Items[1].DstPathName, "/alias/two");
        }

        Y_UNIT_TEST(InvalidChangedTargetReturnsErrorAndKeepsLogicalItems) {
            TFixture fixture({{"^/alias(/|$)", "/MyRoot/../escape\\1"}});
            const auto import = fixture.Import(R"(
            items { source_path: "dir/Table" destination_path: "/alias/Table" }
        )");
            const auto result = fixture.Fill(import);
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT(!result.ErrorMessage.empty());
            UNIT_ASSERT_VALUES_EQUAL(import->Items.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].DstPathName, "/alias/Table");
        }

        Y_UNIT_TEST(ConfigurationMismatchBeforeManifestResolutionFailsClosed) {
            TFixture fixture;
            const auto import = fixture.Import(R"(
            items { source_path: "dir/Table" destination_path: "/alias/Table" }
        )");
            fixture.Runtime.RunCall([] {
                AppData()->PathNormalizer.reset();
                return true;
            });
            const auto result = fixture.Fill(import);
            UNIT_ASSERT(!result.Success);
            UNIT_ASSERT_STRING_CONTAINS(result.ErrorMessage, "configuration changed");
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].DstPathName, "/alias/Table");
        }

        Y_UNIT_TEST(LegacyImportWithoutCapturedContextDoesNotUseCurrentRules) {
            TFixture fixture;
            const auto import = fixture.Import();
            import->LogicalDatabase.clear();
            import->PathRewriteFingerprint.clear();
            fixture.Success(import);
            UNIT_ASSERT_VALUES_EQUAL(import->Items.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(import->Items[0].DstPathName, "/MyRoot/dir/Table");
        }
    } // Y_UNIT_TEST_SUITE(ImportPathAliasing)

} // namespace NKikimr::NSchemeShard
