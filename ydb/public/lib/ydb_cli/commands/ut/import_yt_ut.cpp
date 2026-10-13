#include <ydb/public/lib/ydb_cli/commands/ydb_service_import.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NYdb::NConsoleClient {
    namespace {

        class TTestImportCommand: public TCommandImportFromS3 {
        public:
            void AddItem(const TString& source, const TString& destination) {
                auto& item = Items.emplace_back();
                item.Source = source;
                item.Destination = destination;
            }
        };

        NImport::TImportFromS3Settings ParseImportSettings(std::initializer_list<TString> options) {
            TVector<TString> args = {
                "s3", "--s3-endpoint", "s3.invalid", "--bucket", "test-bucket",
                "--access-key", "test-access-key", "--secret-key", "test-secret-key",
                "--item", "src=backups/table_2026,dst=/Root/table"
            };
            args.insert(args.end(), options.begin(), options.end());
            TVector<char*> argv;
            for (auto& arg : args) {
                argv.push_back(const_cast<char*>(arg.c_str()));
            }
            TTestImportCommand command;
            TClientCommand::TConfig config(argv.size(), argv.data());
            // Prepare runs Config, real option parsing and Parse without starting an RPC.
            command.Prepare(config);
            return command.MakeImportSettings();
        }

    } // namespace

    Y_UNIT_TEST_SUITE(ImportYtSource) {
        Y_UNIT_TEST(PreservesExactPrefixesWithoutS3Discovery) {
            TTestImportCommand command;
            command.AddItem("backups/table", "/Root/table");
            command.AddItem("backups/with_under_scores.", "/Root/other");
            command.AddItem("backups/trailing/", "/Root/third");

            NImport::TImportFromS3Settings settings;
            settings.SourceFormat(NImport::EImportSourceFormat::Yt);
            command.FillItemsFromItemParam(settings);

            UNIT_ASSERT_VALUES_EQUAL(settings.Item_.size(), 3);
            UNIT_ASSERT_VALUES_EQUAL(settings.Item_[0].Src, "backups/table");
            UNIT_ASSERT_VALUES_EQUAL(settings.Item_[0].Dst, "/Root/table");
            UNIT_ASSERT_VALUES_EQUAL(settings.Item_[1].Src, "backups/with_under_scores.");
            UNIT_ASSERT_VALUES_EQUAL(settings.Item_[2].Src, "backups/trailing/");
        }

        Y_UNIT_TEST(ParsesExplicitRowDestination) {
            const auto settings = ParseImportSettings({"--source-format", "yt", "--destination-store", "row"});
            UNIT_ASSERT(settings.SourceFormat_ == NImport::EImportSourceFormat::Yt);
            UNIT_ASSERT(settings.DestinationStoreType_.has_value());
            UNIT_ASSERT(*settings.DestinationStoreType_ == NImport::EImportDestinationStoreType::Row);
            UNIT_ASSERT_VALUES_EQUAL(settings.Endpoint_, "s3.invalid");
            UNIT_ASSERT_VALUES_EQUAL(settings.AccessKey_, "test-access-key");
            UNIT_ASSERT_VALUES_EQUAL(settings.SecretKey_, "test-secret-key");
            UNIT_ASSERT_VALUES_EQUAL(settings.Item_.size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(settings.Item_[0].Src, "backups/table_2026");
            UNIT_ASSERT_VALUES_EQUAL(settings.Item_[0].Dst, "/Root/table");
        }

        Y_UNIT_TEST(ParsesExplicitColumnDestination) {
            const auto settings = ParseImportSettings({"--source-format", "yt", "--destination-store", "column"});
            UNIT_ASSERT(settings.SourceFormat_ == NImport::EImportSourceFormat::Yt);
            UNIT_ASSERT(settings.DestinationStoreType_.has_value());
            UNIT_ASSERT(*settings.DestinationStoreType_ == NImport::EImportDestinationStoreType::Column);
        }

        Y_UNIT_TEST(AbsentDestinationLeavesOptionalEmpty) {
            const auto yt = ParseImportSettings({"--source-format", "yt"});
            UNIT_ASSERT(yt.SourceFormat_ == NImport::EImportSourceFormat::Yt);
            UNIT_ASSERT(!yt.DestinationStoreType_.has_value());
        }

        Y_UNIT_TEST(RejectsDestinationForNativeSource) {
            UNIT_ASSERT_EXCEPTION(ParseImportSettings({"--source-format", "ydb", "--destination-store", "row"}), TMisuseException);
            UNIT_ASSERT_EXCEPTION(ParseImportSettings({"--source-format", "ydb", "--destination-store", "column"}), TMisuseException);
            UNIT_ASSERT_EXCEPTION(ParseImportSettings({"--destination-store", "column"}), TMisuseException);
        }

        Y_UNIT_TEST(RejectsInvalidDestination) {
            UNIT_ASSERT_EXCEPTION(ParseImportSettings({"--source-format", "yt", "--destination-store", "invalid"}), TMisuseException);
            UNIT_ASSERT_EXCEPTION(ParseImportSettings({"--source-format", "yt", "--destination-store", ""}), TMisuseException);
        }
    } // Y_UNIT_TEST_SUITE(ImportYtSource)

} // namespace NYdb::NConsoleClient
