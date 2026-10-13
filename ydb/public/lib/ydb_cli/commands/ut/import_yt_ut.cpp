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
    } // Y_UNIT_TEST_SUITE(ImportYtSource)

} // namespace NYdb::NConsoleClient
