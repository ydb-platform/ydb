#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpSchemeFulltext) {
    
    Y_UNIT_TEST(CreateTableWithIndex) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    Data String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext
                        ON (Text)
                        COVER (Data)
                        WITH (layout=flat, tokenizer=whitespace, use_filter_lowercase=true)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            // TDescribeTableResult describe = session.DescribeTable("/Root/TestTable").GetValueSync();
            // UNIT_ASSERT_EQUAL_C(describe.GetStatus(), EStatus::SUCCESS, describe.GetIssues().ToString());
            // auto indexes = describe.GetTableDescription().GetIndexDescriptions();
            // UNIT_ASSERT_VALUES_EQUAL(indexes.size(), 1);
            // Ydb::Table::TableIndex index;
            // indexes.at(0).SerializeTo(index);

            // UNIT_ASSERT_VALUES_EQUAL(index.GetIndexName(), "fulltext_idx");

            // // // UNIT_ASSERT_VALUES_EQUAL(index.GetIndexColumns(), "Text");
            // // // UNIT_ASSERT_VALUES_EQUAL(index.GetDataColumns(), "Data");

            // // no sdk support yet:
            // UNIT_ASSERT_VALUES_EQUAL(index.GetIndexType(), EIndexType::Unknown); 
            // UNIT_ASSERT(std::holds_alternative<std::monostate>(index.GetIndexSettings()));
        }
    }

    Y_UNIT_TEST(CreateTableWithIndexNoFeatureFlag) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(false);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TString create_index_query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext
                        ON (Text)
                        WITH (layout=flat, tokenizer=whitespace, use_filter_lowercase=true)
                );
            )";
            auto result = session.ExecuteSchemeQuery(create_index_query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Fulltext index support is disabled");
        }
    }

}

}
