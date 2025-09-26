#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

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
    }

    Y_UNIT_TEST(AlterTableWithIndex) {
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
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext
                    ON (Text)
                    COVER (Data)
                    WITH (layout=flat, tokenizer=whitespace, use_filter_lowercase=true)
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            // TODO: implement build index
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
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
            TString query = R"(
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
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Fulltext index support is disabled");
        }
    }

    Y_UNIT_TEST(AlterTableWithIndexNoFeatureFlag) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(false);
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
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext
                    ON (Text)
                    WITH (layout=flat, tokenizer=whitespace, use_filter_lowercase=true)
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Fulltext index support is disabled");
        }
    }

    Y_UNIT_TEST(CreateTableWithIndexCaseInsensitive) {
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
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING FULLtext
                        ON (Text)
                        WITH (layOUT=FLat, tokenIZER=WHITEspace, use_FILTER_lowercase=TRUE)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableWithIndexInvalidSettings) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        { // tokenizer=asdf
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext
                        ON (Text)
                        WITH (layout=flat, tokenizer=asdf, use_filter_lowercase=true)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "<main>:10:54: Error: Invalid tokenizer: asdf");
        }
        { // no tokenizer
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext
                        ON (Text)
                        WITH (layout=flat, use_filter_lowercase=true)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "<main>:9:29: Error: tokenizer should be set");
        }
        { // no WITH section
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext
                        ON (Text)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "<main>:9:29: Error: layout should be set");
        }
    }

    Y_UNIT_TEST(AlterTableWithIndexInvalidSettings) {
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
                    PRIMARY KEY (Key)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        { // tokenizer=asdf
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext
                    ON (Text)
                    WITH (layout=flat, tokenizer=asdf, use_filter_lowercase=true)
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "<main>:6:50: Error: Invalid tokenizer: asdf");
        }
        { // no tokenizer
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext
                    ON (Text)
                    WITH (layout=flat, use_filter_lowercase=true)
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "<main>:5:25: Error: tokenizer should be set");
        }
        { // no WITH section
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext
                    ON (Text)
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "<main>:5:25: Error: layout should be set");
        }
    }

    Y_UNIT_TEST(CreateTableWithIndexInvalidTextType) {
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
                    Text Uint64,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext
                        ON (Text)
                        WITH (layout=flat, tokenizer=whitespace, use_filter_lowercase=true)
                );
            )";
            auto result = session.ExecuteSchemeQuery(query).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Error: Fulltext column 'Text' expected type 'String' but got Uint64");
        }
    }

}

}
