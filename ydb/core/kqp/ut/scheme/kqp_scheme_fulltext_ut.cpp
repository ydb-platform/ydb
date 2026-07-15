#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/testlib/common_helper.h>
#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

Y_UNIT_TEST_SUITE(KqpSchemeFulltext) {

    TStatus ExecuteSchemeQuery(TKikimrRunner& kikimr, const TString& query, bool useQueryClient) {
        if (useQueryClient) {
            auto db = kikimr.GetQueryClient();
            return db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        } else {
            auto db = kikimr.GetTableClient();
            auto session = db.CreateSession().GetValueSync().GetSession();
            return session.ExecuteSchemeQuery(query).ExtractValueSync();
        }
    }

    Y_UNIT_TEST_TWIN(CreateTableWithIndex, UseQueryClient) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        {
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    Data String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext_plain
                        ON (Text)
                        COVER (Data)
                        WITH (tokenizer=whitespace, use_filter_lowercase=true)
                );
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(CreateTableWithIndexPublicApi) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            TFulltextIndexSettings indexSettings;

            TFulltextIndexSettings::TAnalyzers analyzers;
            analyzers.Tokenizer = TFulltextIndexSettings::ETokenizer::Whitespace;
            analyzers.UseFilterLowercase = true;

            TFulltextIndexSettings::TColumnAnalyzers columnAnalyzers;
            columnAnalyzers.Column = "Text";
            columnAnalyzers.Analyzers = analyzers;
            indexSettings.Columns.push_back(columnAnalyzers);
            auto builder = TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Text", EPrimitiveType::String)
                .AddNullableColumn("Data", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key")
                .AddFulltextIndex("fulltext_idx", {"Text"}, {"Data"}, indexSettings);

            auto result = session.CreateTable("/Root/TestTable", builder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetIndexDescriptions().size(), 1);

            auto indexDesc = result.GetTableDescription().GetIndexDescriptions()[0];
            Cout << indexDesc.ToString() << Endl;
            // { name: "fulltext_idx", type: GlobalFulltextPlain, index_columns: [Text], data_columns: [Data],
            //     fulltext_settings: { layout: flat, columns: [{ column: Text,
            //         analyzers: { tokenizer: whitespace, use_filter_lowercase: true } }] } }
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexName(), "fulltext_idx");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexType(), EIndexType::GlobalFulltextPlain);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns()[0], "Text");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns()[0], "Data");
            auto indexSettings = std::get<TFulltextIndexSettings>(indexDesc.GetIndexSettings());
            UNIT_ASSERT_VALUES_EQUAL(indexSettings.Columns.size(), 1);
            auto columnSettings = indexSettings.Columns[0];
            UNIT_ASSERT_VALUES_EQUAL(columnSettings.Column, "Text");
            UNIT_ASSERT_VALUES_EQUAL(columnSettings.Analyzers->Tokenizer.value_or(TFulltextIndexSettings::ETokenizer::Unspecified), TFulltextIndexSettings::ETokenizer::Whitespace);
            UNIT_ASSERT_VALUES_EQUAL(columnSettings.Analyzers->UseFilterLowercase.value_or(false), true);
            UNIT_ASSERT(!columnSettings.Analyzers->UseFilterLength.has_value());
        }
    }

    Y_UNIT_TEST_TWIN(AlterTableWithIndex, UseQueryClient) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
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
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    COVER (Data)
                    WITH (tokenizer=whitespace, use_filter_lowercase=true)
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterTableWithIndexPublicApi) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        {
            auto builder = TTableBuilder()
                .AddNullableColumn("Key", EPrimitiveType::Uint64)
                .AddNullableColumn("Text", EPrimitiveType::String)
                .AddNullableColumn("Data", EPrimitiveType::String)
                .SetPrimaryKeyColumn("Key");

            auto result = session.CreateTable("/Root/TestTable", builder.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TFulltextIndexSettings indexSettings;

            TFulltextIndexSettings::TAnalyzers analyzers;
            analyzers.Tokenizer = TFulltextIndexSettings::ETokenizer::Whitespace;
            analyzers.UseFilterLowercase = true;

            TFulltextIndexSettings::TColumnAnalyzers columnAnalyzers;
            columnAnalyzers.Column = "Text";
            columnAnalyzers.Analyzers = analyzers;
            indexSettings.Columns.push_back(columnAnalyzers);

            TAlterTableSettings alterSettings;
            alterSettings.AppendAddIndexes({ "fulltext_idx", EIndexType::GlobalFulltextPlain, {"Text"}, {"Data"}, {}, indexSettings });

            auto result = session.AlterTable("/Root/TestTable", alterSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            auto result = session.DescribeTable("/Root/TestTable").ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetTableDescription().GetIndexDescriptions().size(), 1);

            auto indexDesc = result.GetTableDescription().GetIndexDescriptions()[0];
            Cout << indexDesc.ToString() << Endl;
            // { name: "fulltext_idx", type: GlobalFulltextPlain, index_columns: [Text], data_columns: [Data],
            //     fulltext_settings: { layout: flat, columns: [{ column: Text,
            //         analyzers: { tokenizer: whitespace, use_filter_lowercase: true } }] } }
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexName(), "fulltext_idx");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexType(), EIndexType::GlobalFulltextPlain);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetIndexColumns()[0], "Text");
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns().size(), 1);
            UNIT_ASSERT_VALUES_EQUAL(indexDesc.GetDataColumns()[0], "Data");
            auto indexSettings = std::get<TFulltextIndexSettings>(indexDesc.GetIndexSettings());
            UNIT_ASSERT_VALUES_EQUAL(indexSettings.Columns.size(), 1);
            auto columnSettings = indexSettings.Columns[0];
            UNIT_ASSERT_VALUES_EQUAL(columnSettings.Column, "Text");
            UNIT_ASSERT_VALUES_EQUAL(columnSettings.Analyzers->Tokenizer.value_or(TFulltextIndexSettings::ETokenizer::Unspecified), TFulltextIndexSettings::ETokenizer::Whitespace);
            UNIT_ASSERT_VALUES_EQUAL(columnSettings.Analyzers->UseFilterLowercase.value_or(false), true);
            UNIT_ASSERT(!columnSettings.Analyzers->UseFilterLength.has_value());
        }
    }

    Y_UNIT_TEST_TWIN(CreateTableWithIndexNoFeatureFlag, UseQueryClient) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(false);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        {
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext_plain
                        ON (Text)
                        WITH (tokenizer=whitespace, use_filter_lowercase=true)
                );
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Fulltext index support is disabled");
        }
    }

    Y_UNIT_TEST_TWIN(AlterTableWithIndexNoFeatureFlag, UseQueryClient) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(false);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
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
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        {
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    WITH (tokenizer=whitespace, use_filter_lowercase=true)
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Fulltext index support is disabled");
        }
    }

    Y_UNIT_TEST_TWIN(CreateTableWithIndexCaseInsensitive, UseQueryClient) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        {
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING FULLtext_PLAIN
                        ON (Text)
                        WITH (tokenIZER=WHITEspace, use_FILTER_lowercase=TRUE)
                );
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_TWIN(CreateTableWithIndexInvalidSettings, UseQueryClient) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        { // tokenizer=asdf
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext_plain
                        ON (Text)
                        WITH (tokenizer=asdf, use_filter_lowercase=true)
                );
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid tokenizer: asdf");
        }
        { // no tokenizer
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext_plain
                        ON (Text)
                        WITH (use_filter_lowercase=true)
                );
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "tokenizer should be set");
        }
        { // no WITH section
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text String,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext_plain
                        ON (Text)
                );
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "column analyzers should be set");
        }
    }

    Y_UNIT_TEST_TWIN(AlterTableWithIndexInvalidSettings, UseQueryClient) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
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
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }
        { // tokenizer=asdf
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    WITH (tokenizer=asdf, use_filter_lowercase=true)
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Invalid tokenizer: asdf");
        }
        { // no tokenizer
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
                    WITH (use_filter_lowercase=true)
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "tokenizer should be set");
        }
        { // no WITH section
            TString query = R"(
                --!syntax_v1
                ALTER TABLE `/Root/TestTable` ADD INDEX fulltext_idx
                    GLOBAL USING fulltext_plain
                    ON (Text)
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "columns should be set");
        }
    }

    Y_UNIT_TEST_TWIN(CreateTableWithIndexInvalidTextType, UseQueryClient) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableFulltextIndex(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        TKikimrRunner kikimr(settings);
        {
            TString query = R"(
                --!syntax_v1
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text Uint64,
                    PRIMARY KEY (Key),
                    INDEX fulltext_idx
                        GLOBAL USING fulltext_plain
                        ON (Text)
                        WITH (tokenizer=whitespace, use_filter_lowercase=true)
                );
            )";
            auto result = ExecuteSchemeQuery(kikimr, query, UseQueryClient);
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Error: Fulltext column 'Text' expected type 'String' or 'Utf8' but got Uint64");
        }
    }

}

}
