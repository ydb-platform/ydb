#include "s3_recipe_ut_helpers.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace NTestUtils;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpFederatedSchemeTest) {
    Y_UNIT_TEST(ExplainExternalTableLocationValidation) {
        auto kikimr = NTestUtils::MakeKikimrRunner();
        NScripting::TScriptingClient client(kikimr->GetDriver());
        const auto result = client.ExplainYqlScript(R"sql(
            CREATE EXTERNAL TABLE validated_table (data String NOT NULL) WITH (
                DATA_SOURCE="not_created_yet", LOCATION="missing/", FORMAT="raw", VALIDATE_EXTERNAL="true"
            );
        )sql", NScripting::TExplainYqlRequestSettings().Mode(NScripting::ExplainYqlRequestMode::Plan)).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT(!kikimr->GetSchemeClient().DescribePath("/Root/validated_table").GetValueSync().IsSuccess());
    }

    Y_UNIT_TEST_TWIN(ValidateExternalTable, UseQueryService) {
        const TString bucket = UseQueryService ? "validate-location-query" : "validate-location-scheme";
        CreateBucketWithObject(bucket, "data/file.json", TEST_CONTENT);
        UploadObject(bucket, "empty/", "");
        UploadObject(bucket, "nested/child/file.json", TEST_CONTENT);
        UploadObject(bucket, "empty-file", "");

        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto queryClient = kikimr->GetQueryClient();
        auto session = kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto execute = [&](const TString& sql) -> NYdb::TStatus {
            if constexpr (UseQueryService) {
                return queryClient.ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
            } else {
                return session.ExecuteSchemeQuery(sql).GetValueSync();
            }
        };
        auto result = execute(fmt::format(R"sql(
            CREATE EXTERNAL DATA SOURCE source WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="{}", AUTH_METHOD="NONE"
            );
        )sql", GetBucketLocation(bucket)));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        size_t id = 0;
        auto create = [&](const TString& location, const TString& validation, bool success, const TString& error = "") {
            const TString name = TStringBuilder() << "table_" << id++;
            auto result = execute(fmt::format(R"sql(
                CREATE EXTERNAL TABLE {name} (data String NOT NULL) WITH (
                    DATA_SOURCE="source", LOCATION="{location}", FORMAT="raw"{validation}
                );
            )sql", "name"_a = name, "location"_a = location,
                "validation"_a = validation.empty() ? TString{} : TStringBuilder() << ", VALIDATE_EXTERNAL=\"" << validation << "\""));
            UNIT_ASSERT_VALUES_EQUAL_C(result.IsSuccess(), success, result.GetIssues().ToString());
            if (!success) {
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), error);
            }
            const auto description = kikimr->GetSchemeClient().DescribePath("/Root/" + name).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(description.IsSuccess(), success, description.GetIssues().ToString());
        };

        for (const TString location : {"/data/file.json", "data/*.json", "data/", "nested/", "empty/", "empty-file", "/"}) {
            create(location, "true", true);
        }
        for (const TString location : {"/missing/", "data/missing.json", "data/*.csv", "data/file"}) {
            create(location, "true", false, "Location does not exist");
        }
        create("new-output/", "", true);
        create("new-output/", "false", true);
        create("/", "yes", false, "VALIDATE_EXTERNAL must be 'true' or 'false'");
        create("", "true", false, "Cannot read from empty path");
        create("data/{", "true", false, "Invalid LOCATION");
        // Disabling remote checks must still reject invalid local definitions.
        create("data/{", "false", false, "invalid wildcard");

        if constexpr (UseQueryService) {
            result = execute(R"sql(
                CREATE EXTERNAL TABLE IF NOT EXISTS table_0 (data String NOT NULL) WITH (
                    DATA_SOURCE="source", LOCATION="missing/", FORMAT="raw", VALIDATE_EXTERNAL="true"
                );
            )sql");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const TString emptyBucket = bucket + "-empty";
        CreateBucket(emptyBucket);
        result = execute(fmt::format(R"sql(
            CREATE EXTERNAL DATA SOURCE empty_source WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="{}", AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE empty_bucket_table (data String NOT NULL) WITH (
                DATA_SOURCE="empty_source", LOCATION="/", FORMAT="raw", VALIDATE_EXTERNAL="true"
            );
        )sql", GetBucketLocation(emptyBucket)));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(ValidateExternalTableMissingBucket, UseQueryService) {
        const TString bucket = UseQueryService ? "missing-location-query" : "missing-location-scheme";
        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto queryClient = kikimr->GetQueryClient();
        auto session = kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto execute = [&](const TString& sql) -> NYdb::TStatus {
            if constexpr (UseQueryService) {
                return queryClient.ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
            } else {
                return session.ExecuteSchemeQuery(sql).GetValueSync();
            }
        };
        auto result = execute(fmt::format(R"sql(
            CREATE EXTERNAL DATA SOURCE source WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="{}", AUTH_METHOD="NONE"
            );
        )sql", GetBucketLocation(bucket)));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = execute(R"sql(
            CREATE EXTERNAL TABLE validated_table (data String NOT NULL) WITH (
                DATA_SOURCE="source", LOCATION="/", FORMAT="raw", VALIDATE_EXTERNAL="true"
            );
        )sql");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "NoSuchBucket");
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), GetBucketLocation(bucket));
        UNIT_ASSERT(!kikimr->GetSchemeClient().DescribePath("/Root/validated_table").GetValueSync().IsSuccess());

        // Neither the default nor explicit opt-out should require the bucket to exist.
        result = execute(R"sql(
            CREATE EXTERNAL TABLE default_table (data String NOT NULL) WITH (
                DATA_SOURCE="source", LOCATION="/", FORMAT="raw"
            );
            CREATE EXTERNAL TABLE unchecked_table (data String NOT NULL) WITH (
                DATA_SOURCE="source", LOCATION="/", FORMAT="raw", VALIDATE_EXTERNAL="false"
            );
        )sql");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    Y_UNIT_TEST_TWIN(ValidateExternalTableAwsSecrets, UseQueryService) {
        const TString bucket = UseQueryService ? "location-aws-query" : "location-aws-scheme";
        CreateBucketWithObject(bucket, "data.json", TEST_CONTENT);
        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto queryClient = kikimr->GetQueryClient();
        auto session = kikimr->GetTableClient().CreateSession().GetValueSync().GetSession();
        auto execute = [&](const TString& sql) -> NYdb::TStatus {
            if constexpr (UseQueryService) {
                return queryClient.ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
            } else {
                return session.ExecuteSchemeQuery(sql).GetValueSync();
            }
        };
        auto result = execute(fmt::format(R"sql(
            CREATE SECRET access_key WITH (value="test-access-key");
            CREATE SECRET secret_key WITH (value="test-secret-key");
            CREATE EXTERNAL DATA SOURCE source WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="{}", AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_PATH="access_key",
                AWS_SECRET_ACCESS_KEY_SECRET_PATH="secret_key", AWS_REGION="us-east-1"
            );
            CREATE EXTERNAL TABLE validated_table (data String NOT NULL) WITH (
                DATA_SOURCE="source", LOCATION="data.json", FORMAT="raw", VALIDATE_EXTERNAL="true"
            );
        )sql", GetBucketLocation(bucket)));
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        result = execute("DROP SECRET secret_key;");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        result = execute(R"sql(
            CREATE EXTERNAL TABLE missing_secret_table (data String NOT NULL) WITH (
                DATA_SOURCE="source", LOCATION="data.json", FORMAT="raw", VALIDATE_EXTERNAL="true"
            );
        )sql");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "secret_key");
        UNIT_ASSERT(!kikimr->GetSchemeClient().DescribePath("/Root/missing_secret_table").GetValueSync().IsSuccess());

        // Opting out skips credential resolution as well as S3 location checks.
        result = execute(R"sql(
            CREATE EXTERNAL TABLE unchecked_table (data String NOT NULL) WITH (
                DATA_SOURCE="source", LOCATION="missing/", FORMAT="raw", VALIDATE_EXTERNAL="false"
            );
        )sql");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        if constexpr (UseQueryService) {
            // An existing table must not require another remote check or available secrets.
            result = execute(R"sql(
                CREATE EXTERNAL TABLE IF NOT EXISTS validated_table (data String NOT NULL) WITH (
                    DATA_SOURCE="source", LOCATION="missing/", FORMAT="raw", VALIDATE_EXTERNAL="true"
                );
            )sql");
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(ValidateExternalTableReplacement) {
        const TString bucket = "validate-location-replace";
        CreateBucketWithObject(bucket, "original.json", TEST_CONTENT);
        UploadObject(bucket, "replacement.json", R"({"key":"3","value":"replacement"})");
        NKikimrConfig::TAppConfig config;
        config.MutableFeatureFlags()->SetEnableReplaceIfExistsForExternalEntities(true);
        auto kikimr = NTestUtils::MakeKikimrRunner(config);
        auto client = kikimr->GetQueryClient();
        auto result = client.ExecuteQuery(fmt::format(R"sql(
            CREATE EXTERNAL DATA SOURCE source WITH (
                SOURCE_TYPE="ObjectStorage", LOCATION="{}", AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE validated_table (key Utf8 NOT NULL, value Utf8 NOT NULL) WITH (
                DATA_SOURCE="source", LOCATION="original.json", FORMAT="json_each_row", VALIDATE_EXTERNAL="true"
            );
        )sql", GetBucketLocation(bucket)), TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

        auto replace = [&](const TString& location) {
            return client.ExecuteQuery(fmt::format(R"sql(
                CREATE OR REPLACE EXTERNAL TABLE validated_table (key Utf8 NOT NULL, value Utf8 NOT NULL) WITH (
                    DATA_SOURCE="source", LOCATION="{}", FORMAT="json_each_row", VALIDATE_EXTERNAL="true"
                );
            )sql", location), TTxControl::NoTx()).GetValueSync();
        };
        auto checkRows = [&](size_t rows) {
            auto read = client.ExecuteQuery("SELECT * FROM validated_table;", TTxControl::BeginTx().CommitTx()).GetValueSync();
            UNIT_ASSERT_C(read.IsSuccess(), read.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(read.GetResultSetParser(0).RowsCount(), rows);
        };
        checkRows(2);
        result = replace("missing.json");
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Location does not exist");
        checkRows(2);
        result = replace("replacement.json");
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        checkRows(1);
    }

    Y_UNIT_TEST(ExternalTableDdl) {
        enum EEx {
            Empty,
            IfExists,
            IfNotExists,
        };

        CreateBucketWithObject("CreateExternalDataSourceBucket", "obj", TEST_CONTENT);

        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto queryClient = kikimr->GetQueryClient();

        auto logSql = [](const TString& sql, bool expectSuccess) {
            Cerr << "Execute sql in test (expect " << (expectSuccess ? "success" : "fail") << "):\n"
                 << sql << Endl;
        };

        auto checkCreate = [&](bool expectSuccess, EEx exMode, int nameSuffix) {
            UNIT_ASSERT_UNEQUAL(exMode, EEx::IfExists);
            const TString ifNotExistsStatement = exMode == EEx::IfNotExists ? "IF NOT EXISTS" : "";
            const TString sql = fmt::format(R"sql(
                CREATE EXTERNAL DATA SOURCE {if_not_exists} test_data_source_{name_suffix} WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="{location}",
                    AUTH_METHOD="NONE"
                );

                CREATE EXTERNAL TABLE {if_not_exists} test_table_{name_suffix} (
                    key Utf8 NOT NULL,
                    value Utf8 NOT NULL
                ) WITH (
                    DATA_SOURCE="test_data_source_{name_suffix}",
                    LOCATION="obj",
                    FORMAT="json_each_row"
                );
                )sql",
                "location"_a = GetBucketLocation("CreateExternalDataSourceBucket"),
                "name_suffix"_a = nameSuffix,
                "if_not_exists"_a = ifNotExistsStatement
            );
            logSql(sql, expectSuccess);
            auto result = queryClient.ExecuteQuery(
                sql,
                TTxControl::NoTx()).GetValueSync();

            if (expectSuccess) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                UNIT_ASSERT(!result.IsSuccess());
            }
        };

        auto checkTableExists = [&](bool expectSuccess, int nameSuffix) {
            // Check that we can use created external table
            const TString sql = fmt::format(R"sql(
                SELECT * FROM test_table_{name_suffix};
                )sql",
                "name_suffix"_a = nameSuffix
            );
            logSql(sql, expectSuccess);
            auto result = queryClient.ExecuteQuery(
                sql,
                TTxControl::BeginTx().CommitTx()).GetValueSync();

            if (expectSuccess) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                auto resultSet = result.GetResultSetParser(0);
                UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
            } else {
                UNIT_ASSERT(!result.IsSuccess());
            }
        };

        auto checkDrop = [&](bool expectSuccess, EEx exMode, int nameSuffix) {
            UNIT_ASSERT_UNEQUAL(exMode, EEx::IfNotExists);
            const TString ifExistsStatement = exMode == EEx::IfExists ? "IF EXISTS" : "";
            const TString sql = fmt::format(R"sql(
                DROP EXTERNAL TABLE {if_exists} test_table_{name_suffix};
                DROP EXTERNAL DATA SOURCE {if_exists} test_data_source_{name_suffix};
                )sql",
                "name_suffix"_a = nameSuffix,
                "if_exists"_a = ifExistsStatement,
                "name_suffix"_a = nameSuffix
            );
            logSql(sql, expectSuccess);
            auto result = queryClient.ExecuteQuery(
                sql,
                TTxControl::NoTx()).GetValueSync();

            if (expectSuccess) {
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            } else {
                UNIT_ASSERT(!result.IsSuccess());
            }
        };

        // usual create
        checkCreate(true, EEx::Empty, 0);
        checkTableExists(true, 0);

        // create already existing table
        checkCreate(false, EEx::Empty, 0); // already
        checkCreate(true, EEx::IfNotExists, 0);
        checkTableExists(true, 0);

        // usual drop
        checkDrop(true, EEx::Empty, 0);
        checkTableExists(false, 0);
        checkDrop(false, EEx::Empty, 0); // no such table

        // drop if exists
        checkDrop(true, EEx::IfExists, 0);
        checkTableExists(false, 0);

        // failed attempt to drop nonexisting table
        checkDrop(false, EEx::Empty, 0);

        // create with if not exists
        checkCreate(true, EEx::IfNotExists, 1); // real creation
        checkTableExists(true, 1);
        checkCreate(true, EEx::IfNotExists, 1);

        // drop if exists
        checkDrop(true, EEx::IfExists, 1); // real drop
        checkTableExists(false, 1);
        checkDrop(true, EEx::IfExists, 1);
    }

    void TestInvalidDropForExternalTableWithAuth(std::function<std::pair<bool, TString>(const TString&)> queryExecuter, TString tableSuffix) {
        const TString externalDataSourceName = "test_data_source_" + tableSuffix;
        const TString externalTableName = "test_table_" + tableSuffix;

        // Create external table
        {
            const TString sql = TStringBuilder() << R"(
                CREATE SECRET mysasignature WITH (value = "mysasignaturevalue");
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_ID="mysa",
                    SERVICE_ACCOUNT_SECRET_PATH="mysasignature"
                );
                CREATE EXTERNAL TABLE `)" << externalTableName << R"(` (
                    Key Uint64
                ) WITH (
                    DATA_SOURCE=")" << externalDataSourceName << R"(",
                    LOCATION="/",
                    FORMAT="json_each_row"
                );)";
            const auto& [success, issues] = queryExecuter(sql);
            UNIT_ASSERT_C(success, issues);
        }

        // Drop secret object
        {
            const TString sql = "DROP SECRET mysasignature";
            const auto& [success, issues] = queryExecuter(sql);
            UNIT_ASSERT_C(success, issues);
        }

        // Drop external table
        {
            const TString sql = TStringBuilder() << "DROP TABLE `" << externalTableName << "`";
            const auto& [success, issues] = queryExecuter(sql);
            UNIT_ASSERT(!success);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Cannot drop external entity by using DROP TABLE. Please use DROP EXTERNAL TABLE");
        }

        // Drop external data source
        {
            const TString sql = TStringBuilder() << "DROP TABLE `" << externalDataSourceName << "`";
            const auto& [success, issues] = queryExecuter(sql);
            UNIT_ASSERT(!success);
            UNIT_ASSERT_STRING_CONTAINS(issues, "Cannot drop external entity by using DROP TABLE. Please use DROP EXTERNAL DATA SOURCE");
        }
    }

    Y_UNIT_TEST(InvalidDropForExternalTableWithAuth) {
        auto kikimr = NTestUtils::MakeKikimrRunner();

        auto driver = kikimr->GetDriver();
        NScripting::TScriptingClient yqlScriptClient(driver);
        auto yqlScriptClientExecutor = [&](const TString& sql) {
            Cerr << "Execute sql by yql script client:\n" << sql << Endl;
            auto result = yqlScriptClient.ExecuteYqlScript(sql).GetValueSync();
            return std::make_pair(result.IsSuccess(), result.GetIssues().ToString());
        };
        TestInvalidDropForExternalTableWithAuth(yqlScriptClientExecutor, "yql_script");

        auto queryClient = kikimr->GetQueryClient();
        auto queryClientExecutor = [&](const TString& sql) {
            Cerr << "Execute sql by query client:\n" << sql << Endl;
            auto result = queryClient.ExecuteQuery(sql, TTxControl::NoTx()).GetValueSync();
            return std::make_pair(result.IsSuccess(), result.GetIssues().ToString());
        };
        TestInvalidDropForExternalTableWithAuth(queryClientExecutor, "generic_query");
    }

    Y_UNIT_TEST(ExternalTableDdlLocationValidation) {
        auto kikimr = NTestUtils::MakeKikimrRunner();
        auto db = kikimr->GetTableClient();
        auto session = db.CreateSession().GetValueSync().GetSession();
        auto query = TStringBuilder() << R"(
            CREATE EXTERNAL DATA SOURCE `/Root/ExternalDataSource` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="my-bucket",
                AUTH_METHOD="NONE"
            );
            CREATE EXTERNAL TABLE `/Root/ExternalTable` (
                Key Uint64,
                Value String
            ) WITH (
                DATA_SOURCE="/Root/ExternalDataSource",
                LOCATION="{"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Location '{' contains invalid wildcard:");
    }
}

} // namespace NKikimr::NKqp
