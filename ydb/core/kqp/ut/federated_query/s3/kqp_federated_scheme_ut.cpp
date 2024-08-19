#include "s3_recipe_ut_helpers.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/library/yql/utils/log/log.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace NTestUtils;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(KqpFederatedSchemeTest) {
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
                UPSERT OBJECT mysasignature (TYPE SECRET) WITH (value = "mysasignaturevalue");
                CREATE EXTERNAL DATA SOURCE `)" << externalDataSourceName << R"(` WITH (
                    SOURCE_TYPE="ObjectStorage",
                    LOCATION="my-bucket",
                    AUTH_METHOD="SERVICE_ACCOUNT",
                    SERVICE_ACCOUNT_ID="mysa",
                    SERVICE_ACCOUNT_SECRET_NAME="mysasignature"
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
            const TString sql = "DROP OBJECT mysasignature (TYPE SECRET)";
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
                LOCATION="}"
            );)";
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(result.GetStatus(), EStatus::SCHEME_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Location '}' contains invalid wildcard:");
    }
}

} // namespace NKikimr::NKqp
