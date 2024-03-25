#include "s3_recipe_ut_helpers.h"

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
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

        auto kikimr = MakeKikimrRunner(true);

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
}

} // namespace NKikimr::NKqp
