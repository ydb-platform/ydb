#include "common.h"

#include <ydb/core/kqp/federated_query/kqp_federated_query_helpers.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb-cpp-sdk/client/draft/ydb_scripting.h>
#include <ydb-cpp-sdk/client/operation/operation.h>
#include <ydb-cpp-sdk/client/proto/accessor.h>
#include <ydb-cpp-sdk/client/table/table.h>
#include <ydb-cpp-sdk/client/types/operation/operation.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/strbuf.h>
#include <util/generic/string.h>
#include <util/system/env.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NKikimr::NKqp::NFederatedQueryTest;
using namespace fmt::literals;

Y_UNIT_TEST_SUITE(S3Inset) {
    Y_UNIT_TEST(TestInsertEscaping) {
        const TString externalDataSourceName = "/Root/external_data_source";
        auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
        auto kikimr = MakeKikimrRunner(true, nullptr, nullptr, std::nullopt, s3ActorsFactory);

        auto tc = kikimr->GetTableClient();
        auto session = tc.CreateSession().GetValueSync().GetSession();
        const TString query = fmt::format(R"(
            CREATE OBJECT id (TYPE SECRET) WITH (value=`minio`);
            CREATE OBJECT key (TYPE SECRET) WITH (value=`minio123`);
            CREATE EXTERNAL DATA SOURCE `{external_source}` WITH (
                SOURCE_TYPE="ObjectStorage",
                LOCATION="{location}",
                AUTH_METHOD="AWS",
                AWS_ACCESS_KEY_ID_SECRET_NAME="id",
                AWS_SECRET_ACCESS_KEY_SECRET_NAME="key",
                AWS_REGION="ru-central-1"
            );
            )",
            "external_source"_a = externalDataSourceName,
            "location"_a = "localhost:" + GetExternalPort("minio", "9000") + "/datalake/"
            );

        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_C(result.GetStatus() == NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

        WaitBucket(kikimr, externalDataSourceName);

        auto db = kikimr->GetQueryClient();

        // TODO: remove ';' from skip list
        TString path = TStringBuilder() << "exp_folder/some_" << EscapeC(GetSymbolsString(' ', '~', "*?{}`;")) << "\\`";

        {
            auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                INSERT INTO `{external_source}`.`{path}/` WITH (FORMAT = "csv_with_names")
                SELECT * FROM `{external_source}`.`/a/` WITH (
                    format="json_each_row",
                    schema(
                        key Utf8 NOT NULL,
                        value Utf8 NOT NULL
                    )
                )
            )", "external_source"_a = externalDataSourceName, "path"_a = path)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
        }

        {
            auto scriptExecutionOperation = db.ExecuteScript(fmt::format(R"(
                SELECT * FROM `{external_source}`.`{path}/` WITH (
                    format="csv_with_names",
                    schema(
                        key Utf8 NOT NULL,
                        value Utf8 NOT NULL
                    )
                )
            )", "external_source"_a = externalDataSourceName, "path"_a = path)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(scriptExecutionOperation.Status().GetStatus(), EStatus::SUCCESS, scriptExecutionOperation.Status().GetIssues().ToString());
            UNIT_ASSERT(!scriptExecutionOperation.Metadata().ExecutionId.empty());

            NYdb::NQuery::TScriptExecutionOperation readyOp = WaitScriptExecutionOperation(scriptExecutionOperation.Id(), kikimr->GetDriver());
            UNIT_ASSERT_EQUAL_C(readyOp.Metadata().ExecStatus, EExecStatus::Completed, readyOp.Status().GetIssues().ToString());
            TFetchScriptResultsResult results = db.FetchScriptResults(scriptExecutionOperation.Id(), 0).ExtractValueSync();
            UNIT_ASSERT_C(results.IsSuccess(), results.GetIssues().ToString());

            TResultSetParser resultSet(results.ExtractResultSet());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnsCount(), 2);
            UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 2);
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "1");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "trololo");
            UNIT_ASSERT(resultSet.TryNextRow());
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(0).GetUtf8(), "2");
            UNIT_ASSERT_VALUES_EQUAL(resultSet.ColumnParser(1).GetUtf8(), "hello world");
        }
    }
}

} // namespace NKikimr::NKqp
