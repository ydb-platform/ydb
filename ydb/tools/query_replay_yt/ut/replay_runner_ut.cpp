#include "plan_check.h"
#include "replay_runner.h"

#include <library/cpp/json/json_reader.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NQueryReplay;

namespace {

NJson::TJsonValue MakeInvalidSqlReplayDetails() {
    NJson::TJsonValue details(NJson::JSON_MAP);
    details["query_id"] = "invalid-sql-test";
    details["query_text"] = "SELECT * FROM";
    details["query_type"] = "QUERY_TYPE_SQL_SCAN";
    details["query_database"] = "/local";
    details["query_syntax"] = "1";
    details["query_cluster"] = "local";
    details["query_plan"] = "{}";
    details["table_metadata"] = NJson::TJsonValue(NJson::JSON_ARRAY);
    return details;
}

} // namespace

Y_UNIT_TEST_SUITE(TQueryReplayRunner) {
    Y_UNIT_TEST(CompileErrorOnInvalidSql) {
        TQueryReplayRunner runner;
        runner.Init({}, 2, false);
        auto response = runner.RunReplay(MakeInvalidSqlReplayDetails());
        UNIT_ASSERT(response);
        UNIT_ASSERT_EQUAL(response->Status, TQueryReplayEvents::CompileError);
        UNIT_ASSERT_STRINGS_EQUAL(StatusToFailReason(response->Status), "compile_error");
        runner.Stop();
    }
}
