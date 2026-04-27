#include "indexes_ut_helper.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status_codes.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

void ExecQuery(TKikimrRunner& kikimr, bool useQueryService, const TString& query) {
    if (useQueryService) {
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    } else {
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }
}

void ExecQueryExpectErrorContains(TKikimrRunner& kikimr, bool useQueryService, const TString& query, TStringBuf needle) {
    if (useQueryService) {
        auto session = kikimr.GetQueryClient().GetSession().GetValueSync().GetSession();
        auto result = session.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().ToString().contains(needle),
            "Expected error containing '" << needle << "', got: " << result.GetIssues().ToString());
    } else {
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_UNEQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
        UNIT_ASSERT_C(result.GetIssues().ToString().contains(needle),
            "Expected error containing '" << needle << "', got: " << result.GetIssues().ToString());
    }
}

} // namespace NKikimr::NKqp
