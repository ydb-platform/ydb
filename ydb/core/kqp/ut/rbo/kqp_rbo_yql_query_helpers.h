#pragma once

#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp::NYqlTests {

using namespace NYdb;
using namespace NYdb::NTable;

inline std::pair<ui32, ui32> GetNewRBOCompileCounters(TKikimrRunner& kikimr) {
    auto counters = TKqpCounters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    return {counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Success")->Val(),
            counters.GetKqpCounters()->GetCounter("Compilation/NewRBO/Failed")->Val()};
}

inline TString SqlInPeepholePragma(bool peephole) {
    return TStringBuilder()
        << "PRAGMA ydb.EnableNewRBOPhysicalStagePeephole = \"" << (peephole ? "true" : "false") << "\";\n";
}

inline void InsertIntoSchema0(NYdb::NTable::TTableClient& db, std::string tableName, ui32 numRows) {
    NYdb::TValueBuilder rows;
    rows.BeginList();
    for (size_t i = 0; i < numRows; ++i) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("a").Int64(i)
            .AddMember("b").String(std::to_string(i) + "_b")
            .AddMember("c").Int64(i + 1)
            .EndStruct();
    }
    rows.EndList();
    auto resultUpsert = db.BulkUpsert(tableName, rows.Build()).GetValueSync();
    UNIT_ASSERT_C(resultUpsert.IsSuccess(), resultUpsert.GetIssues().ToString());
}

inline ui32 CountNumberOfCallables(const std::string& ast, const std::string_view callable) {
    ui32 count = 0;
    auto pos = ast.find(callable);
    while (pos != std::string::npos) {
        pos = ast.find(callable, pos + 1);
        ++count;
    }
    return count;
}

} // namespace NKikimr::NKqp::NYqlTests
