#include "query_executor.h"
#include "get_value.h"
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NKikimr::NKqp {

TVector<THashMap<TString, NYdb::TValue>> CollectRows(NYdb::NTable::TScanQueryPartIterator& it, NJson::TJsonValue* statInfo /*= nullptr*/, NJson::TJsonValue* diagnostics /*= nullptr*/) {
    TVector<THashMap<TString, NYdb::TValue>> rows;
    if (statInfo) {
        *statInfo = NJson::JSON_NULL;
    }
    if (diagnostics) {
        *diagnostics = NJson::JSON_NULL;
    }
    for (;;) {
        auto streamPart = it.ReadNext().GetValueSync();
        if (!streamPart.IsSuccess()) {
            UNIT_ASSERT_C(streamPart.EOS(), streamPart.GetIssues().ToString());
            break;
        }

        UNIT_ASSERT_C(streamPart.HasResultSet() || streamPart.HasQueryStats(),
            "Unexpected empty scan query response.");

        if (streamPart.HasQueryStats()) {
            auto plan = streamPart.GetQueryStats().GetPlan();
            if (plan && statInfo) {
                UNIT_ASSERT(NJson::ReadJsonFastTree(*plan, statInfo));
            }
        }

        if (streamPart.HasDiagnostics()) {
            TString diagnosticsString = streamPart.GetDiagnostics();
            if (!diagnosticsString.empty() && diagnostics) {
                UNIT_ASSERT(NJson::ReadJsonFastTree(diagnosticsString, diagnostics));
            }
        }

        if (streamPart.HasResultSet()) {
            auto resultSet = streamPart.ExtractResultSet();
            NYdb::TResultSetParser rsParser(resultSet);
            while (rsParser.TryNextRow()) {
                THashMap<TString, NYdb::TValue> row;
                for (size_t ci = 0; ci < resultSet.ColumnsCount(); ++ci) {
                    row.emplace(resultSet.GetColumnsMeta()[ci].Name, rsParser.GetValue(ci));
                }
                rows.emplace_back(std::move(row));
            }
        }
    }
    return rows;
}

TVector<THashMap<TString, NYdb::TValue>> ExecuteScanQuery(NYdb::NTable::TTableClient& tableClient, const TString& query, const bool verbose /*= true*/) {
    if (verbose) {
        Cerr << "====================================\n"
            << "QUERY:\n" << query
            << "\n\nRESULT:\n";
    }

    NYdb::NTable::TStreamExecScanQuerySettings scanSettings;
    auto it = tableClient.StreamExecuteScanQuery(query, scanSettings).GetValueSync();
    auto rows = CollectRows(it);
    if (verbose) {
        PrintRows(Cerr, rows);
        Cerr << "\n";
    }

    return rows;
}

}