#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb::NConsoleClient {

class TTransactionSupport {
public:
    struct TExecutionTimes {
        TDuration SelectTime;
        TDuration UpsertTime;
        TDuration CommitTime;
    };

    TTransactionSupport(const NYdb::TDriver& driver,
                        const TString& readOnlyTableName,
                        const TString& writeOnyTableName);

    void BeginTx();
    TExecutionTimes CommitTx(bool useTableSelect, bool useTableUpsert);
    void AppendRow(const TString& data);

    struct TRow {
        ui64 Key;
        TString Value;
    };

    std::optional<NYdb::NTable::TTransaction> Transaction;
    TVector<TRow> Rows;

private:
    void CreateSession();
    TDuration SelectFromTable();
    TDuration UpsertIntoTable();
    TDuration Commit();

    NYdb::NTable::TTableClient TableClient;
    TString ReadOnlyTableName;
    TString WriteOnlyTableName;
    std::optional<NYdb::NTable::TSession> Session;
};

}
