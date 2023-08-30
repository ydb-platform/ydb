#pragma once

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NYdb::NConsoleClient {

class TTransactionSupport {
public:
    TTransactionSupport(const NYdb::TDriver& driver,
                        const TString& tableName);

    void BeginTx();
    void CommitTx();
    void AppendRow(const TString& data);

    struct TRow {
        ui64 Key;
        TString Value;
    };

    std::optional<NYdb::NTable::TTransaction> Transaction;
    TVector<TRow> Rows;

private:
    void CreateSession();
    void UpsertIntoTable();
    void Commit();

    NYdb::NTable::TTableClient TableClient;
    TString TableName;
    std::optional<NYdb::NTable::TSession> Session;
};

}
