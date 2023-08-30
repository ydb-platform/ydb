#include "topic_workload_reader_transaction_support.h"

#include <util/random/random.h>

namespace NYdb::NConsoleClient {

static void EnsureSuccess(const NYdb::TStatus& status, std::string_view name)
{
    Y_VERIFY(!status.IsTransportError());

    if (!status.IsSuccess()) {
        ythrow yexception() << "error on " << name << ": " << status;
    }
}

TTransactionSupport::TTransactionSupport(const NYdb::TDriver& driver,
                                         const TString& tableName) :
    TableClient(driver),
    TableName(tableName)
{
}

void TTransactionSupport::BeginTx()
{
    Y_VERIFY(!Transaction);

    if (!Session) {
        CreateSession();
    }

    Y_VERIFY(Session);

    auto settings = NYdb::NTable::TTxSettings::SerializableRW();
    auto result = Session->BeginTransaction(settings).GetValueSync();
    EnsureSuccess(result, "BeginTransaction");

    Transaction = result.GetTransaction();
}

void TTransactionSupport::CommitTx()
{
    Y_VERIFY(Transaction);

    UpsertIntoTable();
    Commit();

    Rows.clear();
    Transaction = std::nullopt;
}

void TTransactionSupport::AppendRow(const TString& m)
{
    TRow row;
    row.Key = RandomNumber<ui64>();
    row.Value = m;

    Rows.push_back(std::move(row));
}

void TTransactionSupport::CreateSession()
{
    Y_VERIFY(!Session);

    auto result = TableClient.GetSession(NYdb::NTable::TCreateSessionSettings()).GetValueSync();
    EnsureSuccess(result, "GetSession");

    Session = result.GetSession();
}

void TTransactionSupport::UpsertIntoTable()
{
    Y_VERIFY(Transaction);

    TString query = R"(
        DECLARE $rows AS List<Struct<
            id: Uint64,
            value: String
        >>;

        UPSERT INTO `)" + TableName + R"(` (SELECT id, value FROM AS_TABLE($rows));
    )";

    NYdb::TParamsBuilder builder;

    auto& rows = builder.AddParam("$rows");
    rows.BeginList();
    for (auto& row : Rows) {
        rows.AddListItem()
            .BeginStruct()
            .AddMember("id").Uint64(row.Key)
            .AddMember("value").String(row.Value)
            .EndStruct();
    }
    rows.EndList();
    rows.Build();

    auto params = builder.Build();

    NYdb::NTable::TExecDataQuerySettings settings;
    settings.KeepInQueryCache(true);

    auto runQuery = [this, &query, &params, &settings](NYdb::NTable::TSession) -> NYdb::TStatus {
        return Transaction->GetSession().ExecuteDataQuery(query,
                                                          NYdb::NTable::TTxControl::Tx(*Transaction),
                                                          params,
                                                          settings).GetValueSync();
    };

    auto result = TableClient.RetryOperationSync(runQuery);
    EnsureSuccess(result, "UPSERT");
}

void TTransactionSupport::Commit()
{
    Y_VERIFY(Transaction);

    auto settings = NYdb::NTable::TCommitTxSettings();
    auto result = Transaction->Commit(settings).GetValueSync();
    EnsureSuccess(result, "COMMIT");
}

}
