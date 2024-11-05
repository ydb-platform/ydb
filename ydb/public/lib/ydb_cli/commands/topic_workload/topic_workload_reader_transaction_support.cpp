#include "topic_workload_reader_transaction_support.h"

#include <util/random/random.h>

namespace NYdb::NConsoleClient {

static void EnsureSuccess(const NYdb::TStatus& status, std::string_view name)
{
    if (status.IsTransportError()) {
        Cerr << "transport error on " << name << ": " << status << Endl;
        ythrow yexception() << "transport error on " << name << ": " << status;
    }

    if (!status.IsSuccess()) {
        Cerr << "error on " << name << ": " << status << Endl;
        ythrow yexception() << "error on " << name << ": " << status;
    }
}

TTransactionSupport::TTransactionSupport(const NYdb::TDriver& driver,
                                         const TString& readOnlyTableName,
                                         const TString& writeOnlyTableName) :
    TableClient(driver),
    ReadOnlyTableName(readOnlyTableName),
    WriteOnlyTableName(writeOnlyTableName)
{
}

void TTransactionSupport::BeginTx()
{
    Y_ABORT_UNLESS(!Transaction);

    if (!Session) {
        CreateSession();
    }

    Y_ABORT_UNLESS(Session);

    auto settings = NYdb::NTable::TTxSettings::SerializableRW();
    auto result = Session->BeginTransaction(settings).GetValueSync();
    EnsureSuccess(result, "BeginTransaction");

    Transaction = result.GetTransaction();
}

auto TTransactionSupport::CommitTx(bool useTableSelect, bool useTableUpsert) -> TExecutionTimes
{
    Y_ABORT_UNLESS(Transaction);

    TExecutionTimes result;

    result.SelectTime = useTableSelect ? SelectFromTable() : TDuration::Seconds(0);
    result.UpsertTime = useTableUpsert ? UpsertIntoTable() : TDuration::Seconds(0);
    result.CommitTime = Commit();

    Rows.clear();
    Transaction = std::nullopt;

    return result;
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
    Y_ABORT_UNLESS(!Session);

    auto result = TableClient.GetSession(NYdb::NTable::TCreateSessionSettings()).GetValueSync();
    EnsureSuccess(result, "GetSession");

    Session = result.GetSession();
}

TDuration TTransactionSupport::SelectFromTable()
{
    Y_ABORT_UNLESS(Transaction);
    Y_ABORT_UNLESS(ReadOnlyTableName);

    ui64 left = RandomNumber<ui64>();
    ui64 right = RandomNumber<ui64>();

    if (right < left) {
        std::swap(left, right);
    }

    TString query = "                                                                            \
        DECLARE $left AS Uint64;                                                                 \
        DECLARE $right AS Uint64;                                                                \
                                                                                                 \
        SELECT COUNT(id) FROM `" + ReadOnlyTableName + "` WHERE ($left <= id) AND (id < $right); \
    ";

    NYdb::TParamsBuilder builder;

    builder.AddParam("$left").Uint64(left).Build();
    builder.AddParam("$right").Uint64(right).Build();

    auto params = builder.Build();

    NYdb::NTable::TExecDataQuerySettings settings;
    settings.KeepInQueryCache(true);

    auto runQuery = [this, &query, &params, &settings](NYdb::NTable::TSession) -> NYdb::TStatus {
        return Transaction->GetSession().ExecuteDataQuery(query,
                                                          NYdb::NTable::TTxControl::Tx(*Transaction),
                                                          params,
                                                          settings).GetValueSync();
    };

    auto beginTime = TInstant::Now();
    auto result = TableClient.RetryOperationSync(runQuery);
    auto duration = TInstant::Now() - beginTime;

    EnsureSuccess(result, "SELECT");

    return duration;
}

TDuration TTransactionSupport::UpsertIntoTable()
{
    Y_ABORT_UNLESS(Transaction);
    Y_ABORT_UNLESS(WriteOnlyTableName);

    TString query = "                                                                     \
        DECLARE $rows AS List<Struct<                                                     \
            id: Uint64,                                                                   \
            value: String                                                                 \
        >>;                                                                               \
                                                                                          \
        UPSERT INTO `" + WriteOnlyTableName + "` (SELECT id, value FROM AS_TABLE($rows)); \
    ";

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


    auto beginTime = TInstant::Now();
    auto result = TableClient.RetryOperationSync(runQuery);
    auto duration = TInstant::Now() - beginTime;

    EnsureSuccess(result, "UPSERT");

    return duration;
}

TDuration TTransactionSupport::Commit()
{
    Y_ABORT_UNLESS(Transaction);

    auto settings = NYdb::NTable::TCommitTxSettings();
    auto beginTime = TInstant::Now();
    auto result = Transaction->Commit(settings).GetValueSync();
    auto duration = TInstant::Now() - beginTime;

    EnsureSuccess(result, "COMMIT");

    return duration;
}

}
