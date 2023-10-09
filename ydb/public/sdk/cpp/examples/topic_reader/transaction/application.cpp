#include "application.h"
#include <util/system/env.h>

TApplication::TRow::TRow(ui64 key, const TString& value) :
    Key(key),
    Value(value)
{
}

TApplication::TApplication(const TOptions& options)
{
    auto config = NYdb::TDriverConfig()
        .SetNetworkThreadsNum(2)
        .SetEndpoint(options.Endpoint)
        .SetDatabase(options.Database)
        .SetAuthToken(GetEnv("YDB_TOKEN"))
        .SetLog(CreateLogBackend("cerr", Min(options.LogPriority, TLOG_RESOURCES)));
    if (options.UseSecureConnection) {
        config.UseSecureConnection();
    }

    Driver.emplace(config);
    TopicClient.emplace(*Driver);
    TableClient.emplace(*Driver);

    CreateTopicReadSession(options);
    CreateTableSession();

    TablePath = options.TablePath;
}

void TApplication::CreateTopicReadSession(const TOptions& options)
{
    NYdb::NTopic::TReadSessionSettings settings;

    settings.ConsumerName(options.ConsumerName);
    settings.AppendTopics(options.TopicPath);

    ReadSession = TopicClient->CreateReadSession(settings);

    Cout << "Topic session was created" << Endl;
}

void TApplication::CreateTableSession()
{
    NYdb::NTable::TCreateSessionSettings settings;

    auto result = TableClient->GetSession(settings).GetValueSync();

    TableSession = result.GetSession();

    Cout << "Table session was created" << Endl;
}

void TApplication::Run()
{
    for (bool stop = false; !stop; ) {
        ReadSession->WaitEvent().Wait(TDuration::Seconds(1));

        if (!Transaction) {
            BeginTransaction();
        }

        NYdb::NTopic::TReadSessionGetEventSettings settings;
        settings.Block(false);
        settings.Tx(*Transaction);

        auto events = ReadSession->GetEvents(settings);

        if (events.empty()) {
            break;
        }

        for (auto& event : events) {
            if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                auto& messages = e->GetMessages();
                for (const auto& message : messages) {
                    AppendTableRow(message);
                }
            } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                e->Confirm();
            } else if (auto* e = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                PendingStopEvents.push_back(std::move(*e));
            } else if (auto* e = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
                Y_UNUSED(e);
                stop = true;
            }
        }

        TryCommitTransaction();
    }
}

void TApplication::Stop()
{
    ReadSession->Close(TDuration::Seconds(3));
}

void TApplication::Finalize()
{
    Stop();
    Driver->Stop();
}

void TApplication::BeginTransaction()
{
    Y_ABORT_UNLESS(!Transaction);
    Y_ABORT_UNLESS(TableSession);

    auto settings = NYdb::NTable::TTxSettings::SerializableRW();
    auto result = TableSession->BeginTransaction(settings).GetValueSync();

    Transaction = result.GetTransaction();
}

void TApplication::CommitTransaction()
{
    Y_ABORT_UNLESS(Transaction);

    NYdb::NTable::TCommitTxSettings settings;

    auto result = Transaction->Commit(settings).GetValueSync();

    Cout << "Commit: " << static_cast<NYdb::TStatus&>(result) << Endl;
}

void TApplication::TryCommitTransaction()
{
    if (!Rows.empty()) {
        Y_ABORT_UNLESS(Transaction);

        InsertRowsIntoTable();
        CommitTransaction();

        Rows.clear();
    }

    if (!PendingStopEvents.empty()) {
        for (auto& event : PendingStopEvents) {
            event.Confirm();
        }
        PendingStopEvents.clear();
    }

    Transaction = std::nullopt;
}

void TApplication::InsertRowsIntoTable()
{
    Y_ABORT_UNLESS(Transaction);

    TString query = "                                                            \
        DECLARE $rows AS List<Struct<                                            \
            id: Uint64,                                                          \
            value: String                                                        \
        >>;                                                                      \
                                                                                 \
        UPSERT INTO `" + TablePath + "` (SELECT id, value FROM AS_TABLE($rows)); \
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
        auto result =
            Transaction->GetSession().ExecuteDataQuery(query,
                                                       NYdb::NTable::TTxControl::Tx(*Transaction),
                                                       params,
                                                       settings).GetValueSync();

        return result;
    };

    TableClient->RetryOperationSync(runQuery);
}

void TApplication::AppendTableRow(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message)
{
    Rows.emplace_back(RandomNumber<ui64>(), message.GetData());
}
