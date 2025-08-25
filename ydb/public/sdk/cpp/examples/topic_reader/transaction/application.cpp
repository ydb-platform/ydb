#include "application.h"

TApplication::TRow::TRow(uint64_t key, const std::string& value) :
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
        .SetAuthToken(std::getenv("YDB_TOKEN") ? std::getenv("YDB_TOKEN") : "")
        .SetLog(std::unique_ptr<TLogBackend>(CreateLogBackend("cerr", std::min(options.LogPriority, TLOG_RESOURCES)).Release()));
    if (options.UseSecureConnection) {
        config.UseSecureConnection();
    }

    Driver.emplace(config);
    TopicClient.emplace(*Driver);
    QueryClient.emplace(*Driver);

    CreateTopicReadSession(options);
    CreateQuerySession();

    TablePath = options.TablePath;
}

void TApplication::CreateTopicReadSession(const TOptions& options)
{
    NYdb::NTopic::TReadSessionSettings settings;

    settings.ConsumerName(options.ConsumerName);
    settings.AppendTopics(options.TopicPath);

    ReadSession = TopicClient->CreateReadSession(settings);

    std::cout << "Topic session was created" << std::endl;
}

void TApplication::CreateQuerySession()
{
    NYdb::NQuery::TCreateSessionSettings settings;

    auto result = QueryClient->GetSession(settings).GetValueSync();

    QuerySession = result.GetSession();

    std::cout << "Query session was created" << std::endl;
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
    Y_ABORT_UNLESS(QuerySession);

    auto settings = NYdb::NQuery::TTxSettings::SerializableRW();
    auto result = QuerySession->BeginTransaction(settings).GetValueSync();

    Transaction = result.GetTransaction();
}

void TApplication::CommitTransaction()
{
    Y_ABORT_UNLESS(Transaction);

    NYdb::NQuery::TCommitTxSettings settings;

    auto result = Transaction->Commit(settings).GetValueSync();

    std::cout << "Commit: " << ToString(static_cast<const NYdb::TStatus&>(result)) << std::endl;
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

    std::string query = "                                                            \
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

    auto runQuery = [this, &query, &params](NYdb::NQuery::TSession) -> NYdb::TStatus {
        auto result =
            Transaction->GetSession().ExecuteQuery(query,
                                                   NYdb::NQuery::TTxControl::Tx(*Transaction),
                                                   params).GetValueSync();

        return result;
    };

    QueryClient->RetryQuerySync(runQuery);
}

void TApplication::AppendTableRow(const NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent::TMessage& message)
{
    Rows.emplace_back(Dist(MersenneEngine), message.GetData());
}
