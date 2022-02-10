#include "index_events_processor.h"
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>

namespace NKikimr::NSQS {
using namespace NActors;
using namespace NJson;
using namespace NYdb;

constexpr TDuration DEFAULT_RETRY_TIMEOUT = TDuration::Seconds(10);
constexpr TDuration SHORT_RETRY_TIMEOUT = TDuration::Seconds(2);
constexpr TDuration DEFAULT_QUERY_TIMEOUT = TDuration::Seconds(30);

TSearchEventsProcessor::TSearchEventsProcessor(
        const TString& root, const TDuration& reindexInterval, const TDuration& rescanInterval,
        const TSimpleSharedPtr<NTable::TTableClient>& tableClient,
        IEventsWriterWrapper::TPtr eventsWriter,
        bool waitForWake
)
        : RescanInterval(rescanInterval)
        , ReindexInterval(reindexInterval)
        , TableClient(tableClient)
        , EventsWriter(eventsWriter)
        , WaitForWake(waitForWake)
{
    InitQueries(root);
}

void TSearchEventsProcessor::InitQueries(const TString& root) {
    auto getTableFullPath = [&](const TString& tableName) {
        if (!root.empty()) {
            return root + "/" + tableName;
        } else {
            return tableName;
        }
    };
    TString eventsTablePath = getTableFullPath(".Events");

    SelectQueuesQuery = TStringBuilder()
            << "--!syntax_v1\n"
            << "DECLARE $Account as Utf8; DECLARE $QueueName as Utf8; "
            << "SELECT Account, QueueName, CustomQueueName, CreatedTimestamp, FolderId from `"
            << getTableFullPath(".Queues") << "` "
            << "WHERE Account > $Account OR (Account = $Account AND QueueName > $QueueName);";

    SelectEventsQuery = TStringBuilder()
            << "--!syntax_v1\n"
            << "SELECT Account, QueueName, EventType, CustomQueueName, EventTimestamp, FolderId "
            << "FROM `"<< eventsTablePath << "`;";

    DeleteEventQuery = TStringBuilder()
            << "--!syntax_v1\n"
            << "DECLARE $Events AS List<Struct<Account:Utf8, QueueName:Utf8, EventType:Uint64>>;\n"
            << "$EventsSource = (SELECT item.Account AS Account, item.QueueName AS QueueName, item.EventType AS EventType \n"
            << "FROM (SELECT $Events AS events) FLATTEN LIST BY events as item);\n"
            << "DELETE FROM `" << eventsTablePath << "` ON SELECT * FROM $EventsSource;";
}

TSearchEventsProcessor::~TSearchEventsProcessor() {
    EventsWriter->Close();
}

void TSearchEventsProcessor::Bootstrap(const TActorContext& ctx) {
    Become(&TSearchEventsProcessor::StateFunc);
    if (!WaitForWake)
        ctx.Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup(StartQueuesListingTag));
}

STFUNC(TSearchEventsProcessor::StateFunc) {
    switch (ev->GetTypeRewrite()) {
        HFunc(TEvWakeup, HandleWakeup);
        default:
            Y_FAIL();
    };
}

void TSearchEventsProcessor::HandleWakeup(TEvWakeup::TPtr& ev, const TActorContext& ctx) {
    if (Stopping)
        return;
    switch (ev->Get()->Tag) {
        case StartQueuesListingTag:
            StartQueuesListing(ctx);
            break;
        case QueuesListSessionStartedTag:
            OnQueuesListSessionReady(ctx);
            break;
        case QueuesListPreparedTag:
            OnQueuesListPrepared(ctx);
            break;
         case QueuesListQueryCompleteTag:
            OnQueuesListQueryComplete(ctx);
            break;
        case RunEventsListingTag:
            RunEventsListing(ctx);
            break;
        case EventsListingDoneTag:
            OnEventsListingDone(ctx);
            break;
        case StartCleanupTag:
            StartCleanupSession(ctx);
            break;
        case CleanupSessionReadyTag:
            OnCleanupSessionReady(ctx);
            break;
        case CleanupTxReadyTag:
            OnCleanupTxReady(ctx);
            break;
        case CleanupPreparedTag:
            OnCleanupPrepared(ctx);
            break;
        case CleanupQueryCompleteTag:
            OnCleanupQueryComplete(ctx);
            break;
        case CleanupTxCommittedTag:
            OnCleanupTxCommitted(ctx);
            break;
        case StopAllTag:
            Stopping = true;
            EventsWriter->Close();
            break;
        default:
            Y_FAIL();
    }
}

void TSearchEventsProcessor::StartQueuesListing(const TActorContext& ctx) {
    LastQueuesKey = {};
    StartSession(QueuesListSessionStartedTag, ctx);
}

void TSearchEventsProcessor::OnQueuesListSessionReady(const TActorContext &ctx) {
    if (!SessionStarted(ctx)) {
        return ctx.Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup(StartQueuesListingTag));
    }
    PrepareDataQuery(SelectQueuesQuery, QueuesListPreparedTag, ctx);
}

void TSearchEventsProcessor::OnQueuesListPrepared(const TActorContext &ctx) {
    if (!QueryPrepared(ctx)) {
        return ctx.Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup(StartQueuesListingTag));
    }
    RunQueuesListQuery(ctx, true);
}

void TSearchEventsProcessor::RunQueuesListQuery(const TActorContext& ctx, bool initial) {
    if (initial) {
        ExistingQueues.clear();
    }
    Y_VERIFY(Session.Defined());
    Y_VERIFY(PreparedQuery.Defined());

    auto builder = PreparedQuery.Get()->GetParamsBuilder();
    {
        auto &param = builder.AddParam("$Account");
        param.Utf8(LastQueuesKey.Account);
        param.Build();
    }
    {
        auto &param = builder.AddParam("$QueueName");
        param.Utf8(LastQueuesKey.QueueName);
        param.Build();
    }
    RunPrepared(builder.Build(), QueuesListQueryCompleteTag, ctx);
}

void TSearchEventsProcessor::OnQueuesListQueryComplete(const TActorContext& ctx) {
    if (!QueryComplete(ctx)) {
        return ctx.Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup(StartQueuesListingTag));
    }

    const auto& result = QueryResult.Get()->GetResultSet(0);

    TResultSetParser parser(result);
    while (parser.TryNextRow()) {
        TString queueName = *parser.ColumnParser("QueueName").GetOptionalUtf8();
        TString cloudId = *parser.ColumnParser("Account").GetOptionalUtf8();
        TString customName = *parser.ColumnParser("CustomQueueName").GetOptionalUtf8();
        ui64 createTs = *parser.ColumnParser("CreatedTimestamp").GetOptionalUint64();
        TString folderId = *parser.ColumnParser("FolderId").GetOptionalUtf8();
        auto insResult = ExistingQueues.insert(std::make_pair(
                queueName, TQueueEvent{EQueueEventType::Existed, createTs, customName, cloudId, folderId}
        ));
        Y_VERIFY(insResult.second);
        LastQueuesKey.QueueName = queueName;
        LastQueuesKey.Account = cloudId;
    }
    if (result.Truncated()) {
        RunQueuesListQuery(ctx, false);
    } else {
        Send(ctx.SelfID, new TEvWakeup(RunEventsListingTag));
    }
}

void TSearchEventsProcessor::RunEventsListing(const TActorContext& ctx) {
    QueryResult = Nothing();
    Status = TableClient->RetryOperation<NTable::TDataQueryResult>([this, query = SelectEventsQuery](NTable::TSession session) {
        auto tx = NTable::TTxControl::BeginTx().CommitTx();
        return session.ExecuteDataQuery(
                query, tx, NTable::TExecDataQuerySettings().ClientTimeout(DEFAULT_QUERY_TIMEOUT)
            ).Apply([this](const auto& future) mutable {
                QueryResult = future.GetValue();
                return future;
        });
    });
    Apply(&Status, EventsListingDoneTag, ctx);
}

void TSearchEventsProcessor::OnEventsListingDone(const TActorContext& ctx) {
    auto& status = Status.GetValue();
    if (!status.IsSuccess()) {
        Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup(RunEventsListingTag));
        return;
    }

    Y_VERIFY(QueryResult.Defined());
    Y_VERIFY(QueryResult.Get()->GetResultSets().size() == 1);
    const auto& result = QueryResult.Get()->GetResultSet(0);
    TResultSetParser parser(result);
    QueuesEvents.clear();

    while (parser.TryNextRow()) {
        TString cloudId = *parser.ColumnParser("Account").GetOptionalUtf8();
        TString queueName = *parser.ColumnParser("QueueName").GetOptionalUtf8();
        ui64 evType = *parser.ColumnParser("EventType").GetOptionalUint64();
        TString customName = *parser.ColumnParser("CustomQueueName").GetOptionalUtf8();
        TString folderId = *parser.ColumnParser("FolderId").GetOptionalUtf8();
        ui64 timestamp = *parser.ColumnParser("EventTimestamp").GetOptionalUint64();
        auto& qEvents = QueuesEvents[queueName];
        auto insResult = qEvents.insert(std::make_pair(
                timestamp, TQueueEvent{EQueueEventType(evType), timestamp, customName, cloudId, folderId}
        ));
        Y_VERIFY(insResult.second);
    }
    ProcessEventsQueue(ctx);
}

void TSearchEventsProcessor::StartCleanupSession(const TActorContext& ctx) {
    StartSession(CleanupSessionReadyTag, ctx);
}

void TSearchEventsProcessor::OnCleanupSessionReady(const TActorContext &ctx) {
    if (!SessionStarted(ctx)) {
        return ctx.Schedule(SHORT_RETRY_TIMEOUT, new TEvWakeup(StartCleanupTag));
    }
    StartTx(CleanupTxReadyTag, ctx);
}

void TSearchEventsProcessor::OnCleanupTxReady(const TActorContext &ctx) {
    if (!TxStarted(ctx)) {
        return ctx.Schedule(SHORT_RETRY_TIMEOUT, new TEvWakeup(StartCleanupTag));
    }
    PrepareDataQuery(DeleteEventQuery, CleanupPreparedTag, ctx);
}

void TSearchEventsProcessor::OnCleanupPrepared(const TActorContext &ctx) {
    if (!QueryPrepared(ctx)) {
        return ctx.Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup(StartCleanupTag));
    }
    RunEventsCleanup(ctx);
}

void TSearchEventsProcessor::RunEventsCleanup(const TActorContext& ctx) {
    Y_VERIFY(PreparedQuery.Defined());
    Y_VERIFY(CurrentTx.Defined());

    auto builder = PreparedQuery.Get()->GetParamsBuilder();
    auto &param = builder.AddParam("$Events");
    param.BeginList();
    for (const auto&[qName, events] : QueuesEvents) {
        for (const auto&[_, event]: events) {
            param.AddListItem().BeginStruct().AddMember("Account").Utf8(event.CloudId)
                                             .AddMember("QueueName").Utf8(qName)
                                             .AddMember("EventType").Uint64(static_cast<ui64>(event.Type))
                                             .EndStruct();

        }
    }
    param.EndList().Build();
    RunPrepared(builder.Build(), CleanupQueryCompleteTag, ctx);
}

void TSearchEventsProcessor::OnCleanupQueryComplete(const TActorContext& ctx) {
    if (!QueryComplete(ctx)) {
        return ctx.Schedule(SHORT_RETRY_TIMEOUT, new TEvWakeup(StartCleanupTag));
    }
    CommitTx(CleanupTxCommittedTag, ctx);
}

void TSearchEventsProcessor::OnCleanupTxCommitted(const TActorContext &ctx) {
    if (!TxCommitted(ctx)) {
        return ctx.Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup(StartCleanupTag));
    }
    QueuesEvents.clear();
    ProcessReindexIfRequired(ctx);
}

void TSearchEventsProcessor::ProcessEventsQueue(const TActorContext& ctx) {

    for (const auto& [qName, events] : QueuesEvents) {
        for (const auto& [ts, event]: events) {
            auto existsIter = ExistingQueues.find(qName);
            if (event.Type == EQueueEventType::Deleted) {
                if (existsIter != ExistingQueues.end() && existsIter->second.CustomName == event.CustomName
                    && existsIter->second.Timestamp < ts) {
                    ExistingQueues.erase(existsIter);
                }
            } else if (event.Type == EQueueEventType::Created) {
                if (existsIter == ExistingQueues.end()) {
                    auto iter = ExistingQueues.insert(std::make_pair(
                            qName, event
                    )).first;
                    iter->second.Type = EQueueEventType::Existed;
                }
            }
            SaveQueueEvent(qName, event, ctx);
        }
    }
    if (!QueuesEvents.empty()) {
        Send(ctx.SelfID, new TEvWakeup(StartCleanupTag));
    } else {
        ProcessReindexIfRequired(ctx);
    }
}

void TSearchEventsProcessor::ProcessReindexIfRequired(const TActorContext &ctx) {
    if (LastReindex == TInstant::Zero() || LastReindex + ReindexInterval < ctx.Now()) {
        if (ExistingQueues) {
            TStringStream ss;
            NJson::TJsonWriter writer(&ss, false);
            for (const auto&[queue, event] : ExistingQueues) {
                SaveQueueEvent(queue, event, ctx);
            }
        }
        ProcessReindexResult(ctx);
    } else {
        WaitNextCycle(ctx);
    }
}
void TSearchEventsProcessor::SendJson(const TString& json, const TActorContext &) {
    EventsWriter->Write(json);
}

void TSearchEventsProcessor::ProcessReindexResult(const TActorContext &ctx) {
    ReindexComplete.Inc();
    LastReindex = ctx.Now();
    WaitNextCycle(ctx);
}

void TSearchEventsProcessor::WaitNextCycle(const TActorContext &ctx) {
    ctx.Schedule(RescanInterval, new TEvWakeup(StartQueuesListingTag));
}

ui64 TSearchEventsProcessor::GetReindexCount() const {
    return ReindexComplete.Val();
}

NActors::TActorSystem* TSearchEventsProcessor::GetActorSystem() {
    return TActivationContext::ActorSystem();
}

void TSearchEventsProcessor::StartSession(ui32 evTag, const TActorContext &ctx) {
    Session = Nothing();
    CurrentTx = Nothing();
    PreparedQuery = Nothing();
    QueryResult = Nothing();
    SessionFuture = TableClient->GetSession();
    Apply(&SessionFuture, evTag, ctx);
}

void TSearchEventsProcessor::PrepareDataQuery(const TString& query, ui32 evTag, const TActorContext& ctx) {
    Y_VERIFY(Session.Defined());
    PreparedQuery = Nothing();
    QueryResult = Nothing();
    PrepQueryFuture = Session.Get()->PrepareDataQuery(query);
    Apply(&PrepQueryFuture, evTag, ctx);
}

void TSearchEventsProcessor::StartTx(ui32 evTag, const TActorContext &ctx) {
    Y_VERIFY(Session.Defined());
    CurrentTx = Nothing();
    TxFuture = Session.Get()->BeginTransaction();
    Apply(&TxFuture, evTag, ctx);
}

void TSearchEventsProcessor::CommitTx(ui32 evTag, const TActorContext &ctx) {
    Y_VERIFY(Session.Defined());
    Y_VERIFY(CurrentTx.Defined());
    CommitTxFuture = CurrentTx.Get()->Commit();
    Apply(&CommitTxFuture, evTag, ctx);
}

void TSearchEventsProcessor::RunPrepared(NYdb::TParams&& params, ui32 evTag, const TActorContext& ctx) {
    Y_VERIFY(Session.Defined());
    Y_VERIFY(PreparedQuery.Defined());
    QueryResult = Nothing();
    NTable::TTxControl txControl = CurrentTx.Defined() ? NTable::TTxControl::Tx(*CurrentTx)
                                                       : NTable::TTxControl::BeginTx().CommitTx();
    QueryResultFuture = PreparedQuery.Get()->Execute(txControl, std::forward<NYdb::TParams>(params));
    Apply(&QueryResultFuture, evTag, ctx);
}

bool TSearchEventsProcessor::SessionStarted(const TActorContext&) {
    Y_VERIFY(SessionFuture.HasValue());
    auto& value = SessionFuture.GetValueSync();
    if (!value.IsSuccess())
        return false;
    Session = value.GetSession();
    return true;
}

bool TSearchEventsProcessor::TxStarted(const TActorContext&) {
    Y_VERIFY(Session.Defined());
    Y_VERIFY(TxFuture.HasValue());
    auto& value = TxFuture.GetValueSync();
    if (!value.IsSuccess())
        return false;
    CurrentTx = value.GetTransaction();
    return true;
}
bool TSearchEventsProcessor::TxCommitted(const TActorContext&) {
    Y_VERIFY(Session.Defined());
    Y_VERIFY(CurrentTx.Defined());
    Y_VERIFY(CommitTxFuture.HasValue());
    auto& value = CommitTxFuture.GetValueSync();
    if (!value.IsSuccess())
        return false;
    CurrentTx = Nothing();
    return true;
}

bool TSearchEventsProcessor::QueryPrepared(const TActorContext&) {
    Y_VERIFY(Session.Defined());
    Y_VERIFY(PrepQueryFuture.HasValue());
    auto& value = PrepQueryFuture.GetValueSync();
    if (!value.IsSuccess()) {
        return false;
    }
    PreparedQuery = value.GetQuery();
    return true;
}
bool TSearchEventsProcessor::QueryComplete(const TActorContext&) {
    Y_VERIFY(Session.Defined());
    Y_VERIFY(QueryResultFuture.HasValue());
    QueryResult = QueryResultFuture.GetValueSync();
    if (!QueryResult.Get()->IsSuccess()) {
        return false;
    }
    return true;
}


void TSearchEventsProcessor::SaveQueueEvent(

        const TString& queueName, const TQueueEvent& event, const TActorContext& ctx
) {
    auto tsIsoString = TInstant::MilliSeconds(event.Timestamp).ToIsoStringLocal();
    auto nowIsoString = TInstant::Now().ToIsoStringLocal();
    TStringStream ss;
    NJson::TJsonWriter writer(&ss, false);

    writer.OpenMap();
    {
        writer.Write("resource_type", "message-queue");
        writer.Write("timestamp", tsIsoString);
        writer.Write("resource_id", queueName);
        writer.Write("name", event.CustomName);
        writer.Write("service", "message-queue");
        if (event.Type == EQueueEventType::Deleted)
            writer.Write("deleted",  tsIsoString);
        if (event.Type == EQueueEventType::Existed)
            writer.Write("reindex_timestamp", nowIsoString);
        writer.Write("permission", "ymq.queues.list");
        writer.Write("cloud_id", event.CloudId);
        writer.Write("folder_id", event.FolderId);
        writer.OpenArray("resource_path");
        writer.OpenMap();
        writer.Write("resource_type", "resource-manager.folder");
        writer.Write("resource_id", event.FolderId);
        writer.CloseMap();
        writer.CloseArray(); // resource_path
    }
    writer.CloseMap();
    writer.Flush();
    SendJson(ss.Str(), ctx);
}

void IEventsWriterWrapper::Close() {
    if (!Closed) {
        Closed = true;
        CloseImpl();
    }
}

}; // namespace NKikimr::NSQS
