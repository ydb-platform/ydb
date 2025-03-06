#include "index_events_processor.h"
#include <ydb/public/sdk/cpp/client/ydb_types/ydb.h>

namespace NKikimr::NSQS {
using namespace NActors;
using namespace NJson;
using namespace NKikimr;

constexpr TDuration DEFAULT_RETRY_TIMEOUT = TDuration::Seconds(10);

TSearchEventsProcessor::TSearchEventsProcessor(
        const TString& root, const TDuration& reindexInterval, const TDuration& rescanInterval,
        const TString& database, IEventsWriterWrapper::TPtr eventsWriter, bool waitForWake
)
        : RescanInterval(rescanInterval)
        , ReindexInterval(reindexInterval)
        , EventsWriter(eventsWriter)
        , Database(database)
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
            << "SELECT Account, QueueName, CustomQueueName, CreatedTimestamp, FolderId, Tags from `"
            << getTableFullPath(".Queues") << "` "
            << "WHERE Account > $Account OR (Account = $Account AND QueueName > $QueueName);";

    SelectEventsQuery = TStringBuilder()
            << "--!syntax_v1\n"
            << "SELECT Account, QueueName, EventType, CustomQueueName, EventTimestamp, FolderId, Labels "
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
    State = EState::QueuesListingExecute;
    if (!WaitForWake)
        ctx.Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup());
}

void TSearchEventsProcessor::HandleWakeup(TEvWakeup::TPtr&, const TActorContext& ctx) {
    switch (State) {
        case EState::Stopping:
            return;
        case EState::QueuesListingExecute:
            return StartQueuesListing(ctx);
        case EState::CleanupExecute:
            return RunEventsCleanup(ctx);
        default:
            Y_ABORT();
    }
}

void TSearchEventsProcessor::HandleQueryResponse(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    const auto& record = ev->Get()->Record.GetRef();
    if (record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
        LOG_ERROR_S(ctx, NKikimrServices::SQS,
                    "YC Search events processor: Got error trying to perform request: " << record);
        HandleFailure(ctx);
        return;
    }
    switch (State) {
        case EState::QueuesListingExecute:
            return OnQueuesListQueryComplete(ev, ctx);
        case EState::EventsListingExecute:
            return OnEventsListingDone(ev, ctx);
        case EState::CleanupExecute:
            return OnCleanupQueryComplete(ctx);
        case EState::Stopping:
            return StopSession(ctx);
        default:
            Y_ABORT();
    }
}

void TSearchEventsProcessor::HandleFailure(const TActorContext& ctx) {
    StopSession(ctx);
    switch (State) {
        case EState::EventsListingExecute:
            State = EState::QueuesListingExecute;
        case EState::QueuesListingExecute:
        case EState::CleanupExecute:
            return ctx.Schedule(DEFAULT_RETRY_TIMEOUT, new TEvWakeup());
        case EState::Stopping:
            return;
        default:
            Y_ABORT();
    }
}

void TSearchEventsProcessor::StartQueuesListing(const TActorContext& ctx) {
    LastQueuesKey = {};
    StopSession(ctx);
    ExistingQueues.clear();
    RunQueuesListQuery(ctx);
}

void TSearchEventsProcessor::RunQueuesListQuery(const TActorContext& ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    auto* request = ev->Record.MutableRequest();

    request->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    request->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    request->SetKeepSession(false);
    request->SetPreparedQuery(SelectQueuesQuery);

    NYdb::TParams params = NYdb::TParamsBuilder()
        .AddParam("$Account")
            .Utf8(LastQueuesKey.Account)
            .Build()
        .AddParam("$QueueName")
            .Utf8(LastQueuesKey.QueueName)
            .Build()
        .Build();

    RunQuery(SelectQueuesQuery, &params, true, ctx);
}

void TSearchEventsProcessor::OnQueuesListQueryComplete(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev,
                                                       const TActorContext& ctx) {

    auto& response = ev->Get()->Record.GetRef().GetResponse();

    Y_ABORT_UNLESS(response.YdbResultsSize() == 1);
    TString queueName, cloudId;
    NYdb::TResultSetParser parser(response.GetYdbResults(0));
    // "SELECT Account, QueueName, CustomQueueName, CreatedTimestamp, FolderId"
    while (parser.TryNextRow()) {
        cloudId = *parser.ColumnParser(0).GetOptionalUtf8();
        queueName = *parser.ColumnParser(1).GetOptionalUtf8();
        auto customName = *parser.ColumnParser(2).GetOptionalUtf8();
        auto createTs = *parser.ColumnParser(3).GetOptionalUint64();
        auto folderId = *parser.ColumnParser(4).GetOptionalUtf8();
        auto tags = parser.ColumnParser(5).GetOptionalUtf8().GetOrElse("{}");
        auto insResult = ExistingQueues.insert(std::make_pair(
                queueName, TQueueEvent{EQueueEventType::Existed, createTs, customName, cloudId, folderId, tags}
        ));
        Y_ABORT_UNLESS(insResult.second);
    }
    if (SessionId.empty()) {
        SessionId = response.GetSessionId();
    } else {
        Y_ABORT_UNLESS(SessionId == response.GetSessionId());
    }

    LastQueuesKey.QueueName = queueName;
    LastQueuesKey.Account = cloudId;

    if (parser.RowsCount() > 0) {
        RunQueuesListQuery(ctx);
    } else {
        StopSession(ctx);
        State = EState::EventsListingExecute;
        RunEventsListing(ctx);
    }
}

void TSearchEventsProcessor::RunEventsListing(const TActorContext& ctx) {
    RunQuery(SelectEventsQuery, nullptr, true, ctx);
}

void TSearchEventsProcessor::OnEventsListingDone(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev, const TActorContext& ctx) {
    QueuesEvents.clear();
    const auto& record = ev->Get()->Record.GetRef();
    Y_ABORT_UNLESS(record.GetResponse().YdbResultsSize() == 1);
    NYdb::TResultSetParser parser(record.GetResponse().GetYdbResults(0));

    while (parser.TryNextRow()) {
        // "SELECT Account, QueueName, EventType, CustomQueueName, EventTimestamp, FolderId, Labels
        auto cloudId = *parser.ColumnParser(0).GetOptionalUtf8();
        auto queueName = *parser.ColumnParser(1).GetOptionalUtf8();
        auto evType = *parser.ColumnParser(2).GetOptionalUint64();
        auto customName = *parser.ColumnParser(3).GetOptionalUtf8();
        auto timestamp = *parser.ColumnParser(4).GetOptionalUint64();
        auto folderId = *parser.ColumnParser(5).GetOptionalUtf8();
        auto labels = parser.ColumnParser(6).GetOptionalUtf8().GetOrElse("{}");
        auto& qEvents = QueuesEvents[queueName];
        auto insResult = qEvents.insert(std::make_pair(
                timestamp, TQueueEvent{EQueueEventType(evType), timestamp, customName, cloudId, folderId, labels}
        ));
        Y_ABORT_UNLESS(insResult.second);
    }
    ProcessEventsQueue(ctx);
}

void TSearchEventsProcessor::RunEventsCleanup(const TActorContext& ctx) {
    State = EState::CleanupExecute;

    NYdb::TParamsBuilder paramsBuilder;

    auto& param = paramsBuilder.AddParam("$Events");
    param.BeginList();

    for (const auto&[qName, events] : QueuesEvents) {
        for (const auto&[_, event]: events) {
            param.AddListItem()
                .BeginStruct()
                .AddMember("Account")
                    .Utf8(event.CloudId)
                .AddMember("QueueName")
                    .Utf8(qName)
                .AddMember("EventType")
                    .Uint64(static_cast<ui64>(event.Type))
                .EndStruct();
        }
    }
    param.EndList();
    param.Build();

    auto params = paramsBuilder.Build();

    RunQuery(DeleteEventQuery, &params, false, ctx);
}

void TSearchEventsProcessor::OnCleanupQueryComplete(const TActorContext& ctx) {
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
        State = EState::CleanupExecute;
        RunEventsCleanup(ctx);
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
    State = EState::QueuesListingExecute;
    ctx.Schedule(RescanInterval, new TEvWakeup());
}

ui64 TSearchEventsProcessor::GetReindexCount() const {
    return ReindexComplete.Val();
}

NActors::TActorSystem* TSearchEventsProcessor::GetActorSystem() {
    return TActivationContext::ActorSystem();
}

void TSearchEventsProcessor::StopSession(const TActorContext& ctx) {
    if (!SessionId.empty()) {
        auto ev = MakeHolder<NKqp::TEvKqp::TEvCloseSessionRequest>();
        ev->Record.MutableRequest()->SetSessionId(SessionId);
        Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());
        SessionId = TString();
    }
}

void TSearchEventsProcessor::RunQuery(const TString& query, NYdb::TParams* params, bool readonly,
                                      const TActorContext& ctx) {
    auto ev = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
    auto* request = ev->Record.MutableRequest();

    request->SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
    request->SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
    request->SetKeepSession(State == EState::QueuesListingExecute);
    request->SetQuery(query);
    request->SetUsePublicResponseDataFormat(true);

    if (!SessionId.empty()) {
        request->SetSessionId(SessionId);
    }
    if (!Database.empty())
        request->SetDatabase(Database);

    request->MutableQueryCachePolicy()->set_keep_in_cache(true);

    if (readonly) {
        request->MutableTxControl()->mutable_begin_tx()->mutable_stale_read_only();
    } else {
        request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
    }
    request->MutableTxControl()->set_commit_tx(true);
    if (params != nullptr) {
        request->MutableYdbParameters()->swap(*(NYdb::TProtoAccessor::GetProtoMapPtr(*params)));
    }
    Send(NKqp::MakeKqpProxyID(ctx.SelfID.NodeId()), ev.Release());

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
        if (event.Type == EQueueEventType::Deleted) {
            writer.Write("deleted",  tsIsoString);
        }
        if (event.Type == EQueueEventType::Existed) {
            writer.Write("reindex_timestamp", nowIsoString);
        }
        writer.Write("permission", "ymq.queues.list");
        writer.Write("cloud_id", event.CloudId);
        writer.Write("folder_id", event.FolderId);

        {
            writer.OpenArray("resource_path");
            {
                writer.OpenMap();
                writer.Write("resource_type", "resource-manager.folder");
                writer.Write("resource_id", event.FolderId);
                writer.CloseMap();
            }
            writer.CloseArray();
        }

        if (!event.Labels.empty() && event.Labels != "{}") {
            writer.OpenMap("attributes");
            writer.UnsafeWrite("labels", event.Labels);
            writer.CloseMap();
        }
    }
    writer.CloseMap();
    writer.Flush();
    SendJson(ss.Str(), ctx);
}

void TSearchEventsProcessor::HandlePoisonPill(TEvents::TEvPoisonPill::TPtr&, const TActorContext& ctx) {
    Die(ctx);
}

void IEventsWriterWrapper::Close() {
    if (!Closed) {
        Closed = true;
        CloseImpl();
    }
}

}; // namespace NKikimr::NSQS
