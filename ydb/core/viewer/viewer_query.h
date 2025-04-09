#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NMonitoring;
using namespace NNodeWhiteboard;

class TJsonQuery : public TViewerPipeClient {
    using TThis = TJsonQuery;
    using TBase = TViewerPipeClient;
    std::vector<std::vector<Ydb::ResultSet>> ResultSets;
    TString Query;
    TString Action;
    TString Stats;
    TString Syntax;
    TString QueryId;
    TString ResourcePool;
    TString TransactionMode;
    bool IsBase64Encode = true;
    int LimitRows = 10000;
    int TotalRows = 0;
    bool CollectDiagnostics = true;
    TDuration StatsPeriod;
    TDuration KeepAlive = TDuration::MilliSeconds(10000);
    TInstant LastSendTime;
    static constexpr TDuration WakeupPeriod = TDuration::Seconds(1);

    enum ESchemaType {
        Classic,
        Modern,
        Multi,
        Ydb,
        Ydb2,
    };
    ESchemaType Schema = ESchemaType::Classic;
    TRequestResponse<NKqp::TEvKqp::TEvCreateSessionResponse> CreateSessionResponse;
    TRequestResponse<NKqp::TEvKqp::TEvQueryResponse> QueryResponse;
    TString SessionId;
    ui64 OutputChunkMaxSize = 0;
    bool Streaming = false;
    NHttp::THttpOutgoingResponsePtr HttpResponse;
    std::vector<bool> ResultSetHasColumns;
    bool ConcurrentResults = false;
    TString ContentType;

public:
    ESchemaType StringToSchemaType(const TString& schemaStr) {
        if (schemaStr == "classic") {
            return ESchemaType::Classic;
        } else if (schemaStr == "modern") {
            return ESchemaType::Modern;
        } else if (schemaStr == "multi" || schemaStr == "multipart") {
            return ESchemaType::Multi;
        } else if (schemaStr == "ydb") {
            return ESchemaType::Ydb;
        } else if (schemaStr == "ydb2") {
            return ESchemaType::Ydb2;
        } else {
            return ESchemaType::Classic;
        }
    }

    void InitConfig(const TCgiParameters& params) {
        Timeout = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("timeout"), 60000)); // override default timeout to 60 seconds
        if (params.Has("query")) {
            Query = params.Get("query");
        }
        if (params.Has("database")) {
            Database = params.Get("database");
        }
        if (params.Has("stats")) {
            Stats = params.Get("stats");
        }
        if (params.Has("action")) {
            Action = params.Get("action");
        }
        if (params.Has("schema")) {
            Schema = StringToSchemaType(params.Get("schema"));
            if (Params.Get("schema") == "multipart") {
                Streaming = true;
                if (params.Has("concurrent_results")) {
                    ConcurrentResults = FromStringWithDefault<bool>(params.Get("concurrent_results"), ConcurrentResults);
                }
                if (params.Has("keep_alive")) {
                    KeepAlive = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("keep_alive"), KeepAlive.MilliSeconds()));
                }
            }
        }
        if (params.Has("syntax")) {
            Syntax = params.Get("syntax");
        }
        if (params.Has("query_id")) {
            QueryId = params.Get("query_id");
        }
        if (params.Has("transaction_mode")) {
            TransactionMode = params.Get("transaction_mode");
        }
        if (params.Has("base64")) {
            IsBase64Encode = FromStringWithDefault<bool>(params.Get("base64"), true);
        }
        if (params.Has("limit_rows")) {
            LimitRows = std::clamp<int>(FromStringWithDefault<int>(params.Get("limit_rows"), 10000), 1, Streaming ? std::numeric_limits<int>::max() : 100000);
        }
        if (params.Has("resource_pool")) {
            ResourcePool = params.Get("resource_pool");
        }
        if (params.Has("output_chunk_max_size")) {
            OutputChunkMaxSize = FromStringWithDefault<ui64>(params.Get("output_chunk_max_size"), OutputChunkMaxSize);
        }
        CollectDiagnostics = FromStringWithDefault<bool>(params.Get("collect_diagnostics"), CollectDiagnostics);
        if (params.Has("stats_period")) {
            StatsPeriod = TDuration::MilliSeconds(std::clamp<ui64>(FromStringWithDefault<ui64>(params.Get("stats_period"), StatsPeriod.MilliSeconds()), 1000, 600000));
        }
    }

    TJsonQuery(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TBase(viewer, ev)
    {
        InitConfig(Params);
    }

    void Bootstrap() override {
        if (NeedToRedirect()) {
            return;
        }
        if (Query.empty() && Action != "cancel-query") {
            return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Query is empty"), "EmptyQuery");
        }
        if (Streaming) {
            NHttp::THeaders headers(HttpEvent->Get()->Request->Headers);
            TStringBuf accept = headers["Accept"];
            auto posMixedReplace = accept.find("multipart/x-mixed-replace");
            auto posFormData = accept.find("multipart/form-data");
            auto posFirst = std::min(posMixedReplace, posFormData);
            if (posFirst == TString::npos) {
                return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Multipart request must accept multipart content-type"), "BadRequest");
            }
            if (posFirst == posMixedReplace) {
                ContentType = "multipart/x-mixed-replace";
            } else if (posFirst == posFormData) {
                ContentType = "multipart/form-data";
            }
        }
        if (Streaming && QueryId.empty()) {
            QueryId = CreateGuidAsString();
        }
        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvSubscribeForCancel(), IEventHandle::FlagTrackDelivery);
        SendKpqProxyRequest();
        Become(&TThis::StateWork);
        if (Timeout || KeepAlive) {
            Schedule(WakeupPeriod, new TEvents::TEvWakeup());
        }
        LastSendTime = TActivationContext::Now();
    }

    void CancelQuery() {
        if (SessionId) {
            auto event = std::make_unique<NKqp::TEvKqp::TEvCancelQueryRequest>();
            event->Record.MutableRequest()->SetSessionId(SessionId);
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
            if (QueryResponse && !QueryResponse.IsDone()) {
                QueryResponse.Error("QueryCancelled");
            }
        }
    }

    void CloseSession() {
        if (SessionId) {
            if (QueryResponse && !QueryResponse.IsDone()) {
                CancelQuery();
            }
            auto event = std::make_unique<NKqp::TEvKqp::TEvCloseSessionRequest>();
            event->Record.MutableRequest()->SetSessionId(SessionId);
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
        }
    }

    void Cancelled() {
        CancelQuery();
        PassAway();
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr& ev) {
        if (ev->Get()->SourceType == NHttp::TEvHttpProxy::EvSubscribeForCancel) {
            Cancelled();
        }
    }

    void PassAway() override {
        if (QueryId) {
            Viewer->EndRunningQuery(QueryId, SelfId());
        }
        CloseSession();
        TBase::PassAway();
    }

    void ReplyAndPassAway() override {}

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvCreateSessionResponse, HandleReply);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleReply);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleReply);
            hFunc(NKqp::TEvKqp::TEvPingSessionResponse, HandleReply);
            hFunc(NKqp::TEvKqpExecuter::TEvExecuterProgress, HandleReply);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, HandleReply);
            cFunc(NHttp::TEvHttpProxy::EvRequestCancelled, Cancelled);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        }
    }

    void SendKpqProxyRequest() {
        if (QueryId) {
            TActorId actorId = Viewer->FindRunningQuery(QueryId);
            if (actorId) {
                auto event = std::make_unique<NKqp::TEvKqp::TEvAbortExecution>();
                Ydb::Issue::IssueMessage* issue = event->Record.AddIssues();
                issue->set_message("Query was cancelled");
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                Send(actorId, event.release());

                if (Action == "cancel-query") {
                    return TBase::ReplyAndPassAway(GetHTTPOK("text/plain", "Query was cancelled"));
                }
            } else {
                if (Action == "cancel-query") {
                    return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Query not found"), "BadRequest");
                }
            }
            Viewer->AddRunningQuery(QueryId, SelfId());
        }

        auto event = std::make_unique<NKqp::TEvKqp::TEvCreateSessionRequest>();
        if (Database) {
            event->Record.MutableRequest()->SetDatabase(Database);
            if (Span) {
                Span.Attribute("database", Database);
            }
        }
        auto request = GetRequest();
        event->Record.SetApplicationName("ydb-ui");
        event->Record.SetClientAddress(request.GetRemoteAddr());
        event->Record.SetClientUserAgent(TString(request.GetHeader("User-Agent")));
        if (TString tokenString = request.GetUserTokenObject()) {
            NACLibProto::TUserToken userToken;
            if (userToken.ParseFromString(tokenString)) {
                event->Record.SetUserSID(userToken.GetUserSID());
            }
        }
        CreateSessionResponse = MakeRequest<NKqp::TEvKqp::TEvCreateSessionResponse>(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
        if (Streaming) {
            HttpResponse = HttpEvent->Get()->Request->CreateResponseString(Viewer->GetChunkedHTTPOK(GetRequest(), ContentType + ";boundary=boundary"));
            Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(HttpResponse));
        }
    }

    void SetTransactionMode(NKikimrKqp::TQueryRequest& request) {
        if (TransactionMode == "serializable-read-write") {
            request.mutable_txcontrol()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_txcontrol()->set_commit_tx(true);
        } else if (TransactionMode == "online-read-only") {
            request.mutable_txcontrol()->mutable_begin_tx()->mutable_online_read_only();
            request.mutable_txcontrol()->set_commit_tx(true);
        } else if (TransactionMode == "stale-read-only") {
            request.mutable_txcontrol()->mutable_begin_tx()->mutable_stale_read_only();
            request.mutable_txcontrol()->set_commit_tx(true);
        } else if (TransactionMode == "snapshot-read-only") {
            request.mutable_txcontrol()->mutable_begin_tx()->mutable_snapshot_read_only();
            request.mutable_txcontrol()->set_commit_tx(true);
        }
    }

    void PingSession() {
        auto event = std::make_unique<NKqp::TEvKqp::TEvPingSessionRequest>();
        event->Record.MutableRequest()->SetSessionId(SessionId);
        ActorIdToProto(SelfId(), event->Record.MutableRequest()->MutableExtSessionCtrlActorId());
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
    }

    void HandleReply(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev) {
        if (ev->Get()->Record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            CreateSessionResponse.Set(std::move(ev));
        } else {
            CreateSessionResponse.Error("FailedToCreateSession");
            NYdb::NIssue::TIssue issue;
            issue.SetMessage("Failed to create session");
            issue.SetCode(ev->Get()->Record.GetYdbStatus(), NYdb::NIssue::ESeverity::Error);
            NJson::TJsonValue json;
            TString message;
            MakeJsonErrorReply(json, message, NYdb::TStatus(NYdb::EStatus(ev->Get()->Record.GetYdbStatus()), NYdb::NIssue::TIssues{issue}));
            return ReplyWithJsonAndPassAway(json, message);
        }

        SessionId = CreateSessionResponse->Record.GetResponse().GetSessionId();
        PingSession();

        if (Streaming) {
            NJson::TJsonValue json;
            json["version"] = Viewer->GetCapabilityVersion("/viewer/query");
            NJson::TJsonValue& jsonMeta = json["meta"];
            jsonMeta["event"] = "SessionCreated";
            jsonMeta["session_id"] = SessionId;
            jsonMeta["query_id"] = QueryId;
            jsonMeta["node_id"] = SelfId().NodeId();
            StreamJsonResponse(json);
        }

        auto event = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
        request.SetQuery(Query);
        request.SetSessionId(SessionId);
        if (Database) {
            request.SetDatabase(Database);
        }
        if (TString userToken = GetRequest().GetUserTokenObject()) {
            event->Record.SetUserToken(userToken);
        }
        if (ResourcePool) {
            request.SetPoolId(ResourcePool);
        }
        request.SetClientAddress(GetRequest().GetRemoteAddr());
        if (Action == "execute-script") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
            request.SetKeepSession(false);
        } else if (Action.empty() || Action == "execute-query" || Action == "execute") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(ConcurrentResults ? NKikimrKqp::QUERY_TYPE_SQL_GENERIC_CONCURRENT_QUERY : NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
            request.SetKeepSession(false);
            SetTransactionMode(request);
        } else if (Action == "explain-query") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
            request.SetKeepSession(false);
        } else if (Action == "execute-scan") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
            request.SetKeepSession(false);
        } else if (Action == "execute-data") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
            request.SetKeepSession(false);
            SetTransactionMode(request);
            if (!request.txcontrol().has_begin_tx()) {
                request.mutable_txcontrol()->mutable_begin_tx()->mutable_serializable_read_write();
                request.mutable_txcontrol()->set_commit_tx(true);
            }
        } else if (Action == "explain" || Action == "explain-ast" || Action == "explain-data") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DML);
        } else if (Action == "explain-scan") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCAN);
        } else if (Action == "explain-script") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXPLAIN);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
        }
        if (Stats == "none") {
            request.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_NONE);
            request.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_NONE);
        } else if (Stats == "basic") {
            request.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_BASIC);
            request.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_BASIC);
        } else if (Stats == "profile") {
            request.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_PROFILE);
            request.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE);
        } else if (Stats == "full") {
            request.SetStatsMode(NYql::NDqProto::DQ_STATS_MODE_FULL);
            request.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL);
        }
        if (Syntax == "yql_v1") {
            request.SetSyntax(Ydb::Query::Syntax::SYNTAX_YQL_V1);
        } else if (Syntax == "pg") {
            request.SetSyntax(Ydb::Query::Syntax::SYNTAX_PG);
        }
        if (OutputChunkMaxSize) {
            request.SetOutputChunkMaxSize(OutputChunkMaxSize);
        }
        request.SetCollectDiagnostics(CollectDiagnostics);
        if (StatsPeriod) {
            event->SetProgressStatsPeriod(StatsPeriod);
        }
        ActorIdToProto(SelfId(), event->Record.MutableRequestActorId());
        QueryResponse = MakeRequest<NKqp::TEvKqp::TEvQueryResponse>(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());

    }

private:
    NJson::TJsonValue ColumnPrimitiveValueToJsonValue(NYdb::TValueParser& valueParser) {
        switch (const auto primitive = valueParser.GetPrimitiveType()) {
            case NYdb::EPrimitiveType::Bool:
                return valueParser.GetBool();
            case NYdb::EPrimitiveType::Int8:
                return valueParser.GetInt8();
            case NYdb::EPrimitiveType::Uint8:
                return valueParser.GetUint8();
            case NYdb::EPrimitiveType::Int16:
                return valueParser.GetInt16();
            case NYdb::EPrimitiveType::Uint16:
                return valueParser.GetUint16();
            case NYdb::EPrimitiveType::Int32:
                return valueParser.GetInt32();
            case NYdb::EPrimitiveType::Uint32:
                return valueParser.GetUint32();
            case NYdb::EPrimitiveType::Int64:
                return TStringBuilder() << valueParser.GetInt64();
            case NYdb::EPrimitiveType::Uint64:
                return TStringBuilder() << valueParser.GetUint64();
            case NYdb::EPrimitiveType::Float:
                return valueParser.GetFloat();
            case NYdb::EPrimitiveType::Double:
                return valueParser.GetDouble();
            case NYdb::EPrimitiveType::Utf8:
                return valueParser.GetUtf8();
            case NYdb::EPrimitiveType::Date:
                return valueParser.GetDate().ToString();
            case NYdb::EPrimitiveType::Datetime:
                return valueParser.GetDatetime().ToString();
            case NYdb::EPrimitiveType::Timestamp:
                return valueParser.GetTimestamp().ToString();
            case NYdb::EPrimitiveType::Interval:
                return TStringBuilder() << valueParser.GetInterval();
            case NYdb::EPrimitiveType::Date32:
                return valueParser.GetDate32();
            case NYdb::EPrimitiveType::Datetime64:
                return valueParser.GetDatetime64();
            case NYdb::EPrimitiveType::Timestamp64:
                return valueParser.GetTimestamp64();
            case NYdb::EPrimitiveType::Interval64:
                return valueParser.GetInterval64();
            case NYdb::EPrimitiveType::TzDate:
                return valueParser.GetTzDate();
            case NYdb::EPrimitiveType::TzDatetime:
                return valueParser.GetTzDatetime();
            case NYdb::EPrimitiveType::TzTimestamp:
                return valueParser.GetTzTimestamp();
            case NYdb::EPrimitiveType::String:
                return IsBase64Encode ? Base64Encode(valueParser.GetString()) : valueParser.GetString();
            case NYdb::EPrimitiveType::Yson:
                return valueParser.GetYson();
            case NYdb::EPrimitiveType::Json:
                return valueParser.GetJson();
            case NYdb::EPrimitiveType::JsonDocument:
                return valueParser.GetJsonDocument();
            case NYdb::EPrimitiveType::DyNumber:
                return valueParser.GetDyNumber();
            case NYdb::EPrimitiveType::Uuid:
                return valueParser.GetUuid().ToString();
        }
        return NJson::JSON_UNDEFINED;
    }

    NJson::TJsonValue ColumnValueToJsonValue(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetKind()) {
            case NYdb::TTypeParser::ETypeKind::Primitive:
                return ColumnPrimitiveValueToJsonValue(valueParser);

            case NYdb::TTypeParser::ETypeKind::Optional:
                {
                    NJson::TJsonValue jsonOptional;
                    valueParser.OpenOptional();
                    if (valueParser.IsNull()) {
                        jsonOptional = NJson::JSON_NULL;
                    } else {
                        switch(valueParser.GetKind()) {
                            case NYdb::TTypeParser::ETypeKind::Primitive:
                                jsonOptional = ColumnPrimitiveValueToJsonValue(valueParser);
                                break;
                            case NYdb::TTypeParser::ETypeKind::Decimal:
                                jsonOptional = valueParser.GetDecimal().ToString();
                                break;
                            default:
                                jsonOptional = NJson::JSON_UNDEFINED;
                                break;
                        }
                    }
                    valueParser.CloseOptional();
                    return jsonOptional;
                }

            case NYdb::TTypeParser::ETypeKind::Tagged:
                {
                    NJson::TJsonValue jsonTagged;
                    valueParser.OpenTagged();
                    jsonTagged = ColumnValueToJsonValue(valueParser);
                    valueParser.CloseTagged();
                    return jsonTagged;
                }

            case NYdb::TTypeParser::ETypeKind::Pg:
                return valueParser.GetPg().Content_;

            case NYdb::TTypeParser::ETypeKind::Decimal:
                return valueParser.GetDecimal().ToString();

            case NYdb::TTypeParser::ETypeKind::List:
                {
                    NJson::TJsonValue jsonList;
                    jsonList.SetType(NJson::JSON_ARRAY);
                    valueParser.OpenList();
                    while (valueParser.TryNextListItem()) {
                        jsonList.AppendValue(ColumnValueToJsonValue(valueParser));
                    }
                    valueParser.CloseList();
                    return jsonList;
                }

            case NYdb::TTypeParser::ETypeKind::Tuple:
                {
                    NJson::TJsonValue jsonTuple;
                    jsonTuple.SetType(NJson::JSON_ARRAY);
                    valueParser.OpenTuple();
                    while (valueParser.TryNextElement()) {
                        jsonTuple.AppendValue(ColumnValueToJsonValue(valueParser));
                    }
                    valueParser.CloseTuple();
                    return jsonTuple;
                }

            case NYdb::TTypeParser::ETypeKind::Struct:
                {
                    NJson::TJsonValue jsonStruct;
                    jsonStruct.SetType(NJson::JSON_MAP);
                    valueParser.OpenStruct();
                    while (valueParser.TryNextMember()) {
                        jsonStruct[valueParser.GetMemberName()] = ColumnValueToJsonValue(valueParser);
                    }
                    valueParser.CloseStruct();
                    return jsonStruct;
                }

            case NYdb::TTypeParser::ETypeKind::Dict:
                {
                    NJson::TJsonValue jsonDict;
                    jsonDict.SetType(NJson::JSON_MAP);
                    valueParser.OpenDict();
                    while (valueParser.TryNextDictItem()) {
                        valueParser.DictKey();
                        NJson::TJsonValue jsonDictKey = ColumnValueToJsonValue(valueParser);
                        valueParser.DictPayload();
                        jsonDict[jsonDictKey.GetStringRobust()] = ColumnValueToJsonValue(valueParser);
                    }
                    valueParser.CloseDict();
                    return jsonDict;
                }

            case NYdb::TTypeParser::ETypeKind::Variant:
                {
                    valueParser.OpenVariant();
                    NJson::TJsonValue jsonVariant = ColumnValueToJsonValue(valueParser);
                    valueParser.CloseVariant();
                    return jsonVariant;
                }

            case NYdb::TTypeParser::ETypeKind::EmptyList:
                return NJson::JSON_ARRAY;

            case NYdb::TTypeParser::ETypeKind::EmptyDict:
                return NJson::JSON_MAP;

            default:
                return NJson::JSON_UNDEFINED;
        }
    }

    void HandleReply(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        NJson::TJsonValue jsonResponse;
        if (Streaming) {
            NJson::TJsonValue& jsonMeta = jsonResponse["meta"];
            jsonMeta["event"] = "QueryResponse";
        } else {
            jsonResponse["version"] = Viewer->GetCapabilityVersion("/viewer/query");
        }
        if (ev->Get()->Record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            QueryResponse.Set(std::move(ev));
            MakeOkReply(jsonResponse, QueryResponse->Record);
            if (Schema == ESchemaType::Classic && Stats.empty() && (Action.empty() || Action == "execute")) {
                jsonResponse = std::move(jsonResponse["result"]);
            }
        } else {
            QueryResponse.Error("QueryError");
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetResponse().GetQueryIssues(), issues);
            MakeErrorReply(jsonResponse, NYdb::TStatus(NYdb::EStatus(ev->Get()->Record.GetYdbStatus()), NYdb::NAdapters::ToSdkIssues(std::move(issues))));
        }
        ReplyWithJsonAndPassAway(jsonResponse);
    }

    void HandleReply(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev) {
        QueryResponse.Error("Aborted");
        auto& record(ev->Get()->Record);
        NJson::TJsonValue jsonResponse;
        if (record.IssuesSize() > 0) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            MakeErrorReply(jsonResponse, NYdb::TStatus(NYdb::EStatus(record.GetStatusCode()), NYdb::NAdapters::ToSdkIssues(std::move(issues))));
        }
        CancelQuery();
        ReplyWithJsonAndPassAway(jsonResponse);
    }

    void HandleReply(NKqp::TEvKqpExecuter::TEvExecuterProgress::TPtr& ev) {
        if (Streaming) {
            NJson::TJsonValue json;
            NJson::TJsonValue& jsonMeta = json["meta"];
            jsonMeta["event"] = "Progress";
            auto& progress(ev->Get()->Record);
            if (progress.HasQueryPlan()) {
                NJson::ReadJsonTree(progress.GetQueryPlan(), &(json["plan"]));
            }
            if (progress.HasQueryStats()) {
                Proto2Json(progress.GetQueryStats(), json["stats"]);
            }
            StreamJsonResponse(json);
        }
    }

    void HandleReply(NKqp::TEvKqp::TEvPingSessionResponse::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void HandleReply(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        QueryResponse.Event("StreamData");
        NKikimrKqp::TEvExecuterStreamData& data(ev->Get()->Record);

        if (TotalRows < LimitRows) {
            int rowsAvailable = LimitRows - TotalRows;
            if (data.GetResultSet().rows_size() > rowsAvailable) {
                data.MutableResultSet()->mutable_rows()->Truncate(rowsAvailable);
                data.MutableResultSet()->set_truncated(true);
            }
            TotalRows += data.GetResultSet().rows_size();
            if (Streaming) {
                StreamJsonResponse(data);
            } else {
                if (ResultSets.size() <= data.GetQueryResultIndex()) {
                    ResultSets.resize(data.GetQueryResultIndex() + 1);
                }
                ResultSets[data.GetQueryResultIndex()].emplace_back() = std::move(*data.MutableResultSet());
            }
        }

        THolder<NKqp::TEvKqpExecuter::TEvStreamDataAck> ack = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>(ev->Get()->Record.GetSeqNo(), ev->Get()->Record.GetChannelId());
        if (TotalRows >= LimitRows) {
            ack->Record.SetEnough(true);
        }
        Send(ev->Sender, ack.Release());
    }

    void ReplyWithError(const TString& error) {
        if (SessionId) {
            auto event = std::make_unique<NKqp::TEvKqp::TEvCancelQueryRequest>();
            event->Record.MutableRequest()->SetSessionId(SessionId);
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
        }
        NJson::TJsonValue json;
        json["error"]["severity"] = NYql::TSeverityIds::S_ERROR;
        json["error"]["message"] = error;
        NJson::TJsonValue& issue = json["issues"].AppendValue({});
        issue["severity"] = NYql::TSeverityIds::S_ERROR;
        issue["message"] = error;
        ReplyWithJsonAndPassAway(json);
    }

    void HandleWakeup() {
        auto now = TActivationContext::Now();
        if (Timeout && (now - LastSendTime > Timeout)) {
            return ReplyWithError("Timeout executing query");
        }
        if (KeepAlive && (now - LastSendTime > KeepAlive)) {
            SendKeepAlive();
        }
        Schedule(WakeupPeriod, new TEvents::TEvWakeup());
    }

private:
    void RenderResultSetMulti(NJson::TJsonValue& jsonResult, const NYdb::TResultSet& resultSet, bool& hasColumns) {
        if (!hasColumns) {
            NJson::TJsonValue& jsonColumns = jsonResult["columns"];
            jsonColumns.SetType(NJson::JSON_ARRAY);
            const auto& columnsMeta = resultSet.GetColumnsMeta();
            for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                NJson::TJsonValue& jsonColumn = jsonColumns.AppendValue({});
                const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                jsonColumn["name"] = columnMeta.Name;
                jsonColumn["type"] = columnMeta.Type.ToString();
            }
            hasColumns = true;
        }

        NJson::TJsonValue& jsonRows = jsonResult["rows"];
        NYdb::TResultSetParser rsParser(resultSet);
        while (rsParser.TryNextRow()) {
            NJson::TJsonValue& jsonRow = jsonRows.AppendValue({});
            jsonRow.SetType(NJson::JSON_ARRAY);
            for (size_t columnNum = 0; columnNum < rsParser.ColumnsCount(); ++columnNum) {
                NJson::TJsonValue& jsonColumn = jsonRow.AppendValue({});
                jsonColumn = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
            }
        }

        if (resultSet.Truncated()) {
            jsonResult["truncated"] = true;
        }
    }

    void MakeErrorReply(NJson::TJsonValue& jsonResponse, const NYdb::TStatus& status) {
        TString message;

        MakeJsonErrorReply(jsonResponse, message, status);

        if (Span) {
            Span.EndError(message);
        }
    }

    void MakeOkReply(NJson::TJsonValue& jsonResponse, NKikimrKqp::TEvQueryResponse& record) {
        try {
            const auto& response = record.GetResponse();

            if (response.YdbResultsSize() > 0) {

                for (const auto& result : response.GetYdbResults()) {
                    ResultSets.emplace_back().emplace_back(result);
                }

            }

            if (ResultSets.size() > 0 && !Streaming) {
                if (Schema == ESchemaType::Classic) {
                    NJson::TJsonValue& jsonResults = jsonResponse["result"];
                    jsonResults.SetType(NJson::JSON_ARRAY);
                    for (const auto& resultSets : ResultSets) {
                        for (NYdb::TResultSet resultSet : resultSets) {
                            const auto& columnsMeta = resultSet.GetColumnsMeta();
                            NYdb::TResultSetParser rsParser(resultSet);
                            while (rsParser.TryNextRow()) {
                                NJson::TJsonValue& jsonRow = jsonResults.AppendValue({});
                                for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                                    const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                                    jsonRow[columnMeta.Name] = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
                                }
                            }
                        }
                    }
                }

                if (Schema == ESchemaType::Modern) {
                    {
                        NJson::TJsonValue& jsonColumns = jsonResponse["columns"];
                        NYdb::TResultSet resultSet(ResultSets.front().front());
                        const auto& columnsMeta = resultSet.GetColumnsMeta();
                        jsonColumns.SetType(NJson::JSON_ARRAY);
                        for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                            NJson::TJsonValue& jsonColumn = jsonColumns.AppendValue({});
                            const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                            jsonColumn["name"] = columnMeta.Name;
                            jsonColumn["type"] = columnMeta.Type.ToString();
                        }
                    }

                    NJson::TJsonValue& jsonResults = jsonResponse["result"];
                    jsonResults.SetType(NJson::JSON_ARRAY);
                    for (const auto& resultSets : ResultSets) {
                        for (NYdb::TResultSet resultSet : resultSets) {
                            const auto& columnsMeta = resultSet.GetColumnsMeta();
                            NYdb::TResultSetParser rsParser(resultSet);
                            while (rsParser.TryNextRow()) {
                                NJson::TJsonValue& jsonRow = jsonResults.AppendValue({});
                                jsonRow.SetType(NJson::JSON_ARRAY);
                                for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                                    NJson::TJsonValue& jsonColumn = jsonRow.AppendValue({});
                                    jsonColumn = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
                                }
                            }
                        }
                    }
                }

                if (Schema == ESchemaType::Multi) {
                    NJson::TJsonValue& jsonResults = jsonResponse["result"];
                    jsonResults.SetType(NJson::JSON_ARRAY);
                    for (const auto& resultSets : ResultSets) {
                        NJson::TJsonValue& jsonResult = jsonResults.AppendValue({});
                        bool hasColumns = false;
                        for (NYdb::TResultSet resultSet : resultSets) {
                            RenderResultSetMulti(jsonResult, resultSet, hasColumns);
                        }
                    }
                }

                if (Schema == ESchemaType::Ydb) {
                    NJson::TJsonValue& jsonResults = jsonResponse["result"];
                    jsonResults.SetType(NJson::JSON_ARRAY);
                    for (const auto& resultSets : ResultSets) {
                        for (NYdb::TResultSet resultSet : resultSets) {
                            const auto& columnsMeta = resultSet.GetColumnsMeta();
                            NYdb::TResultSetParser rsParser(resultSet);
                            while (rsParser.TryNextRow()) {
                                NJson::TJsonValue& jsonRow = jsonResults.AppendValue({});
                                TString row = NYdb::FormatResultRowJson(rsParser, columnsMeta, IsBase64Encode ? NYdb::EBinaryStringEncoding::Base64 : NYdb::EBinaryStringEncoding::Unicode);
                                NJson::ReadJsonTree(row, &jsonRow);
                            }
                        }
                    }
                }

                if (Schema == ESchemaType::Ydb2) {
                    NJson::TJsonValue& jsonResults = jsonResponse["result"];
                    jsonResults.SetType(NJson::JSON_ARRAY);
                    for (const auto& resultSets : ResultSets) {
                        NJson::TJsonValue& jsonResult = jsonResults.AppendValue({});
                        bool hasColumns = false;
                        for (NYdb::TResultSet resultSet : resultSets) {
                            if (!hasColumns) {
                                NJson::TJsonValue& jsonColumns = jsonResult["columns"];
                                jsonColumns.SetType(NJson::JSON_ARRAY);
                                const auto& columnsMeta = resultSet.GetColumnsMeta();
                                for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                                    NJson::TJsonValue& jsonColumn = jsonColumns.AppendValue({});
                                    const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                                    jsonColumn["name"] = columnMeta.Name;
                                    jsonColumn["type"] = columnMeta.Type.ToString();
                                }
                                hasColumns = true;
                            }
                            NJson::TJsonValue& jsonRows = jsonResult["rows"];
                            const auto& columnsMeta = resultSet.GetColumnsMeta();
                            NYdb::TResultSetParser rsParser(resultSet);
                            while (rsParser.TryNextRow()) {
                                NJson::TJsonValue& jsonRow = jsonRows.AppendValue({});
                                TString row = NYdb::FormatResultRowJson(rsParser, columnsMeta, IsBase64Encode ? NYdb::EBinaryStringEncoding::Base64 : NYdb::EBinaryStringEncoding::Unicode);
                                NJson::ReadJsonTree(row, &jsonRow);
                            }
                        }
                    }
                }
            }
            if (response.HasQueryAst()) {
                jsonResponse["ast"] = response.GetQueryAst();
            }
            if (response.HasQueryPlan()) {
                NJson::ReadJsonTree(response.GetQueryPlan(), &(jsonResponse["plan"]));
            }
            if (response.HasQueryStats()) {
                Proto2Json(response.GetQueryStats(), jsonResponse["stats"]);
            }
        }
        catch (const std::exception& ex) {
            NYdb::NIssue::TIssues issues;
            issues.AddIssue(TStringBuilder() << "Convert error: " << ex.what());
            MakeErrorReply(jsonResponse, NYdb::TStatus(NYdb::EStatus::BAD_REQUEST, std::move(issues)));
            return;
        }
    }

    void StreamJsonResponse(const NJson::TJsonValue& json) {
        constexpr EFloatToStringMode floatMode = EFloatToStringMode::PREC_NDIGITS;
        TStringStream content;
        NJson::WriteJson(&content, &json, {
            .FloatToStringMode = floatMode,
            .ValidateUtf8 = false,
            .WriteNanAsString = true,
        });
        TStringBuilder data;
        data << "--boundary\r\nContent-Type: application/json\r\nContent-Length: " << content.Size() << "\r\n\r\n" << content.Str() << "\r\n";
        auto dataChunk = HttpResponse->CreateDataChunk(data);
        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
        LastSendTime = TActivationContext::Now();
    }

    void StreamJsonResponse(const NKikimrKqp::TEvExecuterStreamData& data) {
        NJson::TJsonValue json;
        NJson::TJsonValue& jsonMeta = json["meta"];
        jsonMeta["event"] = "StreamData";
        jsonMeta["result_index"] = data.GetQueryResultIndex();
        jsonMeta["seq_no"] = data.GetSeqNo();
        if (ResultSetHasColumns.size() <= data.GetQueryResultIndex()) {
            ResultSetHasColumns.resize(data.GetQueryResultIndex() + 1);
        }
        try {
            RenderResultSetMulti(json["result"], data.GetResultSet(), ResultSetHasColumns[data.GetQueryResultIndex()]);
        } catch (const std::exception& ex) {
            return ReplyWithError(ex.what());
        }
        StreamJsonResponse(json);
    }

    void StreamEndOfStream() {
        auto dataChunk = HttpResponse->CreateDataChunk("--boundary--\r\n");
        dataChunk->SetEndOfData();
        Send(HttpEvent->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingDataChunk(dataChunk));
        LastSendTime = TActivationContext::Now();
    }

    void SendKeepAlive() {
        if (Streaming) {
            NJson::TJsonValue json;
            NJson::TJsonValue& jsonMeta = json["meta"];
            jsonMeta["event"] = "KeepAlive";
            StreamJsonResponse(json);
        }
        if (SessionId) {
            PingSession();
        }
    }

    void ReplyWithJsonAndPassAway(const NJson::TJsonValue& json, const TString& error = {}) {
        if (Streaming) {
            StreamJsonResponse(json);
            StreamEndOfStream();
            HttpEvent.Reset(); // to avoid double reply
            TBase::ReplyAndPassAway(error);
        } else {
            TBase::ReplyAndPassAway(GetHTTPOKJSON(json), error);
        }
    }

public:
    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        post:
            tags:
              - viewer
            summary: Executes SQL query
            description: Executes SQL query
            parameters:
              - name: action
                in: query
                type: string
                enum: [execute-scan, execute-script, execute-query, execute-data, explain-ast, explain-scan, explain-script, explain-query, explain-data, cancel-query]
                required: true
                description: >
                    execute method:
                      * `execute-query` - execute query (QueryService)
                      * `execute-data` - execute data query (DataQuery)
                      * `execute-scan` - execute scan query (ScanQuery)
                      * `execute-script` - execute script query (ScriptingService)
                      * `explain-query` - explain query (QueryService)
                      * `explain-data` - explain data query (DataQuery)
                      * `explain-scan` - explain scan query (ScanQuery)
                      * `explain-script` - explain script query (ScriptingService)
                      * `cancel-query` - cancel query (using query_id)
              - name: database
                in: query
                description: database name
                type: string
                required: false
              - name: query
                in: query
                description: SQL query text
                type: string
                required: false
              - name: query_id
                in: query
                description: unique query identifier (uuid) - use the same id to cancel query
                required: false
              - name: syntax
                in: query
                description: >
                    query syntax:
                      * `yql_v1` - YQL v1 (default)
                      * `pg` - PostgreSQL compatible
                type: string
                enum: [yql_v1, pg]
                required: false
              - name: schema
                in: query
                description: >
                    result format schema:
                      * `classic`
                      * `modern`
                      * `multi`
                      * `multipart`
                      * `ydb`
                      * `ydb2`
                type: string
                enum: [classic, modern, ydb, multi]
                required: false
              - name: stats
                in: query
                description: >
                    return stats:
                      * `none`
                      * `basic`
                      * `profile`
                      * `full`
                type: string
                enum: [profile, full]
                required: false
              - name: transaction_mode
                in: query
                description: >
                    transaction mode:
                      * `serializable-read-write`
                      * `online-read-only`
                      * `stale-read-only`
                      * `snapshot-read-only`
                type: string
                enum: [serializable-read-write, online-read-only, stale-read-only, snapshot-read-only]
                required: false
              - name: output_chunk_max_size
                in: query
                description: output chunk max size
                type: integer
                required: false
              - name: limit_rows
                in: query
                description: limit rows
                type: integer
                required: false
                default: 10000
              - name: concurrent_results
                in: query
                description: concurrent results
                type: boolean
                required: false
                default: false
              - name: base64
                in: query
                description: return strings using base64 encoding
                type: string
                required: false
                default: true
              - name: timeout
                in: query
                description: timeout in ms
                type: integer
                required: false
                default: 60000
              - name: ui64
                in: query
                description: return ui64 as number to avoid 56-bit js rounding
                type: boolean
                required: false
              - name: resource_pool
                in: query
                description: resource pool in which the query will be executed
                type: string
                required: false
              - name: keep_alive
                in: query
                description: time of inactivity to send keep-alive in stream (multipart) queries
                type: integer
                default: 10000
              - name: collect_diagnostics
                in: query
                description: collect query diagnostics
                type: boolean
                default: true
              - name: stats_period
                in: query
                description: time interval for sending periodical query statistics in ms
                type: integer
            requestBody:
                description: Executes SQL query
                required: false
                content:
                    application/json:
                        schema:
                            type: object
                            description: the same properties as in query parameters
            responses:
                200:
                    description: OK
                    content:
                        application/json:
                            schema:
                                type: object
                                description: format depends on schema parameter
                        multipart/x-mixed-replace:
                            schema:
                                type: object
                                description: format depends on schema parameter
                        multipart/form-data:
                            schema:
                                type: object
                                description: format depends on schema parameter
                400:
                    description: Bad Request
                403:
                    description: Forbidden
                504:
                    description: Gateway Timeout
            )___");
        return node;
    }
};

}
