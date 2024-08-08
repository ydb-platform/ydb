#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "log.h"
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NMonitoring;
using namespace NNodeWhiteboard;

bool IsPostContent(const NMon::TEvHttpInfo::TPtr& event);

class TJsonQuery : public TViewerPipeClient {
    using TThis = TJsonQuery;
    using TBase = TViewerPipeClient;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TVector<Ydb::ResultSet> ResultSets;
    TString Query;
    TString Database;
    TString Action;
    TString Stats;
    TString Syntax;
    TString QueryId;
    TString TransactionMode;
    bool Direct = false;
    bool IsBase64Encode = true;

    enum ESchemaType {
        Classic,
        Modern,
        Multi,
        Ydb,
    };
    ESchemaType Schema = ESchemaType::Classic;
    TRequestResponse<NKqp::TEvKqp::TEvCreateSessionResponse> CreateSessionResponse;
    TRequestResponse<NKqp::TEvKqp::TEvQueryResponse> QueryResponse;
    TString SessionId;

public:
    ESchemaType StringToSchemaType(const TString& schemaStr) {
        if (schemaStr == "classic") {
            return ESchemaType::Classic;
        } else if (schemaStr == "modern") {
            return ESchemaType::Modern;
        } else if (schemaStr == "multi") {
            return ESchemaType::Multi;
        } else if (schemaStr == "ydb") {
            return ESchemaType::Ydb;
        } else {
            return ESchemaType::Classic;
        }
    }

    void ParseCgiParameters(const TCgiParameters& params) {
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 60000);
        Query = params.Get("query");
        Database = params.Get("database");
        Stats = params.Get("stats");
        Action = params.Get("action");
        TString schemaStr = params.Get("schema");
        Schema = StringToSchemaType(schemaStr);
        Syntax = params.Get("syntax");
        QueryId = params.Get("query_id");
        TransactionMode = params.Get("transaction_mode");
        Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
        IsBase64Encode = FromStringWithDefault<bool>(params.Get("base64"), true);
    }

    bool ParsePostContent(const TStringBuf& content) {
        static NJson::TJsonReaderConfig JsonConfig;
        NJson::TJsonValue requestData;
        bool success = NJson::ReadJsonTree(content, &JsonConfig, &requestData);
        if (success) {
            Query = Query.empty() ? requestData["query"].GetStringSafe({}) : Query;
            Database = Database.empty() ? requestData["database"].GetStringSafe({}) : Database;
            Stats = Stats.empty() ? requestData["stats"].GetStringSafe({}) : Stats;
            Action = Action.empty() ? requestData["action"].GetStringSafe({}) : Action;
            Syntax = Syntax.empty() ? requestData["syntax"].GetStringSafe({}) : Syntax;
            QueryId = QueryId.empty() ? requestData["query_id"].GetStringSafe({}) : QueryId;
            TransactionMode = TransactionMode.empty() ? requestData["transaction_mode"].GetStringSafe({}) : TransactionMode;
        }
        return success;
    }

    bool IsPostContent() const {
        return NViewer::IsPostContent(Event);
    }

    TJsonQuery(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {
    }

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        InitConfig(params);
        ParseCgiParameters(params);
        if (IsPostContent()) {
            TStringBuf content = Event->Get()->Request.GetPostContent();
            if (!ParsePostContent(content)) {
                return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Bad content received"), "BadRequest");
            }
        }
        if (Query.empty() && Action != "cancel-query") {
            return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Query is empty"), "EmptyQuery");
        }

        Direct |= Event->Get()->Request.GetUri().StartsWith("/node/"); // we're already forwarding
        Direct |= (Database == AppData()->TenantName); // we're already on the right node

        if (Database && !Direct) {
            BLOG_TRACE("Requesting StateStorageEndpointsLookup for " << Database);
            RequestStateStorageEndpointsLookup(Database); // to find some dynamic node and redirect query there
        } else {
            SendKpqProxyRequest();
        }
        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void HandleReply(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BLOG_TRACE("Received TEvBoardInfo");
        TBase::ReplyAndPassAway(MakeForward(GetNodesFromBoardReply(ev)));
    }

    void PassAway() override {
        if (QueryId) {
            Viewer->EndRunningQuery(QueryId, SelfId());
        }
        if (SessionId) {
            auto event = std::make_unique<NKqp::TEvKqp::TEvCloseSessionRequest>();
            event->Record.MutableRequest()->SetSessionId(SessionId);
            BLOG_TRACE("Closing session " << SessionId);
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
        }
        TBase::PassAway();
        BLOG_TRACE("PassAway()");
    }

    void ReplyAndPassAway() override {
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, HandleReply);
            hFunc(NKqp::TEvKqp::TEvCreateSessionResponse, HandleReply);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleReply);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleReply);
            hFunc(NKqp::TEvKqp::TEvPingSessionResponse, HandleReply);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, HandleReply);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamProfile, HandleReply);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
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
        BLOG_TRACE("Creating session");
        CreateSessionResponse = MakeRequest<NKqp::TEvKqp::TEvCreateSessionResponse>(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
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

    void HandleReply(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev) {
        if (ev->Get()->Record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            CreateSessionResponse.Set(std::move(ev));
        } else {
            CreateSessionResponse.Error("FailedToCreateSession");
            return TBase::ReplyAndPassAway(
                GetHTTPINTERNALERROR("text/plain",
                    TStringBuilder() << "Failed to create session, error " << ev->Get()->Record.GetYdbStatus()), "InternalError");
        }
        SessionId = CreateSessionResponse->Record.GetResponse().GetSessionId();
        BLOG_TRACE("Session created " << SessionId);

        {
            auto event = std::make_unique<NKqp::TEvKqp::TEvPingSessionRequest>();
            event->Record.MutableRequest()->SetSessionId(SessionId);
            ActorIdToProto(SelfId(), event->Record.MutableRequest()->MutableExtSessionCtrlActorId());
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
        }

        auto event = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
        request.SetQuery(Query);
        request.SetSessionId(SessionId);
        if (Database) {
            request.SetDatabase(Database);
        }
        if (Event->Get()->UserToken) {
            event->Record.SetUserToken(Event->Get()->UserToken);
        }
        if (Action.empty() || Action == "execute-script" || Action == "execute") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
            request.SetKeepSession(false);
        } else if (Action == "execute-query") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
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
        if (Stats == "profile") {
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
        ActorIdToProto(SelfId(), event->Record.MutableRequestActorId());
        QueryResponse = MakeRequest<NKqp::TEvKqp::TEvQueryResponse>(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());
        BLOG_TRACE("Query sent");
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
                return valueParser.GetInt32();
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
            default:
                Y_ENSURE(false, TStringBuilder() << "Unsupported type: " << primitive);        }
    }

    NJson::TJsonValue ColumnValueToJsonValue(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetKind()) {
            case NYdb::TTypeParser::ETypeKind::Primitive:
                return ColumnPrimitiveValueToJsonValue(valueParser);

            case NYdb::TTypeParser::ETypeKind::Optional:
                valueParser.OpenOptional();
                if (valueParser.IsNull()) {
                    return NJson::JSON_NULL;
                }
                switch(valueParser.GetKind()) {
                    case NYdb::TTypeParser::ETypeKind::Primitive:
                        return ColumnPrimitiveValueToJsonValue(valueParser);
                    case NYdb::TTypeParser::ETypeKind::Decimal:
                        return valueParser.GetDecimal().ToString();
                    default:
                        return NJson::JSON_UNDEFINED;
                }

            case NYdb::TTypeParser::ETypeKind::Tagged:
                valueParser.OpenTagged();
                return ColumnValueToJsonValue(valueParser);

            case NYdb::TTypeParser::ETypeKind::Pg:
                return valueParser.GetPg().Content_;

            default:
                return NJson::JSON_UNDEFINED;
        }
    }

    void HandleReply(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        BLOG_TRACE("Query response received");
        NJson::TJsonValue jsonResponse;
        if (ev->Get()->Record.GetRef().GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            QueryResponse.Set(std::move(ev));
            MakeOkReply(jsonResponse, QueryResponse->Record.GetRef());
            if (Schema == ESchemaType::Classic && Stats.empty() && (Action.empty() || Action == "execute")) {
                jsonResponse = std::move(jsonResponse["result"]);
            }
        } else {
            QueryResponse.Error("QueryError");
            NYql::TIssues issues;
            NYql::IssuesFromMessage(ev->Get()->Record.GetRef().GetResponse().GetQueryIssues(), issues);
            MakeErrorReply(jsonResponse, NYdb::TStatus(NYdb::EStatus(ev->Get()->Record.GetRef().GetYdbStatus()), std::move(issues)));
        }

        TStringStream stream;
        constexpr ui32 doubleNDigits = std::numeric_limits<double>::max_digits10;
        constexpr ui32 floatNDigits = std::numeric_limits<float>::max_digits10;
        constexpr EFloatToStringMode floatMode = EFloatToStringMode::PREC_NDIGITS;
        NJson::WriteJson(&stream, &jsonResponse, {
            .DoubleNDigits = doubleNDigits,
            .FloatNDigits = floatNDigits,
            .FloatToStringMode = floatMode,
            .ValidateUtf8 = false,
            .WriteNanAsString = true,
        });

        TBase::ReplyAndPassAway(GetHTTPOKJSON(stream.Str()));
    }

    void HandleReply(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev) {
        QueryResponse.Error("Aborted");
        auto& record(ev->Get()->Record);
        NJson::TJsonValue jsonResponse;
        if (record.IssuesSize() > 0) {
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            MakeErrorReply(jsonResponse, NYdb::TStatus(NYdb::EStatus(record.GetStatusCode()), std::move(issues)));
        }

        TStringStream stream;
        NJson::WriteJson(&stream, &jsonResponse, {
            .ValidateUtf8 = false,
            .WriteNanAsString = true,
        });

        TBase::ReplyAndPassAway(GetHTTPOKJSON(stream.Str()));
    }

    void HandleReply(NKqp::TEvKqpExecuter::TEvStreamProfile::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void HandleReply(NKqp::TEvKqp::TEvPingSessionResponse::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void HandleReply(NKqp::TEvKqpExecuter::TEvStreamData::TPtr& ev) {
        const NKikimrKqp::TEvExecuterStreamData& data(ev->Get()->Record);

        ResultSets.emplace_back();
        ResultSets.back() = std::move(data.GetResultSet());

        THolder<NKqp::TEvKqpExecuter::TEvStreamDataAck> ack = MakeHolder<NKqp::TEvKqpExecuter::TEvStreamDataAck>();
        ack->Record.SetSeqNo(ev->Get()->Record.GetSeqNo());
        Send(ev->Sender, ack.Release());
    }

    void HandleTimeout() {
        TStringBuilder error;
        error << "Timeout executing query";
        if (SessionId) {
            auto event = std::make_unique<NKqp::TEvKqp::TEvCancelQueryRequest>();
            event->Record.MutableRequest()->SetSessionId(SessionId);
            BLOG_TRACE("Cancelling query in session " << SessionId);
            Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
            error << ", query was cancelled";
        }
        NJson::TJsonValue json;
        json["error"]["severity"] = NYql::TSeverityIds::S_ERROR;
        json["error"]["message"] = error;
        NJson::TJsonValue& issue = json["issues"].AppendValue({});
        issue["severity"] = NYql::TSeverityIds::S_ERROR;
        issue["message"] = error;
        TBase::ReplyAndPassAway(GetHTTPOKJSON(NJson::WriteJson(json, false)));
    }

private:
    void MakeErrorReply(NJson::TJsonValue& jsonResponse, const NYdb::TStatus& status) {
        TString message;

        NViewer::MakeErrorReply(jsonResponse, message, status);

        if (Span) {
            Span.EndError(message);
        }
    }

    void MakeOkReply(NJson::TJsonValue& jsonResponse, NKikimrKqp::TEvQueryResponse& record) {
        const auto& response = record.GetResponse();

        if (response.ResultsSize() > 0 || response.YdbResultsSize() > 0) {
            try {
                for (const auto& result : response.GetResults()) {
                    Ydb::ResultSet resultSet;
                    NKqp::ConvertKqpQueryResultToDbResult(result, &resultSet);
                    ResultSets.emplace_back(std::move(resultSet));
                }

                for (const auto& result : response.GetYdbResults()) {
                    ResultSets.emplace_back(result);
                }
            }
            catch (const std::exception& ex) {
                NYql::TIssues issues;
                issues.AddIssue(TStringBuilder() << "Convert error: " << ex.what());
                MakeErrorReply(jsonResponse, NYdb::TStatus(NYdb::EStatus::BAD_REQUEST, std::move(issues)));
                return;
            }
        }

        if (ResultSets.size() > 0) {
            if (Schema == ESchemaType::Classic) {
                NJson::TJsonValue& jsonResults = jsonResponse["result"];
                jsonResults.SetType(NJson::JSON_ARRAY);
                for (auto it = ResultSets.begin(); it != ResultSets.end(); ++it) {
                    NYdb::TResultSet resultSet(*it);
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

            if (Schema == ESchemaType::Modern) {
                {
                    NJson::TJsonValue& jsonColumns = jsonResponse["columns"];
                    NYdb::TResultSet resultSet(ResultSets.front());
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
                for (auto it = ResultSets.begin(); it != ResultSets.end(); ++it) {
                    NYdb::TResultSet resultSet(*it);
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

            if (Schema == ESchemaType::Multi) {
                NJson::TJsonValue& jsonResults = jsonResponse["result"];
                jsonResults.SetType(NJson::JSON_ARRAY);
                for (auto it = ResultSets.begin(); it != ResultSets.end(); ++it) {
                    NYdb::TResultSet resultSet(*it);
                    const auto& columnsMeta = resultSet.GetColumnsMeta();
                    NJson::TJsonValue& jsonResult = jsonResults.AppendValue({});

                    NJson::TJsonValue& jsonColumns = jsonResult["columns"];
                    jsonColumns.SetType(NJson::JSON_ARRAY);
                    for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                        NJson::TJsonValue& jsonColumn = jsonColumns.AppendValue({});
                        const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                        jsonColumn["name"] = columnMeta.Name;
                        jsonColumn["type"] = columnMeta.Type.ToString();
                    }

                    NJson::TJsonValue& jsonRows = jsonResult["rows"];
                    NYdb::TResultSetParser rsParser(resultSet);
                    while (rsParser.TryNextRow()) {
                        NJson::TJsonValue& jsonRow = jsonRows.AppendValue({});
                        jsonRow.SetType(NJson::JSON_ARRAY);
                        for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                            NJson::TJsonValue& jsonColumn = jsonRow.AppendValue({});
                            jsonColumn = ColumnValueToJsonValue(rsParser.ColumnParser(columnNum));
                        }
                    }
                }
            }

            if (Schema == ESchemaType::Ydb) {
                NJson::TJsonValue& jsonResults = jsonResponse["result"];
                jsonResults.SetType(NJson::JSON_ARRAY);
                for (auto it = ResultSets.begin(); it != ResultSets.end(); ++it) {
                    NYdb::TResultSet resultSet(*it);
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
        if (response.HasQueryAst()) {
            jsonResponse["ast"] = response.GetQueryAst();
        }
        if (response.HasQueryPlan()) {
            NJson::ReadJsonTree(response.GetQueryPlan(), &(jsonResponse["plan"]));
        }
        if (response.HasQueryStats()) {
            NProtobufJson::Proto2Json(response.GetQueryStats(), jsonResponse["stats"]);
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
                      * `ydb`
                type: string
                enum: [classic, modern, ydb, multi]
                required: false
              - name: stats
                in: query
                description: >
                    return stats:
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
              - name: direct
                in: query
                description: force processing query on current node
                type: boolean
                required: false
              - name: base64
                in: query
                description: return strings using base64 encoding
                type: string
                required: false
              - name: timeout
                in: query
                description: timeout in ms
                type: integer
                required: false
              - name: ui64
                in: query
                description: return ui64 as number to avoid 56-bit js rounding
                type: boolean
                required: false
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

