#pragma once
#include "viewer.h"
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include "json_pipe_req.h"
#include "viewer_request.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NMonitoring;
using ::google::protobuf::FieldDescriptor;
using namespace NNodeWhiteboard;

class TJsonQuery : public TViewerPipeClient<TJsonQuery> {
    using TThis = TJsonQuery;
    using TBase = TViewerPipeClient<TJsonQuery>;
    IViewer* Viewer;
    TJsonSettings JsonSettings;
    NMon::TEvHttpInfo::TPtr Event;
    TEvViewer::TEvViewerRequest::TPtr ViewerRequest;
    ui32 Timeout = 0;
    TVector<Ydb::ResultSet> ResultSets;
    TString Query;
    TString Database;
    TString Action;
    TString Stats;
    TString Syntax;
    TString QueryId;
    bool Direct = false;
    bool IsBase64Encode = true;

    enum ESchemaType {
        Classic,
        Modern,
        Multi,
        Ydb,
    };
    ESchemaType Schema = ESchemaType::Classic;
    TString SessionId;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

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
        }
        return success;
    }

    bool IsPostContent() const {
        return NViewer::IsPostContent(Event);
    }

    TJsonQuery(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {
    }

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        InitConfig(params);
        ParseCgiParameters(params);
        if (IsPostContent()) {
            TStringBuf content = Event->Get()->Request.GetPostContent();
            if (!ParsePostContent(content)) {
                return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Bad content recieved"));
            }
        }
        if (Query.empty() && Action != "cancel-query") {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Query is empty"));
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
        ReplyAndPassAway(Viewer->MakeForward(Event->Get(), GetNodesFromBoardReply(ev)));
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
                    return ReplyAndPassAway(Viewer->GetHTTPOK(Event->Get(), "text/plain", "Query was cancelled"));
                }
            } else {
                if (Action == "cancel-query") {
                    return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Query not found"));
                }
            }
            Viewer->AddRunningQuery(QueryId, SelfId());
        }

        auto event = std::make_unique<NKqp::TEvKqp::TEvCreateSessionRequest>();
        if (Database) {
            event->Record.MutableRequest()->SetDatabase(Database);
        }
        BLOG_TRACE("Creating session");
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.release());
    }

    void HandleReply(NKqp::TEvKqp::TEvCreateSessionResponse::TPtr& ev) {
        if (ev->Get()->Record.GetYdbStatus() != Ydb::StatusIds::SUCCESS) {
            return ReplyAndPassAway(
                Viewer->GetHTTPINTERNALERROR(Event->Get(), "text/plain",
                    TStringBuilder() << "Failed to create session, error " << ev->Get()->Record.GetYdbStatus()));
        }
        SessionId = ev->Get()->Record.GetResponse().GetSessionId();
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
            request.mutable_txcontrol()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_txcontrol()->set_commit_tx(true);
            request.SetKeepSession(false);
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
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());
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
        auto& record(ev->Get()->Record.GetRef());
        NJson::TJsonValue jsonResponse;
        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            MakeOkReply(jsonResponse, record);
        } else {
            MakeErrorReply(jsonResponse, record.MutableResponse()->MutableQueryIssues());
        }

        if (Schema == ESchemaType::Classic && Stats.empty() && (Action.empty() || Action == "execute")) {
            jsonResponse = std::move(jsonResponse["result"]);
        }

        TStringStream stream;
        NJson::WriteJson(&stream, &jsonResponse, {
            .ValidateUtf8 = false,
            .WriteNanAsString = true,
        });

        ReplyAndPassAway(Viewer->GetHTTPOKJSON(Event->Get(), stream.Str()));
    }

    void HandleReply(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev) {
        auto& record(ev->Get()->Record);
        NJson::TJsonValue jsonResponse;
        if (record.IssuesSize() > 0) {
            MakeErrorReply(jsonResponse, record.MutableIssues());
        }

        TStringStream stream;
        NJson::WriteJson(&stream, &jsonResponse, {
            .ValidateUtf8 = false,
            .WriteNanAsString = true,
        });

        ReplyAndPassAway(Viewer->GetHTTPOKJSON(Event->Get(), stream.Str()));
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
        ReplyAndPassAway(Viewer->GetHTTPOKJSON(Event->Get(), NJson::WriteJson(json, false)));
    }

    void ReplyAndPassAway(TString data) {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

private:
    void MakeErrorReply(NJson::TJsonValue& jsonResponse, google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* protoIssues) {
        NJson::TJsonValue& jsonIssues = jsonResponse["issues"];

        // find first deepest error
        std::stable_sort(protoIssues->begin(), protoIssues->end(), [](const Ydb::Issue::IssueMessage& a, const Ydb::Issue::IssueMessage& b) -> bool {
            return a.severity() < b.severity();
        });
        while (protoIssues->size() > 0 && (*protoIssues)[0].issuesSize() > 0) {
            protoIssues = (*protoIssues)[0].mutable_issues();
        }
        if (protoIssues->size() > 0) {
            const Ydb::Issue::IssueMessage& issue = (*protoIssues)[0];
            NProtobufJson::Proto2Json(issue, jsonResponse["error"]);
        }
        for (const auto& queryIssue : *protoIssues) {
            NJson::TJsonValue& issue = jsonIssues.AppendValue({});
            NProtobufJson::Proto2Json(queryIssue, issue);
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
                Ydb::Issue::IssueMessage* issue = record.MutableResponse()->AddQueryIssues();
                issue->set_message(TStringBuilder() << "Convert error: " << ex.what());
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                MakeErrorReply(jsonResponse, record.MutableResponse()->MutableQueryIssues());
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
};

template <>
YAML::Node TJsonRequestSwagger<TJsonQuery>::GetSwagger() {
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
          - name: database
            in: query
            description: database name
            type: string
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


}
}
