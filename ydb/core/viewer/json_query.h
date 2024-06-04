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
    TString UserToken;
    bool IsBase64Encode;

    enum ESchemaType {
        Classic,
        Modern,
        Multi,
        Ydb,
    };
    ESchemaType Schema = ESchemaType::Classic;

    std::optional<TNodeId> SubscribedNodeId;
    std::vector<TNodeId> TenantDynamicNodes;
    bool Direct = false;
    bool MadeKqpProxyRequest = false;

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
        Direct = FromStringWithDefault<bool>(params.Get("direct"), Direct);
        IsBase64Encode = FromStringWithDefault<bool>(params.Get("base64"), true);
    }

    void ParsePostContent(const TStringBuf& content) {
        static NJson::TJsonReaderConfig JsonConfig;
        NJson::TJsonValue requestData;
        bool success = NJson::ReadJsonTree(content, &JsonConfig, &requestData);
        if (success) {
            Query = Query.empty() ? requestData["query"].GetStringSafe({}) : Query;
            Database = Database.empty() ? requestData["database"].GetStringSafe({}) : Database;
            Stats = Stats.empty() ? requestData["stats"].GetStringSafe({}) : Stats;
            Action = Action.empty() ? requestData["action"].GetStringSafe({}) : Action;
            Syntax = Syntax.empty() ? requestData["syntax"].GetStringSafe({}) : Syntax;
        }
    }

    bool IsPostContent() const {
        return NViewer::IsPostContent(Event);
    }

    TJsonQuery(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {
        const auto& params(Event->Get()->Request.GetParams());
        InitConfig(params);
        ParseCgiParameters(params);
        if (IsPostContent()) {
            TStringBuf content = Event->Get()->Request.GetPostContent();
            ParsePostContent(content);
        }
        UserToken = Event->Get()->UserToken;
    }

    TJsonQuery(TEvViewer::TEvViewerRequest::TPtr& ev)
        : ViewerRequest(ev)
    {
        auto& request = ViewerRequest->Get()->Record.GetQueryRequest();

        TCgiParameters params(request.GetUri());
        InitConfig(params);
        ParseCgiParameters(params);

        TStringBuf content = request.GetContent();
        if (content) {
            ParsePostContent(content);
        }

        Timeout = ViewerRequest->Get()->Record.GetTimeout();
        UserToken = request.GetUserToken();
        Direct = true;
    }

    void PassAway() override {
        if (SubscribedNodeId.has_value()) {
            Send(TActivationContext::InterconnectProxy(SubscribedNodeId.value()), new TEvents::TEvUnsubscribe());
        }
        TBase::PassAway();
        BLOG_TRACE("PassAway()");
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvStateStorage::TEvBoardInfo, HandleReply);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeConnected, Connected);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvViewer::TEvViewerResponse, HandleReply);
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleReply);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleReply);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, HandleReply);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamProfile, HandleReply);

            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void SendKpqProxyRequest() {
        if (MadeKqpProxyRequest) {
            return;
        }
        MadeKqpProxyRequest = true;
        auto event = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
        request.SetQuery(Query);
        if (Action.empty() || Action == "execute-script" || Action == "execute") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
            request.SetKeepSession(false);
        } else if (Action == "execute-query") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
            request.mutable_txcontrol()->mutable_begin_tx()->mutable_serializable_read_write();
            request.mutable_txcontrol()->set_commit_tx(true);
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
        if (Database) {
            request.SetDatabase(Database);
        }
        if (UserToken) {
            event->Record.SetUserToken(UserToken);
        }
        if (Syntax == "yql_v1") {
            request.SetSyntax(Ydb::Query::Syntax::SYNTAX_YQL_V1);
        } else if (Syntax == "pg") {
            request.SetSyntax(Ydb::Query::Syntax::SYNTAX_PG);
        }
        ActorIdToProto(SelfId(), event->Record.MutableRequestActorId());
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());
    }

    void Bootstrap() {
        if (Query.empty()) {
            if (Event) {
                ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request"));
            } else {
                auto* response = new TEvViewer::TEvViewerResponse();
                response->Record.MutableQueryResponse()->SetYdbStatus(Ydb::StatusIds::BAD_REQUEST);
                ReplyAndPassAway(response);
            }
            return;
        }

        if (Database && !Direct) {
            RequestStateStorageEndpointsLookup(Database); // to find some dynamic node and redirect query there
        }

        if (Requests == 0) {
            SendKpqProxyRequest();
        }
        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
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

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr &) {}

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        if (ev->Get()->SourceType == NViewer::TEvViewer::EvViewerRequest) {
            SendKpqProxyRequest();
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &) {
        SendKpqProxyRequest();
    }

    void SendDynamicNodeQueryRequest() {
        ui64 hash = std::hash<TString>()(Event->Get()->Request.GetRemoteAddr());

        auto itPos = std::next(TenantDynamicNodes.begin(), hash % TenantDynamicNodes.size());
        std::nth_element(TenantDynamicNodes.begin(), itPos, TenantDynamicNodes.end());

        TNodeId nodeId = *itPos;
        SubscribedNodeId = nodeId;
        TActorId viewerServiceId = MakeViewerID(nodeId);

        THolder<TEvViewer::TEvViewerRequest> request = MakeHolder<TEvViewer::TEvViewerRequest>();
        request->Record.SetTimeout(Timeout);
        auto queryRequest = request->Record.MutableQueryRequest();
        queryRequest->SetUri(TString(Event->Get()->Request.GetUri()));
        if (IsPostContent()) {
            TStringBuf content = Event->Get()->Request.GetPostContent();
            queryRequest->SetContent(TString(content));
        }
        if (UserToken) {
            queryRequest->SetUserToken(UserToken);
        }

        ViewerWhiteboardCookie cookie(NKikimrViewer::TEvViewerRequest::kQueryRequest, nodeId);
        SendRequest(viewerServiceId, request.Release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, cookie.ToUi64());
    }

    void HandleReply(TEvStateStorage::TEvBoardInfo::TPtr& ev) {
        BLOG_TRACE("Received TEvBoardInfo");
        if (ev->Get()->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok) {
            for (const auto& [actorId, infoEntry] : ev->Get()->InfoEntries) {
                TenantDynamicNodes.emplace_back(actorId.NodeId());
            }
        }
        if (TenantDynamicNodes.empty()) {
            SendKpqProxyRequest();
        } else {
            SendDynamicNodeQueryRequest();
        }
    }

    void Handle(NKikimrKqp::TEvQueryResponse& record) {
        if (Event) {
            NJson::TJsonValue jsonResponse;
            if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
                MakeOkReply(jsonResponse, record);
            } else {
                MakeErrorReply(jsonResponse, record);
            }

            if (Schema == ESchemaType::Classic && Stats.empty() && (Action.empty() || Action == "execute")) {
                jsonResponse = std::move(jsonResponse["result"]);
            }

            TStringStream stream;
            NJson::TJsonWriterConfig config;
            config.ValidateUtf8 = false;
            config.WriteNanAsString = true;
            NJson::WriteJson(&stream, &jsonResponse, config);

            ReplyAndPassAway(stream.Str());
        } else {
            TEvViewer::TEvViewerResponse* response = new TEvViewer::TEvViewerResponse();
            response->Record.MutableQueryResponse()->CopyFrom(record);
            response->Record.MutableQueryResponse()->MutableResponse()->MutableYdbResults()->Add(ResultSets.begin(), ResultSets.end());
            ReplyAndPassAway(response);
        }
    }

    void HandleReply(NKqp::TEvKqp::TEvQueryResponse::TPtr& ev) {
        Handle(ev->Get()->Record.GetRef());
    }

    void HandleReply(TEvViewer::TEvViewerResponse::TPtr& ev) {
        auto& record = ev.Get()->Get()->Record;
        if (record.HasQueryResponse()) {
            Handle(*(ev.Get()->Get()->Record.MutableQueryResponse()));
        } else {
            SendKpqProxyRequest(); // fallback
        }
    }

    void HandleReply(NKqp::TEvKqp::TEvAbortExecution::TPtr& ev) {
        Y_UNUSED(ev);
    }

    void HandleReply(NKqp::TEvKqpExecuter::TEvStreamProfile::TPtr& ev) {
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
        if (Event) {
            Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            PassAway();
        } else {
            auto* response = new TEvViewer::TEvViewerResponse();
            response->Record.MutableQueryResponse()->SetYdbStatus(Ydb::StatusIds::TIMEOUT);
            ReplyAndPassAway(response);
        }
    }

    void ReplyAndPassAway(TEvViewer::TEvViewerResponse* response) {
        Send(ViewerRequest->Sender, response);
        PassAway();
    }

    void ReplyAndPassAway(TString data) {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(data)), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

private:
    void MakeErrorReply(NJson::TJsonValue& jsonResponse, NKikimrKqp::TEvQueryResponse& record) {
        NJson::TJsonValue& jsonIssues = jsonResponse["issues"];

        // find first deepest error
        google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>* protoIssues = record.MutableResponse()->MutableQueryIssues();
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
        for (const auto& queryIssue : record.GetResponse().GetQueryIssues()) {
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
                issue->set_message(Sprintf("Convert error: %s", ex.what()));
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                MakeErrorReply(jsonResponse, record);
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
struct TJsonRequestParameters<TJsonQuery> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
              - name: ui64
                in: query
                description: return ui64 as number
                type: boolean
                required: false
              - name: query
                in: query
                description: query text
                type: string
                required: true
              - name: direct
                in: query
                description: force processing query on current node
                type: boolean
                required: false
              - name: syntax
                in: query
                description: query syntax (yql_v1, pg)
                type: string
                required: false
              - name: database
                in: query
                description: database name
                type: string
                required: false
              - name: schema
                in: query
                description: result format schema (classic, modern, ydb, multi)
                type: string
                required: false
              - name: stats
                in: query
                description: return stats (profile, full)
                type: string
                required: false
              - name: action
                in: query
                description: execute method (execute-scan, execute-script, execute-query, execute-data,explain-ast, explain-scan, explain-script, explain-query, explain-data)
                type: string
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
                )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonQuery> {
    static TString GetSummary() {
        return "Execute query";
    }
};

template <>
struct TJsonRequestDescription<TJsonQuery> {
    static TString GetDescription() {
        return "Executes database query";
    }
};


}
}
