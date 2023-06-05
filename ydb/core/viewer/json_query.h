#pragma once
#include "viewer.h"
#include <unordered_map>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/grpc_services/rpc_kqp_base.h>
#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/executer_actor/kqp_executer.h>
#include <ydb/core/viewer/json/json.h>
//#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using ::google::protobuf::FieldDescriptor;

class TJsonQuery : public TActorBootstrapped<TJsonQuery> {
    using TThis = TJsonQuery;
    using TBase = TActorBootstrapped<TJsonQuery>;
    IViewer* Viewer;
    TJsonSettings JsonSettings;
    TActorId Initiator;
    NMon::TEvHttpInfo::TPtr Event;
    ui32 Timeout = 0;
    TVector<Ydb::ResultSet> ResultSets;
    TString Action;
    TString Stats;
    TString Schema = "classic";

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonQuery(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Initiator(ev->Sender)
        , Event(ev)
    {}

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NKqp::TEvKqp::TEvQueryResponse, HandleReply);
            hFunc(NKqp::TEvKqp::TEvProcessResponse, HandleReply);
            hFunc(NKqp::TEvKqp::TEvAbortExecution, HandleReply);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamData, HandleReply);
            hFunc(NKqp::TEvKqpExecuter::TEvStreamProfile, HandleReply);

            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);

            default: {
                Cerr << "Unexpected event received in TJsonQuery::StateWork: " << ev->GetTypeRewrite() << Endl;
            }
        }
    }

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        auto event = MakeHolder<NKqp::TEvKqp::TEvQueryRequest>();
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 60000);
        TString query = params.Get("query");
        TString database = params.Get("database");
        Stats = params.Get("stats");
        Action = params.Get("action");
        Schema = params.Get("schema");
        if (Schema.empty()) {
            Schema = "classic";
        }
        if (query.empty() && Event->Get()->Request.GetMethod() == HTTP_METHOD_POST) {
            TStringBuf content = Event->Get()->Request.GetPostContent();
            const THttpHeaders& headers = Event->Get()->Request.GetHeaders();
            auto itContentType = FindIf(headers, [](const auto& header) { return header.Name() == "Content-Type"; });
            if (itContentType != headers.end()) {
                TStringBuf contentTypeHeader = itContentType->Value();
                TStringBuf contentType = contentTypeHeader.NextTok(';');
                if (contentType == "application/json") {
                    static NJson::TJsonReaderConfig JsonConfig;
                    NJson::TJsonValue requestData;
                    bool success = NJson::ReadJsonTree(content, &JsonConfig, &requestData);
                    if (success) {
                        query = requestData["query"].GetStringSafe({});
                        database = requestData["database"].GetStringSafe({});
                        Stats = requestData["stats"].GetStringSafe({});
                        Action = requestData["action"].GetStringSafe({});
                    }
                }
            }
        }
        if (query.empty()) {
            ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), {}, "Bad Request"));
            return;
        }
        NKikimrKqp::TQueryRequest& request = *event->Record.MutableRequest();
        request.SetQuery(query);
        if (Action.empty() || Action == "execute-script" || Action == "execute") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_SCRIPT);
            request.SetKeepSession(false);
        } else if (Action == "execute-query") {
            request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
            request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY);
            request.mutable_txcontrol()->mutable_begin_tx()->mutable_serializable_read_write();
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
        }
        if (database) {
            request.SetDatabase(database);
        }
        if (!Event->Get()->UserToken.empty()) {
            event->Record.SetUserToken(Event->Get()->UserToken);
        }
        ActorIdToProto(SelfId(), event->Record.MutableRequestActorId());
        Send(NKqp::MakeKqpProxyID(SelfId().NodeId()), event.Release());

        Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

private:
    static NJson::TJsonValue ColumnPrimitiveValueToJsonValue(NYdb::TValueParser& valueParser) {
        switch (valueParser.GetPrimitiveType()) {
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
                return Base64Encode(valueParser.GetString());
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
    }

    static NJson::TJsonValue ColumnValueToJsonValue(NYdb::TValueParser& valueParser) {
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
        TStringBuilder out;
        NJson::TJsonValue jsonResponse;
        NKikimrKqp::TEvQueryResponse& record = ev->Get()->Record.GetRef();
        if (record.GetYdbStatus() == Ydb::StatusIds::SUCCESS) {
            MakeOkReply(out, jsonResponse, record);
        } else {
            MakeErrorReply(out, jsonResponse, record);
        }

        if (Schema == "classic" && Stats.empty() && (Action.empty() || Action == "execute")) {
            jsonResponse = std::move(jsonResponse["result"]);
        }

        out << NJson::WriteJson(jsonResponse, false);

        ReplyAndPassAway(out);
    }

    void HandleReply(NKqp::TEvKqp::TEvProcessResponse::TPtr& ev) {
        Y_UNUSED(ev);
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
        ReplyAndPassAway(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()));
    }

    void ReplyAndPassAway(TString data) {
        Send(Initiator, new NMon::TEvHttpInfoRes(std::move(data), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

private:
    void MakeErrorReply(TStringBuilder& out, NJson::TJsonValue& jsonResponse, NKikimrKqp::TEvQueryResponse& record) {
        out << Viewer->GetHTTPBADREQUEST(Event->Get(), "application/json");
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

    void MakeOkReply(TStringBuilder& out, NJson::TJsonValue& jsonResponse, NKikimrKqp::TEvQueryResponse& record) {
        const auto& response = record.GetResponse();

        if (response.ResultsSize() > 0) {
            try {
                for (const auto& result : response.GetResults()) {
                    Ydb::ResultSet resultSet;
                    NKqp::ConvertKqpQueryResultToDbResult(result, &resultSet);
                    ResultSets.emplace_back(std::move(resultSet));
                }
            }
            catch (const std::exception& ex) {
                Ydb::Issue::IssueMessage* issue = record.MutableResponse()->AddQueryIssues();
                issue->set_message(Sprintf("Convert error: %s", ex.what()));
                issue->set_severity(NYql::TSeverityIds::S_ERROR);
                MakeErrorReply(out, jsonResponse, record);
                return;
            }
        }

        out << Viewer->GetHTTPOKJSON(Event->Get());
        if (ResultSets.size() > 0) {
            if (Schema == "classic") {
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

            if (Schema == "modern") {
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

            if (Schema == "ydb") {
                NJson::TJsonValue& jsonResults = jsonResponse["result"];
                jsonResults.SetType(NJson::JSON_ARRAY);
                for (auto it = ResultSets.begin(); it != ResultSets.end(); ++it) {
                    NYdb::TResultSet resultSet(*it);
                    const auto& columnsMeta = resultSet.GetColumnsMeta();
                    NYdb::TResultSetParser rsParser(resultSet);
                    while (rsParser.TryNextRow()) {
                        NJson::TJsonValue& jsonRow = jsonResults.AppendValue({});
                        TString row = NYdb::FormatResultRowJson(rsParser, columnsMeta, NYdb::EBinaryStringEncoding::Base64);
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
    static TString GetParameters() {
        return R"___([{"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"query","in":"query","description":"query text","required":true,"type":"string"},
                      {"name":"database","in":"query","description":"database name","required":false,"type":"string"},
                      {"name":"schema","in":"query","description":"result format schema (classic, modern, ydb)","required":false,"type":"string"},
                      {"name":"stats","in":"query","description":"return stats (profile)","required":false,"type":"string"},
                      {"name":"action","in":"query","description":"execute method (execute-scan, execute-script, explain, explain-ast, explain-scan, explain-script)","required":false,"type":"string"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonQuery> {
    static TString GetSummary() {
        return "\"Execute query\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonQuery> {
    static TString GetDescription() {
        return "\"Executes database query\"";
    }
};


}
}
