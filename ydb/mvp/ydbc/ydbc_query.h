#pragma once
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
#include <library/cpp/yson/json/json_writer.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/api/grpc/ydb_scheme_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_operation_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_table_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_cms_v1.grpc.pb.h>
#include <ydb/public/api/grpc/ydb_scripting_v1.grpc.pb.h>
#include <ydb/public/api/protos/ydb_discovery.pb.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_proto/accessor.h>
#include <ydb/public/sdk/cpp/client/draft/ydb_scripting.h>
#include <ydb/public/sdk/cpp/client/ydb_result/result.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/lib/json_value/ydb_json_value.h>
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/security/util.h>
#include <util/charset/utf8.h>
#include "mvp.h"
#include "ydbc_query_helper.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include "yql.h"
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcQueryCollector : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcQueryCollector> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcQueryCollector>;
    const TYdbcLocation& Location;
    std::unique_ptr<NYdb::NScripting::TScriptingClient> ScriptingClient;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    NKikimr::NGRpcProxy::TGRpcClientConfig GrpcConfig;
    TString Action;
    TString Database;
    TString Query;
    NJson::TJsonValue QueryParameters;
    bool InjectSyntaxVersion = true;
    bool InjectDeclare = true;
    static constexpr TStringBuf SYNTAX_VERSION = "--!syntax_v1";
    TString Schema; // "yql" | "yql2"
    TInstant Deadline;
    static constexpr TDuration MAX_RETRY_TIME = TDuration::Seconds(10);

    THolder<NYdb::TParamsBuilder> params;
    std::optional<NYdb::NScripting::TYqlResultPartIterator> PartIterator;
    TVector<TVector<NYdb::TResultSet>> ResultGroups;
    ui32 CurrentResultGroupIndex;
    TMaybe<NYdb::NTable::TQueryStats> LastStats;

    THandlerActorYdbcQueryCollector(
            const TYdbcLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            const NActors::TActorId& httpProxyId)
        : Location(location)
        , Request(sender, request)
        , HttpProxyId(httpProxyId)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        TString databaseId = Request.Parameters["databaseId"];
        if (IsValidDatabaseId(databaseId)) {
            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequestGet(
                        TMVP::GetAppropriateEndpoint(Request.Request) + "/ydbc/" + Location.Name + "/database" + "?databaseId=" + databaseId);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        } else {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest("Invalid databaseId", "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        Deadline = ctx.Now() + MAX_RETRY_TIME;
        Become(&THandlerActorYdbcQueryCollector::StateResolveDatabase, GetQueryTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void SendExecuteRequest(const NActors::TActorContext& ctx) {
        TStringStream injectData;
        if (InjectSyntaxVersion) {
            injectData << SYNTAX_VERSION << "\n";
        }
        try {
            auto processResult = ProcessQueryParameters(QueryParameters, InjectDeclare);
            if (processResult->HasError) {
                auto response = Request.Request->CreateResponseBadRequest(processResult->ErrorMessage, "text/plain");
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
                TBase::Die(ctx);
                return;
            }
            params = std::move(processResult->Params);
            if (InjectDeclare) {
                injectData << processResult->InjectData;
            }
        }
        catch (const std::exception& e) {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest(e.what(), "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }
        if (!injectData.empty()) {
            auto itInsertPoint = Query.find(SYNTAX_VERSION);
            if (itInsertPoint != TString::npos) {
                itInsertPoint += SYNTAX_VERSION.size();
                while (itInsertPoint < Query.size() && Query[itInsertPoint] < ' ') {
                    ++itInsertPoint;
                }
            } else {
                itInsertPoint = 0;
            }
            Query.insert(itInsertPoint, injectData.Str());
        }
        {
            NJson::TJsonValue queryInfo;
            queryInfo["token"] = MaskTicket(Request.GetAuthToken());
            queryInfo["query"] = Query;
            queryInfo["host"] = GrpcConfig.Locator;
            queryInfo["database"] = Database;
            queryInfo["parameters"] = QueryParameters;
            BLOG_QUERY_I(NJson::WriteJson(queryInfo, false));
        }

        Become(&THandlerActorYdbcQueryCollector::StateExecuteRequest);
        StartRead(ctx);
    }

    void StartRead(const NActors::TActorContext& ctx) {
        PartIterator.reset();
        ResultGroups.clear();
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;

        NYdb::NScripting::TExecuteYqlRequestSettings settings;
        if (Request.Parameters["syntax"] == "SQLv1") {
            settings.Syntax(Ydb::Query::SYNTAX_YQL_V1);
        } else if (Request.Parameters["syntax"] == "PG") {
            settings.Syntax(Ydb::Query::SYNTAX_PG);
        }
        settings.CollectQueryStats(Request.Parameters["stats"]
            ? NYdb::NTable::ECollectQueryStatsMode::Basic
            : NYdb::NTable::ECollectQueryStatsMode::None);
        NYdb::NScripting::TAsyncYqlResultPartIterator it = ScriptingClient->StreamExecuteYqlScript(Query, params->Build(), settings);
        it.Subscribe([actorId, actorSystem](const NYdb::NScripting::TAsyncYqlResultPartIterator& asyncResult) mutable {
            NYdb::NScripting::TAsyncYqlResultPartIterator res(asyncResult);
            NYdb::NScripting::TYqlResultPartIterator partIterator = res.ExtractValue();
            if (!partIterator.IsSuccess()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvReadFailed(std::move(partIterator)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvReadStarted(std::move(partIterator)));
            }
        });
    }

    void Continue(const NActors::TActorContext& ctx) {
        NActors::TActorId actorId = ctx.SelfID;
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        PartIterator->ReadNext().Subscribe([actorId, actorSystem](NYdb::NScripting::TAsyncYqlResultPart asyncResult) {
            NYdb::NScripting::TAsyncYqlResultPart res(asyncResult);
            NYdb::NScripting::TYqlResultPart resultPart = asyncResult.ExtractValue();
            if (resultPart.EOS()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvQueryBatch(std::move(resultPart)));
            } else if (!resultPart.IsSuccess()) {
                actorSystem->Send(actorId, new TEvPrivate::TEvReadFailed(std::move(resultPart)));
            } else {
                actorSystem->Send(actorId, new TEvPrivate::TEvQueryBatch(std::move(resultPart)));
            }
        });
    }

    void Handle(TEvPrivate::TEvReadFailed::TPtr& ev, const NActors::TActorContext& ctx) {
        NYdb::TStatus& status(ev->Get()->Status);
        if (IsRetryableError(status) && (ctx.Now() < Deadline)) {
            StartRead(ctx);
        } else {
            NHttp::THttpOutgoingResponsePtr response = CreateStatusResponse(Request.Request, status);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
        }
    }

    void Handle(TEvPrivate::TEvReadStarted::TPtr& ev, const NActors::TActorContext& ctx) {
        PartIterator = std::move(ev->Get()->PartIterator);
        Continue(ctx);
    }

    void SendExplainRequest(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;
        NYdbGrpc::TResponseCallback<Ydb::Scripting::ExplainYqlResponse> responseCb =
            [actorSystem, actorId](NYdbGrpc::TGrpcStatus&& status, Ydb::Scripting::ExplainYqlResponse&& response) -> void {
                if (status.Ok()) {
                    actorSystem->Send(actorId, new TEvPrivate::TEvExplainYqlResponse(std::move(*response.mutable_operation())));
                } else {
                    actorSystem->Send(actorId, new TEvPrivate::TEvErrorResponse(status));
                }
        };

        Ydb::Scripting::ExplainYqlRequest explainRequest;
        explainRequest.set_mode(Ydb::Scripting::ExplainYqlRequest::PLAN);
        explainRequest.set_script(Query);
        {
            NJson::TJsonValue queryInfo;
            queryInfo["token"] = MaskTicket(Request.GetAuthToken());
            queryInfo["query"] = Query;
            queryInfo["host"] = GrpcConfig.Locator;
            queryInfo["database"] = Database;
            BLOG_QUERY_I(NJson::WriteJson(queryInfo, false));
        }

        NYdbGrpc::TCallMeta meta;
        meta.Aux.push_back({NYdb::YDB_DATABASE_HEADER, Database});
        Request.ForwardHeadersOnlyForIAM(meta);
        auto connection = Location.CreateGRpcServiceConnection<Ydb::Scripting::V1::ScriptingService>(GrpcConfig);
        connection->DoRequest(explainRequest, std::move(responseCb), &Ydb::Scripting::V1::ScriptingService::Stub::AsyncExplainYql, meta);
        Become(&THandlerActorYdbcQueryCollector::StateExplainRequest);
    }

    TString GetAuthTokenWithScheme() {
        NHttp::THeaders headers(Request.Request->Headers);
        NHttp::TCookies cookies(headers["Cookie"]);
        TStringBuf authorization = headers["Authorization"];
        if (!authorization.empty()) {
            return TString(authorization);
        }
        TStringBuf subjectToken = headers["x-yacloud-subjecttoken"];
        if (!subjectToken.empty()) {
            return TString(subjectToken);
        }
        TStringBuf sessionId = cookies["Session_id"];
        if (!sessionId.empty()) {
            return BlackBoxTokenFromSessionId(sessionId);
        }
        return TString();
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            NJson::TJsonValue responseData;
            bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
            if (success) {
                TString endpoint = responseData["endpoint"].GetStringRobust();
                TStringBuf scheme = "grpc";
                TStringBuf host;
                TStringBuf uri;
                NHttp::CrackURL(endpoint, scheme, host, uri);
                NHttp::TUrlParameters urlParams(uri);
                Database = urlParams["database"];
                TString hostStr = TString(host);
                TString schemeStr = TString(scheme);

                NJson::TJsonValue* jsonDiscovery;
                if (responseData.GetValuePointer("discovery", &jsonDiscovery)) {
                    NJson::TJsonValue* jsonEndpoints;
                    if (jsonDiscovery->GetValuePointer("endpoints", &jsonEndpoints)) {
                        for (const NJson::TJsonValue& jsonEndpoint : jsonEndpoints->GetArraySafe()) {
                            hostStr = jsonEndpoint["address"].GetStringRobust() + ":" + jsonEndpoint["port"].GetStringRobust();
                            if (jsonEndpoint["ssl"].GetBooleanRobust()) {
                                schemeStr = "grpcs";
                            } else {
                                schemeStr = "grpc";
                            }
                        }
                    }
                }

                if (!Database.empty()) {
                    if (Request.Request->ContentType == "text/plain") {
                        Query = Request.Request->Body;
                    } else {
                        Query = Request.Parameters["query"];
                        QueryParameters = Request.Parameters.PostData["parameters"];
                        InjectSyntaxVersion = FromStringWithDefault(Request.Parameters["injectSyntaxVersion"], InjectSyntaxVersion);
                        InjectDeclare = FromStringWithDefault(Request.Parameters["injectDeclare"], InjectDeclare);
                    }
                    Action = Request.Parameters["action"];
                    Schema = Request.Parameters["schema"];
                    GrpcConfig.Locator = hostStr;
                    NYdb::TSslCredentials sslCredentials;
                    if (schemeStr == "grpcs") {
                        GrpcConfig.SslCredentials.pem_root_certs = Location.CaCertificate;
                        GrpcConfig.EnableSsl = true;

                        sslCredentials.CaCert = Location.CaCertificate;
                        sslCredentials.IsEnabled = true;
                    }

                    ScriptingClient = Location.GetScriptingClientPtr(host, scheme, NYdb::TCommonClientSettings()
                        .Database(Database)
                        .AuthToken(GetAuthTokenWithScheme())
                        .SslCredentials(sslCredentials));

                    if (Action.empty() || Action == "execute") {
                        SendExecuteRequest(ctx);
                    } else if (Action == "explain") {
                        SendExplainRequest(ctx);
                    }

                    return;
                } else {
                    message = "Invalid database endpoint";
                    status = "400";
                }
            } else {
                message = "Unable to parse database information";
                status = "500";
            }
        } else {
            message = event->Get()->Response->Message;
            contentType = event->Get()->Response->ContentType;
            body = event->Get()->Response->Body;
        }
        NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponse(status, message, contentType, body);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    NYdb::TResultSet MergeResultSets(TVector<NYdb::TResultSet>& group) {
        if (group.size() == 1) {
            return group[0];
        }

        auto queryResults = NYdb::TProtoAccessor::GetProto(*group.begin());
        for (auto it = group.begin() + 1; it != group.end(); ++it) {
            auto& partialResults = NYdb::TProtoAccessor::GetProto(*it);
            for (auto& row : partialResults.rows()) {
                *queryResults.add_rows() = std::move(row);
            }
        }
        return NYdb::TResultSet(queryResults);
    }

    void Reply(const NActors::TActorContext& ctx, const std::multimap<NYdb::TStringType, NYdb::TStringType> meta) {
        NHttp::THttpOutgoingResponsePtr response;

        NHttp::THeaders headers(Request.Request->Headers);
        TStringBuf accept(headers["Accept"]);
        for (TStringBuf type = accept.NextTok(", "); !type.empty(); type = accept.NextTok(", ")) {
            TStringBuf contentType = type.NextTok(';');
            if (contentType == "application/yson") {
                TStringStream results;
                results << '[';
                for (auto it = ResultGroups.begin(); it != ResultGroups.end(); ++it) {
                    if (it != ResultGroups.begin()) {
                        results << ';';
                    }
                    const NYdb::TResultSet& resultSet = MergeResultSets(*it);
                    NKikimrMiniKQL::TResult kqpResult;
                    NKikimr::ConvertYdbResultToKqpResult(NYdb::TProtoAccessor::GetProto(resultSet), kqpResult);
                    NYql::TExprContext exprContext;
                    TMaybe<TString> result = KqpResultToYson(kqpResult, NYson::EYsonFormat::Binary, exprContext);
                    if (!result.Defined()) {
                        response = Request.Request->CreateResponse("500", "Internal Server Error", "text/plain", "Unable to convert result");
                        break;
                    }
                    results << result.GetRef();
                }
                results << ']';
                if (response == nullptr) {
                    response = Request.Request->CreateResponseOK(results.Str(), "application/yson");
                }
                break;
            } else if (contentType == "application/json") {
                if (Action.empty()) {
                    NJson::TJsonValue jsonResults;
                    jsonResults.SetType(NJson::JSON_ARRAY);
                    for (auto it = ResultGroups.begin(); it != ResultGroups.end(); ++it) {
                        const NYdb::TResultSet& resultSet = MergeResultSets(*it);
                        NJson::TJsonValue& jsonResult = jsonResults.AppendValue(NJson::TJsonValue());

                        NJson::TJsonValue& columns = jsonResult["columns"];
                        const auto& columnsMeta = resultSet.GetColumnsMeta();
                        WriteColumns(columns, columnsMeta);

                        NJson::TJsonValue& data = jsonResult["data"];
                        data.SetType(NJson::JSON_ARRAY);

                        NYdb::TResultSetParser rsParser(resultSet);
                        while (rsParser.TryNextRow()) {
                            NJson::TJsonValue& row = data.AppendValue(NJson::TJsonValue());
                            for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                                const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                                row[columnMeta.Name] = ColumnValueToString(rsParser.ColumnParser(columnNum));
                            }
                        }
                    }
                    TString body(NJson::WriteJson(jsonResults, false));
                    response = Request.Request->CreateResponseOK(body, "application/json; charset=utf-8");
                    break;
                } else if (Action == "execute") {
                    if (Schema == "yql") {
                        TStringStream results;
                        results << '[';
                        for (auto it = ResultGroups.begin(); it != ResultGroups.end(); ++it) {
                            if (it != ResultGroups.begin()) {
                                results << ',';
                            }
                            const NYdb::TResultSet& resultSet = MergeResultSets(*it);
                            NKikimrMiniKQL::TResult kqpResult;
                            NKikimr::ConvertYdbResultToKqpResult(NYdb::TProtoAccessor::GetProto(resultSet), kqpResult);
                            NYql::TExprContext exprContext;
                            TMaybe<TString> resultYson = KqpResultToYson(kqpResult, NYson::EYsonFormat::Text, exprContext);
                            if (!resultYson.Defined()) {
                                response = Request.Request->CreateResponse("500", "Internal Server Error", "text/plain", "Unable to convert result");
                                break;
                            }
                            TString resultJson;
                            TStringOutput str(resultJson);
                            NYT::TJsonWriter writer(&str);
                            writer.OnRaw(resultYson.GetRef(), ::NYson::EYsonType::Node);
                            writer.Flush();

                            NJson::TJsonValue jsonData;
                            bool success = NJson::ReadJsonTree(resultJson, &JsonReaderConfig, &jsonData);
                            if (success) {
                                if (LastStats.Defined()) {
                                    NProtobufJson::Proto2Json(NYdb::TProtoAccessor::GetProto(LastStats.GetRef()), jsonData["Stats"], Proto2JsonConfig);
                                }
                                auto itConsumedUnits = meta.find("x-ydb-consumed-units");
                                if (itConsumedUnits != meta.end()) {
                                    jsonData["costs"]["ydbConsumedUnits"] = itConsumedUnits->second;
                                }
                                resultJson = NJson::WriteJson(jsonData, false);
                            }
                            results << resultJson;
                        }
                        results << ']';
                        if (response == nullptr) {
                            response = Request.Request->CreateResponseOK(results.Str(), "application/json");
                        }
                        break;
                    } else if (Schema == "yql2") {
                        auto itConsumedUnits = meta.find("x-ydb-consumed-units");
                        NJson::TJsonValue jsonData;
                        NJson::TJsonValue& jsonResults = jsonData["results"];
                        jsonResults.SetType(NJson::JSON_ARRAY);
                        for (auto it = ResultGroups.begin(); it != ResultGroups.end(); ++it) {
                            const NYdb::TResultSet& resultSet = MergeResultSets(*it);
                            NJson::TJsonValue& jsonResult = jsonResults.AppendValue(NJson::TJsonValue());
                            NKikimrMiniKQL::TResult kqpResult;
                            NKikimr::ConvertYdbResultToKqpResult(NYdb::TProtoAccessor::GetProto(resultSet), kqpResult);
                            NYql::TExprContext exprContext;
                            TMaybe<TString> resultYson = KqpResultToYson(kqpResult, NYson::EYsonFormat::Text, exprContext);
                            if (!resultYson.Defined()) {
                                response = Request.Request->CreateResponse("500", "Internal Server Error", "text/plain", "Unable to convert result");
                                break;
                            }

                            TString resultJson;
                            TStringOutput str(resultJson);
                            NYT::TJsonWriter writer(&str);
                            writer.OnRaw(resultYson.GetRef(), ::NYson::EYsonType::Node);
                            writer.Flush();
                            NJson::ReadJsonTree(resultJson, &JsonReaderConfig, &jsonResult);
                        }
                        if (LastStats.Defined()) {
                            NProtobufJson::Proto2Json(NYdb::TProtoAccessor::GetProto(LastStats.GetRef()), jsonData["stats"], Proto2JsonConfig);
                        }
                        if (itConsumedUnits != meta.end()) {
                            jsonData["costs"]["ydbConsumedUnits"] = itConsumedUnits->second;
                        }
                        if (response == nullptr) {
                            response = Request.Request->CreateResponseOK(NJson::WriteJson(jsonData, false), "application/json");
                        }
                        break;
                    } else {
                        NJson::TJsonValue jsonRoot;
                        jsonRoot.SetType(NJson::JSON_MAP);
                        NJson::TJsonValue& jsonResults = jsonRoot["ResultSets"];
                        jsonResults.SetType(NJson::JSON_ARRAY);
                        for (auto it = ResultGroups.begin(); it != ResultGroups.end(); ++it) {
                            const NYdb::TResultSet& resultSet = MergeResultSets(*it);
                            NJson::TJsonValue& jsonResult = jsonResults.AppendValue(NJson::TJsonValue());

                            NJson::TJsonValue& columns = jsonResult["columns"];
                            const auto& columnsMeta = resultSet.GetColumnsMeta();
                            WriteColumns(columns, columnsMeta);

                            NJson::TJsonValue& data = jsonResult["data"];
                            data.SetType(NJson::JSON_ARRAY);

                            NYdb::TResultSetParser rsParser(resultSet);
                            while (rsParser.TryNextRow()) {
                                NJson::TJsonValue& row = data.AppendValue(NJson::TJsonValue());
                                for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                                    const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                                    row[columnMeta.Name] = ColumnValueToString(rsParser.ColumnParser(columnNum));
                                }
                            }
                        }
                        if (LastStats.Defined()) {
                            NProtobufJson::Proto2Json(NYdb::TProtoAccessor::GetProto(LastStats.GetRef()), jsonRoot["stats"], Proto2JsonConfig);
                        }
                        auto itConsumedUnits = meta.find("x-ydb-consumed-units");
                        if (itConsumedUnits != meta.end()) {
                            jsonRoot["costs"]["ydbConsumedUnits"] = itConsumedUnits->second;
                        }
                        TString body(NJson::WriteJson(jsonRoot, false));
                        response = Request.Request->CreateResponseOK(body, "application/json; charset=utf-8");
                        break;
                    }
                }
            }
        }
        if (response == nullptr) {
            response = Request.Request->CreateResponseBadRequest("Unsupported accept content-type", "text/plain");
        }

        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void Handle(TEvPrivate::TEvQueryBatch::TPtr event, const NActors::TActorContext& ctx) {
        NYdb::NScripting::TYqlResultPart& streamPart(event->Get()->Result);

        if (streamPart.EOS()) {
            Reply(ctx, streamPart.GetResponseMetadata());
            return;
        }

        if (streamPart.HasPartialResult()) {
            const NYdb::NScripting::TYqlPartialResult& result = streamPart.GetPartialResult();
            if (ResultGroups.empty() || CurrentResultGroupIndex != result.GetResultSetIndex()) {
                ResultGroups.push_back({ result.GetResultSet() });
                CurrentResultGroupIndex = result.GetResultSetIndex();
            } else {
                ResultGroups.back().push_back(result.GetResultSet());
            }
        }
        if (streamPart.HasQueryStats()) {
            LastStats = streamPart.ExtractQueryStats();
        }
        Continue(ctx);
    }

    void Handle(TEvPrivate::TEvExplainYqlResponse::TPtr event, const NActors::TActorContext& ctx) {
        Ydb::Operations::Operation& operation(event->Get()->Operation);
        if (operation.status() == Ydb::StatusIds::SUCCESS) {
            Ydb::Scripting::ExplainYqlResult result;
            operation.result().UnpackTo(&result);
            NJson::TJsonValue jsonRoot;
            jsonRoot.SetType(NJson::JSON_MAP);
            NProtobufJson::Proto2Json(result, jsonRoot, Proto2JsonConfig);
            NJson::TJsonValue* jsonPlan;
            if (jsonRoot.GetValuePointer("plan", &jsonPlan) && jsonPlan->GetType() == NJson::JSON_STRING) {
                NJson::TJsonValue jsonDecodedPlan;
                if (NJson::ReadJsonTree(jsonPlan->GetString(), &jsonDecodedPlan, false)) {
                    *jsonPlan = std::move(jsonDecodedPlan);
                }
            }
            TStringStream body;
            NJson::WriteJson(&body, &jsonRoot, JsonWriterConfig);
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseOK(body.Str(), "application/json; charset=utf-8");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        } else if (IsRetryableError(operation) && (ctx.Now() < Deadline)) {
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
            return;
        }
        NHttp::THttpOutgoingResponsePtr response = CreateStatusResponse(Request.Request, operation);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void HandleExecuteRequest(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        SendExecuteRequest(ctx);
    }

    void HandleExplainRequest(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        SendExplainRequest(ctx);
    }

    void Handle(TEvPrivate::TEvErrorResponse::TPtr event, const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(CreateErrorResponse(Request.Request, event->Get())));
        Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        Die(ctx);
    }

    STFUNC(StateResolveDatabase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateExecuteRequest) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvReadStarted, Handle);
            HFunc(TEvPrivate::TEvReadFailed, Handle);
            HFunc(TEvPrivate::TEvQueryBatch, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, HandleExecuteRequest);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateExplainRequest) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvExplainYqlResponse, Handle);
            HFunc(TEvPrivate::TEvErrorResponse, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, HandleExplainRequest);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcQuery : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcQuery> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcQuery>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcQuery(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcQuery::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET" || request->Method == "POST") {
            ctx.Register(new THandlerActorYdbcQueryCollector(Location, event->Sender, request, HttpProxyId));
            return;
        }
        auto response = event->Get()->Request->CreateResponseBadRequest();
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
