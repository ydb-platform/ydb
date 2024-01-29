#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/public/lib/deprecated/client/grpc_client.h>
#include <ydb/library/grpc/client/grpc_client_low.h>
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
#include <ydb/core/kqp/provider/yql_kikimr_results.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include "yql.h"
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcQueryCollector1 : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcQueryCollector1> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcQueryCollector1>;
    const TYdbcLocation& Location;
    std::unique_ptr<NYdb::NScripting::TScriptingClient> ScriptingClient;
    Ydb::Scripting::ExecuteYqlResult ExecuteYqlResult;
    TRequest Request;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcQueryCollector1(
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
                        TMVP::GetAppropriateEndpoint(Request.Request)
                        + "/ydbc/" + Location.Name + "/database" + "?databaseId=" + databaseId);
            TString token = Request.GetAuthToken();
            if (!token.empty()) {
                NHttp::THeadersBuilder httpHeaders;
                httpHeaders.Set("Authorization", "Bearer " + token);
                httpRequest->Set(httpHeaders);
            }
            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        } else {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest("Invalid databaseId", "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        Become(&THandlerActorYdbcQueryCollector1::StateWork, GetQueryTimeout(), new NActors::TEvents::TEvWakeup());
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
                TString database = urlParams["database"];
                TString token = Request.GetAuthToken();
                TString query;

                if (!database.empty()) {
                    ScriptingClient = Location.GetScriptingClientPtr(host, scheme, NYdb::TCommonClientSettings().Database(database).AuthToken(token));

                    NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
                    NActors::TActorId actorId = ctx.SelfID;

                    if (Request.Request->ContentType == "text/plain") {
                        query = Request.Request->Body;
                    } else {
                        query = Request.Parameters["query"];
                    }
                    NYdb::TParamsBuilder params;
                    ScriptingClient->ExecuteYqlScript(query, params.Build()).Subscribe([actorId, actorSystem](const NYdb::NScripting::TAsyncExecuteYqlResult& result) mutable {
                        NYdb::NScripting::TAsyncExecuteYqlResult res(result);
                        actorSystem->Send(actorId, new TEvPrivate::TEvExecuteYqlScriptResult(res.ExtractValue()));
                    });
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

    void Handle(TEvPrivate::TEvExecuteYqlScriptResult::TPtr event, const NActors::TActorContext& ctx) {
        NYdb::NScripting::TExecuteYqlResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            NHttp::THeaders headers(Request.Request->Headers);
            TStringBuf accept(headers["Accept"]);
            for (TStringBuf type = accept.NextTok(", "); !type.empty(); type = accept.NextTok(", ")) {
                TStringBuf contentType = type.NextTok(';');
                if (contentType == "application/yson") {
                    TVector<NYdb::TResultSet> resultSets = result.GetResultSets();
                    TStringStream results;
                    results << '[';
                    for (auto it = resultSets.begin(); it != resultSets.end(); ++it) {
                        if (it != resultSets.begin()) {
                            results << ';';
                        }
                        const NYdb::TResultSet& resultSet = *it;
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
                    TVector<NYdb::TResultSet> resultSets = result.GetResultSets();
                    NJson::TJsonValue jsonResults;
                    jsonResults.SetType(NJson::JSON_ARRAY);
                    for (auto it = resultSets.begin(); it != resultSets.end(); ++it) {
                        const NYdb::TResultSet& resultSet = *it;
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
                }
            }
            if (response == nullptr) {
                response = Request.Request->CreateResponseBadRequest("Unsupported accept content-type", "text/plain");
            }
        } else {
            response = CreateStatusResponse(Request.Request, result, JsonSettings);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvExecuteYqlScriptResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcQuery1 : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcQuery1> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcQuery1>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcQuery1(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcQuery1::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET" || request->Method == "POST") {
            ctx.Register(new THandlerActorYdbcQueryCollector1(Location, event->Sender, request, HttpProxyId));
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
