#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/mvp/core/appdata.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

using namespace NKikimr;

class THandlerActorYdbcDynamoDescribeTablePost : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDynamoDescribeTablePost> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDynamoDescribeTablePost>;
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> SchemeClient;

    THandlerActorYdbcDynamoDescribeTablePost(
            const TYdbcLocation& location,
            const NActors::TActorId& httpProxyId,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request)
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
        Become(&THandlerActorYdbcDynamoDescribeTablePost::StateResolveDatabase, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void HandleResolveDatabase(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            NJson::TJsonValue responseData;
            bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &responseData);
            if (success) {
                TString endpoint = responseData["endpoint"].GetStringRobust();
                TStringBuf scheme;
                TStringBuf host;
                TStringBuf uri;
                NHttp::CrackURL(endpoint, scheme, host, uri);
                NHttp::TUrlParameters urlParams(uri);
                TString database = urlParams["database"];
                if (!database.empty()) {
                    TString body(Request.Request->Body);
                    NHttp::THttpOutgoingRequestPtr httpRequest = NHttp::THttpOutgoingRequest::CreateRequestPost(Location.GetServerlessProxyUrl(database));
                    Request.ForwardHeaders(httpRequest);
                    httpRequest->Set("X-Amz-Target", "DynamoDB_20120810.DescribeTable");
                    httpRequest->Set<&NHttp::THttpRequest::ContentType>("application/x-amz-json-1.0");
                    httpRequest->Set<&NHttp::THttpRequest::Body>(body);
                    ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
                    Become(&THandlerActorYdbcDynamoDescribeTablePost::StateRequest);
                    return;
                } else {
                    if (database.empty()) {
                        message = "Invalid database endpoint";
                        status = "400";
                    }
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

    void HandleRequest(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        if (event->Get()->Response != nullptr) {
            TStringBuf status = event->Get()->Response->Status;
            TStringBuf message = event->Get()->Response->Message;
            TStringBuf contentType = event->Get()->Response->ContentType;
            TStringBuf body = event->Get()->Response->Body;

            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponse(status, message, contentType, body);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        } else {
            NHttp::THttpOutgoingResponsePtr response = CreateErrorResponse(Request.Request, event->Get()->Error);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        }
        TBase::Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateResolveDatabase) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleResolveDatabase);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateRequest) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, HandleRequest);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcDynamoDescribeTable : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcDynamoDescribeTable> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcDynamoDescribeTable>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcDynamoDescribeTable(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcDynamoDescribeTable::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            ctx.Register(new THandlerActorYdbcDynamoDescribeTablePost(Location, HttpProxyId, event->Sender, request));
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

}
