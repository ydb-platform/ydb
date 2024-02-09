#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include "mvp.h"
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

class THandlerActorYdbcBrowseGet : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcBrowseGet> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcBrowseGet>;
    const TYdbcLocation& Location;
    TRequest Request;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> SchemeClient;
    NActors::TActorId HttpProxyId;
    bool SkipDotNames = true;
    bool SkipStreams = true;
    TInstant Deadline;
    static constexpr TDuration MAX_RETRY_TIME = TDuration::Seconds(10);
    TString Database;
    TString Path;

    THandlerActorYdbcBrowseGet(
            const TYdbcLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            const NActors::TActorId& httpProxyId)
        : Location(location)
        , Request(sender, request)
        , HttpProxyId(httpProxyId)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        SkipDotNames = FromStringWithDefault(Request.Parameters["skipDotNames"], SkipDotNames);
        SkipStreams = FromStringWithDefault(Request.Parameters["skipStreams"], SkipStreams);
        TString databaseId = Request.Parameters["databaseId"];
        if (IsValidDatabaseId(databaseId)) {
            NHttp::THttpOutgoingRequestPtr httpRequest =
                    NHttp::THttpOutgoingRequest::CreateRequestGet(
                        TMVP::GetAppropriateEndpoint(Request.Request)
                        + "/ydbc/" + Location.Name + "/database" + "?databaseId=" + databaseId);
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        } else {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest("Invalid databaseId", "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        Become(&THandlerActorYdbcBrowseGet::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void RequestListDirectory(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
        NActors::TActorId actorId = ctx.SelfID;
        SchemeClient->ListDirectory(Path, NYdb::NScheme::TListDirectorySettings().ClientTimeout(GetClientTimeout()))
                .Subscribe([actorSystem, actorId](const NYdb::NScheme::TAsyncListDirectoryResult& result) mutable {
            NYdb::NScheme::TAsyncListDirectoryResult res(result);
            actorSystem->Send(actorId, new TEvPrivate::TEvListDirectoryResult(res.ExtractValue()));
        });
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
                TStringBuf scheme;
                TStringBuf host;
                TStringBuf uri;
                NHttp::CrackURL(endpoint, scheme, host, uri);
                NHttp::TUrlParameters urlParams(uri);
                Database = urlParams["database"];
                Path = Request.Parameters["path"];
                TString token = Request.GetAuthTokenForIAM();

                if (!Database.empty() && !Path.empty()) {
                    // TODO: validation
                    Path.insert(0, Database);
                    SchemeClient = Location.GetSchemeClientPtr(host, scheme, NYdb::TCommonClientSettings().Database(Database).AuthToken(token));
                    Deadline = ctx.Now() + MAX_RETRY_TIME;
                    RequestListDirectory(ctx);
                    return;
                } else {
                    if (Database.empty()) {
                        message = "Invalid database endpoint";
                        status = "400";
                    }
                    if (Path.empty()) {
                        message = "Invalid path argument";
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

    void Handle(TEvPrivate::TEvListDirectoryResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::NScheme::TListDirectoryResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            NJson::TJsonValue root;
            root.SetType(NJson::JSON_ARRAY);
            TVector<NYdb::NScheme::TSchemeEntry> children = result.GetChildren();
            for (const NYdb::NScheme::TSchemeEntry& entry : children) {
                if (entry.Name.StartsWith('.') && SkipDotNames || entry.Type == NYdb::NScheme::ESchemeEntryType::PqGroup && SkipStreams) {
                    continue;
                }
                NJson::TJsonValue& schemeEntry(root.AppendValue(NJson::TJsonValue()));
                WriteSchemeEntry(schemeEntry, entry);
            }
            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else if (IsRetryableError(result) && (ctx.Now() < Deadline)) {
            ctx.Schedule(TDuration::MilliSeconds(200), new TEvPrivate::TEvRetryRequest());
            return;
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(TEvPrivate::TEvRetryRequest::TPtr, const NActors::TActorContext& ctx) {
        RequestListDirectory(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvListDirectoryResult, Handle);
            HFunc(TEvPrivate::TEvRetryRequest, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcBrowse : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcBrowse> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcBrowse>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcBrowse(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcBrowse::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new THandlerActorYdbcBrowseGet(Location, event->Sender, request, HttpProxyId));
            return;
        }
        ctx.Send(event->Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(request->CreateResponseBadRequest("Invalid request")));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
