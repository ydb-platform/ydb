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

using namespace NKikimr;

class THandlerActorYdbcDirectoryReader : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDirectoryReader> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDirectoryReader>;
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> SchemeClient;

    THandlerActorYdbcDirectoryReader(
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
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        } else {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest("Invalid databaseId", "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        Become(&THandlerActorYdbcDirectoryReader::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
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
                TString database = urlParams["database"];
                TString path = Request.Parameters["path"];
                TString token = Request.GetAuthToken();

                if (!database.empty() && !path.empty()) {
                    SchemeClient = Location.GetSchemeClientPtr(host, scheme, NYdb::TCommonClientSettings().Database(database).AuthToken(token));

                    // TODO: validation
                    path.insert(0, database);
                    NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
                    NActors::TActorId actorId = ctx.SelfID;
                    SchemeClient->ListDirectory(path, NYdb::NScheme::TListDirectorySettings().ClientTimeout(GetClientTimeout()))
                            .Subscribe([actorSystem, actorId](const NYdb::NScheme::TAsyncListDirectoryResult& result) mutable {
                        NYdb::NScheme::TAsyncListDirectoryResult res(result);
                        actorSystem->Send(actorId, new TEvPrivate::TEvListDirectoryResult(res.ExtractValue()));
                    });
                    return;
                } else {
                    if (database.empty()) {
                        message = "Invalid database endpoint";
                        status = "400";
                    }
                    if (path.empty()) {
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
        NJson::TJsonValue root;
        root.SetType(NJson::JSON_ARRAY);
        const NYdb::NScheme::TListDirectoryResult& result(event->Get()->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (result.IsSuccess()) {
            TVector<NYdb::NScheme::TSchemeEntry> children = result.GetChildren();
            for (const NYdb::NScheme::TSchemeEntry& entry : children) {
                NJson::TJsonValue& schemeEntry(root.AppendValue(NJson::TJsonValue()));
                WriteSchemeEntry(schemeEntry, entry);
            }
            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, result);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvListDirectoryResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcDirectoryCreator : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDirectoryCreator> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDirectoryCreator>;
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> SchemeClient;

    THandlerActorYdbcDirectoryCreator(
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
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        } else {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest("Invalid databaseId", "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        Become(&THandlerActorYdbcDirectoryCreator::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
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
                TString database = urlParams["database"];
                TString path = Request.Parameters["path"];
                TString token = Request.GetAuthTokenForIAM();

                if (!database.empty() && !path.empty()) {
                    SchemeClient = Location.GetSchemeClientPtr(host, scheme, NYdb::TCommonClientSettings().Database(database).AuthToken(token));

                    // TODO: validation
                    path.insert(0, database);
                    NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
                    NActors::TActorId actorId = ctx.SelfID;
                    SchemeClient->MakeDirectory(path, NYdb::NScheme::TMakeDirectorySettings().ClientTimeout(GetClientTimeout()))
                            .Subscribe([actorSystem, actorId](const NYdb::TAsyncStatus& result) mutable {
                        NYdb::TAsyncStatus res(result);
                        actorSystem->Send(actorId, new TEvPrivate::TEvMakeDirectoryResult(res.ExtractValue()));
                    });
                    return;
                } else {
                    if (database.empty()) {
                        message = "Invalid database endpoint";
                        status = "400";
                    }
                    if (path.empty()) {
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

    void Handle(TEvPrivate::TEvMakeDirectoryResult::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response;
        response = CreateStatusResponse(Request.Request, event->Get()->Result);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvMakeDirectoryResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcDirectoryRemover : THandlerActorYdbc, public NActors::TActorBootstrapped<THandlerActorYdbcDirectoryRemover> {
public:
    using TBase = NActors::TActorBootstrapped<THandlerActorYdbcDirectoryRemover>;
    const TYdbcLocation& Location;
    TRequest Request;
    NActors::TActorId HttpProxyId;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> SchemeClient;

    THandlerActorYdbcDirectoryRemover(
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
            Request.ForwardHeadersOnlyForIAM(httpRequest);
            ctx.Send(HttpProxyId, new NHttp::TEvHttpProxy::TEvHttpOutgoingRequest(httpRequest));
        } else {
            NHttp::THttpOutgoingResponsePtr response = Request.Request->CreateResponseBadRequest("Invalid databaseId", "text/plain");
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
            return;
        }

        Become(&THandlerActorYdbcDirectoryRemover::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
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
                TString database = urlParams["database"];
                TString path = Request.Parameters["path"];
                TString token = Request.GetAuthTokenForIAM();

                if (!database.empty() && !path.empty()) {
                    SchemeClient = Location.GetSchemeClientPtr(host, scheme, NYdb::TCommonClientSettings().Database(database).AuthToken(token));

                    // TODO: validation
                    path.insert(0, database);
                    NActors::TActorSystem* actorSystem = ctx.ExecutorThread.ActorSystem;
                    NActors::TActorId actorId = ctx.SelfID;
                    SchemeClient->RemoveDirectory(path, NYdb::NScheme::TRemoveDirectorySettings().ClientTimeout(GetClientTimeout()))
                            .Subscribe([actorSystem, actorId](const NYdb::TAsyncStatus& result) mutable {
                        NYdb::TAsyncStatus res(result);
                        actorSystem->Send(actorId, new TEvPrivate::TEvRemoveDirectoryResult(res.ExtractValue()));
                    });
                    return;
                } else {
                    if (database.empty()) {
                        message = "Invalid database endpoint";
                        status = "400";
                    }
                    if (path.empty()) {
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

    void Handle(TEvPrivate::TEvRemoveDirectoryResult::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpOutgoingResponsePtr response;
        response = CreateStatusResponse(Request.Request, event->Get()->Result);
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvRemoveDirectoryResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcDirectory : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcDirectory> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcDirectory>;
    const TYdbcLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcDirectory(const TYdbcLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcDirectory::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "POST") {
            ctx.Register(new THandlerActorYdbcDirectoryCreator(Location, HttpProxyId, event->Sender, request));
            return;
        } else if (request->Method == "DELETE") {
            ctx.Register(new THandlerActorYdbcDirectoryRemover(Location, HttpProxyId, event->Sender, request));
            return;
        } else if (request->Method == "GET") {
            ctx.Register(new THandlerActorYdbcDirectoryReader(Location, HttpProxyId, event->Sender, request));
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
