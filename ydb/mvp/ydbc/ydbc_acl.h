#pragma once
#include <util/generic/hash_set.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/http/http.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_scheme/scheme.h>
#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/mvp/core/core_ydbc.h>
#include <ydb/mvp/core/core_ydbc_impl.h>
#include <ydb/mvp/core/merger.h>

namespace NMVP {

template <typename DerivedType>
class THandlerActorYdbcAclBase : public THandlerActorYdbc, public NActors::TActorBootstrapped<DerivedType> {
public:
    using TBase = NActors::TActorBootstrapped<DerivedType>;
    const TYdbLocation& Location;
    TRequest Request;
    std::unique_ptr<NYdb::NScheme::TSchemeClient> SchemeClient;
    NActors::TActorId HttpProxyId;
    TString Scheme;
    TString Host;
    TString Database;
    TString Path;
    TString FullPath;
    TString Token;
    TString Subject;
    NActors::TActorId ActorId;
    NActors::TActorSystem* ActorSystem;
    NJson::TJsonValue DatabaseInfo;
    THolder<typename TEvPrivate::TEvDescribePathResult> DescribePathResult;

    THandlerActorYdbcAclBase(
            const TYdbLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            const NActors::TActorId& httpProxyId)
        : Location(location)
        , Request(sender, request)
        , HttpProxyId(httpProxyId)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        ActorId = ctx.SelfID;
        ActorSystem = ctx.ExecutorThread.ActorSystem;
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
        TBase::Become(&THandlerActorYdbcAclBase<DerivedType>::StateWork, GetTimeout(), new NActors::TEvents::TEvWakeup());
    }

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingResponse::TPtr event, const NActors::TActorContext& ctx) {
        TStringBuf status = event->Get()->Response->Status;
        TStringBuf message;
        TStringBuf contentType;
        TStringBuf body;
        if (event->Get()->Error.empty() && status == "200") {
            bool success = NJson::ReadJsonTree(event->Get()->Response->Body, &JsonReaderConfig, &DatabaseInfo);
            if (success) {
                TString endpoint = DatabaseInfo["endpoint"].GetStringRobust();
                TStringBuf scheme;
                TStringBuf host;
                TStringBuf uri;
                NHttp::CrackURL(endpoint, scheme, host, uri);
                Scheme = scheme;
                Host = host;
                NHttp::TUrlParameters urlParams(uri);
                Database = urlParams["database"];
                Path = Request.Parameters["path"];
                Token = Request.GetAuthTokenForIAM();
                if (!Database.empty() && !Path.empty()) {
                    // TODO: validation
                    FullPath = Database + Path;
                    if (FullPath.size() > 1 && FullPath.EndsWith('/')) {
                        FullPath.resize(FullPath.size() - 1);
                    }
                    SchemeClient = Location.GetSchemeClientPtr(Host, Scheme, NYdb::TCommonClientSettings().Database(Database).AuthToken(Token));
                    static_cast<DerivedType*>(this)->Init(ctx);
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

    void Handle(typename TEvPrivate::TEvDescribePathResult::TPtr event, const NActors::TActorContext& ctx) {
        DescribePathResult = event->Release();
        NJson::TJsonValue root;
        const auto& describePathResult(DescribePathResult->Result);
        NHttp::THttpOutgoingResponsePtr response;
        if (describePathResult.IsSuccess()) {
            WriteAccessEntry(root, describePathResult.GetEntry(), Subject);
            response = Request.Request->CreateResponseOK(NJson::WriteJson(root, false), "application/json; charset=utf-8");
        } else {
            response = CreateStatusResponse(Request.Request, describePathResult);
        }
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        TBase::Die(ctx);
    }

    void Handle(typename TEvPrivate::TEvModifyPermissionsResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::TStatus& result(event->Get()->Result);
        if (!result.IsSuccess()) {
            NHttp::THttpOutgoingResponsePtr response = CreateStatusResponse(Request.Request, result);
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
            TBase::Die(ctx);
        } else {
            Subject.clear(); // return full ACL on successfull modify
            SchemeClient->DescribePath(FullPath).Subscribe([this](const NYdb::NScheme::TAsyncDescribePathResult& result) mutable {
                NYdb::NScheme::TAsyncDescribePathResult res(result);
                ActorSystem->Send(ActorId, new typename TEvPrivate::TEvDescribePathResult(res.ExtractValue()));
            });
        }
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseGatewayTimeout()));
        TBase::Die(ctx);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingResponse, Handle);
            HFunc(TEvPrivate::TEvModifyPermissionsResult, Handle);
            HFunc(TEvPrivate::TEvDescribePathResult, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class THandlerActorYdbcAclUpdater : public THandlerActorYdbcAclBase<THandlerActorYdbcAclUpdater> {
public:
    TVector<TString> Permissions;

    THandlerActorYdbcAclUpdater(
            const TYdbLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            const NActors::TActorId& httpProxyId)
        : THandlerActorYdbcAclBase(location, sender, request, httpProxyId)
    {}

    void Init(const NActors::TActorContext& ctx) {
        if (Request.Parameters.Success) {
            Subject = Request.Parameters["subject"];
            if (!Subject.empty() && !FullPath.empty()) {
                for (const NJson::TJsonValue& permission : Request.Parameters.PostData["permissions"].GetArray()) {
                    Permissions.emplace_back(permission.GetStringRobust());
                }
                NYdb::NScheme::TModifyPermissionsSettings settings;
                settings.AddSetPermissions(NYdb::NScheme::TPermissions(Subject, Permissions));
                SchemeClient->ModifyPermissions(FullPath, settings).Subscribe([actorSystem = ActorSystem, actorId = ActorId](const NYdb::TAsyncStatus& result) mutable {
                    NYdb::TAsyncStatus res(result);
                    actorSystem->Send(actorId, new TEvPrivate::TEvModifyPermissionsResult(res.ExtractValue()));
                });
            } else {
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest("Invalid arguments")));
                Die(ctx);
                return;
            }
        } else {
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest("Bad JSON data")));
            Die(ctx);
            return;
        }
    }
};

class THandlerActorYdbcAclRemover : public THandlerActorYdbcAclBase<THandlerActorYdbcAclRemover> {
public:
    THandlerActorYdbcAclRemover(
            const TYdbLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            const NActors::TActorId& httpProxyId)
        : THandlerActorYdbcAclBase(location, sender, request, httpProxyId)
    {}

    void Init(const NActors::TActorContext& ctx) {
        if (Request.Parameters.Success) {
            Subject = Request.Parameters["subject"];
            if (!Subject.empty() && !FullPath.empty()) {
                NYdb::NScheme::TModifyPermissionsSettings settings;
                settings.AddSetPermissions(NYdb::NScheme::TPermissions(Subject));
                SchemeClient->ModifyPermissions(FullPath, settings).Subscribe([actorSystem = ActorSystem, actorId = ActorId](const NYdb::TAsyncStatus& result) mutable {
                    NYdb::TAsyncStatus res(result);
                    actorSystem->Send(actorId, new TEvPrivate::TEvModifyPermissionsResult(res.ExtractValue()));
                });
            } else {
                ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest("Invalid arguments")));
                Die(ctx);
                return;
            }
        } else {
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest("Bad JSON data")));
            Die(ctx);
            return;
        }
    }
};

class THandlerActorYdbcAclGetter : public THandlerActorYdbcAclBase<THandlerActorYdbcAclGetter> {
public:
    THandlerActorYdbcAclGetter(
            const TYdbLocation& location,
            const NActors::TActorId& sender,
            const NHttp::THttpIncomingRequestPtr& request,
            const NActors::TActorId& httpProxyId)
        : THandlerActorYdbcAclBase(location, sender, request, httpProxyId)
    {}

    void Init(const NActors::TActorContext& ctx) {
        ActorId = ctx.SelfID;
        ActorSystem = ctx.ExecutorThread.ActorSystem;
        Subject = Request.Parameters["subject"];
        if (!FullPath.empty()) {
            SchemeClient->DescribePath(FullPath).Subscribe([actorSystem = ActorSystem, actorId = ActorId](const NYdb::NScheme::TAsyncDescribePathResult& result) mutable {
                NYdb::NScheme::TAsyncDescribePathResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvDescribePathResult(res.ExtractValue()));
            });
        } else {
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(Request.Request->CreateResponseBadRequest("Invalid arguments")));
            Die(ctx);
            return;
        }
    }
};

class THandlerActorYdbcAcl : THandlerActorYdbc, public NActors::TActor<THandlerActorYdbcAcl> {
public:
    using TBase = NActors::TActor<THandlerActorYdbcAcl>;
    const TYdbLocation& Location;
    NActors::TActorId HttpProxyId;

    THandlerActorYdbcAcl(const TYdbLocation& location, const NActors::TActorId& httpProxyId)
        : TBase(&THandlerActorYdbcAcl::StateWork)
        , Location(location)
        , HttpProxyId(httpProxyId)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        TStringBuf contentTypeHeader = request->ContentType;
        TStringBuf contentType = contentTypeHeader.NextTok(';');
        if (request->Method == "POST" && contentType == "application/json") {
            ctx.Register(new THandlerActorYdbcAclUpdater(Location, event->Sender, request, HttpProxyId));
            return;
        } else if (request->Method == "DELETE") {
            ctx.Register(new THandlerActorYdbcAclRemover(Location, event->Sender, request, HttpProxyId));
            return;
        } else if (request->Method == "GET") {
            ctx.Register(new THandlerActorYdbcAclGetter(Location, event->Sender, request, HttpProxyId));
            return;
        }
        ctx.Send(event->Sender,
                 new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(event->Get()->Request->CreateResponseBadRequest("Invalid request")));
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NHttp::TEvHttpProxy::TEvHttpIncomingRequest, Handle);
        }
    }
};

} // namespace NMVP
