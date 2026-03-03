#pragma once

#include <ydb/mvp/core/core_ydb.h>
#include <ydb/mvp/core/core_ydb_impl.h>
#include <ydb/mvp/meta/support_links/resolver_factory.h>
#include <ydb/mvp/meta/support_links/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/http/http.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/result/result.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>

#include <util/generic/hash.h>

namespace NMVP {

using namespace NKikimr;

class TMetaSupportLinksGetHandlerActor : THandlerActorYdb, public NActors::TActorBootstrapped<TMetaSupportLinksGetHandlerActor> {
public:
    enum class EEntityType {
        Cluster,
        Database,
    };

    struct TSourceSlot {
        TString Source;
        bool Ready = false;
        TVector<NSupportLinks::TResolvedLink> Links;
        TVector<NSupportLinks::TSupportError> Errors;
    };

    using TBase = NActors::TActorBootstrapped<TMetaSupportLinksGetHandlerActor>;

    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;
    TRequest Request;
    TMaybe<NYdb::NTable::TSession> Session;
    TMaybe<EEntityType> EntityType;
    THashMap<TString, TString> ClusterColumns;
    TVector<std::pair<TString, TString>> QueryParams;
    TVector<TSupportLinkEntryConfig> RequestedLinks;
    TVector<TSourceSlot> SourceSlots;
    ui32 PendingSources = 0;

    TMetaSupportLinksGetHandlerActor(
        const NActors::TActorId& httpProxyId,
        const TYdbLocation& location,
        const NActors::TActorId& sender,
        const NHttp::THttpIncomingRequestPtr& request)
        : HttpProxyId(httpProxyId)
        , Location(location)
        , Request(sender, request)
    {}

    void Bootstrap(const NActors::TActorContext& ctx) {
        Become(&TMetaSupportLinksGetHandlerActor::StateWork, GetTimeout(Request, TDuration::Seconds(60)), new NActors::TEvents::TEvWakeup());

        if (!InitEntityType()) {
            ReplyBadRequestAndDie(ctx);
            return;
        }

        const auto& entityLinks = GetEntityLinks();
        RequestedLinks.assign(entityLinks.begin(), entityLinks.end());
        SourceSlots.resize(RequestedLinks.size());
        for (size_t i = 0; i < RequestedLinks.size(); ++i) {
            SourceSlots[i].Source = RequestedLinks[i].Source;
        }

        for (const auto& [name, value] : Request.Parameters.UrlParameters.Parameters) {
            QueryParams.emplace_back(TString(name), TString(value));
        }

        if (RequestedLinks.empty()) {
            ReplyOkAndDie(ctx);
            return;
        }

        RequestClusterInfo(ctx);
    }

    bool InitEntityType() {
        const TString cluster = Request.Parameters["cluster"];
        const TString database = Request.Parameters["database"];

        if (cluster.empty()) {
            SourceSlots.resize(1);
            SourceSlots[0].Errors.emplace_back(NSupportLinks::TSupportError{
                .Source = TString(NSupportLinks::SOURCE_META),
                .Message = TString(NSupportLinks::INVALID_IDENTITY_PARAMS_MESSAGE)
            });
            return false;
        }
        EntityType = database.empty() ? EEntityType::Cluster : EEntityType::Database;
        return true;
    }

    const TVector<TSupportLinkEntryConfig>& GetEntityLinks() const {
        if (EntityType && *EntityType == EEntityType::Database) {
            return InstanceMVP->MetaSettings.SupportLinksConfig.Database;
        }
        return InstanceMVP->MetaSettings.SupportLinksConfig.Cluster;
    }

    virtual void RequestClusterInfo(const NActors::TActorContext& ctx) {
        NActors::TActorSystem* actorSystem = ctx.ActorSystem();
        NActors::TActorId actorId = ctx.SelfID;
        Location.GetTableClient(TMVP::GetMetaDatabaseClientSettings(Request, Location))
            .CreateSession()
            .Subscribe([actorId, actorSystem](const NYdb::NTable::TAsyncCreateSessionResult& result) {
                NYdb::NTable::TAsyncCreateSessionResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvCreateSessionResult(res.ExtractValue()));
            });
    }

    virtual NActors::IActor* CreateSourceHandler(NSupportLinks::TLinkResolveContext sourceContext) {
        return NSupportLinks::BuildSourceHandler(std::move(sourceContext));
    }

    void Handle(TEvPrivate::TEvCreateSessionResult::TPtr event, const NActors::TActorContext& ctx) {
        const NYdb::NTable::TCreateSessionResult& result(event->Get()->Result);
        if (!result.IsSuccess()) {
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(CreateStatusResponse(Request.Request, result)));
            Die(ctx);
            return;
        }

        Session = result.GetSession();
        TString query = TStringBuilder() << "DECLARE $name AS Utf8; SELECT * FROM `" << Location.RootDomain << "/ydb/MasterClusterExt.db` WHERE name=$name";
        NYdb::TParamsBuilder params;
        params.AddParam("$name", NYdb::TValueBuilder().Utf8(Request.Parameters["cluster"]).Build());

        NActors::TActorSystem* actorSystem = ctx.ActorSystem();
        NActors::TActorId actorId = ctx.SelfID;
        Session->ExecuteDataQuery(
            query,
            NYdb::NTable::TTxControl::BeginTx(
                NYdb::NTable::TTxSettings::OnlineRO(
                    NYdb::NTable::TTxOnlineSettings().AllowInconsistentReads(true))).CommitTx(),
            params.Build())
            .Subscribe([actorId, actorSystem, session = Session](const NYdb::NTable::TAsyncDataQueryResult& result) {
                NYdb::NTable::TAsyncDataQueryResult res(result);
                actorSystem->Send(actorId, new TEvPrivate::TEvDataQueryResult(res.ExtractValue()));
            });
    }

    void Handle(TEvPrivate::TEvDataQueryResult::TPtr event, const NActors::TActorContext& ctx) {
        NYdb::NTable::TDataQueryResult& result(event->Get()->Result);
        if (!result.IsSuccess()) {
            ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(CreateStatusResponse(Request.Request, result)));
            Die(ctx);
            return;
        }

        auto resultSet = result.GetResultSet(0);
        const auto& columnsMeta = resultSet.GetColumnsMeta();
        NYdb::TResultSetParser rsParser(resultSet);
        if (!rsParser.TryNextRow()) {
            AddCommonError({
                .Source = TString(NSupportLinks::SOURCE_META),
                .Message = TStringBuilder() << "Cluster '" << Request.Parameters["cluster"] << "' is not found in MasterClusterExt.db"
            });
        } else {
            for (size_t columnNum = 0; columnNum < columnsMeta.size(); ++columnNum) {
                const NYdb::TColumn& columnMeta = columnsMeta[columnNum];
                ClusterColumns[columnMeta.Name] = ColumnValueToString(rsParser.ColumnParser(columnNum));
            }
        }

        SpawnSourceActors(ctx);
    }

    void SpawnSourceActors(const NActors::TActorContext& ctx) {
        PendingSources = 0;
        for (size_t place = 0; place < RequestedLinks.size(); ++place) {
            NSupportLinks::TLinkResolveContext sourceContext{
                .Place = place,
                .LinkConfig = RequestedLinks[place],
                .ClusterColumns = ClusterColumns,
                .QueryParams = QueryParams,
                .Parent = ctx.SelfID,
                .HttpProxyId = HttpProxyId,
            };
            ctx.Register(CreateSourceHandler(std::move(sourceContext)));
            ++PendingSources;
        }

        if (PendingSources == 0) {
            ReplyOkAndDie(ctx);
        }
    }

    void Handle(NSupportLinks::TEvPrivate::TEvSourceResponse::TPtr event, const NActors::TActorContext& ctx) {
        const auto* msg = event->Get();
        if (msg->Place < SourceSlots.size() && !SourceSlots[msg->Place].Ready) {
            SourceSlots[msg->Place].Ready = true;
            SourceSlots[msg->Place].Links = msg->Links;
            auto& slotErrors = SourceSlots[msg->Place].Errors;
            slotErrors.insert(slotErrors.end(), msg->Errors.begin(), msg->Errors.end());
            if (PendingSources > 0) {
                --PendingSources;
            }
        }

        if (PendingSources == 0) {
            ReplyOkAndDie(ctx);
        }
    }

    void HandleTimeout(const NActors::TActorContext& ctx) {
        for (auto& slot : SourceSlots) {
            if (!slot.Ready) {
                slot.Errors.emplace_back(NSupportLinks::TSupportError{
                    .Source = slot.Source,
                    .Message = "Timeout while resolving support links source"
                });
                slot.Ready = true;
            }
        }
        ReplyOkAndDie(ctx);
    }

    void AddCommonError(NSupportLinks::TSupportError error) {
        if (SourceSlots.empty()) {
            SourceSlots.resize(1);
            SourceSlots[0].Source = TString(NSupportLinks::SOURCE_META);
        }
        SourceSlots[0].Errors.emplace_back(std::move(error));
    }

    void ReplyBadRequestAndDie(const NActors::TActorContext& ctx) {
        auto response = CreateResponseBadRequest(Request.Request, BuildResponseBody(), "application/json; charset=utf-8");
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    void ReplyOkAndDie(const NActors::TActorContext& ctx) {
        auto response = CreateResponseOK(Request.Request, BuildResponseBody(), "application/json; charset=utf-8");
        ctx.Send(Request.Sender, new NHttp::TEvHttpProxy::TEvHttpOutgoingResponse(response));
        Die(ctx);
    }

    TString BuildResponseBody() const {
        NJson::TJsonValue root;
        NJson::TJsonValue& linksJson = root["links"];
        linksJson.SetType(NJson::JSON_ARRAY);

        NJson::TJsonValue errorsJson;
        errorsJson.SetType(NJson::JSON_ARRAY);
        bool hasErrors = false;

        for (const auto& slot : SourceSlots) {
            for (const auto& link : slot.Links) {
                NJson::TJsonValue& item = linksJson.AppendValue(NJson::TJsonValue());
                if (!link.Title.empty()) {
                    item["title"] = link.Title;
                }
                item["url"] = link.Url;
            }
            for (const auto& error : slot.Errors) {
                NJson::TJsonValue& item = errorsJson.AppendValue(NJson::TJsonValue());
                item["source"] = error.Source;
                if (error.Status) {
                    item["status"] = *error.Status;
                }
                if (!error.Reason.empty()) {
                    item["reason"] = error.Reason;
                }
                if (!error.Message.empty()) {
                    item["message"] = error.Message;
                }
                hasErrors = true;
            }
        }

        if (hasErrors) {
            root["errors"] = std::move(errorsJson);
        }
        return NJson::WriteJson(root, false);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvCreateSessionResult, Handle);
            HFunc(TEvPrivate::TEvDataQueryResult, Handle);
            HFunc(NSupportLinks::TEvPrivate::TEvSourceResponse, Handle);
            CFunc(NActors::TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }
};

class TMetaSupportLinksHandlerActor : THandlerActorYdb, public NActors::TActor<TMetaSupportLinksHandlerActor> {
public:
    using TBase = NActors::TActor<TMetaSupportLinksHandlerActor>;
    NActors::TActorId HttpProxyId;
    const TYdbLocation& Location;

    TMetaSupportLinksHandlerActor(const NActors::TActorId& httpProxyId, const TYdbLocation& location)
        : TBase(&TMetaSupportLinksHandlerActor::StateWork)
        , HttpProxyId(httpProxyId)
        , Location(location)
    {}

    void Handle(NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr event, const NActors::TActorContext& ctx) {
        NHttp::THttpIncomingRequestPtr request = event->Get()->Request;
        if (request->Method == "GET") {
            ctx.Register(new TMetaSupportLinksGetHandlerActor(HttpProxyId, Location, event->Sender, request));
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
