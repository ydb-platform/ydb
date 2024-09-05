#include "browse.h"
#include "json_handlers.h"
#include "viewer.h"
#include "wb_aggregate.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonMetaInfo : public TActorBootstrapped<TJsonMetaInfo> {
    using TBase = TActorBootstrapped<TJsonMetaInfo>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    bool Counters = false;
    NKikimrViewer::TMetaInfo MetaInfo;
    TActorId BrowseActorID;
    using TBrowseRequestKey = std::tuple<TActorId, TTabletId, ui32>;
    std::unordered_multiset<TBrowseRequestKey> BrowseRequestsInFlight;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonMetaInfo(IViewer *viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Counters = FromStringWithDefault(params.Get("counters"), false);
        TString path = params.Get("path");
        BrowseActorID = ctx.RegisterWithSameMailbox(new TBrowse(Viewer, ctx.SelfID, path, Event->Get()->UserToken));
        Become(&TThis::StateWait, ctx, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void Die(const TActorContext& ctx) override {
        ctx.Send(BrowseActorID, new TEvents::TEvPoisonPill());
        TBase::Die(ctx);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NViewerEvents::TEvBrowseResponse, Handle);
            HFunc(NViewerEvents::TEvBrowseRequestSent, Handle);
            HFunc(NViewerEvents::TEvBrowseRequestCompleted, Handle);
            HFunc(NMon::TEvHttpInfoRes, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
        ctx.ExecutorThread.Send(ev->Forward(Event->Sender));
        Die(ctx);
    }

    void Handle(NViewerEvents::TEvBrowseResponse::TPtr &ev, const TActorContext &ctx) {
        NViewerEvents::TEvBrowseResponse& event(*ev->Get());
        if (!event.Error.empty()) {
            ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(event.Error, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return Die(ctx);
        }
        MetaInfo.MergeFrom(event.MetaInfo);
        if (!Counters) {
            // TODO(xenoxeno): it could be a little bit more effective
            MetaInfo.ClearCounters();
        }
        ReplyAndDie(ctx);
    }

    void Handle(NViewerEvents::TEvBrowseRequestSent::TPtr& ev, const TActorContext&) {
        NViewerEvents::TEvBrowseRequestSent& event(*ev->Get());
        BrowseRequestsInFlight.emplace(event.Actor, event.Tablet, event.Event);
    }

    void Handle(NViewerEvents::TEvBrowseRequestCompleted::TPtr& ev, const TActorContext&) {
        NViewerEvents::TEvBrowseRequestCompleted& event(*ev->Get());
        auto it = BrowseRequestsInFlight.find({event.Actor, event.Tablet, event.Event});
        if (it != BrowseRequestsInFlight.end()) {
            // we could not delete by key, it could be many items with the same key
            BrowseRequestsInFlight.erase(it);
        }
        BrowseRequestsInFlight.emplace(event.Actor, event.Tablet, event.Event);
    }

    void ReplyAndDie(const TActorContext &ctx) {
        TStringStream json;
        TProtoToJson::ProtoToJson(json, MetaInfo, JsonSettings);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        TStringStream result;
        RenderPendingRequests(result);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get(), result.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void RenderPendingRequests(IOutputStream& html) {
        for (const auto& request : BrowseRequestsInFlight) {
            html << request << Endl;
        }
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "obsolete",
            .Summary = "Schema meta information",
            .Description = "Returns meta information about schema path",
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "schema path",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "counters",
            .Description = "return tablet counters",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "ui64",
            .Description = "return ui64 as number",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TMetaInfo>());
        return yaml;
    }
};

}
