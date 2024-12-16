#include "browse.h"
#include "browse_db.h"
#include "json_handlers.h"
#include "viewer.h"
#include "wb_aggregate.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonBrowse : public TActorBootstrapped<TJsonBrowse> {
    using TBase = TActorBootstrapped<TJsonBrowse>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    bool Recursive = false;

    struct TPathStateInfo {
        TString Name;
        TString Path;
        TActorId BrowseActorId;
        NKikimrViewer::TBrowseInfo BrowseInfo;

        TPathStateInfo(const TString& name, const TString& path, const TActorId& browseActorId)
            : Name(name)
            , Path(path)
            , BrowseActorId(browseActorId)
        {}

        operator const TString&() const {
            return Path;
        }

        bool operator== (const TString& otherPath) const {
            return Path == otherPath;
        }
    };

    TVector<TPathStateInfo> Paths;

    using TBrowseRequestKey = std::tuple<TActorId, TTabletId, ui32>;
    std::unordered_multiset<TBrowseRequestKey> BrowseRequestsInFlight;
    ui32 Responses = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonBrowse(IViewer *viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void ParsePath(const TString& path, const TActorContext& ctx) {
        size_t prevpos = 0;
        size_t pos = 0;
        size_t len = path.size();
        while (pos < len) {
            if (path[pos] == '/') {
                TString n = path.substr(prevpos, pos - prevpos);
                TString p = path.substr(0, pos);
                if (n.empty() && p.empty()) {
                    n = p = "/";
                }
                Paths.emplace_back(n, p, ctx.RegisterWithSameMailbox(new TBrowse(Viewer, ctx.SelfID, p, Event->Get()->UserToken)));
                ++pos;
                prevpos = pos;
            } else {
                ++pos;
            }
        }
        if (pos != prevpos) {
            TString n = path.substr(prevpos, pos - prevpos);
            TString p = path.substr(0, pos);
            Paths.emplace_back(n, p, ctx.RegisterWithSameMailbox(new TBrowse(Viewer, ctx.SelfID, p, Event->Get()->UserToken)));
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Recursive = FromStringWithDefault(params.Get("recursive"), false);
        TString path = params.Get("path");
        if (Recursive) {
            ParsePath(path, ctx);
        } else {
            Paths.emplace_back(path, path, ctx.RegisterWithSameMailbox(new TBrowse(Viewer, ctx.SelfID, path, Event->Get()->UserToken)));
        }
        Become(&TThis::StateWait, ctx, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
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

    void Handle(NViewerEvents::TEvBrowseResponse::TPtr &ev, const TActorContext &ctx) {
        NViewerEvents::TEvBrowseResponse& event(*ev->Get());
        if (!event.Error.empty()) {
            ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(event.Error, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return Die(ctx);
        }
        auto it = std::find(Paths.begin(), Paths.end(), event.BrowseInfo.GetPath());
        if (it != Paths.end()) {
            it->BrowseInfo.MergeFrom(event.BrowseInfo);
            it->BrowseActorId = TActorId();
        }
        // TODO: error handling?
        ++Responses;
        if (Responses == Paths.size()) {
            ReplyAndDie(ctx);
        }
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

    void Handle(NMon::TEvHttpInfoRes::TPtr &ev, const TActorContext &ctx) {
        ctx.ExecutorThread.Send(ev->Forward(Event->Sender));
        Die(ctx);
    }

    void ReplyAndDie(const TActorContext &ctx) {
        TStringStream json;
        if (!Paths.empty()) {
            NKikimrViewer::TBrowseInfo browseInfo;
            auto pi = Paths.begin();
            browseInfo.MergeFrom(pi->BrowseInfo);
            if (Recursive) {
                browseInfo.SetPath(Paths.back().BrowseInfo.GetPath());
                browseInfo.SetName("/");
            }
            NKikimrViewer::TBrowseInfo* pBrowseInfo = &browseInfo;
            ++pi;
            while (pi != Paths.end()) {
                TString name = pi->Name;
                for (NKikimrViewer::TBrowseInfo& child : *pBrowseInfo->MutableChildren()) {
                    if (child.GetName() == name) {
                        pBrowseInfo = &child;
                        pBrowseInfo->MergeFrom(pi->BrowseInfo);
                        pBrowseInfo->ClearPath();
                        break;
                    }
                }
                ++pi;
            }
            TProtoToJson::ProtoToJson(json, browseInfo, JsonSettings);
        }
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        for (auto& pathInfo : Paths) {
            if (pathInfo.BrowseActorId) {
                ctx.Send(pathInfo.BrowseActorId, new TEvents::TEvPoisonPill());
            }
        }
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
            .Summary = "Schema information",
            .Description = "Returns brief information about schema object"
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "schema path",
            .Type = "string",
            .Required = true
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean"
        });
        yaml.AddParameter({
            .Name = "ui64",
            .Description = "return ui64 as number",
            .Type = "boolean"
        });
        yaml.AddParameter({
            .Name = "timeout",
            .Description = "timeout in ms",
            .Type = "integer"
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TBrowseInfo>());
        return yaml;
    }
};

}
