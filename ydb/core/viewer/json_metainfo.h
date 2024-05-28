#pragma once
#include <unordered_map>
#include <unordered_set>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/viewer/protos/viewer.pb.h>
#include "browse.h"
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"
#include "wb_aggregate.h"

namespace NKikimr {
namespace NViewer {

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
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        TStringStream result;
        result << Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get());
        RenderPendingRequests(result);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(result.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void RenderPendingRequests(IOutputStream& html) {
        for (const auto& request : BrowseRequestsInFlight) {
            html << request << Endl;
        }
    }
};

template <>
struct TJsonRequestSchema<TJsonMetaInfo> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TMetaInfo>();
    }
};

template <>
struct TJsonRequestParameters<TJsonMetaInfo> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: path
              in: query
              description: schema path
              required: false
              type: string
            - name: tablet_id
              in: query
              description: tablet identifier
              required: false
              type: integer
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: counters
              in: query
              description: return tablet counters
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonMetaInfo> {
    static TString GetSummary() {
        return "Schema meta information";
    }
};

template <>
struct TJsonRequestDescription<TJsonMetaInfo> {
    static TString GetDescription() {
        return "Returns meta information about schema path";
    }
};

}
}
