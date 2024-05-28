#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonTopicInfo : public TActorBootstrapped<TJsonTopicInfo> {
    using TBase = TActorBootstrapped<TJsonTopicInfo>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    NKikimrLabeledCounters::TEvTabletLabeledCountersResponse TopicInfoResult;
    TJsonSettings JsonSettings;
    TString Topic;
    TString Client;
    TString GroupNames;
    bool ShowAll = false;
    ui32 Timeout = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonTopicInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Topic = params.Get("path");
        Client = params.Has("client") ? params.Get("client") : "total";
        GroupNames = params.Get("group_names");
        ShowAll = FromStringWithDefault<bool>(params.Get("all"), false);
        size_t pos = Topic.rfind('/');
        if (pos != TString::npos)
            Topic = Topic.substr(pos + 1);
        //proxy is not used
        CreateClusterLabeledCountersAggregator(ctx.SelfID, TTabletTypes::PersQueue, ctx);

        Become(&TThis::StateRequestedTopicInfo, ctx, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void Die(const TActorContext& ctx) override {
        TBase::Die(ctx);
    }

    STFUNC(StateRequestedTopicInfo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTabletCounters::TEvTabletLabeledCountersResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvTabletCounters::TEvTabletLabeledCountersResponse::TPtr &ev, const TActorContext &ctx) {
        TString groupPrefix = Client + "/";
        TString groupSuffix = "/" + Topic;
        for (ui32 i = 0; i < ev->Get()->Record.LabeledCountersByGroupSize(); ++i) {
            const auto& uc = ev->Get()->Record.GetLabeledCountersByGroup(i);
            const TString& group(uc.GetGroup());
            if (ShowAll
                    || (group.StartsWith(groupPrefix) && group.EndsWith(groupSuffix))
                    || uc.GetGroup() == Topic
                    || uc.GetGroupNames() == GroupNames) {
                TopicInfoResult.AddLabeledCountersByGroup()->CopyFrom(uc);
            }
        }
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext &ctx) {
        TStringStream json;
        TProtoToJson::ProtoToJson(json, TopicInfoResult, JsonSettings);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }
};

template <>
struct TJsonRequestSchema<TJsonTopicInfo> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<TEvTabletCounters::TEvTabletLabeledCountersResponse::ProtoRecordType>();
    }
};

template <>
struct TJsonRequestParameters<TJsonTopicInfo> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: path
              in: query
              description: schema path
              required: true
              type: string
            - name: client
              in: query
              description: client name
              required: false
              type: string
              default: total
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: all
              in: query
              description: return all topics and all clients
              required: false
              type: boolean
              default: false
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
              default: 10000
            )___");
    }
};

template <>
struct TJsonRequestSummary<TJsonTopicInfo> {
    static TString GetSummary() {
        return "Topic information";
    }
};

template <>
struct TJsonRequestDescription<TJsonTopicInfo> {
    static TString GetDescription() {
        return "Information about topic";
    }
};

}
}
