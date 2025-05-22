#pragma once
#include "json_handlers.h"
#include "viewer.h"
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NViewer {

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
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Topic information",
            .Description = "Information about topic",
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "schema path",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "client",
            .Description = "client name",
            .Type = "string",
            .Default = "total",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "all",
            .Description = "return all topics and all clients",
            .Type = "boolean",
            .Default = "false",
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
            .Default = "10000",
        });
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TEvTabletCounters::TEvTabletLabeledCountersResponse::ProtoRecordType>());
        return yaml;
    }
};

}
