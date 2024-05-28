#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/util/wildcard.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonLabeledCounters : public TActorBootstrapped<TJsonLabeledCounters> {
    using TBase = TActorBootstrapped<TJsonLabeledCounters>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    NKikimrLabeledCounters::TEvTabletLabeledCountersResponse LabeledCountersResult;
    TJsonSettings JsonSettings;
    TString Groups;
    TString GroupNames;
    TString Topic;
    TString Consumer;
    TString DC;
    TVector<TString> Counters;
    ui32 Version = 1;
    ui32 Timeout = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonLabeledCounters(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Groups = params.Get("group");
        Topic = NPersQueue::ConvertNewTopicName(params.Get("topic"));
        if (Topic.empty())
            Topic = "*";
        Consumer = NPersQueue::ConvertNewConsumerName(params.Get("consumer"), ctx);
        DC = params.Get("dc");
        if (DC.empty())
            DC = "*";
        GroupNames = params.Get("group_names");
        Split(params.Get("counters"), ",", Counters);
        Version = FromStringWithDefault<ui32>(params.Get("version"), Version);
        Sort(Counters);
        if (Version >= 3) {
            TString topic = "rt3." + DC + "--" + Topic;
            if (!Consumer.empty()) {
                Groups = Consumer + "/*/" + topic;
                if (Topic != "*") {
                    Groups += "," + topic;
                }
            } else {
                Groups = topic;
            }
        }
        CreateClusterLabeledCountersAggregator(ctx.SelfID, TTabletTypes::PersQueue, ctx, Version, Version >= 2 ? Groups : TString());
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
        if (Version == 1) {
            for (ui32 i = 0; i < ev->Get()->Record.LabeledCountersByGroupSize(); ++i) {
                auto& uc = *ev->Get()->Record.MutableLabeledCountersByGroup(i);
                if (!Groups.empty() && !IsMatchesWildcards(uc.GetGroup(), Groups)) {
                    continue;
                }
                if (!GroupNames.empty() && !IsMatchesWildcard(uc.GetGroupNames(), GroupNames)) {
                    continue;
                }
                if (Counters.empty()) {
                    LabeledCountersResult.AddLabeledCountersByGroup()->Swap(&uc);
                } else {
                    auto& lc = *LabeledCountersResult.AddLabeledCountersByGroup();
                    lc.SetGroup(uc.GetGroup());
                    lc.SetGroupNames(uc.GetGroupNames());
                    for (auto& c : *uc.MutableLabeledCounter()) {
                        if (BinarySearch(Counters.begin(), Counters.end(), c.GetName())) {
                            lc.AddLabeledCounter()->Swap(&c);
                        }
                    }
                }
            }
        } else if (Version >= 2) {
            const NKikimrLabeledCounters::TEvTabletLabeledCountersResponse& source(ev->Get()->Record);
            TVector<TMaybe<ui32>> counterNamesMapping;
            counterNamesMapping.reserve(source.CounterNamesSize());
            for (const TString& counterName : source.GetCounterNames()) {
                if (Counters.empty() || BinarySearch(Counters.begin(), Counters.end(), counterName)) {
                    counterNamesMapping.push_back(LabeledCountersResult.CounterNamesSize());
                    LabeledCountersResult.AddCounterNames(counterName);
                } else {
                    counterNamesMapping.push_back(Nothing());
                }
            }
            for (ui32 i = 0; i < ev->Get()->Record.LabeledCountersByGroupSize(); ++i) {
                auto& uc = *ev->Get()->Record.MutableLabeledCountersByGroup(i);
                auto& lc = *LabeledCountersResult.AddLabeledCountersByGroup();
                lc.SetGroup(uc.GetGroup());
                for (auto& c : *uc.MutableLabeledCounter()) {
                    ui32 nameId = c.GetNameId();
                    if (counterNamesMapping[c.GetNameId()].Defined()) {
                        nameId = counterNamesMapping[c.GetNameId()].GetRef();
                        auto* lci = lc.AddLabeledCounter();
                        lci->SetValue(c.GetValue());
                        lci->SetNameId(nameId);
                    }
                }
            }
        }
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const TActorContext &ctx) {
        TStringStream json;
        TProtoToJson::ProtoToJson(json, LabeledCountersResult, JsonSettings);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }
};

template <>
struct TJsonRequestSchema<TJsonLabeledCounters> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<TEvTabletCounters::TEvTabletLabeledCountersResponse::ProtoRecordType>();
    }
};

template <>
struct TJsonRequestParameters<TJsonLabeledCounters> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: group
              in: query
              description: group name
              required: false
              type: string
            - name: dc
              in: query
              description: datacenter name
              required: false
              type: string
              default: "*"
            - name: topic
              in: query
              description: topic name
              required: false
              type: string
              default: "*"
            - name: consumer
              in: query
              description: consumer name
              required: false
              type: string
              default: ""
            - name: group_names
              in: query
              description: group names
              required: false
              type: string
            - name: counters
              in: query
              description: counters names
              required: false
              type: string
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
              default: false
            - name: all
              in: query
              description: return information about all topics and clients
              required: false
              type: boolean
              default: false
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
              default: false
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
struct TJsonRequestSummary<TJsonLabeledCounters> {
    static TString GetSummary() {
        return "Labeled counters info";
    }
};

template <>
struct TJsonRequestDescription<TJsonLabeledCounters> {
    static TString GetDescription() {
        return "Returns information about labeled counters";
    }
};

}
}
