#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/client/server/msgbus_server_persqueue.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;


class TJsonPQConsumerInfo : public TActorBootstrapped<TJsonPQConsumerInfo> {
    using TBase = TActorBootstrapped<TJsonPQConsumerInfo>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    NKikimrClient::TResponse Result;
    TJsonSettings JsonSettings;
    TString Topic;
    TString Client;
    TString DC;
    ui32 Version = 0;
    ui32 Timeout = 0;
    ui32 Requests = 0;
    ui32 Responses = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonPQConsumerInfo(
        IViewer* viewer,
        NMon::TEvHttpInfo::TPtr& ev
    )
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Topic = params.Get("topic");
        Version = FromStringWithDefault<ui32>(params.Get("version"), 0);
        DC = params.Get("dc");
        //TODO: make here list of topics
        Client = params.Get("client");
        if (Version >= 3) {
            Topic = "rt3." + DC + "--" + NPersQueue::ConvertNewTopicName(Topic);
            Client = NPersQueue::ConvertNewConsumerName(Client, ctx);
        } else {
            size_t pos = Topic.rfind('/');
            if (pos != TString::npos) {
                Topic = Topic.substr(pos + 1);
            }
        }
        {
            NKikimrClient::TPersQueueRequest request;
            request.MutableMetaRequest()->MutableCmdGetPartitionStatus()->SetClientId(Client);
            request.MutableMetaRequest()->MutableCmdGetPartitionStatus()->AddTopicRequest()->SetTopic(Topic);
            ctx.Register(NMsgBusProxy::CreateActorServerPersQueue(
                ctx.SelfID,
                request,
                NMsgBusProxy::CreatePersQueueMetaCacheV2Id()
            ));
            ++Requests;
        }
        {
            NKikimrClient::TPersQueueRequest request;
            request.MutableMetaRequest()->MutableCmdGetReadSessionsInfo()->SetClientId(Client);
            request.MutableMetaRequest()->MutableCmdGetReadSessionsInfo()->AddTopic(Topic);
            ctx.Register(NMsgBusProxy::CreateActorServerPersQueue(
                ctx.SelfID,
                request,
                NMsgBusProxy::CreatePersQueueMetaCacheV2Id()
            ));
            ++Requests;
        }
        Become(&TThis::StateRequestedTopicInfo, ctx, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    void Die(const TActorContext& ctx) override {
        TBase::Die(ctx);
    }

    STFUNC(StateRequestedTopicInfo) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPersQueue::TEvResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvPersQueue::TEvResponse::TPtr &ev, const TActorContext &ctx) {
        Result.MergeFrom(ev->Get()->Record);
        if (++Responses == Requests) {
            ReplyAndDie(ctx);
        }
    }

    void ReplyAndDie(const TActorContext &ctx) {
        TStringStream json;
        TProtoToJson::ProtoToJson(json, Result.GetMetaResponse(), JsonSettings);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }
};

template <>
struct TJsonRequestSchema<TJsonPQConsumerInfo> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<NKikimrClient::TPersQueueMetaResponse>();
    }
};

template <>
struct TJsonRequestParameters<TJsonPQConsumerInfo> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: topic
              in: query
              description: topic name
              required: true
              type: string
            - name: dc
              in: query
              description: dc name (required with version >= 3)
              required: false
              type: string
              default: ""
            - name: version
              in: query
              description: query version
              required: false
              type: integer
              default: 0
            - name: client
              in: query
              description: client name
              required: true
              type: string
            - name: enums
              in: query
              description: convert enums to strings
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
struct TJsonRequestSummary<TJsonPQConsumerInfo> {
    static TString GetSummary() {
        return "Consumer-topic metrics";
    }
};

template <>
struct TJsonRequestDescription<TJsonPQConsumerInfo> {
    static TString GetDescription() {
        return "Returns consumer-topic metrics";
    }
};

}
}
