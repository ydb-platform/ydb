#pragma once
#include "json_handlers.h"
#include "viewer.h"
#include "wb_aggregate.h"
#include <ydb/core/base/tablet.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonTabletCounters : public TActorBootstrapped<TJsonTabletCounters> {
    static const bool WithRetry = false;
    using TBase = TActorBootstrapped<TJsonTabletCounters>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TVector<TActorId> PipeClients;
    TVector<ui64> Tablets;
    TMap<TTabletId, THolder<TEvTablet::TEvGetCountersResponse>> Results;
    THolder<NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult> DescribeResult;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    bool Aggregate = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonTabletCounters(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    static NTabletPipe::TClientConfig InitPipeClientConfig() {
        NTabletPipe::TClientConfig clientConfig;
        if (WithRetry) {
            clientConfig.RetryPolicy = NTabletPipe::TClientRetryPolicy::WithRetries();
        }
        return clientConfig;
    }

    static const NTabletPipe::TClientConfig& GetPipeClientConfig() {
        static NTabletPipe::TClientConfig clientConfig = InitPipeClientConfig();
        return clientConfig;
    }

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Aggregate = FromStringWithDefault<bool>(params.Get("aggregate"), true);
        if (params.Has("path")) {
            THolder<TEvTxUserProxy::TEvNavigate> request(new TEvTxUserProxy::TEvNavigate());
            if (!Event->Get()->UserToken.empty()) {
                request->Record.SetUserToken(Event->Get()->UserToken);
            }
            NKikimrSchemeOp::TDescribePath* record = request->Record.MutableDescribePath();
            record->SetPath(params.Get("path"));

            TActorId txproxy = MakeTxProxyID();
            ctx.Send(txproxy, request.Release());
            Become(&TThis::StateRequestedDescribe, ctx, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
        } else if (params.Has("tablet_id")) {
            TTabletId tabletId = FromStringWithDefault<TTabletId>(params.Get("tablet_id"), 0);
            if (tabletId != 0) {
                Tablets.emplace_back(tabletId);
                TActorId PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, GetPipeClientConfig()));
                NTabletPipe::SendData(ctx, PipeClient, new TEvTablet::TEvGetCounters(), tabletId);
                PipeClients.emplace_back(PipeClient);
                Become(&TThis::StateRequestedGetCounters, ctx, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
            }

            if (PipeClients.empty()) {
                ReplyAndDie(ctx);
            }
        }
    }

    void Die(const TActorContext& ctx) override {
        for (const TActorId& pipeClient : PipeClients) {
            NTabletPipe::CloseClient(ctx, pipeClient);
        }
        TBase::Die(ctx);
    }

    STFUNC(StateRequestedDescribe) {
        switch (ev->GetTypeRewrite()) {
            HFunc(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    STFUNC(StateRequestedGetCounters) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTablet::TEvGetCountersResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NSchemeShard::TEvSchemeShard::TEvDescribeSchemeResult::TPtr &ev, const TActorContext &ctx) {
        DescribeResult = ev->Release();
        if (DescribeResult->GetRecord().GetStatus() == NKikimrScheme::EStatus::StatusSuccess) {
            Tablets.reserve(DescribeResult->GetRecord().GetPathDescription().TablePartitionsSize());
            for (const auto& partition : DescribeResult->GetRecord().GetPathDescription().GetTablePartitions()) {
                Tablets.emplace_back(partition.GetDatashardId());
            }
            Tablets.reserve(DescribeResult->GetRecord().GetPathDescription().GetPersQueueGroup().PartitionsSize());
            for (const auto& partition : DescribeResult->GetRecord().GetPathDescription().GetPersQueueGroup().GetPartitions()) {
                Tablets.emplace_back(partition.GetTabletId());
            }
            Sort(Tablets);
            Tablets.erase(std::unique(Tablets.begin(), Tablets.end()), Tablets.end());
        }
        for (auto tabletId : Tablets) {
            TActorId PipeClient = ctx.RegisterWithSameMailbox(NTabletPipe::CreateClient(ctx.SelfID, tabletId, GetPipeClientConfig()));
            NTabletPipe::SendData(ctx, PipeClient, new TEvTablet::TEvGetCounters(), tabletId);
            PipeClients.emplace_back(PipeClient);
        }
        if (Tablets.empty()) {
            ReplyAndDie(ctx);
        }
        Become(&TThis::StateRequestedGetCounters);
    }

    void Handle(TEvTablet::TEvGetCountersResponse::TPtr &ev, const TActorContext &ctx) {
        Results.emplace(ev->Cookie, ev->Release());
        if (Results.size() == Tablets.size()) {
            ReplyAndDie(ctx);
        }
    }

    void ReplyAndDie(const TActorContext &ctx) {
        TStringStream json;
        if (!Results.empty()) {
            if (Aggregate) {
                THolder<TEvTablet::TEvGetCountersResponse> response = AggregateWhiteboardResponses(Results);
                TProtoToJson::ProtoToJson(json, response->Record, JsonSettings);
            } else {
                json << '{';
                for (auto it = Results.begin(); it != Results.end(); ++it) {
                    if (it != Results.begin()) {
                        json << ',';
                    }
                    json << '"' << it->first << "\":";
                    TProtoToJson::ProtoToJson(json, it->second->Record, JsonSettings);
                }
                json << '}';
            }
        } else {
            json << "null";
        }
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
            .Summary = "Tablet counters info",
            .Description = "Returns information about tablet counters",
        });
        yaml.AddParameter({
            .Name = "path",
            .Description = "schema path",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "tablet_id",
            .Description = "tablet identifier",
            .Type = "integer",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "aggregate",
            .Description = "aggregate tablet counters",
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
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TEvTablet::TEvGetCountersResponse::ProtoRecordType>());
        return yaml;
    }
};

}
