#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonHiveStats : public TViewerPipeClient {
    using TThis = TJsonHiveStats;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvHive::TEvResponseHiveDomainStats> HiveStats;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

public:
    TJsonHiveStats(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        ui64 hiveId = FromStringWithDefault<ui64>(params.Get("hive_id"), 0);
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        InitConfig(params);
        if (hiveId != 0 ) {
            THolder<TEvHive::TEvRequestHiveDomainStats> request = MakeHolder<TEvHive::TEvRequestHiveDomainStats>();
            request->Record.SetReturnFollowers(FromStringWithDefault(params.Get("followers"), false));
            request->Record.SetReturnMetrics(FromStringWithDefault(params.Get("metrics"), true));
            SendRequestToPipe(ConnectTabletPipe(hiveId), request.Release());
            Become(&TThis::StateRequestedInfo, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
        } else {
            ReplyAndPassAway();
        }
    }

    STATEFN(StateRequestedInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHive::TEvResponseHiveDomainStats, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvHive::TEvResponseHiveDomainStats::TPtr& ev) {
        HiveStats = ev->Release();
        RequestDone();
    }

    void ReplyAndPassAway() override {
        TStringStream json;
        if (HiveStats != nullptr) {
            TProtoToJson::ProtoToJson(json, HiveStats->Record, JsonSettings);
        } else {
            json << "null";
        }
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Hive statistics",
            .Description = "Returns information about Hive statistics",
        });
        yaml.AddParameter({
            .Name = "hive_id",
            .Description = "hive identifier (tablet id)",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "followers",
            .Description = "return followers",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "metrics",
            .Description = "return tablet metrics",
            .Type = "boolean",
        });
        yaml.AddParameter({
            .Name = "enums",
            .Description = "convert enums to strings",
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
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TEvHive::TEvResponseHiveDomainStats::ProtoRecordType>());
        return yaml;
    }
};

}
