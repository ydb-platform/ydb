#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "viewer.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonHiveInfo : public TViewerPipeClient {
    using TThis = TJsonHiveInfo;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    TAutoPtr<TEvHive::TEvResponseHiveInfo> HiveInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TNodeId NodeId = 0;

public:
    TJsonHiveInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        ui64 hiveId = FromStringWithDefault<ui64>(params.Get("hive_id"), 0);
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        NodeId = FromStringWithDefault<TNodeId>(params.Get("node"), 0);
        InitConfig(params);
        if (hiveId != 0 ) {
            TAutoPtr<TEvHive::TEvRequestHiveInfo> request = new TEvHive::TEvRequestHiveInfo();
            if (params.Has("tablet_id")) {
                request->Record.SetTabletID(FromStringWithDefault<ui64>(params.Get("tablet_id"), 0));
            }
            if (params.Has("tablet_type")) {
                request->Record.SetTabletType(static_cast<TTabletTypes::EType>(FromStringWithDefault<ui32>(params.Get("tablet_type"), 0)));
            }
            if (FromStringWithDefault(params.Get("followers"), false)) {
                request->Record.SetReturnFollowers(true);
            }
            if (FromStringWithDefault(params.Get("metrics"), false)) {
                request->Record.SetReturnMetrics(true);
            }
            SendRequestToPipe(ConnectTabletPipe(hiveId), request.Release());
            Become(&TThis::StateRequestedInfo, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
        } else {
            ReplyAndPassAway();
        }
    }

    STATEFN(StateRequestedInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvHive::TEvResponseHiveInfo, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvHive::TEvResponseHiveInfo::TPtr& ev) {
        HiveInfo = ev->Release();
        RequestDone();
    }

    void ReplyAndPassAway() override {
        TStringStream json;
        if (HiveInfo != nullptr) {
            if (NodeId != 0) {
                for (auto itRecord = HiveInfo->Record.MutableTablets()->begin(); itRecord != HiveInfo->Record.MutableTablets()->end();) {
                    if (itRecord->GetNodeID() != NodeId) {
                        itRecord = HiveInfo->Record.MutableTablets()->erase(itRecord);
                    } else {
                        ++itRecord;
                    }
                }
            }
            TProtoToJson::ProtoToJson(json, HiveInfo->Record, JsonSettings);
        } else {
            json << "null";
        }
        ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Hive Info",
            .Description = "Returns information about the hive",
        });
        yaml.AddParameter({
            .Name = "hive_id",
            .Description = "hive identifier (tablet id)",
            .Type = "string",
            .Required = true,
        });
        yaml.AddParameter({
            .Name = "tablet_id",
            .Description = "tablet id filter",
            .Type = "string",
        });
        yaml.AddParameter({
            .Name = "tablet_type",
            .Description = "tablet type filter",
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
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<NKikimrHive::TEvResponseHiveInfo>());
        return yaml;
    }
};

}
