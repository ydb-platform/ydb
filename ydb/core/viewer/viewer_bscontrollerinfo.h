#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "viewer.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonBSControllerInfo : public TViewerPipeClient {
    using TThis = TJsonBSControllerInfo;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;
    TAutoPtr<TEvBlobStorage::TEvResponseControllerInfo> ControllerInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

public:
    TJsonBSControllerInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        InitConfig(params);
        RequestBSControllerInfo();
        Become(&TThis::StateRequestedInfo, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvResponseControllerInfo, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, TBase::HandleTimeout);
        }
    }

    void Handle(TEvBlobStorage::TEvResponseControllerInfo::TPtr& ev) {
        ControllerInfo = ev->Release();
        RequestDone();
    }

    void ReplyAndPassAway() override {
        TStringStream json;
        if (ControllerInfo != nullptr) {
            TProtoToJson::ProtoToJson(json, ControllerInfo->Record);
        } else {
            json << "null";
        }
        ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
    }

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Storage controller information",
            .Description = "Returns information about storage controller"
        });
        yaml.AddParameter({
            .Name = "controller_id",
            .Description = "storage controller identifier (tablet id)",
            .Type = "string",
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
        yaml.SetResponseSchema(TProtoToYaml::ProtoToYamlSchema<TEvBlobStorage::TEvResponseControllerInfo::ProtoRecordType>());
        return yaml;
    }
};

}
