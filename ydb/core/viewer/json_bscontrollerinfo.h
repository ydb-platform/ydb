#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include "viewer.h"
#include "json_pipe_req.h"
#include <ydb/core/viewer/json/json.h>

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonBSControllerInfo : public TViewerPipeClient {
    using TThis = TJsonBSControllerInfo;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvBlobStorage::TEvResponseControllerInfo> ControllerInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

public:
    TJsonBSControllerInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        InitConfig(params);
        RequestBSControllerInfo();
        const auto ctx = TActivationContext::ActorContextFor(SelfId());
        Become(&TThis::StateRequestedInfo, ctx, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvResponseControllerInfo, Handle);
            hFunc(TEvTabletPipe::TEvClientConnected, TBase::Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
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
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonBSControllerInfo> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<TEvBlobStorage::TEvResponseControllerInfo::ProtoRecordType>();
    }
};

template <>
struct TJsonRequestParameters<TJsonBSControllerInfo> {
    static YAML::Node GetParameters() {
        return YAML::Node(R"___(
            - name: controller_id
              in: query
              description: storage controller identifier (tablet id)
              required: true
              type: string
            - name: enums
              in: query
              description: convert enums to strings
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
struct TJsonRequestSummary<TJsonBSControllerInfo> {
    static TString GetSummary() {
        return "Storage controller information";
    }
};

template <>
struct TJsonRequestDescription<TJsonBSControllerInfo> {
    static TString GetDescription() {
        return "Returns information about storage controller";
    }
};

}
}
