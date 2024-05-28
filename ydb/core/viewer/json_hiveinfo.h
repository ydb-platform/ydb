#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include "viewer.h"
#include "json_pipe_req.h"
#include <ydb/core/viewer/json/json.h>

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonHiveInfo : public TViewerPipeClient<TJsonHiveInfo> {
    using TBase = TViewerPipeClient<TJsonHiveInfo>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvHive::TEvResponseHiveInfo> HiveInfo;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;
    TNodeId NodeId = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonHiveInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
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

    void ReplyAndPassAway() {
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
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonHiveInfo> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<TEvHive::TEvResponseHiveInfo::ProtoRecordType>();
    }
};

template <>
struct TJsonRequestParameters<TJsonHiveInfo> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: hive_id
              in: query
              description: hive identifier (tablet id)
              required: true
              type: string
            - name: tablet_id
              in: query
              description: tablet id filter
              required: false
              type: string
            - name: tablet_type
              in: query
              description: tablet type filter
              required: false
              type: string
            - name: followers
              in: query
              description: return followers
              required: false
              type: boolean
            - name: metrics
              in: query
              description: return tablet metrics
              required: false
              type: boolean
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
struct TJsonRequestSummary<TJsonHiveInfo> {
    static TString GetSummary() {
        return "Hive information";
    }
};

template <>
struct TJsonRequestDescription<TJsonHiveInfo> {
    static TString GetDescription() {
        return "Returns information about tablets from Hive";
    }
};

}
}
