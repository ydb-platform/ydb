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

class TJsonHiveStats : public TViewerPipeClient<TJsonHiveStats> {
    using TBase = TViewerPipeClient<TJsonHiveStats>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TAutoPtr<TEvHive::TEvResponseHiveDomainStats> HiveStats;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonHiveStats(IViewer* viewer, NMon::TEvHttpInfo::TPtr &ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
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

    void ReplyAndPassAway() {
        TStringStream json;
        if (HiveStats != nullptr) {
            TProtoToJson::ProtoToJson(json, HiveStats->Record, JsonSettings);
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
struct TJsonRequestSchema<TJsonHiveStats> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<TEvHive::TEvResponseHiveDomainStats::ProtoRecordType>();
    }
};

template <>
struct TJsonRequestParameters<TJsonHiveStats> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: hive_id
              in: query
              description: hive identifier (tablet id)
              required: true
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
struct TJsonRequestSummary<TJsonHiveStats> {
    static TString GetSummary() {
        return "Hive statistics";
    }
};

template <>
struct TJsonRequestDescription<TJsonHiveStats> {
    static TString GetDescription() {
        return "Returns information about Hive statistics";
    }
};

}
}
