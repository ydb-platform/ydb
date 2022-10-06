#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/interconnect.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/blobstorage/base/blobstorage_events.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/services.pb.h>
#include "viewer.h"
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/health_check/health_check.h>
#include <ydb/core/util/proto_duration.h>

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonHealthCheck : public TActorBootstrapped<TJsonHealthCheck> {
    static const bool WithRetry = false;
    using TBase = TActorBootstrapped<TJsonHealthCheck>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    ui32 Timeout = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonHealthCheck(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), true);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        THolder<NHealthCheck::TEvSelfCheckRequest> request = MakeHolder<NHealthCheck::TEvSelfCheckRequest>();
        request->Database = params.Get("tenant");
        request->Request.set_return_verbose_status(FromStringWithDefault<bool>(params.Get("verbose"), false));
        request->Request.set_maximum_level(FromStringWithDefault<ui32>(params.Get("max_level"), 0));
        SetDuration(TDuration::MilliSeconds(Timeout), *request->Request.mutable_operation_params()->mutable_operation_timeout());
        if (params.Has("min_status")) {
            Ydb::Monitoring::StatusFlag::Status minStatus;
            if (Ydb::Monitoring::StatusFlag_Status_Parse(params.Get("min_status"), &minStatus)) {
                request->Request.set_minimum_status(minStatus);
            } else {
                Send(Event->Sender, new NMon::TEvHttpInfoRes(HTTPBADREQUEST, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
                return PassAway();
            }
        }
        Send(NHealthCheck::MakeHealthCheckID(), request.Release());
        Timeout += Timeout * 20 / 100; // we prefer to wait for more (+20%) verbose timeout status from HC
        Become(&TThis::StateRequestedInfo, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateRequestedInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        TStringStream json;
        TProtoToJson::ProtoToJson(json, ev->Get()->Result, JsonSettings);
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
struct TJsonRequestSchema<TJsonHealthCheck> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<Ydb::Monitoring::SelfCheckResult>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonHealthCheck> {
    static TString GetParameters() {
        return R"___([{"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as number","required":false,"type":"boolean"},
                      {"name":"timeout","in":"query","description":"timeout in ms","required":false,"type":"integer"},
                      {"name":"tenant","in":"query","description":"path to database","required":false,"type":"string"},
                      {"name":"verbose","in":"query","description":"return verbose status","required":false,"type":"boolean"},
                      {"name":"max_level","in":"query","description":"max depth of issues to return","required":false,"type":"integer"},
                      {"name":"min_status","in":"query","description":"min status of issues to return","required":false,"type":"string"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonHealthCheck> {
    static TString GetSummary() {
        return "\"Self-check result\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonHealthCheck> {
    static TString GetDescription() {
        return "\"Performs self-check and returns result\"";
    }
};

}
}
