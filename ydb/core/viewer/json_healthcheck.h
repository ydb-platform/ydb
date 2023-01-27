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
    NMon::TEvHttpInfo::TPtr Event;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonHealthCheck(IViewer*, NMon::TEvHttpInfo::TPtr& ev)
//        : Viewer(viewer)
        : Event(ev)
    {}

    void Bootstrap() {
        auto queryString = Event->Get()->Request.GetParams().Print();
        Send(Event->Sender, new NMon::TEvHttpInfoRes("HTTP/1.1 302 Found\r\nLocation: /healthcheck?" + queryString + "\r\n\r\n", 0, NMon::IEvHttpInfoRes::EContentType::Custom));
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
