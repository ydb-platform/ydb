#pragma once
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/mon.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include "viewer.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;

class TJsonWhoAmI : public TActorBootstrapped<TJsonWhoAmI> {
    IViewer* Viewer;
    TJsonSettings JsonSettings;
    NMon::TEvHttpInfo::TPtr Event;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonWhoAmI(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap(const TActorContext& ctx) {
        const auto& params(Event->Get()->Request.GetParams());
        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        ReplyAndDie(ctx);
    }

    void ReplyAndDie(const  TActorContext &ctx) {
        NACLibProto::TUserToken userToken;
        Y_PROTOBUF_SUPPRESS_NODISCARD userToken.ParseFromString(Event->Get()->UserToken);
        TStringStream json;
        TProtoToJson::ProtoToJson(json, userToken, JsonSettings);
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get()) + json.Str(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }
};

template <>
struct TJsonRequestSchema<TJsonWhoAmI> {
    static TString GetSchema() {
        TStringStream stream;
        TProtoToJson::ProtoToJsonSchema<NACLibProto::TUserToken>(stream);
        return stream.Str();
    }
};

template <>
struct TJsonRequestParameters<TJsonWhoAmI> {
    static TString GetParameters() {
        return R"___([{"name":"enums","in":"query","description":"convert enums to strings","required":false,"type":"boolean"},
                      {"name":"ui64","in":"query","description":"return ui64 as numbers","required":false,"type":"boolean"}])___";
    }
};

template <>
struct TJsonRequestSummary<TJsonWhoAmI> {
    static TString GetSummary() {
        return "\"Information about current user\"";
    }
};

template <>
struct TJsonRequestDescription<TJsonWhoAmI> {
    static TString GetDescription() {
        return "\"Returns information about user token\"";
    }
};

}
}
