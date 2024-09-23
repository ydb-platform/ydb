#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <contrib/libs/yaml-cpp/include/yaml-cpp/yaml.h>
#include <library/cpp/yaml/as/tstring.h>
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
        ReplyAndDie(ctx);
    }

    bool CheckGroupMembership(std::unique_ptr<NACLib::TUserToken>& token, const NProtoBuf::RepeatedPtrField<TString>& sids) {
        if (sids.empty()) {
            return true;
        }
        for (const auto& sid : sids) {
            if (token->IsExist(sid)) {
                return true;
            }
        }
        return false;
    }

    void ReplyAndDie(const  TActorContext &ctx) {
        NACLibProto::TUserToken userToken;
        Y_PROTOBUF_SUPPRESS_NODISCARD userToken.ParseFromString(Event->Get()->UserToken);
        NJson::TJsonValue json(NJson::JSON_MAP);
        if (userToken.HasUserSID()) {
            json["UserSID"] = userToken.GetUserSID();
        }
        if (userToken.HasGroupSIDs() && userToken.GetGroupSIDs().BucketsSize() > 0) {
            NJson::TJsonValue& groupSIDs(json["GroupSIDs"]);
            groupSIDs.SetType(NJson::JSON_ARRAY);
            for (const auto& buckets : userToken.GetGroupSIDs().GetBuckets()) {
                for (const auto& group : buckets.GetValues()) {
                    groupSIDs.AppendValue(group);
                }
            }
        }
        if (userToken.HasOriginalUserToken()) {
            json["OriginalUserToken"] = userToken.GetOriginalUserToken();
        }
        if (userToken.HasAuthType()) {
            json["AuthType"] = userToken.GetAuthType();
        }
        auto token = std::make_unique<NACLib::TUserToken>(userToken);
        json["IsViewerAllowed"] = CheckGroupMembership(token, AppData()->DomainsConfig.GetSecurityConfig().GetViewerAllowedSIDs());
        json["IsMonitoringAllowed"] = CheckGroupMembership(token, AppData()->DomainsConfig.GetSecurityConfig().GetMonitoringAllowedSIDs());
        json["IsAdministrationAllowed"] = CheckGroupMembership(token, AppData()->DomainsConfig.GetSecurityConfig().GetAdministrationAllowedSIDs());
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), NJson::WriteJson(json, false)), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }

    void HandleTimeout(const TActorContext &ctx) {
        ctx.Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        Die(ctx);
    }
};

template <>
struct TJsonRequestSchema<TJsonWhoAmI> {
    static YAML::Node GetSchema() {
        return YAML::Load(R"___(
            type: object
            title: WhoAmI
            properties:
                UserSID:
                    type: string
                    description: User ID / name
                GroupSID:
                    type: array
                    items:
                        type: string
                    description: User groups
                OriginalUserToken:
                    type: string
                    description: User's token used to authenticate
                AuthType:
                    type: string
                    description: Authentication type
                IsViewerAllowed:
                    type: boolean
                    description: Is user allowed to view data
                IsMonitoringAllowed:
                    type: boolean
                    description: Is user allowed to view deeper and make simple changes
                IsAdministrationAllowed:
                    type: boolean
                    description: Is user allowed to do unrestricted changes in the system
            )___");
    }
};

template <>
struct TJsonRequestParameters<TJsonWhoAmI> {
    static YAML::Node GetParameters() {
        return {};
    }
};

template <>
struct TJsonRequestSummary<TJsonWhoAmI> {
    static TString GetSummary() {
        return "Information about current user";
    }
};

template <>
struct TJsonRequestDescription<TJsonWhoAmI> {
    static TString GetDescription() {
        return "Returns information about user token";
    }
};

}
}
