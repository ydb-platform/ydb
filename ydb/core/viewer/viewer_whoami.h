#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/core/base/appdata_fwd.h>
#include <ydb/core/base/auth.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/json/json_writer.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonWhoAmI : public TViewerPipeClient {
    using TBase = TViewerPipeClient;
public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonWhoAmI(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() {
        if (NeedToRedirect()) {
            return;
        }
        ReplyAndPassAway();
    }

    void ReplyAndPassAway() {
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
        if (userToken.HasAuthType()) {
            json["AuthType"] = userToken.GetAuthType();
        }

        NACLib::TUserToken token(std::move(userToken));
        json["IsTokenRequired"] = AppData()->EnforceUserTokenRequirement;
        bool isAdministrationAllowed = IsTokenAllowed(&token, AppData()->DomainsConfig.GetSecurityConfig().GetAdministrationAllowedSIDs());
        bool isMonitoringAllowed = isAdministrationAllowed || IsTokenAllowed(&token, AppData()->DomainsConfig.GetSecurityConfig().GetMonitoringAllowedSIDs());
        bool isViewerAllowed = isMonitoringAllowed || IsTokenAllowed(&token, AppData()->DomainsConfig.GetSecurityConfig().GetViewerAllowedSIDs());
        bool isDatabaseAllowed = isViewerAllowed || IsTokenAllowed(&token, AppData()->DomainsConfig.GetSecurityConfig().GetDatabaseAllowedSIDs());
        json["IsAdministrationAllowed"] = isAdministrationAllowed;
        json["IsMonitoringAllowed"] = isMonitoringAllowed;
        json["IsViewerAllowed"] = isViewerAllowed;
        json["IsDatabaseAllowed"] = isDatabaseAllowed;
        TBase::ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    static YAML::Node GetSwaggerSchema() {
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

    static YAML::Node GetSwagger() {
        TSimpleYamlBuilder yaml({
            .Method = "get",
            .Tag = "viewer",
            .Summary = "Information about current user",
            .Description = "Returns information about user token",
        });
        yaml.SetResponseSchema(GetSwaggerSchema());
        return yaml;
    }
};

}
