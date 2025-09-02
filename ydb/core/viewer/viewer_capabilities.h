#pragma once
#include "json_pipe_req.h"

namespace NKikimr::NViewer {

using namespace NActors;

class TViewerCapabilities : public TViewerPipeClient {
public:
    using TThis = TViewerCapabilities;
    using TBase = TViewerPipeClient;

    TViewerCapabilities(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        if (TBase::NeedToRedirect()) {
            return;
        }
        ReplyAndPassAway();
    }

    NJson::TJsonValue GetSettings() {
        NJson::TJsonValue json;

        NJson::TJsonValue& security(json["Security"]);
        security["IsTokenRequired"] = AppData()->EnforceUserTokenRequirement;
        security["UseLoginProvider"] = AppData()->AuthConfig.GetUseLoginProvider();
        security["DomainLoginOnly"] = AppData()->AuthConfig.GetDomainLoginOnly();

        if (DatabaseNavigateResponse && DatabaseNavigateResponse->IsOk()) {
            if (DatabaseNavigateResponse->Get()->Request && !DatabaseNavigateResponse->Get()->Request->ResultSet.empty()) {
                NJson::TJsonValue& database(json["Database"]);
                TSchemeCacheNavigate::TEntry& entry = DatabaseNavigateResponse->Get()->Request->ResultSet.front();
                if (entry.DomainInfo) {
                    database["GraphShardExists"] = entry.DomainInfo->Params.GetGraphShard() != 0;
                }
            }
        }

        if (AppData()->BridgeModeEnabled) {
            json["Cluster"]["BridgeModeEnabled"] = true;
        }
        return json;
    }

    void ReplyAndPassAway() override {
        NJson::TJsonValue json;
        json["Capabilities"] = Viewer->GetCapabilities();
        json["Settings"] = GetSettings();
        TBase::ReplyAndPassAway(GetHTTPOKJSON(json));
    }
};

}
