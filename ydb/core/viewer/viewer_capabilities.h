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
