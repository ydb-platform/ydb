#pragma once
#include "json_pipe_req.h"
#include <ydb/core/protos/feature_flags.pb.h>

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
        if (TBase::NeedToRedirect(false/* don't check auth for capabilities on purpose */)) {
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
<<<<<<< HEAD
=======

        NJson::TJsonValue& features = json["Features"];
        NKikimrConfig::TFeatureFlags featureFlagsProto = AppData()->FeatureFlags;
        const auto* descriptor = featureFlagsProto.GetDescriptor();
        const auto* reflection = featureFlagsProto.GetReflection();
        for (int i = 0; i < descriptor->field_count(); ++i) {
            const auto* field = descriptor->field(i);
            if (field->type() == google::protobuf::FieldDescriptor::TYPE_BOOL && reflection->GetBool(featureFlagsProto, field)) {
                features[field->name()] = true;
            }
        }

>>>>>>> d42b2839e80 (return all enabled feature flags in capabilities response (#35770))
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
