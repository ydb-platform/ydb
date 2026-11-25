#pragma once
#include "json_pipe_req.h"
#include "viewer_bridge_storage.h"
#include <ydb/core/protos/feature_flags.pb.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TViewerCapabilities : public TViewerPipeClient {
    std::optional<bool> DatabaseBridgeMode;

public:
    using TThis = TViewerCapabilities;
    using TBase = TViewerPipeClient;

    TViewerCapabilities(IViewer* viewer, NHttp::TEvHttpProxy::TEvHttpIncomingRequest::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        if (TBase::NeedToRedirect(false/* don't check auth for capabilities on purpose */)) {
            return;
        }
        if (DatabaseNavigateResponse && DatabaseNavigateResponse->IsOk()) {
            const auto& navigate = *DatabaseNavigateResponse->Get()->Request;
            const auto& entry = navigate.ResultSet.front();
            if (!navigate.UserToken || !entry.SecurityObject || !entry.SecurityObject->CheckAccess(NACLib::DescribeSchema, *navigate.UserToken)) {
                return ReplyAndPassAway();
            }
            if (!AppData()->BridgeModeEnabled) {
                DatabaseBridgeMode = false;
                return ReplyAndPassAway();
            }
            const auto* resource = ResourceNavigateResponse && ResourceNavigateResponse->IsOk()
                ? &ResourceNavigateResponse->Get()->Request->ResultSet.front() : nullptr;
            auto poolNames = GetBridgeDatabaseStoragePools(entry, resource);
            if (poolNames && !poolNames->empty()) {
                Send(MakeViewerID(SelfId().NodeId()), new TEvViewer::TEvGetDatabaseBridgeMode(std::move(*poolNames)));
                Become(&TThis::StateWaitPools, Min(Timeout, TEvViewer::TEvGetDatabaseBridgeMode::MaxWait), new TEvents::TEvWakeup());
                return;
            }
        }
        ReplyAndPassAway();
    }

    void HandleMode(TEvViewer::TEvDatabaseBridgeMode::TPtr& ev) {
        DatabaseBridgeMode = ev->Get()->BridgeMode;
        ReplyAndPassAway();
    }

    STATEFN(StateWaitPools) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvViewer::TEvDatabaseBridgeMode, HandleMode);
            cFunc(TEvents::TEvWakeup::EventType, ReplyAndPassAway);
            default:
                return TBase::StateWork(ev);
        }
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
                    if (DatabaseBridgeMode) {
                        database["BridgeModeEnabled"] = *DatabaseBridgeMode;
                    }
                }
            }
        }

        if (AppData()->BridgeModeEnabled) {
            json["Cluster"]["BridgeModeEnabled"] = true;
        }

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
