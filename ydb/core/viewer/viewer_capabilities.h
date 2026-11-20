#pragma once
#include "json_pipe_req.h"
#include "viewer_bridge_storage.h"
#include <ydb/core/protos/feature_flags.pb.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TViewerCapabilities : public TViewerPipeClient {
    std::optional<THashSet<TString>> DatabasePoolNames;
    TRequestResponse<NSysView::TEvSysView::TEvGetStoragePoolsResponse> PoolsResponse;

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
        if (AppData()->BridgeModeEnabled && DatabaseNavigateResponse && DatabaseNavigateResponse->IsOk()) {
            const auto& entry = DatabaseNavigateResponse->Get()->Request->ResultSet.front();
            const auto* resource = ResourceNavigateResponse && ResourceNavigateResponse->IsOk()
                ? &ResourceNavigateResponse->Get()->Request->ResultSet.front() : nullptr;
            DatabasePoolNames = GetBridgeDatabaseStoragePools(entry, resource);
            if (DatabasePoolNames && !DatabasePoolNames->empty()) {
                UseCache = FromStringWithDefault<bool>(Params.Get("use_cache"), true);
                PoolsResponse = MakeCachedRequestBSControllerPools();
                if (!PoolsResponse.IsDone()) {
                    // Bound the extra wait on a cold cache: the remaining capabilities are already available.
                    Become(&TThis::StateWaitPools, Min(Timeout, TDuration::MilliSeconds(250)), new TEvents::TEvWakeup());
                    return;
                }
            }
        }
        ReplyAndPassAway();
    }

    void HandlePools(NSysView::TEvSysView::TEvGetStoragePoolsResponse::TPtr& ev) {
        if (PoolsResponse.Set(std::move(ev))) {
            RequestDone();
        }
    }

    STATEFN(StateWaitPools) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSysView::TEvSysView::TEvGetStoragePoolsResponse, HandlePools);
            cFunc(TEvents::TEvWakeup::EventType, ReplyAndPassAway);
            default:
                return TBase::StateWork(ev);
        }
    }

    std::optional<bool> GetDatabaseBridgeMode() const {
        if (!AppData()->BridgeModeEnabled) {
            return false;
        }
        if (!PoolsResponse.IsOk()) {
            return std::nullopt;
        }
        const auto pools = FindBridgeDatabasePoolEntries(*DatabasePoolNames, PoolsResponse->Record);
        if (!pools) {
            return std::nullopt;
        }
        std::optional<bool> mode;
        for (const auto& [_, pool] : *pools) {
            const auto& info = pool->GetInfo();
            if (!info.HasBridgeMode() || (mode && *mode != info.GetBridgeMode())) {
                return std::nullopt;
            }
            mode = info.GetBridgeMode();
        }
        return mode;
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
                    if (const auto mode = GetDatabaseBridgeMode()) {
                        database["BridgeModeEnabled"] = *mode;
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
