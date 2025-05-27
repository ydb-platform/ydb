#pragma once
#include "json_handlers.h"
#include "json_pipe_req.h"
#include "viewer.h"
#include <library/cpp/json/json_writer.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonVDiskEvict : public TViewerPipeClient {
    enum EEv {
        EvRetryNodeRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE)");

    struct TEvRetryNodeRequest : NActors::TEventLocal<TEvRetryNodeRequest, EvRetryNodeRequest> {
        TEvRetryNodeRequest()
        {}
    };

protected:
    using TThis = TJsonVDiskEvict;
    using TBase = TViewerPipeClient;

    TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> Response;


public:
    TJsonVDiskEvict(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    inline ui32 GetRequiredParam(const TCgiParameters& params, const std::string& name, ui32& obj) {
        if (!TryFromString<ui32>(params.Get(name), obj)) {
            TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", TStringBuilder() << "field '" << name << "' is required"));
            return false;
        }
        return true;
    }

    void Bootstrap() override {
        ui32 groupId = 0;
        ui32 groupGeneration = 0;
        ui32 failRealmIdx = 0;
        ui32 failDomainIdx = 0;
        ui32 vDiskIdx = 0;
        bool force = false;
        TString vdisk_id = Params.Get("vdisk_id");
        if (vdisk_id) {
            TVector<TString> parts = StringSplitter(vdisk_id).Split('-').SkipEmpty();
            if (parts.size() == 5) {
                groupId = FromStringWithDefault<ui32>(parts[0], Max<ui32>());
                groupGeneration = FromStringWithDefault<ui32>(parts[1], Max<ui32>());
                failRealmIdx = FromStringWithDefault<ui32>(parts[2], Max<ui32>());
                failDomainIdx = FromStringWithDefault<ui32>(parts[3], Max<ui32>());
                vDiskIdx = FromStringWithDefault<ui32>(parts[4], Max<ui32>());
            }
            if (parts.size() != 5 || groupId == Max<ui32>()
                    || groupGeneration == Max<ui32>() || failRealmIdx  == Max<ui32>()
                    || failDomainIdx == Max<ui32>() || vDiskIdx == Max<ui32>()) {
                return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Unable to parse the 'vdisk_id' parameter"), "BadRequest");
            }
        } else if (!GetRequiredParam(Params, "group_id", groupId)
                || !GetRequiredParam(Params, "group_generation_id", groupGeneration)
                || !GetRequiredParam(Params, "fail_realm_idx", failRealmIdx)
                || !GetRequiredParam(Params, "fail_domain_idx", failDomainIdx)
                || !GetRequiredParam(Params, "vdisk_idx", vDiskIdx)) {
            return;
            //return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Parameter 'vdisk_id' is required"), "BadRequest");
        }

        if (!PostData.IsDefined()) {
            return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Only POST method is allowed"), "BadRequest");
        }

        force = FromStringWithDefault<bool>(Params.Get("force"), false);
        if (force && !Viewer->CheckAccessAdministration(Event->Get())) {
            return TBase::ReplyAndPassAway(GetHTTPFORBIDDEN(), "BadRequest");
        }

        Response = RequestBSControllerVDiskEvict(groupId, groupGeneration, failRealmIdx, failDomainIdx, vDiskIdx, force);
        TBase::Become(&TThis::StateWork, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        if (Response.Set(std::move(ev))) {
            RequestDone();
        }
    }
    void ReplyAndPassAway() override {
        NJson::TJsonValue json;
        if (Response.IsOk()) {
            if (Response->Record.GetResponse().GetSuccess()) {
                json["result"] = true;
            } else {
                json["result"] = false;
                TString error;
                bool forceRetryPossible = false;
                Viewer->TranslateFromBSC2Human(Response->Record.GetResponse(), error, forceRetryPossible);
                json["error"] = error;
                if (forceRetryPossible && Viewer->CheckAccessAdministration(Event->Get())) {
                    json["forceRetryPossible"] = true;
                }
            }
            json["debugMessage"] = Response->Record.ShortDebugString();
        } else {
            json["result"] = false;
            json["error"] = "No response was received from BSC";
        }
        TBase::ReplyAndPassAway(GetHTTPOKJSON(json));
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
        post:
            tags:
              - vdisk
            summary: VDisk evict
            description: VDisk evict
            parameters:
              - name: vdisk_id
                in: query
                description: vdisk identifier
                required: true
                type: string
              - name: force
                in: query
                description: attempt forced operation, ignore warnings, for admin only and only if previous call returned forceRetryPossible = true
                required: false
                type: boolean
              - name: timeout
                in: query
                description: timeout in ms
                required: false
                type: integer
            responses:
                200:
                    description: OK
                    content:
                        application/json:
                            schema:
                                type: object
                                properties:
                                    result:
                                        type: boolean
                                        description: was operation successful or not
                                    error:
                                        type: string
                                        description: details about failed operation
                                    forceRetryPossible:
                                        type: boolean
                                        description: if true, operation can be retried with force flag
                400:
                    description: Bad Request
                403:
                    description: Forbidden
                504:
                    description: Gateway Timeout
            )___");
    }
};

}
