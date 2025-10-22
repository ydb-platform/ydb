#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <library/cpp/json/json_writer.h>
#include <ydb/core/viewer/yaml/yaml.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonPDiskRestart : public TViewerPipeClient {
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
    using TThis = TJsonPDiskRestart;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

    TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> Response;

    ui32 NodeId = 0;
    ui32 PDiskId = 0;
    bool Force = false;

public:
    TJsonPDiskRestart(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        ui32 nodeId = FromStringWithDefault<ui32>(Params.Get("node_id"), 0);
        ui32 pDiskId = FromStringWithDefault<ui32>(Params.Get("pdisk_id"), Max<ui32>());
        bool force = FromStringWithDefault<bool>(Params.Get("force"), false);

        if (pDiskId == Max<ui32>()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "field 'pdisk_id' is required"));
        }
        if (!PostData.IsDefined()) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Only POST method is allowed"));
        }
        if (force && !Viewer->CheckAccessAdministration(Event->Get())) {
            return ReplyAndPassAway(GetHTTPFORBIDDEN("text/html", "<html><body><h1>403 Forbidden</h1></body></html>"), "Access denied");
        }

        if (!nodeId) {
            nodeId = TlsActivationContext->ActorSystem()->NodeId;
        }

        Response = RequestBSControllerPDiskRestart(nodeId, pDiskId, force);

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
            ReplyAndPassAway(GetHTTPOKJSON(json));
        } else {
            ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", Response.GetError()));
        }
    }

    static YAML::Node GetSwagger() {
        return YAML::Load(R"___(
            post:
                tags:
                  - pdisk
                summary: Restart PDisk
                description: Restart PDisk on the specified node
                parameters:
                  - name: node_id
                    in: query
                    description: node identifier
                    type: integer
                    required: true
                  - name: pdisk_id
                    in: query
                    description: pdisk identifier
                    required: true
                    type: integer
                  - name: timeout
                    in: query
                    description: timeout in ms
                    required: false
                    type: integer
                  - name: force
                    in: query
                    description: attempt forced operation, ignore warnings, for admin only and only if previous call returned forceRetryPossible = true
                    required: false
                    type: boolean
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
