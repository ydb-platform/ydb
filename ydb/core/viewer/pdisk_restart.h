#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <library/cpp/json/json_writer.h>
#include <ydb/core/viewer/yaml/yaml.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TJsonPDiskRestart : public TViewerPipeClient {
protected:
    using TThis = TJsonPDiskRestart;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

    TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> Response;
    bool Force = false;

public:
    TJsonPDiskRestart(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TViewerPipeClient(viewer, ev)
    {}

    void Bootstrap() override {
        if (!PostData.IsDefined()) {
            return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Only POST method is allowed"));
        }
        if (!Viewer->CheckAccessMonitoring(GetRequest())) {
            return TBase::ReplyAndPassAway(GETHTTPACCESSDENIED("text/plain", "Access denied"));
        }
        ui32 nodeId = 0;
        ui32 pDiskId = 0;
        TVector<TString> parts = StringSplitter(Params.Get("pdisk_id")).Split('-').SkipEmpty();
        if (parts.size() > 2) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Unable to parse the 'pdisk_id' parameter"));
        }
        if (parts.size() == 2) {
            nodeId = FromStringWithDefault<ui32>(parts[0]);
            pDiskId = FromStringWithDefault<ui32>(parts[1]);
        } else {
            pDiskId = FromStringWithDefault<ui32>(parts[0]);
            nodeId = FromStringWithDefault<ui32>(Params.Get("node_id"));
        }
        if (nodeId == 0 || pDiskId == 0) {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Unable to parse the 'pdisk_id' parameter"));
        }
        Force = FromStringWithDefault<bool>(Params.Get("force"), Force);
        Response = RequestBSControllerPDiskRestart(nodeId, pDiskId, Force);

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
        if (Response.IsOk()) {
            NJson::TJsonValue json;
            if (Response->Record.GetResponse().GetSuccess()) {
                json["result"] = true;
            } else {
                json["result"] = false;
                Viewer->BSCError2JSON(Response->Record.GetResponse(), GetRequest(), json, Force);
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
                    required: false
                  - name: pdisk_id
                    in: query
                    description: pdisk identifier in format 'node_id-pdisk_id' or 'pdisk_id' if node_id is also specified
                    required: true
                    type: string
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
