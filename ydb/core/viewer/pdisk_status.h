#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <library/cpp/json/json_writer.h>
#include <library/cpp/json/json_reader.h>
#include <ydb/core/viewer/yaml/yaml.h>

namespace NKikimr::NViewer {

using namespace NActors;
extern bool IsPostContent(const NMon::TEvHttpInfo::TPtr& event);

class TPDiskStatus : public TViewerPipeClient {
protected:
    using TThis = TPDiskStatus;
    using TBase = TViewerPipeClient;
    using TBase::ReplyAndPassAway;

    TRequestResponse<TEvBlobStorage::TEvControllerConfigResponse> Response;

    NKikimrBlobStorage::TUpdateDriveStatus DriveStatus;
    bool Force = false;

public:
    TPDiskStatus(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
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
        DriveStatus.MutableHostKey()->SetNodeId(nodeId);
        DriveStatus.SetPDiskId(pDiskId);
        Force = FromStringWithDefault<bool>(Params.Get("force"), Force);
        if (PostData.IsMap()) {
            if (PostData.Has("decommit_status")) {
                NKikimrBlobStorage::EDecommitStatus decommitStatus = NKikimrBlobStorage::EDecommitStatus::DECOMMIT_UNSET;
                if (EDecommitStatus_Parse(PostData["decommit_status"].GetStringRobust(), &decommitStatus)) {
                    DriveStatus.SetDecommitStatus(decommitStatus);
                } else {
                    return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Invalid 'decommit_status' received"));
                }
            }
            if (PostData.Has("status")) {
                NKikimrBlobStorage::EDriveStatus status = NKikimrBlobStorage::EDriveStatus::UNKNOWN;
                if (EDriveStatus_Parse(PostData["status"].GetStringRobust(), &status)) {
                    DriveStatus.SetStatus(status);
                } else {
                    return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Invalid 'status' received"));
                }
            }
        } else {
            return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Invalid json received"));
        }

        Response = RequestBSControllerPDiskUpdateStatus(DriveStatus, Force);

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
                Viewer->BSCError2JSON(Response->Record.GetResponse(), GetRequest(), json, Force);
            }
            json["debugMessage"] = Response->Record.ShortDebugString();
            ReplyAndPassAway(GetHTTPOKJSON(json));
        } else {
            ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "No response was received from BSC"));
        }
    }

    static YAML::Node GetSwagger() {
        YAML::Node node = YAML::Load(R"___(
        post:
            tags:
              - pdisk
            summary: Changes current PDisk status
            description: Changes current PDisk status
            parameters:
              - name: node_id
                in: query
                description: node identifier
                type: integer
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
            requestBody:
                description: Updates of PDisk statuses
                required: true
                content:
                    application/json:
                        schema:
                            type: object
                            properties:
                                status:
                                    type: string
                                    enum: [ACTIVE, INACTIVE, BROKEN, FAULTY, TO_BE_REMOVED, UNKNOWN]
                                    description: >
                                        PDisk operational status:
                                        * `ACTIVE` - working as expected
                                        * `INACTIVE` - new groups are not created using this drive, but existing ones continue to work as expected
                                        * `BROKEN` - drive is marked as not working, groups will be immediately moved out of this drive upon receiving this status
                                        * `FAULTY` - drive is expected to become BROKEN soon, new groups are not created, old groups are asynchronously moved out from this drive
                                        * `TO_BE_REMOVED` - same as INACTIVE, but drive is counted in fault model as not working
                                decommit_status:
                                    type: string
                                    enum: [DECOMMIT_NONE, DECOMMIT_PENDING, DECOMMIT_IMMINENT, DECOMMIT_REJECTED]
                                    description: >
                                        PDisk decommission status:
                                        * `DECOMMIT_NONE` - disk is not in decomission state
                                        * `DECOMMIT_PENDING` - decomission is planned for this disk, but not started yet. existing slots are not moved from the disk, but no new slots are allocated on it
                                        * `DECOMMIT_IMMINENT` - decomission has started for this disk. existing slots are moved from the disk
                                        * `DECOMMIT_REJECTED` - no slots from other disks are placed on this disk in the process of decommission
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
        return node;
    }
};

}
