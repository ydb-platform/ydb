#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/viewer/json/json.h>
#include <ydb/core/viewer/yaml/yaml.h>
#include <ydb/core/util/wildcard.h>
#include <library/cpp/json/json_writer.h>
#include "viewer.h"
#include "json_pipe_req.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
extern bool IsPostContent(const NMon::TEvHttpInfo::TPtr& event);

class TPDiskStatus : public TViewerPipeClient {
protected:
    using TThis = TPDiskStatus;
    using TBase = TViewerPipeClient;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    ui32 Timeout = 0;

    std::unique_ptr<TEvBlobStorage::TEvControllerConfigResponse> Response;

    NKikimrBlobStorage::TUpdateDriveStatus DriveStatus;
    bool Force = false;

public:
    TPDiskStatus(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() override {
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_POST) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only POST method is allowed"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        const auto& params(Event->Get()->Request.GetParams());
        if (!params.Has("pdisk_id")) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'pdisk_id' is required"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        DriveStatus.MutableHostKey()->SetNodeId(FromStringWithDefault<ui32>(params.Get("node_id"), TlsActivationContext->ActorSystem()->NodeId));
        DriveStatus.SetPDiskId(FromStringWithDefault<ui32>(params.Get("pdisk_id"), Max<ui32>()));
        Force = FromStringWithDefault<bool>(params.Get("force"), false);
        if (Force && !Viewer->CheckAccessAdministration(Event->Get())) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPFORBIDDEN(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        if (IsPostContent(Event)) {
            NJson::TJsonValue contentData;
            bool success = NJson::ReadJsonTree(Event->Get()->Request.GetPostContent(), &contentData);
            if (success && contentData.IsMap()) {
                if (contentData.Has("decommit_status")) {
                    NKikimrBlobStorage::EDecommitStatus decommitStatus = NKikimrBlobStorage::EDecommitStatus::DECOMMIT_UNSET;
                    if (EDecommitStatus_Parse(contentData["decommit_status"].GetStringRobust(), &decommitStatus)) {
                        DriveStatus.SetDecommitStatus(decommitStatus);
                    } else {
                        TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                            Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Invalid 'decommit_status' received"),
                            0, NMon::IEvHttpInfoRes::EContentType::Custom));
                        return PassAway();
                    }
                }
                if (contentData.Has("status")) {
                    NKikimrBlobStorage::EDriveStatus status = NKikimrBlobStorage::EDriveStatus::UNKNOWN;
                    if (EDriveStatus_Parse(contentData["status"].GetStringRobust(), &status)) {
                        DriveStatus.SetStatus(status);
                    } else {
                        TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                            Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Invalid 'status' received"),
                            0, NMon::IEvHttpInfoRes::EContentType::Custom));
                        return PassAway();
                    }
                }
            } else {
                TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                    Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Invalid json received"),
                    0, NMon::IEvHttpInfoRes::EContentType::Custom));
                return PassAway();
            }

        } else {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Content in json format is required for POST method"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }

        TBase::InitConfig(params);

        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);

        RequestBSControllerPDiskUpdateStatus(DriveStatus, Force);

        TBase::Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        Response.reset(ev->Release().Release());
        ReplyAndPassAway();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(
            Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get(), "text/plain", "Timeout receiving response from BSC"),
            0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void PassAway() override {
        TBase::PassAway();
    }

    void ReplyAndPassAway() override {
        NJson::TJsonValue json;
        if (Response != nullptr) {
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
            TBase::Send(Event->Sender,
                new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), NJson::WriteJson(json)),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
        } else {
            TBase::Send(Event->Sender,
                new NMon::TEvHttpInfoRes(Viewer->GetHTTPINTERNALERROR(Event->Get(), "text/plain", "No response was received from BSC"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
        }
        PassAway();
    }
};

template <>
YAML::Node TJsonRequestSwagger<TPDiskStatus>::GetSwagger() {
    YAML::Node node = YAML::Load(R"___(
        post:
          tags:
          - pdisk
          summary: Updates PDisk status
          description: Updates PDisk status in BSC
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
            description: attempt forced operation, ignore warnings
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

}
}
