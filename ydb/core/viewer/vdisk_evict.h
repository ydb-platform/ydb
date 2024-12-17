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
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    ui32 Timeout = 0;
    ui32 ActualRetries = 0;
    ui32 Retries = 0;
    TDuration RetryPeriod = TDuration::MilliSeconds(500);

    std::unique_ptr<TEvBlobStorage::TEvControllerConfigResponse> Response;

    ui32 GroupId = 0;
    ui32 GroupGeneration = 0;
    ui32 FailRealmIdx = 0;
    ui32 FailDomainIdx = 0;
    ui32 VdiskIdx = 0;
    bool Force = false;

public:
    TJsonVDiskEvict(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    inline ui32 GetRequiredParam(const TCgiParameters& params, const std::string& name, ui32& obj) {
        if (!TryFromString<ui32>(params.Get(name), obj)) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", TStringBuilder() << "field '" << name << "' or 'vdisk_id' are required"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return false;
        }
        return true;
    }

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        TString vdisk_id = params.Get("vdisk_id");
        if (vdisk_id) {
            TVector<TString> parts = StringSplitter(vdisk_id).Split('-').SkipEmpty();
            if (parts.size() == 5) {
                GroupId = FromStringWithDefault<ui32>(parts[0], Max<ui32>());
                GroupGeneration = FromStringWithDefault<ui32>(parts[1], Max<ui32>());
                FailRealmIdx = FromStringWithDefault<ui32>(parts[2], Max<ui32>());
                FailDomainIdx = FromStringWithDefault<ui32>(parts[3], Max<ui32>());
                VdiskIdx = FromStringWithDefault<ui32>(parts[4], Max<ui32>());
            }
            if (parts.size() != 5 || GroupId == Max<ui32>()
                    || GroupGeneration == Max<ui32>() || FailRealmIdx  == Max<ui32>()
                    || FailDomainIdx == Max<ui32>() || VdiskIdx == Max<ui32>()) {
                TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                    Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", TStringBuilder() << "Unable to parse the 'vdisk_id' parameter"),
                    0, NMon::IEvHttpInfoRes::EContentType::Custom));
                return PassAway();
            }
        } else if (!GetRequiredParam(params, "group_id", GroupId)
                || !GetRequiredParam(params, "group_generation_id", GroupGeneration)
                || !GetRequiredParam(params, "fail_realm_idx", FailRealmIdx)
                || !GetRequiredParam(params, "fail_domain_idx", FailDomainIdx)
                || !GetRequiredParam(params, "vdisk_idx", VdiskIdx)) {
            return PassAway();
        }

        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_POST) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only POST method is allowed"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        TBase::InitConfig(params);

        Force = FromStringWithDefault<bool>(params.Get("force"), false);
        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Retries = FromStringWithDefault<ui32>(params.Get("retries"), 0);
        RetryPeriod = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("retry_period"), RetryPeriod.MilliSeconds()));

        if (Force && !Viewer->CheckAccessAdministration(Event->Get())) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPFORBIDDEN(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }

        SendRequest();

        TBase::Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvControllerConfigResponse, Handle);
            cFunc(TEvRetryNodeRequest::EventType, HandleRetry);
            cFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void SendRequest() {
        RequestBSControllerVDiskEvict(GroupId, GroupGeneration, FailRealmIdx, FailDomainIdx, VdiskIdx, Force);
    }

    bool RetryRequest() {
        if (Retries) {
            if (++ActualRetries <= Retries) {
                TBase::Schedule(RetryPeriod, new TEvRetryNodeRequest());
                return true;
            }
        }
        return false;
    }

    void Undelivered() {
        if (!RetryRequest()) {
            TBase::RequestDone();
        }
    }

    void Handle(TEvBlobStorage::TEvControllerConfigResponse::TPtr& ev) {
        Response.reset(ev->Release().Release());
        ReplyAndPassAway();
    }

    void HandleRetry() {
        SendRequest();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(
            Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get(), "text/plain", "Timeout receiving response from BSC"),
            0, NMon::IEvHttpInfoRes::EContentType::Custom));
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
        } else {
            json["result"] = false;
            json["error"] = "No response was received from BSC";
        }
        TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), NJson::WriteJson(json)), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
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
                required: false
                type: string
              - name: group_id
                in: query
                description: group identifier
                required: false
                type: integer
              - name: group_generation_id
                in: query
                description: group generation identifier
                required: false
                type: integer
              - name: fail_realm_idx
                in: query
                description: fail realm identifier
                required: false
                type: integer
              - name: fail_domain_ids
                in: query
                description: fail domain identifier
                required: false
                type: integer
              - name: vdisk_idx
                in: query
                description: vdisk idx identifier
                required: false
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
