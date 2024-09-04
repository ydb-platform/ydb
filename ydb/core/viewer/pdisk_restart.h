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
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    ui32 Timeout = 0;
    ui32 ActualRetries = 0;
    ui32 Retries = 0;
    TDuration RetryPeriod = TDuration::MilliSeconds(500);

    std::unique_ptr<TEvBlobStorage::TEvControllerConfigResponse> Response;

    ui32 NodeId = 0;
    ui32 PDiskId = 0;
    bool Force = false;

public:
    TJsonPDiskRestart(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        NodeId = FromStringWithDefault<ui32>(params.Get("node_id"), 0);
        PDiskId = FromStringWithDefault<ui32>(params.Get("pdisk_id"), Max<ui32>());
        Force = FromStringWithDefault<bool>(params.Get("force"), false);

        if (PDiskId == Max<ui32>()) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'pdisk_id' is required"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_POST) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only POST method is allowed"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        if (Force && !Viewer->CheckAccessAdministration(Event->Get())) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPFORBIDDEN(Event->Get()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }

        if (!NodeId) {
            NodeId = TlsActivationContext->ActorSystem()->NodeId;
        }
        TBase::InitConfig(params);

        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Retries = FromStringWithDefault<ui32>(params.Get("retries"), 0);
        RetryPeriod = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("retry_period"), RetryPeriod.MilliSeconds()));

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
        RequestBSControllerPDiskRestart(NodeId, PDiskId, Force);
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
