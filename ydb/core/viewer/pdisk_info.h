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

class TPDiskInfo : public TViewerPipeClient<TPDiskInfo> {
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
    using TThis = TPDiskInfo;
    using TBase = TViewerPipeClient<TThis>;
    IViewer* Viewer;
    NMon::TEvHttpInfo::TPtr Event;
    ui32 Timeout = 0;
    ui32 ActualRetries = 0;
    ui32 Retries = 0;
    TDuration RetryPeriod = TDuration::MilliSeconds(500);

    std::unique_ptr<NSysView::TEvSysView::TEvGetPDisksResponse> BSCResponse;
    std::unique_ptr<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse> WhiteboardResponse;

    ui32 NodeId = 0;
    ui32 PDiskId = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TPDiskInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        NodeId = FromStringWithDefault<ui32>(params.Get("node_id"), 0);
        PDiskId = FromStringWithDefault<ui32>(params.Get("pdisk_id"), Max<ui32>());

        if (PDiskId == Max<ui32>()) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'pdisk_id' is required"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_GET) {
            TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(
                Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only GET method is allowed"),
                0, NMon::IEvHttpInfoRes::EContentType::Custom));
            return PassAway();
        }

        if (!NodeId) {
            NodeId = TlsActivationContext->ActorSystem()->NodeId;
        }
        TBase::InitConfig(params);

        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Retries = FromStringWithDefault<ui32>(params.Get("retries"), 3);
        RetryPeriod = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("retry_period"), RetryPeriod.MilliSeconds()));

        SendWhiteboardRequest();
        SendBSCRequest();

        TBase::Become(&TThis::StateWork, TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetPDisksResponse, Handle);
            cFunc(TEvRetryNodeRequest::EventType, HandleRetry);
            cFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void SendWhiteboardRequest() {
        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(NodeId);
        auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateRequest>();
        TBase::SendRequest(whiteboardServiceId, request.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, NodeId);
    }

    void SendBSCRequest() {
        RequestBSControllerPDiskInfo(NodeId, PDiskId);
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

    void Handle(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        BSCResponse.reset(ev->Release().Release());
        TBase::RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        WhiteboardResponse.reset(ev->Release().Release());
        TBase::RequestDone();
    }

    void HandleRetry() {
        SendWhiteboardRequest();
    }

    void HandleTimeout() {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(
            Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get(), "text/plain", "Timeout receiving response"),
            0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }

    void PassAway() override {
        TBase::Send(TActivationContext::InterconnectProxy(NodeId), new TEvents::TEvUnsubscribe());
        TBase::PassAway();
    }

    void ReplyAndPassAway() {
        NKikimrViewer::TPDiskInfo proto;
        TStringStream json;
        if (WhiteboardResponse != nullptr && WhiteboardResponse->Record.PDiskStateInfoSize() > 0) {
            proto.MutableWhiteboard()->CopyFrom(WhiteboardResponse->Record.GetPDiskStateInfo(0));
        }
        if (BSCResponse != nullptr && BSCResponse->Record.EntriesSize() > 0) {
            proto.MutableBSC()->CopyFrom(BSCResponse->Record.GetEntries(0).GetInfo());
        }
        TProtoToJson::ProtoToJson(json, proto, {
            .EnumAsNumbers = false,
        });
        TBase::Send(Event->Sender, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        PassAway();
    }
};

template <>
YAML::Node TJsonRequestSwagger<TPDiskInfo>::GetSwagger() {
    YAML::Node node = YAML::Load(R"___(
        get:
          tags:
          - pdisk
          summary: Gets PDisk info
          description: Gets PDisk information from Whiteboard and BSC
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
          responses:
            200:
              description: OK
              content:
                application/json:
                  schema: {}
            400:
              description: Bad Request
            403:
              description: Forbidden
            504:
              description: Gateway Timeout
        )___");

    node["get"]["responses"]["200"]["content"]["application/json"]["schema"] = TProtoToYaml::ProtoToYamlSchema<NKikimrViewer::TPDiskInfo>();
    YAML::Node properties(node["get"]["responses"]["200"]["content"]["application/json"]["schema"]["properties"]["BSC"]["properties"]);
    TProtoToYaml::FillEnum(properties["StatusV2"], NProtoBuf::GetEnumDescriptor<NKikimrBlobStorage::EDriveStatus>());
    TProtoToYaml::FillEnum(properties["DecommitStatus"], NProtoBuf::GetEnumDescriptor<NKikimrBlobStorage::EDecommitStatus>());
    TProtoToYaml::FillEnum(properties["Type"], NProtoBuf::GetEnumDescriptor<NKikimrBlobStorage::EPDiskType>());
    return node;
}

}
}
