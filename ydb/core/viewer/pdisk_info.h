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

    std::unique_ptr<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse> PDiskStateResponse;
    NWilson::TSpan PDiskStateSpan;
    std::unique_ptr<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse> VDiskStateResponse;
    NWilson::TSpan VDiskStateSpan;
    std::unique_ptr<NSysView::TEvSysView::TEvGetPDisksResponse> PDiskResponse;
    NWilson::TSpan PDiskSpan;
    std::unique_ptr<NSysView::TEvSysView::TEvGetVSlotsResponse> VDiskResponse;
    NWilson::TSpan VDiskSpan;

    ui32 NodeId = 0;
    ui32 PDiskId = 0;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TPDiskInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(ev)
        , Viewer(viewer)
        , Event(ev)
    {}

    void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        NodeId = FromStringWithDefault<ui32>(params.Get("node_id"), 0);
        PDiskId = FromStringWithDefault<ui32>(params.Get("pdisk_id"), Max<ui32>());

        if (PDiskId == Max<ui32>()) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "field 'pdisk_id' is required"), "BadRequest");
        }
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_GET) {
            return ReplyAndPassAway(Viewer->GetHTTPBADREQUEST(Event->Get(), "text/plain", "Only GET method is allowed"), "BadRequest");
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
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetPDisksResponse, Handle);
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            cFunc(TEvRetryNodeRequest::EventType, HandleRetry);
            cFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void SendWhiteboardRequest() {
        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(NodeId);
        {
            auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateRequest>();
            if (Span) {
                PDiskStateSpan = Span.CreateChild(TComponentTracingLevels::THttp::Detailed, "TEvPDiskStateRequest");
            }
            TBase::SendRequest(whiteboardServiceId, request.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, NodeId, PDiskStateSpan.GetTraceId());
        }
        {
            auto request = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateRequest>();
            if (Span) {
                VDiskStateSpan = Span.CreateChild(TComponentTracingLevels::THttp::Detailed, "TEvVDiskStateRequest");
            }
            TBase::SendRequest(whiteboardServiceId, request.release(), 0, NodeId, VDiskStateSpan.GetTraceId());
        }
    }

    void SendBSCRequest() {
        if (Span) {
            PDiskSpan = Span.CreateChild(TComponentTracingLevels::THttp::Detailed, "TEvGetPDisksRequest");
        }
        RequestBSControllerPDiskInfo(NodeId, PDiskId, PDiskSpan.GetTraceId());
        if (Span) {
            VDiskSpan = Span.CreateChild(TComponentTracingLevels::THttp::Detailed, "TEvGetVSlotsRequest");
        }
        RequestBSControllerVDiskInfo(NodeId, PDiskId, VDiskSpan.GetTraceId());
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
        if (PDiskStateSpan) {
            PDiskStateSpan.EndError("Undelivered");
        }
        if (VDiskStateSpan) {
            VDiskStateSpan.EndError("Undelivered");
        }
        if (!RetryRequest()) {
            TBase::RequestDone(2);
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        if (PDiskStateSpan) {
            PDiskStateSpan.EndError("NodeDisconnected");
        }
        if (VDiskStateSpan) {
            VDiskStateSpan.EndError("NodeDisconnected");
        }
        if (!RetryRequest()) {
            TBase::RequestDone(2);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            if (PDiskSpan) {
                PDiskSpan.EndError("ClientNotConnected");
            }
            if (VDiskSpan) {
                VDiskSpan.EndError("ClientNotConnected");
            }
            TBase::RequestDone(2);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        if (PDiskSpan) {
            PDiskSpan.EndError("ClientDestroyed");
        }
        if (VDiskSpan) {
            VDiskSpan.EndError("ClientDestroyed");
        }
        TBase::RequestDone(2);
    }

    void Handle(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        PDiskResponse.reset(ev->Release().Release());
        if (PDiskSpan) {
            PDiskSpan.EndOk();
        }
        TBase::RequestDone();
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        VDiskResponse.reset(ev->Release().Release());
        if (VDiskSpan) {
            VDiskSpan.EndOk();
        }
        TBase::RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        PDiskStateResponse.reset(ev->Release().Release());
        if (PDiskStateSpan) {
            PDiskStateSpan.EndOk();
        }
        TBase::RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        VDiskStateResponse.reset(ev->Release().Release());
        if (VDiskStateSpan) {
            VDiskStateSpan.EndOk();
        }
        TBase::RequestDone();
    }

    void HandleRetry() {
        SendWhiteboardRequest();
    }

    void HandleTimeout() {
        ReplyAndPassAway(Viewer->GetHTTPGATEWAYTIMEOUT(Event->Get(), "text/plain", "Timeout receiving response"), "Timeout");
    }

    void PassAway() override {
        TBase::Send(TActivationContext::InterconnectProxy(NodeId), new TEvents::TEvUnsubscribe());
        TBase::PassAway();
    }

    void ReplyAndPassAway(TString data, const TString& error = {}) {
        Send(Event->Sender, new NMon::TEvHttpInfoRes(data, 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        if (Span) {
            if (error) {
                Span.EndError(error);
            } else {
                Span.EndOk();
            }
        }
        PassAway();
    }

    void ReplyAndPassAway() {
        NKikimrViewer::TPDiskInfo proto;
        bool hasPDisk = false;
        bool hasVDisk = false;
        if (PDiskStateResponse != nullptr && PDiskStateResponse->Record.PDiskStateInfoSize() > 0) {
            for (const auto& pdisk : PDiskStateResponse->Record.GetPDiskStateInfo()) {
                if (pdisk.GetPDiskId() == PDiskId) {
                    proto.MutableWhiteboard()->MutablePDisk()->CopyFrom(pdisk);
                    hasPDisk = true;
                    break;
                }
            }
        }
        if (VDiskStateResponse != nullptr && VDiskStateResponse->Record.VDiskStateInfoSize() > 0) {
            for (const auto& vdisk : VDiskStateResponse->Record.GetVDiskStateInfo()) {
                if (vdisk.GetPDiskId() == PDiskId) {
                    proto.MutableWhiteboard()->AddVDisks()->CopyFrom(vdisk);
                    hasVDisk = true;
                }
            }
        }
        if (PDiskResponse != nullptr && PDiskResponse->Record.EntriesSize() > 0) {
            const auto& bscInfo(PDiskResponse->Record.GetEntries(0).GetInfo());
            proto.MutableBSC()->MutablePDisk()->CopyFrom(bscInfo);
            if (!hasPDisk) {
                auto& pdiskInfo(*proto.MutableWhiteboard()->MutablePDisk());
                pdiskInfo.SetPDiskId(PDiskId);
                pdiskInfo.SetPath(bscInfo.GetPath());
                pdiskInfo.SetGuid(bscInfo.GetGuid());
                pdiskInfo.SetCategory(bscInfo.GetCategory());
                pdiskInfo.SetAvailableSize(bscInfo.GetAvailableSize());
                pdiskInfo.SetTotalSize(bscInfo.GetTotalSize());
            }
        }
        if (VDiskResponse != nullptr && VDiskResponse->Record.EntriesSize() > 0) {
            for (const auto& vdisk : VDiskResponse->Record.GetEntries()) {
                proto.MutableBSC()->AddVDisks()->CopyFrom(vdisk);
                if (!hasVDisk) {
                    const auto& bscInfo(vdisk.GetInfo());
                    auto& vdiskInfo(*proto.MutableWhiteboard()->AddVDisks());
                    vdiskInfo.SetPDiskId(PDiskId);
                    vdiskInfo.MutableVDiskId()->SetGroupID(bscInfo.GetGroupId());
                    vdiskInfo.MutableVDiskId()->SetGroupGeneration(bscInfo.GetGroupGeneration());
                    vdiskInfo.MutableVDiskId()->SetRing(bscInfo.GetFailRealm());
                    vdiskInfo.MutableVDiskId()->SetDomain(bscInfo.GetFailDomain());
                    vdiskInfo.MutableVDiskId()->SetVDisk(bscInfo.GetVDisk());
                    vdiskInfo.SetAllocatedSize(bscInfo.GetAllocatedSize());
                    vdiskInfo.SetAvailableSize(bscInfo.GetAvailableSize());
                }
            }
        }
        TStringStream json;
        TProtoToJson::ProtoToJson(json, proto, {
            .EnumAsNumbers = false,
        });
        ReplyAndPassAway(Viewer->GetHTTPOKJSON(Event->Get(), json.Str()));
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
    TProtoToYaml::FillEnum(properties["PDisk"]["properties"]["StatusV2"], NProtoBuf::GetEnumDescriptor<NKikimrBlobStorage::EDriveStatus>());
    TProtoToYaml::FillEnum(properties["PDisk"]["properties"]["DecommitStatus"], NProtoBuf::GetEnumDescriptor<NKikimrBlobStorage::EDecommitStatus>());
    TProtoToYaml::FillEnum(properties["PDisk"]["properties"]["Type"], NProtoBuf::GetEnumDescriptor<NKikimrBlobStorage::EPDiskType>());
    TProtoToYaml::FillEnum(properties["VDisks"]["items"]["properties"]["StatusV2"], NProtoBuf::GetEnumDescriptor<NKikimrBlobStorage::EVDiskStatus>());
    return node;
}

}
}
