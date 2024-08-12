#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/core/viewer/yaml/yaml.h>

namespace NKikimr::NViewer {

using namespace NActors;

class TPDiskInfo : public TViewerPipeClient {
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
    using TBase = TViewerPipeClient;
    ui32 Timeout = 0;
    ui32 ActualRetries = 0;
    ui32 Retries = 0;
    TDuration RetryPeriod = TDuration::MilliSeconds(500);

    TRequestResponse<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse> WhiteboardPDisk;
    TRequestResponse<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse> WhiteboardVDisk;
    TRequestResponse<NSysView::TEvSysView::TEvGetPDisksResponse> SysViewPDisks;
    TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse> SysViewVSlots;

    ui32 NodeId = 0;
    ui32 PDiskId = 0;

public:
    TPDiskInfo(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        const auto& params(Event->Get()->Request.GetParams());
        NodeId = FromStringWithDefault<ui32>(params.Get("node_id"), 0);
        PDiskId = FromStringWithDefault<ui32>(params.Get("pdisk_id"), Max<ui32>());

        if (PDiskId == Max<ui32>()) {
            return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "field 'pdisk_id' is required"), "BadRequest");
        }
        if (Event->Get()->Request.GetMethod() != HTTP_METHOD_GET) {
            return TBase::ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "Only GET method is allowed"), "BadRequest");
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
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            hFunc(TEvTabletPipe::TEvClientConnected, Handle);
            hFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void SendWhiteboardRequest() {
        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(NodeId);
        WhiteboardPDisk = TBase::MakeRequest<NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse>(
            whiteboardServiceId,
            new NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateRequest,
            IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, // we only need it once because we are sending to the same node
            NodeId);
        WhiteboardVDisk = TBase::MakeRequest<NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse>(
            whiteboardServiceId,
            new NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateRequest,
            0,
            NodeId);
    }

    void SendBSCRequest() {
        SysViewPDisks = RequestBSControllerPDiskInfo(NodeId, PDiskId);
        SysViewVSlots = RequestBSControllerVDiskInfo(NodeId, PDiskId);
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

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr&) {
        WhiteboardPDisk.Error("NodeDisconnected");
        WhiteboardVDisk.Error("NodeDisconnected");
        if (!RetryRequest()) {
            TBase::RequestDone(2);
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev) {
        if (ev->Get()->Status != NKikimrProto::OK) {
            SysViewPDisks.Error("ClientNotConnected");
            SysViewVSlots.Error("ClientNotConnected");
            TBase::RequestDone(2);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr&) {
        SysViewPDisks.Error("ClientDestroyed");
        SysViewVSlots.Error("ClientDestroyed");
        TBase::RequestDone(2);
    }

    void Handle(NSysView::TEvSysView::TEvGetPDisksResponse::TPtr& ev) {
        SysViewPDisks.Set(std::move(ev));
        TBase::RequestDone();
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        SysViewVSlots.Set(std::move(ev));
        TBase::RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse::TPtr& ev) {
        WhiteboardPDisk.Set(std::move(ev));
        TBase::RequestDone();
    }

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse::TPtr& ev) {
        WhiteboardVDisk.Set(std::move(ev));
        TBase::RequestDone();
    }

    void HandleRetry() {
        SendWhiteboardRequest();
    }

    void HandleTimeout() {
        TBase::ReplyAndPassAway(GetHTTPGATEWAYTIMEOUT("text/plain", "Timeout receiving response"), "Timeout");
    }

    void PassAway() override {
        TBase::Send(TActivationContext::InterconnectProxy(NodeId), new TEvents::TEvUnsubscribe());
        TBase::PassAway();
    }

    void ReplyAndPassAway() override {
        NKikimrViewer::TPDiskInfo proto;
        bool hasPDisk = false;
        bool hasVDisk = false;
        if (WhiteboardPDisk && WhiteboardPDisk->Record.PDiskStateInfoSize() > 0) {
            for (const auto& pdisk : WhiteboardPDisk->Record.GetPDiskStateInfo()) {
                if (pdisk.GetPDiskId() == PDiskId) {
                    proto.MutableWhiteboard()->MutablePDisk()->CopyFrom(pdisk);
                    hasPDisk = true;
                    break;
                }
            }
        }
        if (WhiteboardVDisk && WhiteboardVDisk->Record.VDiskStateInfoSize() > 0) {
            for (const auto& vdisk : WhiteboardVDisk->Record.GetVDiskStateInfo()) {
                if (vdisk.GetPDiskId() == PDiskId) {
                    proto.MutableWhiteboard()->AddVDisks()->CopyFrom(vdisk);
                    hasVDisk = true;
                }
            }
        }
        if (SysViewPDisks && SysViewPDisks->Record.EntriesSize() > 0) {
            const auto& bscInfo(SysViewPDisks->Record.GetEntries(0).GetInfo());
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
        if (SysViewVSlots && SysViewVSlots->Record.EntriesSize() > 0) {
            for (const auto& vdisk : SysViewVSlots->Record.GetEntries()) {
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
        TBase::ReplyAndPassAway(GetHTTPOKJSON(json.Str()));
    }

    static YAML::Node GetSwagger() {
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
};

}
