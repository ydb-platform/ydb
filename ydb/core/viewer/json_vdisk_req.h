#pragma once
#include "json_pipe_req.h"
#include "viewer.h"
#include <ydb/core/viewer/yaml/yaml.h>

namespace NKikimr::NViewer {

using namespace NActors;

template <typename RequestType, typename ResponseType>
struct TJsonVDiskRequestHelper  {
    static std::unique_ptr<RequestType> MakeRequest(NMon::TEvHttpInfo::TPtr &, TString *) {
        return std::make_unique<RequestType>();
    }

    static TString GetAdditionalParameters() {
        return "";
    }
};

struct TVDiskID : NKikimr::TVDiskID {
    using NKikimr::TVDiskID::TVDiskID;

    TVDiskID& operator =(TStringBuf vdiskId) {
        GroupID = TGroupId::FromValue(FromStringWithDefault<ui32>(vdiskId.NextTok('-')));
        GroupGeneration = FromStringWithDefault<ui32>(vdiskId.NextTok('-'));
        FailRealm = FromStringWithDefault<ui8>(vdiskId.NextTok('-'));
        FailDomain = FromStringWithDefault<ui8>(vdiskId.NextTok('-'));
        VDisk = FromStringWithDefault<ui8>(vdiskId);
        return *this;
    }

    operator bool() const {
        return GroupGeneration != 0 || FailRealm != 0 || FailDomain != 0 || VDisk != 0;
    }

    TString ToString() const {
        return TStringBuilder() << GroupID << "-" << GroupGeneration << "-" << ui32(FailRealm) << "-" << ui32(FailDomain) << "-" << ui32(VDisk);
    }
};

template <typename RequestType, typename ResponseType>
class TJsonVDiskRequest : public TViewerPipeClient {
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
    using TThis = TJsonVDiskRequest<RequestType, ResponseType>;
    using TBase = TViewerPipeClient;
    using THelper = TJsonVDiskRequestHelper<RequestType, ResponseType>;
    using TBase::ReplyAndPassAway;
    ui32 ActualRetries = 0;
    ui32 Retries = 0;
    TDuration RetryPeriod = TDuration::MilliSeconds(500);
    std::optional<TRequestResponse<NSysView::TEvSysView::TEvGetVSlotsResponse>> GetVSlotsResponse;
    TRequestResponse<ResponseType> Response;
    ui32 NodeId = 0;
    ui32 PDiskId = 0;
    ui32 VSlotId = 0;
    TVDiskID VDiskId;

public:
    TJsonVDiskRequest(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : TBase(viewer, ev)
    {}

    void Bootstrap() override {
        Retries = FromStringWithDefault<ui32>(Params.Get("retries"), 0);
        RetryPeriod = TDuration::MilliSeconds(FromStringWithDefault<ui32>(Params.Get("retry_period"), RetryPeriod.MilliSeconds()));
        VDiskId = Params.Get("vdisk_id");
        if (Span) {
            Span.Attribute("parsed_vdisk_id", VDiskId.ToString());
        }
        if (VDiskId) {
            GetVSlotsResponse = MakeCachedRequestBSControllerVSlots();
        } else {
            NodeId = FromStringWithDefault<ui32>(Params.Get("node_id"), 0);
            PDiskId = FromStringWithDefault<ui32>(Params.Get("pdisk_id"), Max<ui32>());
            VSlotId = FromStringWithDefault<ui32>(Params.Get("vslot_id"), Max<ui32>());
            if (PDiskId == Max<ui32>() || VSlotId == Max<ui32>()) {
                return ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", "You must specify either vdisk_id, or all three of the following: node_id, pdisk_id, and vslot_id"));
            }
            if (!NodeId) {
                NodeId = TlsActivationContext->ActorSystem()->NodeId;
            }
            SendRequest();
        }
        Become(&TThis::WaitState, Timeout, new TEvents::TEvWakeup());
    }

    STATEFN(WaitState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSysView::TEvSysView::TEvGetVSlotsResponse, Handle);
            hFunc(ResponseType, Handle);
            cFunc(TEvRetryNodeRequest::EventType, HandleRetry);
            cFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, Disconnected);
            cFunc(TEvents::TSystem::Wakeup, TBase::HandleTimeout);
        }
    }

    void SendRequest() {
        TString error;
        auto req = THelper::MakeRequest(Event, &error);
        if (req) {
            TActorId vdiskServiceId = MakeBlobStorageVDiskID(NodeId, PDiskId, VSlotId);
            Response = MakeRequest<ResponseType>(vdiskServiceId, req.release());
        } else {
            ReplyAndPassAway(error);
        }
    }

    TVDiskID GetVDiskId(const NKikimrSysView::TVSlotEntry& vslot) const {
        const auto& info = vslot.GetInfo();
        return TVDiskID(
            info.GetGroupId(),
            info.GetGroupGeneration(),
            info.GetFailRealm(),
            info.GetFailDomain(),
            info.GetVDisk());
    }

    void FindVDisk() {
        if (GetVSlotsResponse && GetVSlotsResponse->IsOk()) {
            for (const NKikimrSysView::TVSlotEntry& entry : GetVSlotsResponse->Get()->Record.GetEntries()) {
                if (GetVDiskId(entry) == VDiskId) {
                    NodeId = entry.GetKey().GetNodeId();
                    PDiskId = entry.GetKey().GetPDiskId();
                    VSlotId = entry.GetKey().GetVSlotId();
                    SendRequest();
                    return;
                }
            }
            ReplyAndPassAway(GetHTTPBADREQUEST("text/plain", TStringBuilder() << "VDiskId '" << VDiskId.ToString() << "' not found"));
        } else {
            ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", "Failed to get VSlots information"));
        }
    }

    bool RetryRequest() {
        if (Retries) {
            if (++ActualRetries <= Retries) {
                Schedule(RetryPeriod, new TEvRetryNodeRequest());
                return true;
            }
        }
        return false;
    }

    void Undelivered() {
        if (!RetryRequest()) {
            RequestDone();
        }
    }

    void Disconnected() {
        if (!RetryRequest()) {
            RequestDone();
        }
    }

    void Handle(NSysView::TEvSysView::TEvGetVSlotsResponse::TPtr& ev) {
        if (GetVSlotsResponse->Set(std::move(ev))) {
            FindVDisk();
            RequestDone();
        }
    }

    void Handle(typename ResponseType::TPtr& ev) {
        if (Response.Set(std::move(ev))) {
            RequestDone();
        }
    }

    void HandleRetry() {
        SendRequest();
    }

    void ReplyAndPassAway() override {
        if (Response.IsOk()) {
            TBase::ReplyAndPassAway(GetHTTPOKJSON(Response->Record));
        } else {
            TBase::ReplyAndPassAway(GetHTTPINTERNALERROR("text/plain", Response.GetError()));
        }
    }

    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<typename ResponseType::ProtoRecordType>();
    }

    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: vdisk_id
              in: query
              description: vdisk identifier
              required: true
              type: string
            - name: node_id
              in: query
              description: node identifier
              type: integer
            - name: pdisk_id
              in: query
              description: pdisk identifier
              type: integer
            - name: vslot_id
              in: query
              description: vdisk slot identifier
              type: integer
            )___" + TJsonVDiskRequestHelper<RequestType, ResponseType>::GetAdditionalParameters());
    }
};

}
