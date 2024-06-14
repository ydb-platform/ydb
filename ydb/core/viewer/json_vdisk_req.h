#pragma once
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/viewer/json/json.h>
#include "viewer.h"
#include "json_pipe_req.h"

namespace NKikimr {
namespace NViewer {

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


template <typename RequestType, typename ResponseType>
class TJsonVDiskRequest : public TViewerPipeClient<TJsonVDiskRequest<RequestType, ResponseType>> {
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
    using TBase = TViewerPipeClient<TThis>;
    using THelper = TJsonVDiskRequestHelper<RequestType, ResponseType>;
    IViewer* Viewer;
    TActorId Initiator;
    NMon::TEvHttpInfo::TPtr Event;
    TJsonSettings JsonSettings;
    bool AllEnums = false;
    ui32 Timeout = 0;
    ui32 ActualRetries = 0;
    ui32 Retries = 0;
    TDuration RetryPeriod = TDuration::MilliSeconds(500);

    std::unique_ptr<ResponseType> Response;

    ui32 NodeId = 0;
    ui32 PDiskId = 0;
    ui32 VSlotId = 0;

    std::optional<TActorId> TcpProxyId;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TJsonVDiskRequest(IViewer* viewer, NMon::TEvHttpInfo::TPtr& ev)
        : Viewer(viewer)
        , Initiator(ev->Sender)
        , Event(ev)
    {}

    virtual void Bootstrap() {
        const auto& params(Event->Get()->Request.GetParams());
        NodeId = FromStringWithDefault<ui32>(params.Get("node_id"), 0);
        PDiskId = FromStringWithDefault<ui32>(params.Get("pdisk_id"), Max<ui32>());
        VSlotId = FromStringWithDefault<ui32>(params.Get("vslot_id"), Max<ui32>());

        if (PDiskId == Max<ui32>()) {
            ReplyAndPassAway("field 'pdisk_id' is required");
            return;
        }

        if (VSlotId == Max<ui32>()) {
            ReplyAndPassAway("field 'vslot_id' is required");
            return;
        }

        if (!NodeId) {
            NodeId = TlsActivationContext->ActorSystem()->NodeId;
        }
        TBase::InitConfig(params);


        JsonSettings.EnumAsNumbers = !FromStringWithDefault<bool>(params.Get("enums"), false);
        JsonSettings.UI64AsString = !FromStringWithDefault<bool>(params.Get("ui64"), false);
        JsonSettings.EmptyRepeated = FromStringWithDefault<bool>(params.Get("empty_repeated"), false);

        Timeout = FromStringWithDefault<ui32>(params.Get("timeout"), 10000);
        Retries = FromStringWithDefault<ui32>(params.Get("retries"), 0);
        RetryPeriod = TDuration::MilliSeconds(FromStringWithDefault<ui32>(params.Get("retry_period"), RetryPeriod.MilliSeconds()));

        SendRequest();
        TBase::Become(&TThis::WaitState);
        TBase::Schedule(TDuration::MilliSeconds(Timeout), new TEvents::TEvWakeup());
    }

    STATEFN(WaitState) {
        switch (ev->GetTypeRewrite()) {
            hFunc(ResponseType, Handle);
            cFunc(TEvRetryNodeRequest::EventType, HandleRetry);
            cFunc(TEvents::TEvUndelivered::EventType, Undelivered);
            hFunc(TEvInterconnect::TEvNodeConnected, Connected);
            cFunc(TEvInterconnect::TEvNodeDisconnected::EventType, Disconnected);
            cFunc(TEvents::TSystem::Wakeup, HandleTimeout);
        }
    }

    void SendRequest() {
        TString error;
        auto req = THelper::MakeRequest(Event, &error);
        if (req) {
            TActorId vdiskServiceId = MakeBlobStorageVDiskID(NodeId, PDiskId, VSlotId);
            TBase::SendRequest(vdiskServiceId, req.release(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, NodeId);
        } else {
            ReplyAndPassAway(error);
        }
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

    void Connected(TEvInterconnect::TEvNodeConnected::TPtr &ev) {
        TcpProxyId = ev->Sender;
    }

    void Disconnected() {
        TcpProxyId = {};
        if (!RetryRequest()) {
            TBase::RequestDone();
        }
    }

    void Handle(typename ResponseType::TPtr& ev) {
        Response.reset(ev->Release().Release());
        ReplyAndPassAway();
    }

    void HandleRetry() {
        SendRequest();
    }

    void HandleTimeout() {
        ReplyAndPassAway("Timeout");
    }

    void RenderResponse(TStringStream& json) {
        if (Response != nullptr) {
            TProtoToJson::ProtoToJson(json, Response->Record, JsonSettings);
        } else {
            json << "null";
        }
    }

    void PassAway() override {
        if (TcpProxyId) {
            this->Send(*TcpProxyId, new TEvents::TEvUnsubscribe);
        }
        TBase::PassAway();
    }

    void ReplyAndPassAway(const TString &error = "") {
        try {
            TStringStream json;
            if (error) {
                json << "{\"Error\":\"" << TProtoToJson::EscapeJsonString(error) << "\"}";
            } else {
                RenderResponse(json);
            }
            TBase::Send(Initiator, new NMon::TEvHttpInfoRes(Viewer->GetHTTPOKJSON(Event->Get(), std::move(json.Str())), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        } catch (const std::exception& e) {
            TBase::Send(Initiator, new NMon::TEvHttpInfoRes(TString("HTTP/1.1 400 Bad Request\r\n\r\n") + e.what(), 0, NMon::IEvHttpInfoRes::EContentType::Custom));
        }
        PassAway();
    }
};

template <typename RequestType, typename ResponseType>
struct TJsonRequestParameters<TJsonVDiskRequest<RequestType, ResponseType>> {
    static YAML::Node GetParameters() {
        return YAML::Load(R"___(
            - name: node_id
              in: query
              description: node identifier
              required: true
              type: integer
            - name: pdisk_id
              in: query
              description: pdisk identifier
              required: true
              type: integer
            - name: vslot_id
              in: query
              description: vdisk slot identifier
              required: true
              type: integer
            )___" + TJsonVDiskRequestHelper<RequestType, ResponseType>::GetAdditionalParameters() + R"___(
            - name: enums
              in: query
              description: convert enums to strings
              required: false
              type: boolean
            - name: ui64
              in: query
              description: return ui64 as number
              required: false
              type: boolean
            - name: timeout
              in: query
              description: timeout in ms
              required: false
              type: integer
            - name: retries
              in: query
              description: number of retries
              required: false
              type: integer
            - name: retry_period
              in: query
              description: retry period in ms
              required: false
              type: integer
              default: 500
            )___");
    }
};

template <typename RequestType, typename ResponseType>
struct TJsonRequestSchema<TJsonVDiskRequest<RequestType, ResponseType>> {
    static YAML::Node GetSchema() {
        return TProtoToYaml::ProtoToYamlSchema<typename ResponseType::ProtoRecordType>();
    }
};

}
}
