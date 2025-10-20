#include "db_metadata_cache.h"
#include "service_monitoring.h"

#include "rpc_kqp_base.h"
#include "rpc_request_base.h"

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/mon/mon.h>
#include <yql/essentials/public/issue/yql_issue_message.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <ydb/library/actors/core/interconnect.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <library/cpp/digest/old_crc/crc.h>

#include <util/random/shuffle.h>

#include <ydb/core/health_check/health_check.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <google/protobuf/util/json_util.h>
#include <ydb/core/health_check/health_check.h>

namespace NKikimr {
namespace NGRpcService {

using namespace NActors;
using namespace Ydb;

using TEvClusterStateRequest = TGrpcRequestOperationCall<Ydb::Monitoring::ClusterStateRequest, Ydb::Monitoring::ClusterStateResponse>;

class TClusterStateRPC : public TRpcRequestActor<TClusterStateRPC, TEvClusterStateRequest, true> {
public:
    using TRpcRequestActor::TRpcRequestActor;
    using TThis = TClusterStateRPC;
    using TBase = TRpcRequestActor<TClusterStateRPC, TEvClusterStateRequest, true>;

    ui32 Requested;
    ui32 Received;
    ui32 CountersRequested;

    TVector<TEvInterconnect::TNodeInfo> Nodes;
    TMap<ui32, NKikimrWhiteboard::TEvVDiskStateResponse> VDiskInfo;
    TMap<ui32, NKikimrWhiteboard::TEvPDiskStateResponse> PDiskInfo;
    TMap<ui32, NKikimrWhiteboard::TEvTabletStateResponse> TabletInfo;
    TMap<ui32, NKikimrWhiteboard::TEvBSGroupStateResponse> BSGroupInfo;
    TMap<ui32, NKikimrWhiteboard::TEvSystemStateResponse> SystemInfo;
    TMap<ui32, NKikimrWhiteboard::TEvBridgeInfoResponse> BridgeInfo;
    TMap<ui32, NKikimrWhiteboard::TEvNodeStateResponse> NodeInfo;
    TMap<ui32, TVector<TString>> CountersInfo;
    Ydb::Monitoring::SelfCheckResult SelfCheck;
    Ydb::StatusIds_StatusCode Status = Ydb::StatusIds::SUCCESS;
    TInstant Started;
    ui32 Duration;
    ui32 Period;

    void SendRequest(ui32 nodeId) {
        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
#define request(NAME) \
        Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::NAME(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, nodeId); \
        Requested++;

        request(TEvVDiskStateRequest);
        request(TEvPDiskStateRequest);
        request(TEvTabletStateRequest);
        request(TEvBSGroupStateRequest);
        request(TEvSystemStateRequest);
        request(TEvBridgeInfoRequest);
        request(TEvNodeStateRequest);
        request(TEvCountersInfoRequest);
#undef request
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        RequestHealthCheck();
        Nodes = ev->Get()->Nodes;
        for (const auto& ni : Nodes) {
            SendRequest(ni.NodeId);
        }
        CountersRequested = 1;
        Period = GetProtoRequest()->period();
        if (Period > 0) {
            Schedule(TDuration::Seconds(Period), new TEvents::TEvWakeup());
        }
        if (Requested > 0) {
            TBase::Become(&TThis::StateRequestedNodeInfo);
        } else {
            ReplyAndPassAway();
        }
    }

    void Undelivered(TEvents::TEvUndelivered::TPtr &ev) {
        ui32 nodeId = ev.Get()->Cookie;
#define processCase(NAME, INFO) \
    case NNodeWhiteboard::TEvWhiteboard::NAME: \
        if (INFO.emplace(nodeId, NKikimrWhiteboard::T##NAME{}).second) { \
            NodeStateInfoReceived(); \
        } \
        break;
        switch (ev->Get()->SourceType) {
            processCase(EvVDiskStateResponse, VDiskInfo)
            processCase(EvPDiskStateResponse, PDiskInfo)
            processCase(EvTabletStateResponse, TabletInfo)
            processCase(EvBSGroupStateResponse, BSGroupInfo)
            processCase(EvSystemStateResponse, SystemInfo)
            processCase(EvBridgeInfoResponse, BridgeInfo)
            processCase(EvNodeStateResponse, NodeInfo)
            case NNodeWhiteboard::TEvWhiteboard::EvCountersInfoResponse:
                if (CountersInfo[nodeId].size() < CountersRequested) {
                    for (ui32 _ : xrange(CountersRequested - CountersInfo[nodeId].size())) {
                        NodeStateInfoReceived();
                    }
                    CountersInfo[nodeId].resize(CountersRequested);
                }
                break;
        }
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        ui32 nodeId = ev->Get()->NodeId;
#define process(NAME, INFO) \
    if (INFO.emplace(nodeId, NKikimrWhiteboard::NAME{}).second) { \
        NodeStateInfoReceived(); \
    }
        process(TEvVDiskStateResponse, VDiskInfo)
        process(TEvPDiskStateResponse, PDiskInfo)
        process(TEvTabletStateResponse, TabletInfo)
        process(TEvBSGroupStateResponse, BSGroupInfo)
        process(TEvSystemStateResponse, SystemInfo)
        process(TEvBridgeInfoResponse, BridgeInfo)
        process(TEvNodeStateResponse, NodeInfo)
        if (CountersInfo[nodeId].size() < CountersRequested) {
            for (ui32 _ : xrange(CountersRequested - CountersInfo[nodeId].size())) {
                NodeStateInfoReceived();
            }
            CountersInfo[nodeId].resize(CountersRequested);
        }
#undef process
    }

#define HandleWhiteboard(NAME, INFO) \
    void Handle(NNodeWhiteboard::TEvWhiteboard::NAME::TPtr& ev) { \
        ui64 nodeId = ev.Get()->Cookie; \
        INFO[nodeId] = std::move(ev->Get()->Record); \
        NodeStateInfoReceived(); \
    }

    HandleWhiteboard(TEvVDiskStateResponse, VDiskInfo)
    HandleWhiteboard(TEvPDiskStateResponse, PDiskInfo)
    HandleWhiteboard(TEvTabletStateResponse, TabletInfo)
    HandleWhiteboard(TEvBSGroupStateResponse, BSGroupInfo)
    HandleWhiteboard(TEvSystemStateResponse, SystemInfo)
    HandleWhiteboard(TEvBridgeInfoResponse, BridgeInfo)
    HandleWhiteboard(TEvNodeStateResponse, NodeInfo)

    void Handle(NNodeWhiteboard::TEvWhiteboard::TEvCountersInfoResponse::TPtr& ev) {
        ui64 nodeId = ev.Get()->Cookie;
        CountersInfo[nodeId].emplace_back(std::move(ev->Get()->Record.GetResponse()));
        NodeStateInfoReceived();
    }

    void NodeStateInfoReceived() {
        ++Received;
        if (Received == Requested && Period == 0) {
            ReplyAndPassAway();
        }
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        SelfCheck = std::move(ev->Get()->Result);
        NodeStateInfoReceived();
    }

    void RequestHealthCheck() {
        THolder<NHealthCheck::TEvSelfCheckRequest> request = MakeHolder<NHealthCheck::TEvSelfCheckRequest>();
        Send(NHealthCheck::MakeHealthCheckID(), request.Release());
        ++Requested;
    }

    void Bootstrap() {
        constexpr ui32 defaultDuration = 60;
        const TActorId nameserviceId = GetNameserviceActorId();
        Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        TBase::Become(&TThis::StateRequestedBrowse);

        Duration = GetProtoRequest()->duration();
        if (Duration == 0) {
            Duration = defaultDuration;
        }
        Started = TInstant::Now();
        Schedule(TDuration::Seconds(Duration), new TEvents::TEvWakeup());
    }

    void Timeout() {
        if (Period != 0) {
            for (const auto& ni : Nodes) {
                TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(ni.NodeId);
                Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvCountersInfoRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, ni.NodeId);
                Requested++;
            }
            CountersRequested++;
            Schedule(TDuration::Seconds(Period), new TEvents::TEvWakeup());
        }
        if (TInstant::Now() - Started >= TDuration::Seconds(Duration)) {
            ReplyAndPassAway();
        }
    }

    void Die(const TActorContext& ctx) override {
        for (const auto& ni : Nodes) {
            ctx.Send(TActivationContext::InterconnectProxy(ni.NodeId), new TEvents::TEvUnsubscribe());
        }
        TBase::Die(ctx);
    }

    STFUNC(StateRequestedBrowse) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvInterconnect::TEvNodesInfo, HandleBrowse);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
        }
    }

    STFUNC(StateRequestedNodeInfo) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvVDiskStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvPDiskStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvTabletStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvBSGroupStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvSystemStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvBridgeInfoResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvNodeStateResponse, Handle);
            hFunc(NNodeWhiteboard::TEvWhiteboard::TEvCountersInfoResponse, Handle);
            hFunc(TEvents::TEvUndelivered, Undelivered);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            cFunc(TEvents::TSystem::Wakeup, Timeout);
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
        }
    }

    void ReplyAndPassAway() {
        TResponse response;
        Ydb::Operations::Operation& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(Status);
        Ydb::Monitoring::ClusterStateResult result;
        TStringBuilder res;
        res << "{\n";
        auto serializeDict = [&](const char *name, auto &info) {
            res << "\"" << name <<"\" : {\n";
            for(auto &[k, v] : info) {
                TString data;
                google::protobuf::util::MessageToJsonString(v, &data);
                res << "{" << k << ":" << data << "},\n";
            }
            res << "}\n";
        };
        auto serializeArray = [&](auto &info) {
            res << "[";
            for (auto v : info) {
                res << v << ",\n";
            }
            res << "]";
        };
        auto serializeDictOfArray = [&](const char *name, auto &info) {
            res << "\"" << name <<"\" : {\n";
            for(auto &[k, v] : info) {
                res << "{" << k << ":";
                serializeArray(v);
                res << "},\n";
            }
            res << "}\n";
        };
        auto serialize = [&](const char *name, auto &info) {
            res << "\"" << name <<"\": ";
            TString data;
            google::protobuf::util::MessageToJsonString(info, &data);
            res << data << ",\n";
        };
        serializeDict("VDiskInfo", VDiskInfo);
        serializeDict("PDiskInfo", PDiskInfo);
        serializeDict("TabletInfo", TabletInfo);
        serializeDict("BSGroupInfo", BSGroupInfo);
        serializeDict("SystemInfo", SystemInfo);
        serializeDict("BridgeInfo", BridgeInfo);
        serializeDict("NodeInfo", NodeInfo);
        serialize("SelfCheck", SelfCheck);
        serializeDictOfArray("CountersInfo", CountersInfo);

        res << "\"version\": 1}\n";
        result.Setresult(res);
        operation.mutable_result()->PackFrom(result);
        return Reply(response);
    }
};

void DoClusterStateRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TClusterStateRPC(p.release()));
}
} // namespace NGRpcService
} // namespace NKikimr
