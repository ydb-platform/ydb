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

#include <ydb/core/counters_info/counters_info.h>
#include <ydb/core/health_check/health_check.h>
#include <ydb/public/api/protos/ydb_monitoring.pb.h>
#include <ydb/core/protos/cluster_state_info.pb.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/blobstorage/nodewarden/node_warden_events.h>
#include <google/protobuf/util/json_util.h>

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

    TVector<ui32> NodeRequested;
    TVector<ui32> NodeReceived;
    ui32 Requested = 0;
    ui32 Received = 0;

    TVector<TVector<TString>> Counters;
    TVector<TEvInterconnect::TNodeInfo> Nodes;
    NKikimrClusterStateInfoProto::TClusterStateInfo State;
    TInstant Started;
    TDuration Duration;
    TDuration Period;

    void SendRequest(ui32 i) {
        ui32 nodeId = Nodes[i].NodeId;
        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(nodeId);
#define request(NAME) \
        Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::NAME(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, i); \
        NodeRequested[i]++;

        request(TEvVDiskStateRequest);
        request(TEvPDiskStateRequest);
        request(TEvTabletStateRequest);
        request(TEvBSGroupStateRequest);
        request(TEvSystemStateRequest);
        request(TEvBridgeInfoRequest);
        request(TEvNodeStateRequest);
#undef request
    }

    void HandleBrowse(TEvInterconnect::TEvNodesInfo::TPtr& ev) {
        RequestHealthCheck();
        RequestBaseConfig();
        Nodes = ev->Get()->Nodes;
        NodeReceived.resize(Nodes.size());
        NodeRequested.resize(Nodes.size());
        for (ui32 i : xrange(Nodes.size())) {
            const auto& ni = Nodes[i];
            auto* node = State.AddNodeInfos();
            node->SetNodeId(ni.NodeId);
            node->SetHost(ni.Host);
            node->SetPort(ni.Port);
            node->SetLocation(ni.Location.ToString());
            SendRequest(i);
        }
        Counters.resize(Nodes.size());
        RequestCounters();
        Period = TDuration::Seconds(GetProtoRequest()->period_seconds());
        if (Period > TDuration::Zero()) {
            Schedule(Period, new TEvents::TEvWakeup());
        }
        if (NodeRequested.size() > 0) {
            TBase::Become(&TThis::StateRequestedNodeInfo);
        } else {
            ReplyAndPassAway();
        }
    }

    void RequestBaseConfig() {
        Send(MakeBlobStorageNodeWardenID(SelfId().NodeId()), new NKikimr::NStorage::TEvNodeWardenQueryBaseConfig);
        Requested++;
    }

    void Handle(NKikimr::NStorage::TEvNodeWardenBaseConfig::TPtr ev) {
        State.MutableBaseConfig()->CopyFrom(ev->Get()->BaseConfig);
        ++Received;
        CheckReply();
    }

    void Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev) {
        ui32 nodeId = ev->Get()->NodeId;
        for (ui32 i : xrange(Nodes.size())) {
            if (Nodes[i].NodeId == nodeId) {
                NodeReceived[i] = NodeRequested[i];
                CheckReply();
                return;
            }
        }
    }

#define HandleWhiteboard(NAME, INFO) \
    void Handle(NNodeWhiteboard::TEvWhiteboard::NAME::TPtr& ev) { \
        ui32 idx = ev.Get()->Cookie; \
        State.MutableNodeInfos(idx)->Mutable##INFO()->CopyFrom(ev->Get()->Record); \
        NodeStateInfoReceived(idx); \
    }

    HandleWhiteboard(TEvVDiskStateResponse, VDiskInfo)
    HandleWhiteboard(TEvPDiskStateResponse, PDiskInfo)
    HandleWhiteboard(TEvTabletStateResponse, TabletInfo)
    HandleWhiteboard(TEvBSGroupStateResponse, BSGroupInfo)
    HandleWhiteboard(TEvSystemStateResponse, SystemInfo)
    HandleWhiteboard(TEvBridgeInfoResponse, BridgeInfo)
    HandleWhiteboard(TEvNodeStateResponse, NodeStateInfo)

    void Handle(NKikimr::NCountersInfo::TEvCountersInfoResponse::TPtr& ev) {
        ui32 idx = ev.Get()->Cookie;
        Counters[idx].push_back(std::move(ev->Get()->Record.GetResponse()));
        NodeStateInfoReceived(idx);
    }

    void NodeStateInfoReceived(ui32 idx) {
        NodeReceived[idx]++;
        CheckReply();
    }
    void CheckReply() {
        if (Period > TDuration::Zero() || Received < Requested) {
            return;
        }
        for (ui32 i : xrange(NodeRequested.size())) {
            if (NodeReceived[i] < NodeRequested[i]) {
                return;
            }
        }
        ReplyAndPassAway();
    }

    void Handle(NHealthCheck::TEvSelfCheckResult::TPtr& ev) {
        State.MutableSelfCheck()->CopyFrom(ev->Get()->Result);
        ++Received;
        CheckReply();
    }

    void RequestHealthCheck() {
        THolder<NHealthCheck::TEvSelfCheckRequest> request = MakeHolder<NHealthCheck::TEvSelfCheckRequest>();
        Send(NHealthCheck::MakeHealthCheckID(), request.Release());
        ++Requested;
    }

    void Bootstrap() {
        constexpr ui32 defaultDurationSec = 60;
        const TActorId nameserviceId = GetNameserviceActorId();
        Send(nameserviceId, new TEvInterconnect::TEvListNodes());
        TBase::Become(&TThis::StateRequestedBrowse);

        Duration = TDuration::Seconds(GetProtoRequest()->duration_seconds() ? GetProtoRequest()->duration_seconds() : defaultDurationSec);
        Started = TInstant::Now();
        Schedule(Duration, new TEvents::TEvWakeup());
    }

    void RequestCounters() {
        for (ui32 i : xrange(Nodes.size())) {
            const auto& ni = Nodes[i];
            TActorId countersInfoProviderServiceId = NKikimr::NCountersInfo::MakeCountersInfoProviderServiceID(ni.NodeId);
            Send(countersInfoProviderServiceId, new NKikimr::NCountersInfo::TEvCountersInfoRequest(), IEventHandle::FlagTrackDelivery | IEventHandle::FlagSubscribeOnSession, i);
            NodeRequested[i]++;
        }
    }
    void Wakeup() {
        if (Period > TDuration::Zero()) {
            RequestCounters();
            Schedule(Period, new TEvents::TEvWakeup());
        }
        if (TInstant::Now() - Started >= Duration) {
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
            cFunc(TEvents::TSystem::Wakeup, Wakeup);
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
            hFunc(NKikimr::NStorage::TEvNodeWardenBaseConfig, Handle);
            hFunc(NKikimr::NCountersInfo::TEvCountersInfoResponse, Handle);
            hFunc(TEvInterconnect::TEvNodeDisconnected, Disconnected);
            cFunc(TEvents::TSystem::Wakeup, Wakeup);
            hFunc(NHealthCheck::TEvSelfCheckResult, Handle);
        }
    }

    void ReplyAndPassAway() {
        TResponse response;
        Ydb::Operations::Operation& operation = *response.mutable_operation();
        operation.set_ready(true);
        operation.set_status(Ydb::StatusIds::SUCCESS);
        google::protobuf::util::JsonPrintOptions jsonOpts;
        jsonOpts.add_whitespace = true;
        TString data;
        google::protobuf::util::MessageToJsonString(State, &data, jsonOpts);
        Ydb::Monitoring::ClusterStateResult result;
        auto* block = result.Addblocks();
        block->Setname("cluster_state.json");
        block->Setcontent(data);

        for (ui32 node : xrange(Counters.size())) {
            for (ui32 i : xrange(Counters[node].size())) {
                auto* counterBlock = result.Addblocks();
                TStringBuilder sb;
                sb << "node_" << node << "_counters_" << i << ".json";
                counterBlock->Setname(sb);
                counterBlock->Setcontent(Counters[node][i]);
            }
        }
        operation.mutable_result()->PackFrom(result);
        return Reply(response);
    }
};

void DoClusterStateRequest(std::unique_ptr<IRequestOpCtx> p, const IFacilityProvider& f) {
    f.RegisterActor(new TClusterStateRPC(p.release()));
}
} // namespace NGRpcService
} // namespace NKikimr
