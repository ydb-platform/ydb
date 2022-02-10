#include "msgbus_server_pq_read_session_info.h"

namespace NKikimr {
namespace NMsgBusProxy {

IPersQueueGetReadSessionsInfoWorker::IPersQueueGetReadSessionsInfoWorker(
    const TActorId& parentId,
    const THashMap<TString, TActorId>& readSessions,
    std::shared_ptr<const TPersQueueBaseRequestProcessor::TNodesInfo> nodesInfo
)
    : ParentId(parentId)
    , ReadSessions(std::move(readSessions))
    , NodesInfo(nodesInfo)
{ }

TString IPersQueueGetReadSessionsInfoWorker::GetHostName(ui32 hostId) const {
    const auto host = NodesInfo->HostNames.find(hostId);
    return host != NodesInfo->HostNames.end() ? host->second : TString();
}

void IPersQueueGetReadSessionsInfoWorker::Bootstrap(const TActorContext& ctx) {
    Become(&IPersQueueGetReadSessionsInfoWorker::StateFunc);

    for (auto & r : ReadSessions) {
        SendStatusRequest(r.first, r.second, ctx);
        ReadSessionNames.insert(r.first);
    }
}

void IPersQueueGetReadSessionsInfoWorker::ProcessStatus(const NKikimrPQ::TReadSessionStatusResponse& response, const TActorContext& ctx) {
    auto it = ReadSessions.find(response.GetSession());
    if (it == ReadSessions.end())
        return;
    ReadSessions.erase(it);

    ReadSessionStatus[response.GetSession()] = response;

    ReadSessionNames.erase(response.GetSession());

    if (ReadyToAnswer()) {
        Answer(ctx);
    }
}

void IPersQueueGetReadSessionsInfoWorker::Undelivered(TEvents::TEvUndelivered::TPtr &ev, const TActorContext &ctx) {
    for (auto &s: ReadSessions) {
        if (s.second == ev->Sender) {
            ReadSessions.erase(s.first);
            return;
        }
    }

    if (ReadyToAnswer()) {
        Answer(ctx);
    }
}

void IPersQueueGetReadSessionsInfoWorker::Disconnected(TEvInterconnect::TEvNodeDisconnected::TPtr &ev, const TActorContext &ctx) {
    ui32 nodeId = ev->Get()->NodeId;
    for (auto it = ReadSessions.begin(); it != ReadSessions.end();) {
        auto jt = it++;
        if (jt->second.NodeId() == nodeId) {
            ReadSessions.erase(jt);
        }
    }
    if (ReadyToAnswer()) {
        Answer(ctx);
    }

}

bool IPersQueueGetReadSessionsInfoWorker::ReadyToAnswer() const {
    return ReadSessions.empty();
}

void IPersQueueGetReadSessionsInfoWorker::Answer(const TActorContext& ctx) {
    NKikimrClient::TResponse response;
    response.SetStatus(MSTATUS_OK);
    response.SetErrorCode(NPersQueue::NErrorCode::OK);
    auto stat = response.MutableMetaResponse()->MutableCmdGetReadSessionsInfoResult();

    for (const auto& s : ReadSessionStatus) {
        auto res = stat->AddSessionResult();
        res->SetSession(s.second.GetSession());
        res->SetClientNode(s.second.GetClientNode());
        res->SetProxyNode(GetHostName(s.second.GetProxyNodeId()));
        res->SetTimestamp(s.second.GetTimestamp());

        for (const auto& p : s.second.GetPartition()) {
            auto partRes = res->AddPartitionResult();
            partRes->SetTopic(p.GetTopic());
            partRes->SetPartition(p.GetPartition());

            for (const auto& c : p.GetNextCommits()) {
                partRes->AddNextCommits(c);
            }
            partRes->SetLastReadId(p.GetLastReadId());
            partRes->SetReadIdCommitted(p.GetReadIdCommitted());

            partRes->SetTimestamp(p.GetTimestampMs());
        }
    }

    THolder<TEvPersQueue::TEvResponse> result(new TEvPersQueue::TEvResponse());
    result->Record.Swap(&response);

    ctx.Send(ParentId, result.Release());

    Die(ctx);
}

} // namespace NMsgBusProxy
} // namespace NKikimr
