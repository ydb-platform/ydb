#include "console_configuration_info_collector.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/nameservice.h> 
#include <ydb/core/cms/console/configs_dispatcher.h> 
#include <ydb/core/util/stlog.h>

namespace NKikimr::NConsole {

TConfigurationInfoCollector::TConfigurationInfoCollector(TActorId replyToActorId, bool listNodes)
    : ReplyToActorId(replyToActorId)
    , ListNodes(listNodes)
{
}

void TConfigurationInfoCollector::Bootstrap() {
    STLOG(PRI_DEBUG, CMS_CONFIGS, CIG1, "Starting configuration info collection");
    Become(&TThis::StateWork);
    RequestNodeList();
    Schedule(Timeout, new TEvPrivate::TEvTimeout());
}

void TConfigurationInfoCollector::RequestNodeList() {
    Send(GetNameserviceActorId(), new TEvInterconnect::TEvListNodes());
}

void TConfigurationInfoCollector::Handle(TEvInterconnect::TEvNodesInfo::TPtr &ev) {
    auto &nodes = ev->Get()->Nodes;
    if (nodes.empty()) {
        STLOG(PRI_DEBUG, CMS_CONFIGS, CIG2, "Received empty node list from NameService");
        ReplyAndDie();
        return;
    }

    TotalNodes = nodes.size();
    for (const auto& nodeInfo : nodes) {
        PendingNodes[nodeInfo.NodeId] = {nodeInfo.Host, nodeInfo.Port};
    }

    RequestNodeVersions();
}

void TConfigurationInfoCollector::RequestNodeVersions() {
    for (ui32 nodeId : PendingNodes) {
        Send(MakeConfigsDispatcherID(nodeId),
             new TEvConsole::TEvGetNodeConfigurationVersionRequest());
    }
}

void TConfigurationInfoCollector::Handle(TEvConsole::TEvGetNodeConfigurationVersionResponse::TPtr &ev) {
    const auto& msg = ev->Get();
    const ui32 nodeId = ev->Sender.NodeId();
    const auto& record = msg->Record;

    if (PendingNodes.contains(nodeId)) {
        PendingNodes.erase(nodeId);
        if (record.GetVersion() == "v1") {
            V1Nodes++;
            V1NodesList.push_back(nodeId);
        } else if (record.GetVersion() == "v2") {
            V2Nodes++;
            V2NodesList.push_back(nodeId);
        } else {
            STLOG(PRI_DEBUG, CMS_CONFIGS, CIG3, "Received unknown version '" << record.GetVersion() << "' from NodeId: " << nodeId);
            UnknownNodes++;
            UnknownNodesList.push_back(nodeId);
        }

        if (PendingNodes.empty()) {
            ReplyAndDie();
        }
    } else {
        STLOG(PRI_WARN, CMS_CONFIGS, CIG4, "Received unexpected TEvGetNodeConfigurationVersionResponse from NodeId: " << nodeId << " (sender: " << ev->Sender << ")");
    }
}

void TConfigurationInfoCollector::Handle(TEvPrivate::TEvTimeout::TPtr &ev) {
    Y_UNUSED(ev);
    STLOG(PRI_WARN, CMS_CONFIGS, CIG5, "Collection timed out. Missing responses from " << PendingNodes.size() << " nodes.");
    UnknownNodes += PendingNodes.size();
    for (const auto& [nodeId, _] : PendingNodes) {
        UnknownNodesList.push_back(nodeId);
    }
    PendingNodes.clear();
    ReplyAndDie();
}

void TConfigurationInfoCollector::ReplyAndDie() {
    STLOG(PRI_DEBUG, CMS_CONFIGS, CIG6, "Replying with collected info: V1=" << V1Nodes << ", V2=" << V2Nodes << ", Unknown=" << UnknownNodes << " (Total=" << TotalNodes << ")");
    auto response = MakeHolder<TEvConsole::TEvGetConfigurationVersionResponse>(); 
    auto *result = response->Record.MutableResponse();
    result->set_v1_nodes(V1Nodes);
    result->set_v2_nodes(V2Nodes);
    result->set_unknown_nodes(UnknownNodes);

    auto convertToNodesList = [](const std::vector<ui32>& nodeIds) {
        std::vector<Ydb::DynamicConfig::NodeInfo> result;
        for (const auto& nodeId : nodeIds) {
            Ydb::DynamicConfig::NodeInfo nodeInfo;
            nodeInfo.set_node_id(nodeId);
            if (ListNodes) {
                const auto& endpoint = nodeInfo.mutable_endpoint();
                endpoint->set_host(PendingNodes[nodeId].Host);
                endpoint->set_port(PendingNodes[nodeId].Port);
            }
            result.push_back(nodeInfo);
        }
        return result;
    };

    result->set_v1_nodes_list(convertToNodesList(V1NodesList));
    result->set_v2_nodes_list(convertToNodesList(V2NodesList));
    result->set_unknown_nodes_list(convertToNodesList(UnknownNodesList));

    Send(ReplyToActorId, response.Release());
    PassAway();
}

STFUNC(TConfigurationInfoCollector::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);
        hFunc(TEvConsole::TEvGetNodeConfigurationVersionResponse, Handle);
        hFunc(TEvPrivate::TEvTimeout, Handle);
        default:
            STLOG(PRI_DEBUG, CMS_CONFIGS, CIG7, "Unhandled event type: " << ev->GetTypeRewrite() << " sender: " << ev->Sender);
            break;
    }
}

IActor *CreateConfigurationInfoCollector(TActorId replyToActorId, bool listNodes) {
    return new TConfigurationInfoCollector(replyToActorId, listNodes);
}

} // namespace NKikimr::NConsole
