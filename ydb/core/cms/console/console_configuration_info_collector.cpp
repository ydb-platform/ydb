#include "console_configuration_info_collector.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/nameservice.h> 
#include <ydb/core/cms/console/configs_dispatcher.h> 
#include <ydb/core/cms/console/configs_dispatcher_proxy.h> 
#include <ydb/core/util/stlog.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDBLOG_THIS_FILE_COMPONENT CMS_CONFIGS

namespace NKikimr::NConsole {

TConfigurationInfoCollector::TConfigurationInfoCollector(TActorId replyToActorId, bool listNodes)
    : ReplyToActorId(replyToActorId)
    , ListNodes(listNodes)
{
}

void TConfigurationInfoCollector::Bootstrap() {
    YDBLOG_DEBUG("Starting configuration info collection",
        {"Marker", "CIG1"});
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
        YDBLOG_DEBUG("Received empty node list from NameService",
            {"Marker", "CIG2"});
        ReplyAndDie();
        return;
    }

    TotalNodes = nodes.size();
    for (const auto& nodeInfo : nodes) {
        PendingNodes.insert(nodeInfo.NodeId);
        NodesInfo[nodeInfo.NodeId] = {nodeInfo.Host, nodeInfo.Port};
        Cerr << "Adding node " << nodeInfo.NodeId << " to pending nodes with endpoint " << nodeInfo.Host << ":" << nodeInfo.Port << Endl;
    }

    RequestNodeVersions();
}

void TConfigurationInfoCollector::RequestNodeVersions() {
    for (const auto& nodeId : PendingNodes) {
        Send(MakeConfigsDispatcherProxyID(nodeId),
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
            YDBLOG_DEBUG("Received unknown version '" << record.GetVersion() << "' from NodeId: " << nodeId,
                {"Marker", "CIG3"});
            UnknownNodes++;
            UnknownNodesList.push_back(nodeId);
        }

        if (PendingNodes.empty()) {
            ReplyAndDie();
        }
    } else {
        YDBLOG_WARN("Received unexpected TEvGetNodeConfigurationVersionResponse from NodeId: " << nodeId << " (sender: " << ev->Sender << ")",
            {"Marker", "CIG4"});
    }
}

void TConfigurationInfoCollector::Handle(TEvPrivate::TEvTimeout::TPtr &ev) {
    Y_UNUSED(ev);
    YDBLOG_WARN("Collection timed out. Missing responses from " << PendingNodes.size() << " nodes.",
        {"Marker", "CIG5"});
    UnknownNodes += PendingNodes.size();
    for (const auto& nodeId : PendingNodes) {
        UnknownNodesList.push_back(nodeId);
    }
    PendingNodes.clear();
    ReplyAndDie();
}

void TConfigurationInfoCollector::ReplyAndDie() {
    YDBLOG_DEBUG("Replying with collected info: V1=" << V1Nodes << ", V2=" << V2Nodes << ", Unknown=" << UnknownNodes << " (Total=" << TotalNodes << ")",
        {"Marker", "CIG6"});
    auto response = MakeHolder<TEvConsole::TEvGetConfigurationVersionResponse>(); 
    auto *result = response->Record.MutableResponse();
    result->set_v1_nodes(V1Nodes);
    result->set_v2_nodes(V2Nodes);
    result->set_unknown_nodes(UnknownNodes);

#define SET_NODES_LIST(version, upperVersion) \
    for (const auto& nodeId : upperVersion##NodesList) { \
        auto& nodeInfo = *result->add_##version##_nodes_list(); \
        nodeInfo.set_node_id(nodeId); \
        if (ListNodes) { \
            const auto& endpoint = nodeInfo.mutable_endpoint(); \
            Cerr << "Setting endpoint for node " << nodeId << " to " << NodesInfo[nodeId].Host << ":" << NodesInfo[nodeId].Port << Endl; \
            endpoint->set_hostname(NodesInfo[nodeId].Host); \
            endpoint->set_port(NodesInfo[nodeId].Port); \
        } \
    }

    SET_NODES_LIST(v1, V1)
    SET_NODES_LIST(v2, V2)
    SET_NODES_LIST(unknown, Unknown)

    Send(ReplyToActorId, response.Release());
    PassAway();
}

STFUNC(TConfigurationInfoCollector::StateWork) {
    switch (ev->GetTypeRewrite()) {
        hFunc(TEvInterconnect::TEvNodesInfo, Handle);
        hFunc(TEvConsole::TEvGetNodeConfigurationVersionResponse, Handle);
        hFunc(TEvPrivate::TEvTimeout, Handle);
        default:
            YDBLOG_DEBUG("Unhandled event type: " << ev->GetTypeRewrite() << " sender: " << ev->Sender,
                {"Marker", "CIG7"});
            break;
    }
}

IActor *CreateConfigurationInfoCollector(TActorId replyToActorId, bool listNodes) {
    return new TConfigurationInfoCollector(replyToActorId, listNodes);
}

} // namespace NKikimr::NConsole
