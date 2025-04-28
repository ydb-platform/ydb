#include "console_configuration_info_collector.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/core/base/nameservice.h> 
#include <ydb/core/cms/console/configs_dispatcher.h> 
#include <ydb/core/util/stlog.h>

namespace NKikimr::NConsole {

TConfigurationInfoCollector::TConfigurationInfoCollector(TActorId replyToActorId)
    : ReplyToActorId(replyToActorId)
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
        PendingNodes.insert(nodeInfo.NodeId);
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
        } else if (record.GetVersion() == "v2") {
            V2Nodes++;
        } else {
            STLOG(PRI_DEBUG, CMS_CONFIGS, CIG3, "Received unknown version '" << record.GetVersion() << "' from NodeId: " << nodeId);
            UnknownNodes++;
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

IActor *CreateConfigurationInfoCollector(TActorId replyToActorId) {
    return new TConfigurationInfoCollector(replyToActorId);
}

} // namespace NKikimr::NConsole
