#include "worker_filter.h"

namespace NYql {

using namespace NDqs;

TWorkerFilter::TWorkerFilter(const Yql::DqsProto::TWorkerFilter& filter)
    : Filter(filter)
    , FullMatch(
        Filter.GetClusterName()
        || (Filter.GetAddress().size() > 0)
        || (Filter.GetNodeId().size() > 0)
        || Filter.GetRevision())
{
    for (const auto& address : Filter.GetAddress()) {
        Addresses.insert(address);
    }
    for (const auto& nodeId : Filter.GetNodeId()) {
        NodeIds.insert(nodeId);
    }
    for (const auto& nodeId : Filter.GetNodeIdHint()) {
        NodeIdHints.insert(nodeId);
    }
}

TWorkerFilter::EMatchStatus TWorkerFilter::Match(const TWorkerInfo::TPtr& workerInfo, int taskId, TStats* stats) const {
    bool allExists = true;
    bool partial = false;
    if (FullMatch) {
        if (Filter.GetClusterName() && workerInfo->ClusterName != Filter.GetClusterName()) {
            return EFAIL;
        }
        if (!Addresses.empty() && Addresses.find(workerInfo->Address) == Addresses.end()) {
            return EFAIL;
        }
        if (!NodeIds.empty() && NodeIds.find(workerInfo->NodeId) == NodeIds.end()) {
            return EFAIL;
        }
    }
    if (Filter.GetClusterNameHint() && workerInfo->ClusterName != Filter.GetClusterNameHint()) {
        partial = true;
    }
    if (!NodeIdHints.empty() && NodeIdHints.find(workerInfo->NodeId) == NodeIdHints.end()) {
        partial = true;
    }
    for (const auto& file : Filter.GetFile()) {
        const auto& id = file.GetObjectId();
        auto flag = workerInfo->GetResources().contains(id);
        allExists &= flag;
        if (stats) {
            if (flag) {
                (*stats->WaitingResources)[id].insert(taskId);
            } else {
                (*stats->WaitingResources)[id].erase(taskId);
                stats->Uploaded->find(id)->second.TryCount ++;
            }
        }
    }
    return allExists
        ? (partial?EPARTIAL:EOK)
        : EFAIL;
}

void TWorkerFilter::Visit(const std::function<void(const Yql::DqsProto::TFile&)>& visitor) const {
    for (const auto& file : Filter.GetFile()) {
        visitor(file);
    }
}

} // namespace NYql
