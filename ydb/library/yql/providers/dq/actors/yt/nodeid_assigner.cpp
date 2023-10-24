#include "nodeid_assigner.h"

#include <yt/cpp/mapreduce/interface/node.h>
#include <yt/cpp/mapreduce/interface/client.h>
#include <yt/cpp/mapreduce/interface/common.h>

#include <util/system/env.h>
#include <util/generic/hash_set.h>

namespace NYql {

using namespace NYT;

ui32 AssignNodeId(const TAssignNodeIdOptions& options)
{
    Y_ABORT_UNLESS(!options.ClusterName.empty());
    Y_ABORT_UNLESS(!options.NodeName.empty());
    Y_ABORT_UNLESS(!options.Prefix.empty());
    Y_ABORT_UNLESS(!options.Role.empty());

    Y_ABORT_UNLESS(options.MinNodeId > 0);

    auto range = options.MaxNodeId - options.MinNodeId;

    Y_ABORT_UNLESS(range > 0);

    TString token = !options.Token.empty()
        ? options.Token
        : [] () {
            TString home = GetEnv("HOME");
            TString tokenFile = home + "/.yt/token";
            return TFileInput(tokenFile).ReadLine();
        } ();

    Y_ABORT_UNLESS(!token.empty());

    auto lockPath = options.Prefix + "/" + options.Role + "/lock";
    auto nodePath = options.Prefix + "/" + options.Role + "/" + options.NodeName;

    auto client = CreateClient(options.ClusterName, TCreateClientOptions().Token(token));
    // create std directories
    client->Create(options.Prefix, ENodeType::NT_MAP, TCreateOptions().IgnoreExisting(true).Recursive(true));
    client->Create(options.Prefix + "/" + options.Role, ENodeType::NT_MAP, TCreateOptions().IgnoreExisting(true));
    client->Create(options.Prefix + "/service_node", ENodeType::NT_MAP, TCreateOptions().IgnoreExisting(true));
    client->Create(options.Prefix + "/worker_node", ENodeType::NT_MAP, TCreateOptions().IgnoreExisting(true));
    client->Create(options.Prefix + "/operations", ENodeType::NT_MAP, TCreateOptions().IgnoreExisting(true));
    client->Create(options.Prefix + "/locks", ENodeType::NT_MAP, TCreateOptions().IgnoreExisting(true));

    if (options.NodeId) {
        // Use it for debug only
        return *options.NodeId;
    }

    if (client->Exists(lockPath) && client->Get(lockPath).GetType() != TNode::Int64) {
        client->Remove(lockPath);
    }
    client->Create(lockPath, ENodeType::NT_INT64, TCreateOptions().IgnoreExisting(true));
    auto transaction = client->StartTransaction(TStartTransactionOptions());
    transaction->Lock(lockPath, ELockMode::LM_EXCLUSIVE, TLockOptions().Waitable(true))->Wait();

    ui32 nodeId = client->Get(lockPath).AsInt64();
    if (nodeId == 0) {
        nodeId = options.MinNodeId;
    }

    auto listResult = transaction->List(
        options.Prefix + "/" + options.Role,
        TListOptions()
            .MaxSize(1<<18)
            .AttributeFilter(TAttributeFilter().AddAttribute(NCommonAttrs::ACTOR_NODEID_ATTR))
        );

    THashSet<ui32> allNodeIds;
    allNodeIds.reserve(listResult.size());
    for (const auto& node : listResult) {
        const auto& attributes = node.GetAttributes().AsMap();
        auto maybeNodeId = attributes.find(NCommonAttrs::ACTOR_NODEID_ATTR);
        if (maybeNodeId != attributes.end()) {
            auto nodeId = maybeNodeId->second.AsUint64();
            if (options.MinNodeId <= nodeId && nodeId < options.MaxNodeId) {
                Cerr << nodeId << " ";
                allNodeIds.insert(nodeId);
            }
        }
    }
    Cerr << Endl;

    Y_ABORT_UNLESS(allNodeIds.size() < range);

    while (allNodeIds.contains(nodeId)) {
        nodeId = options.MinNodeId + (nodeId + 1 - options.MinNodeId) % range;
    }

    if (options.NodeId.Defined()) {
        nodeId = options.NodeId.GetOrElse(nodeId);
    } else {
        auto nextNodeId = options.MinNodeId + (nodeId + 1 - options.MinNodeId) % range;
        transaction->Set(lockPath, TNode(nextNodeId));
    }

    Y_ABORT_UNLESS(options.MinNodeId <= nodeId && nodeId < options.MaxNodeId);

    // create new node
    transaction->Create(nodePath, NT_STRING, TCreateOptions().IgnoreExisting(true));

    for (const auto& [k, v] : options.Attributes) {
        transaction->Set(nodePath + "/@" + k, TNode(v), TSetOptions());
    }

    transaction->Set(nodePath + "/@" + NCommonAttrs::ACTOR_NODEID_ATTR, TNode(nodeId), TSetOptions());
    transaction->Commit();

    return nodeId;
}

} // namespace NYql
