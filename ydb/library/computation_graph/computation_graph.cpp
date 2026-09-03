#include "computation_graph.h"

namespace NKikimr::NComputationGraph {

namespace {

static ui64 AggrSum(const NJson::TJsonValue& stats, TStringBuf key) {
    if (!stats.Has(key)) {
        return 0;
    }
    const auto& v = stats[key];
    if (!v.IsMap() || !v.Has("Sum")) {
        return 0;
    }
    return v["Sum"].GetUInteger();
}

struct TBuilder {
    TGraph Graph;

    ui32 NewNode(TString name, ENodeType type) {
        TNode node;
        node.Id = (ui32)Graph.Nodes.size() + 1;
        node.Name = std::move(name);
        node.Type = type;
        Graph.Nodes.push_back(std::move(node));
        return Graph.Nodes.back().Id;
    }

    TNode& NodeById(ui32 id) {
        return Graph.Nodes[id - 1];
    }

    void Link(ui32 src, ui32 dst) {
        Graph.Links.push_back({src, dst});
    }

    void ApplyStats(TNode& node, const NJson::TJsonValue& planNode) {
        if (!planNode.Has("Stats")) {
            return;
        }
        const auto& stats = planNode["Stats"];
        if (!stats.IsMap()) {
            return;
        }
        ui32 tasks = (ui32)stats["Tasks"].GetUInteger();
        ui32 finished = (ui32)stats["FinishedTasks"].GetUInteger();
        node.Tasks = tasks;
        node.FinishedTasks = finished;
        node.State = (tasks == 0) ? ENodeState::Pending
                   : (finished == tasks) ? ENodeState::Finished
                   : ENodeState::Running;
        node.PhysicalStageId = (ui32)stats["PhysicalStageId"].GetUInteger();
        node.UpdateTimeMs = stats["UpdateTimeMs"].GetUInteger();
        node.Stats.IngressRows = AggrSum(stats, "IngressRows");
        node.Stats.IngressBytes = AggrSum(stats, "IngressBytes");
        node.Stats.EgressRows = AggrSum(stats, "EgressRows");
        node.Stats.EgressBytes = AggrSum(stats, "EgressBytes");
        node.Stats.InputRows = AggrSum(stats, "InputRows");
        node.Stats.InputBytes = AggrSum(stats, "InputBytes");
        node.Stats.OutputRows = AggrSum(stats, "OutputRows");
        node.Stats.OutputBytes = AggrSum(stats, "OutputBytes");
        node.Stats.CpuTimeUs = AggrSum(stats, "CpuTimeUs");
    }

    // Returns created op-node id (0 for transparent nodes).
    ui32 Visit(const NJson::TJsonValue& planNode, ui32 parentId) {
        if (!planNode.IsMap()) {
            return 0;
        }
        TString planNodeType = planNode["PlanNodeType"].GetString();
        if (planNodeType == "Query" || planNodeType == "Connection" || planNodeType == "Materialize") {
            if (planNode.Has("Plans") && planNode["Plans"].IsArray()) {
                for (const auto& child : planNode["Plans"].GetArray()) {
                    Visit(child, parentId);
                }
            }
            return 0;
        }
        if (planNodeType == "ResultSet") {
            TString rsName = "ResultSet";
            if (planNode.Has("Name")) {
                rsName += " (" + planNode["Name"].GetString() + ")";
            }
            ui32 id = NewNode(rsName, ENodeType::Output);
            if (planNode.Has("Plans") && planNode["Plans"].IsArray()) {
                for (const auto& child : planNode["Plans"].GetArray()) {
                    Visit(child, id);
                }
            }
            return id;
        }
        TString name = planNode["Node Type"].GetString();
        ui32 id = NewNode(name, ENodeType::Operation);
        ApplyStats(NodeById(id), planNode);
        if (planNode.Has("Operators") && planNode["Operators"].IsArray()) {
            for (const auto& op : planNode["Operators"].GetArray()) {
                if (!op.IsMap()) {
                    continue;
                }
                if (op.Has("SourceType")) {
                    TString inName = op.Has("Name") ? op["Name"].GetString() : op["SourceType"].GetString();
                    ui32 inId = NewNode(inName, ENodeType::Input);
                    Link(inId, id);
                }
                if (op.Has("SinkType")) {
                    TString outName = op.Has("Name") ? op["Name"].GetString() : op["SinkType"].GetString();
                    ui32 outId = NewNode(outName, ENodeType::Output);
                    Link(id, outId);
                }
            }
        }
        if (parentId != 0) {
            Link(id, parentId);
        }
        if (planNode.Has("Plans") && planNode["Plans"].IsArray()) {
            for (const auto& child : planNode["Plans"].GetArray()) {
                Visit(child, id);
            }
        }
        return id;
    }

    void ComputeLevels() {
        ui32 n = (ui32)Graph.Nodes.size();
        if (n == 0) {
            return;
        }
        TVector<ui32> inDeg(n + 1, 0);
        TVector<TVector<ui32>> adj(n + 1);
        for (const auto& lnk : Graph.Links) {
            inDeg[lnk.Target]++;
            adj[lnk.Source].push_back(lnk.Target);
        }
        TVector<ui32> level(n + 1, 0);
        TVector<ui32> q;
        q.reserve(n);
        for (ui32 i = 1; i <= n; ++i) {
            if (inDeg[i] == 0) {
                q.push_back(i);
            }
        }
        for (int head = 0; head < (int)q.size(); ++head) {
            ui32 u = q[head];
            for (ui32 v : adj[u]) {
                if (level[v] < level[u] + 1) {
                    level[v] = level[u] + 1;
                }
                if (--inDeg[v] == 0) {
                    q.push_back(v);
                }
            }
        }
        for (auto& node : Graph.Nodes) {
            node.Level = level[node.Id];
        }
    }
};

static TStringBuf NodeTypeStr(ENodeType t) {
    switch (t) {
        case ENodeType::Input:     return "in";
        case ENodeType::Output:    return "out";
        case ENodeType::Operation: return "op";
    }
    return "op";
}

static TStringBuf NodeStateStr(ENodeState s) {
    switch (s) {
        case ENodeState::Pending:  return "Pending";
        case ENodeState::Running:  return "Running";
        case ENodeState::Finished: return "Finished";
    }
    return "Pending";
}

} // namespace

TGraph BuildGraph(const NJson::TJsonValue& doc) {
    if (!doc.IsMap()) {
        return {};
    }
    const NJson::TJsonValue* root = nullptr;
    if (doc.Has("Plan") && doc["Plan"].IsMap()) {
        root = &doc["Plan"];
    } else if (doc.Has("Node Type")) {
        root = &doc;
    } else {
        return {};
    }
    if (!root->Has("Plans") || !(*root)["Plans"].IsArray()) {
        return {};
    }
    TBuilder b;
    for (const auto& child : (*root)["Plans"].GetArray()) {
        b.Visit(child, 0);
    }
    b.ComputeLevels();
    return b.Graph;
}

NJson::TJsonValue ToJson(const TGraph& graph) {
    NJson::TJsonValue result(NJson::JSON_MAP);
    NJson::TJsonValue nodes(NJson::JSON_ARRAY);
    NJson::TJsonValue links(NJson::JSON_ARRAY);
    for (const auto& node : graph.Nodes) {
        NJson::TJsonValue jn(NJson::JSON_MAP);
        jn["id"] = node.Id;
        jn["level"] = node.Level;
        jn["name"] = node.Name;
        jn["type"] = TString(NodeTypeStr(node.Type));
        if (node.Type == ENodeType::Operation) {
            jn["state"] = TString(NodeStateStr(node.State));
            jn["tasks"] = node.Tasks;
            jn["finishedTasks"] = node.FinishedTasks;
            jn["physicalStageId"] = node.PhysicalStageId;
            jn["updateTimeMs"] = node.UpdateTimeMs;
            NJson::TJsonValue stats(NJson::JSON_MAP);
            stats["ingressRows"] = node.Stats.IngressRows;
            stats["ingressBytes"] = node.Stats.IngressBytes;
            stats["egressRows"] = node.Stats.EgressRows;
            stats["egressBytes"] = node.Stats.EgressBytes;
            stats["inputRows"] = node.Stats.InputRows;
            stats["inputBytes"] = node.Stats.InputBytes;
            stats["outputRows"] = node.Stats.OutputRows;
            stats["outputBytes"] = node.Stats.OutputBytes;
            stats["cpuTimeUs"] = node.Stats.CpuTimeUs;
            jn["stats"] = std::move(stats);
        }
        nodes.AppendValue(std::move(jn));
    }
    for (const auto& lnk : graph.Links) {
        NJson::TJsonValue jl(NJson::JSON_MAP);
        jl["source"] = lnk.Source;
        jl["target"] = lnk.Target;
        links.AppendValue(std::move(jl));
    }
    result["nodes"] = std::move(nodes);
    result["links"] = std::move(links);
    return result;
}

} // namespace NKikimr::NComputationGraph
