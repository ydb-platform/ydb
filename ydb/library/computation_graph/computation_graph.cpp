#include "computation_graph.h"

#include <algorithm>
#include <util/string/builder.h>

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

constexpr int NodeRadius  = 36;
constexpr int ColumnStep  = 200;
constexpr int RowStep     = 140;
constexpr int MarginX     = 60;
constexpr int MarginY     = 40;
constexpr int LabelOffset = 60;
constexpr int BadgeRadius = 11;

static TString XmlEscape(const TString& s) {
    TStringBuilder b;
    for (char c : s) {
        switch (c) {
            case '&': b << "&amp;";  break;
            case '<': b << "&lt;";   break;
            case '>': b << "&gt;";   break;
            case '"': b << "&quot;"; break;
            default:  b << c;        break;
        }
    }
    return b;
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

TString ToSvg(const TGraph& graph) {
    if (graph.Nodes.empty()) {
        return "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1\" height=\"1\"></svg>";
    }

    ui32 maxLevel = 0;
    for (const auto& n : graph.Nodes) {
        if (n.Level > maxLevel) {
            maxLevel = n.Level;
        }
    }

    TVector<ui32> levelCnt(maxLevel + 1, 0);
    TVector<ui32> nodeRow(graph.Nodes.size() + 1, 0);
    for (const auto& n : graph.Nodes) {
        nodeRow[n.Id] = levelCnt[n.Level]++;
    }
    ui32 maxRows = *std::max_element(levelCnt.begin(), levelCnt.end());

    int W = MarginX * 2 + (int)maxLevel * ColumnStep + 2 * NodeRadius;
    int H = MarginY * 2 + (int)maxRows * RowStep + LabelOffset;

    auto nodeX = [&](const TNode& n) { return MarginX + NodeRadius + (int)n.Level * ColumnStep; };
    auto nodeY = [&](const TNode& n) { return MarginY + NodeRadius + (int)nodeRow[n.Id] * RowStep; };

    TVector<const TNode*> byId(graph.Nodes.size() + 1, nullptr);
    for (const auto& n : graph.Nodes) {
        byId[n.Id] = &n;
    }

    TVector<const TNode*> sorted;
    sorted.reserve(graph.Nodes.size());
    for (const auto& n : graph.Nodes) {
        sorted.push_back(&n);
    }
    std::sort(sorted.begin(), sorted.end(), [&](const TNode* a, const TNode* b) {
        if (a->Level != b->Level) {
            return a->Level < b->Level;
        }
        return nodeRow[a->Id] < nodeRow[b->Id];
    });

    TStringBuilder b;
    b << "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"" << W << "\" height=\"" << H
      << "\" viewBox=\"0 0 " << W << " " << H << "\" font-family=\"sans-serif\">\n";
    b << "<defs><marker id=\"arrow\" markerWidth=\"10\" markerHeight=\"10\""
      << " refX=\"10\" refY=\"5\" orient=\"auto\">"
      << "<path d=\"M0,0 L10,5 L0,10 z\" fill=\"#9a9a9a\"/></marker></defs>\n";
    b << "<rect width=\"100%\" height=\"100%\" fill=\"#222222\"/>\n";

    for (const auto& lnk : graph.Links) {
        const TNode* src = byId[lnk.Source];
        const TNode* tgt = byId[lnk.Target];
        if (!src || !tgt) {
            continue;
        }
        int x1 = nodeX(*src) + NodeRadius;
        int y1 = nodeY(*src);
        int x2 = nodeX(*tgt) - NodeRadius;
        int y2 = nodeY(*tgt);
        b << "<line x1=\"" << x1 << "\" y1=\"" << y1
          << "\" x2=\"" << x2 << "\" y2=\"" << y2
          << "\" stroke=\"#9a9a9a\" stroke-width=\"1.5\" marker-end=\"url(#arrow)\"/>\n";
    }

    for (const TNode* np : sorted) {
        const TNode& n = *np;
        int x = nodeX(n), y = nodeY(n);
        if (n.Type == ENodeType::Input || n.Type == ENodeType::Output) {
            b << "<rect x=\"" << (x - NodeRadius) << "\" y=\"" << (y - NodeRadius)
              << "\" width=\"" << (2 * NodeRadius) << "\" height=\"" << (2 * NodeRadius)
              << "\" rx=\"8\" fill=\"#3a3a3a\" stroke=\"#6b6b6b\" stroke-width=\"2\"/>\n";
            b << "<path d=\"M" << (x - 13) << "," << (y - 11) << " h26 v22 h-26 z"
              << " M" << (x - 13) << "," << (y - 3) << " h26"
              << " M" << x << "," << (y - 11) << " v22\""
              << " fill=\"none\" stroke=\"#6b6b6b\" stroke-width=\"1.5\"/>\n";
        } else {
            TStringBuf fill   = (n.State == ENodeState::Pending) ? "#4a4a4a" : "#3f6b3f";
            TStringBuf stroke = (n.State == ENodeState::Pending) ? "#7a7a7a" : "#9ccc9c";
            b << "<circle cx=\"" << x << "\" cy=\"" << y << "\" r=\"" << NodeRadius
              << "\" fill=\"" << fill << "\" stroke=\"" << stroke << "\" stroke-width=\"2\"/>\n";
            if (n.Tasks > 0) {
                b << "<text x=\"" << x << "\" y=\"" << (y + 7)
                  << "\" text-anchor=\"middle\" font-size=\"22\" fill=\"#ffffff\">"
                  << n.Tasks << "</text>\n";
            }
            int bx = x + (int)(NodeRadius * 0.7);
            int by = y - (int)(NodeRadius * 0.7);
            if (n.State == ENodeState::Finished) {
                b << "<circle cx=\"" << bx << "\" cy=\"" << by << "\" r=\"" << BadgeRadius
                  << "\" fill=\"#222222\" stroke=\"#9ccc9c\" stroke-width=\"2\"/>\n";
                b << "<text x=\"" << bx << "\" y=\"" << (by + 5)
                  << "\" text-anchor=\"middle\" font-size=\"14\" fill=\"#9ccc9c\">&#x2713;</text>\n";
            } else if (n.State == ENodeState::Running) {
                b << "<circle cx=\"" << bx << "\" cy=\"" << by << "\" r=\"" << BadgeRadius
                  << "\" fill=\"#9ccc9c\"/>\n";
            }
        }
        b << "<text x=\"" << x << "\" y=\"" << (y + LabelOffset)
          << "\" text-anchor=\"middle\" font-size=\"16\" fill=\"#ffffff\">"
          << XmlEscape(n.Name) << "</text>\n";
    }

    b << "</svg>";
    return b;
}

} // namespace NKikimr::NComputationGraph
