#include "computation_graph_renderer.h"

#include <algorithm>
#include <util/string/builder.h>

namespace NKikimr::NComputationGraphRenderer {

namespace {

struct TBuilder {
    TGraph Graph;

    ui32 NewNode(TString name, ENodeType type) {
        TNode node;
        node.Id = static_cast<ui32>(Graph.Nodes.size()) + 1;
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
        ui32 tasks = static_cast<ui32>(stats["Tasks"].GetUInteger());
        ui32 finished = static_cast<ui32>(stats["FinishedTasks"].GetUInteger());
        node.Tasks = tasks;
        node.FinishedTasks = finished;
        node.State = (tasks == 0) ? ENodeState::Pending
                   : (finished == tasks) ? ENodeState::Finished
                   : ENodeState::Running;
    }

    ui32 Visit(const NJson::TJsonValue& planNode, ui32 parentId) {
        if (!planNode.IsMap()) {
            return 0;
        }
        TStringBuf planNodeType = planNode["PlanNodeType"].GetString();
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
            ui32 id = NewNode(std::move(rsName), ENodeType::Output);
            if (planNode.Has("Plans") && planNode["Plans"].IsArray()) {
                for (const auto& child : planNode["Plans"].GetArray()) {
                    Visit(child, id);
                }
            }
            return id;
        }
        TString name = planNode["Node Type"].GetString();
        ui32 id = NewNode(std::move(name), ENodeType::Operation);
        ApplyStats(NodeById(id), planNode);
        if (planNode.Has("Operators") && planNode["Operators"].IsArray()) {
            for (const auto& op : planNode["Operators"].GetArray()) {
                if (!op.IsMap()) {
                    continue;
                }
                if (op.Has("SourceType")) {
                    TString inName = op.Has("Name") ? op["Name"].GetString() : op["SourceType"].GetString();
                    ui32 inId = NewNode(std::move(inName), ENodeType::Input);
                    Link(inId, id);
                }
                if (op.Has("SinkType")) {
                    TString outName = op.Has("Name") ? op["Name"].GetString() : op["SinkType"].GetString();
                    ui32 outId = NewNode(std::move(outName), ENodeType::Output);
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
        ui32 n = static_cast<ui32>(Graph.Nodes.size());
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
        for (ui32 head = 0; head < q.size(); ++head) {
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

constexpr int NodeRadius    = 36;
constexpr int ColumnStep    = 200;
constexpr int RowStep       = 140;
constexpr int MarginX       = 60;
constexpr int MarginY       = 40;
constexpr int LabelOffset   = 60;
constexpr int BadgeRadius   = 11;
constexpr int BadgeOffset   = 25;
constexpr int FontSizeTask  = 22;
constexpr int FontSizeBadge = 14;
constexpr int FontSizeLabel = 16;
constexpr int TextBaselineTask  = 7;
constexpr int TextBaselineBadge = 5;
constexpr int CornerRadius  = 8;
constexpr int ArrowSize     = 10;
constexpr int GlyphHalfW    = 13;
constexpr int GlyphHalfH    = 11;
constexpr int GlyphHeaderH  = 8;

using namespace std::string_view_literals;

constexpr TStringBuf StrokeThin       = "1.5"sv;
constexpr TStringBuf StrokeThick      = "2"sv;
constexpr TStringBuf ColBackground    = "#222222"sv;
constexpr TStringBuf ColEdge          = "#9a9a9a"sv;
constexpr TStringBuf ColIoFill        = "#3a3a3a"sv;
constexpr TStringBuf ColIoBorder      = "#6b6b6b"sv;
constexpr TStringBuf ColPendingFill   = "#4a4a4a"sv;
constexpr TStringBuf ColPendingStr    = "#7a7a7a"sv;
constexpr TStringBuf ColOpActiveFill  = "#3f6b3f"sv;
constexpr TStringBuf ColOpActiveStr   = "#9ccc9c"sv;
constexpr TStringBuf ColText          = "#ffffff"sv;

TString XmlEscape(TStringBuf s) {
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

    ui32 maxId = 0;
    for (const auto& n : graph.Nodes) {
        if (n.Id > maxId) {
            maxId = n.Id;
        }
    }

    TVector<ui32> levelCnt(maxLevel + 1, 0);
    TVector<ui32> nodeRow(maxId + 1, 0);
    for (const auto& n : graph.Nodes) {
        nodeRow[n.Id] = levelCnt[n.Level]++;
    }
    ui32 maxRows = *std::max_element(levelCnt.begin(), levelCnt.end());

    int W = MarginX * 2 + static_cast<int>(maxLevel) * ColumnStep + 2 * NodeRadius;
    int H = MarginY * 2 + static_cast<int>(maxRows) * RowStep + LabelOffset;

    auto nodeX = [&](const TNode& n) { return MarginX + NodeRadius + static_cast<int>(n.Level) * ColumnStep; };
    auto nodeY = [&](const TNode& n) { return MarginY + NodeRadius + static_cast<int>(nodeRow[n.Id]) * RowStep; };

    TVector<const TNode*> byId(maxId + 1, nullptr);
    for (const auto& n : graph.Nodes) {
        byId[n.Id] = &n;
    }

    TStringBuilder b;
    b << "<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"" << W << "\" height=\"" << H
      << "\" viewBox=\"0 0 " << W << " " << H << "\" font-family=\"sans-serif\">\n";
    b << "<defs><marker id=\"arrow\" markerWidth=\"" << ArrowSize << "\" markerHeight=\"" << ArrowSize
      << "\" refX=\"" << ArrowSize << "\" refY=\"" << (ArrowSize / 2) << "\" orient=\"auto\">"
      << "<path d=\"M0,0 L" << ArrowSize << "," << (ArrowSize / 2) << " L0," << ArrowSize << " z\" fill=\""
      << ColEdge << "\"/></marker></defs>\n";
    b << "<rect width=\"100%\" height=\"100%\" fill=\"" << ColBackground << "\"/>\n";

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
          << "\" stroke=\"" << ColEdge << "\" stroke-width=\"" << StrokeThin << "\" marker-end=\"url(#arrow)\"/>\n";
    }

    for (const auto& n : graph.Nodes) {
        int x = nodeX(n), y = nodeY(n);
        if (n.Type == ENodeType::Input || n.Type == ENodeType::Output) {
            b << "<rect x=\"" << (x - NodeRadius) << "\" y=\"" << (y - NodeRadius)
              << "\" width=\"" << (2 * NodeRadius) << "\" height=\"" << (2 * NodeRadius)
              << "\" rx=\"" << CornerRadius << "\" fill=\"" << ColIoFill << "\" stroke=\"" << ColIoBorder
              << "\" stroke-width=\"" << StrokeThick << "\"/>\n";
            b << "<path d=\"M" << (x - GlyphHalfW) << "," << (y - GlyphHalfH)
              << " h" << (2 * GlyphHalfW) << " v" << (2 * GlyphHalfH) << " h-" << (2 * GlyphHalfW) << " z"
              << " M" << (x - GlyphHalfW) << "," << (y - GlyphHalfH + GlyphHeaderH) << " h" << (2 * GlyphHalfW)
              << " M" << x << "," << (y - GlyphHalfH) << " v" << (2 * GlyphHalfH) << "\""
              << " fill=\"none\" stroke=\"" << ColIoBorder << "\" stroke-width=\"" << StrokeThin << "\"/>\n";
        } else {
            TStringBuf fill   = (n.State == ENodeState::Pending) ? ColPendingFill : ColOpActiveFill;
            TStringBuf stroke = (n.State == ENodeState::Pending) ? ColPendingStr  : ColOpActiveStr;
            b << "<circle cx=\"" << x << "\" cy=\"" << y << "\" r=\"" << NodeRadius
              << "\" fill=\"" << fill << "\" stroke=\"" << stroke << "\" stroke-width=\"" << StrokeThick << "\"/>\n";
            if (n.Tasks > 0) {
                b << "<text x=\"" << x << "\" y=\"" << (y + TextBaselineTask)
                  << "\" text-anchor=\"middle\" font-size=\"" << FontSizeTask << "\" fill=\"" << ColText << "\">"
                  << n.Tasks << "</text>\n";
            }
            int bx = x + BadgeOffset;
            int by = y - BadgeOffset;
            if (n.State == ENodeState::Finished) {
                b << "<circle cx=\"" << bx << "\" cy=\"" << by << "\" r=\"" << BadgeRadius
                  << "\" fill=\"" << ColBackground << "\" stroke=\"" << ColOpActiveStr << "\" stroke-width=\"" << StrokeThick << "\"/>\n";
                b << "<text x=\"" << bx << "\" y=\"" << (by + TextBaselineBadge)
                  << "\" text-anchor=\"middle\" font-size=\"" << FontSizeBadge << "\" fill=\"" << ColOpActiveStr << "\">&#x2713;</text>\n";
            } else if (n.State == ENodeState::Running) {
                b << "<circle cx=\"" << bx << "\" cy=\"" << by << "\" r=\"" << BadgeRadius
                  << "\" fill=\"" << ColOpActiveStr << "\"/>\n";
            }
        }
        b << "<text x=\"" << x << "\" y=\"" << (y + LabelOffset)
          << "\" text-anchor=\"middle\" font-size=\"" << FontSizeLabel << "\" fill=\"" << ColText << "\">"
          << XmlEscape(n.Name) << "</text>\n";
    }

    b << "</svg>";
    return b;
}

} // namespace NKikimr::NComputationGraphRenderer
