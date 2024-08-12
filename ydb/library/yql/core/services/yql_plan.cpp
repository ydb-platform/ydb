#include "yql_plan.h"
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <util/generic/map.h>

namespace NYql {

namespace {

struct TPinAttrs {
    TPinInfo Info;
    ui32 ProviderId = 0;
    ui32 PinId = 0;

    TPinAttrs(const TPinInfo& info)
        : Info(info)
    {}
};

struct TNodeInfo {
    ui64 NodeId;
    const TExprNode* const Node;
    IDataProvider* Provider;
    bool IsVisible;
    TExprNode::TListType Dependencies;
    TVector<TPinAttrs> Inputs;
    TVector<TPinAttrs> Outputs;
    ui32 InputsCount = 0;
    ui32 OutputsCount = 0;

    TNodeInfo(ui64 nodeId, const TExprNode* node)
        : NodeId(nodeId)
        , Node(node)
        , Provider(nullptr)
        , IsVisible(true)
    {}
};

struct TBasicNode {
    ui64 Id = 0;
    ui32 Level = 0;
    enum class EType : ui32 {
        Unknown,
        Operation,
        Input,
        Output
    };

    TString DisplayName;
    EType Type = EType::Unknown;
};

struct TPinKey {
    ui32 ProviderId;
    ui32 PinId;
    TBasicNode::EType Type;

    bool operator==(const TPinKey& other) const {
        return ProviderId == other.ProviderId && PinId == other.PinId && Type == other.Type;
    }

    size_t Hash() const {
        return CombineHashes(CombineHashes(IntHash(ProviderId), IntHash(PinId)), IntHash(ui32(Type)));
    }

    struct THash {
        size_t operator()(const TPinKey& x) const {
            return x.Hash();
        }
    };
};

struct TBasicLink {
    const ui64 Source;
    const ui64 Target;

    TBasicLink(ui64 source, ui64 target)
        : Source(source)
        , Target(target)
    {}
};

struct TLevelContext {
    TBasicNode* const Node;
    TVector<ui64> Inputs;
    TVector<ui64> Outputs;

    TLevelContext()
        : Node(nullptr)
    {}

    TLevelContext(TBasicNode* node)
        : Node(node)
    {}
};

struct TProviderInfo {
    ui32 Id;
    const TExprNode* Node;
    IDataProvider* Provider;
    TNodeMap<ui32> Pin;
    std::vector<const TExprNode*> PinOrder;

    TProviderInfo(ui32 id, const TExprNode* node, IDataProvider* provider)
        : Id(id)
        , Node(node)
        , Provider(provider)
    {}
};

using TProviderInfoMap = TMap<TString, TProviderInfo>;

void WriteProviders(const TString& tag, const TProviderInfoMap& providers, NYson::TYsonWriter& writer) {
    writer.OnKeyedItem(tag);
    writer.OnBeginList();
    for (auto& p : providers) {
        writer.OnListItem();
        writer.OnBeginMap();
        writer.OnKeyedItem("Id");
        writer.OnUint64Scalar(p.second.Id);
        writer.OnKeyedItem("Name");
        writer.OnStringScalar(p.second.Node->Child(0)->Content());
        p.second.Provider->GetPlanFormatter().WriteDetails(*p.second.Node, writer);
        writer.OnKeyedItem("Pins");
        writer.OnBeginList();
        for (auto pin : p.second.PinOrder) {
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("Id");
            writer.OnUint64Scalar(p.second.Pin.find(pin)->second);
            p.second.Provider->GetPlanFormatter().WritePinDetails(*pin, writer);
            writer.OnEndMap();
        }

        writer.OnEndList();
        writer.OnEndMap();
    }

    writer.OnEndList();
}

ui32 FillLevels(THashMap<ui32, TLevelContext>& basicNodesMap, ui32 current, THashSet<ui32>& visited) {
    if (visited.contains(current)) {
        return 0;
    }

    auto findPtr = basicNodesMap.FindPtr(current);
    YQL_ENSURE(findPtr);
    auto& ctx = *findPtr;
    if (ctx.Node->Level) {
        return ctx.Node->Level;
    }

    visited.insert(current);
    ui32 maxLevel = 0;
    bool hasOutputs = false;
    for (auto& child : ctx.Inputs) {
        auto findPtr = basicNodesMap.FindPtr(child);
        YQL_ENSURE(findPtr);
        auto& childCtx = *findPtr;
        hasOutputs = hasOutputs || !childCtx.Outputs.empty();
        maxLevel = Max(maxLevel, FillLevels(basicNodesMap, child, visited));
    }

    ctx.Node->Level = maxLevel + (hasOutputs ? 2 : 1);
    for (auto& child : ctx.Outputs) {
        auto findPtr = basicNodesMap.FindPtr(child);
        YQL_ENSURE(findPtr);
        auto& childCtx = *findPtr;
        childCtx.Node->Level = ctx.Node->Level + 1;
    }

    visited.erase(current);
    return ctx.Node->Level;
}

}

class TPlanBuilder : public IPlanBuilder {
public:
    TPlanBuilder(TTypeAnnotationContext& types)
        : Types_(types)
    {}

    void WritePlan(NYson::TYsonWriter& writer, const TExprNode::TPtr& root, const TPlanSettings& settings) override {
        if (!root) {
            return;
        }

        TNodeMap<TNodeInfo> nodes;
        TExprNode::TListType order;
        TProviderInfoMap providers;

        writer.OnBeginMap();
        writer.OnKeyedItem("Detailed");
        writer.OnBeginMap();
        writer.OnKeyedItem("Operations");
        writer.OnBeginList();
        VisitNode(root, nodes, order);
        ui32 lastId = 0;
        TVector<TBasicNode> basicNodes;
        TVector<TBasicLink> basicLinks;
        TMap<TString, ui32> opStats;
        for (auto node : order) {
            auto& info = nodes.find(node.Get())->second;
            if (!info.IsVisible) {
                continue;
            }

            lastId = info.NodeId;
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("Id");
            writer.OnUint64Scalar(info.NodeId);
            writer.OnKeyedItem("Name");
            writer.OnStringScalar(node->Content());
            opStats[TString(node->Content())] += 1;
            if (info.Provider) {
                TVector<TPinInfo> inputs;
                TVector<TPinInfo> outputs;
                info.InputsCount = info.Provider->GetPlanFormatter().GetInputs(*node, inputs, settings.WithLimits);
                info.OutputsCount = info.Provider->GetPlanFormatter().GetOutputs(*node, outputs, settings.WithLimits);
                if (info.InputsCount) {
                    writer.OnKeyedItem("InputsCount");
                    writer.OnUint64Scalar(info.InputsCount);
                }

                if (info.OutputsCount) {
                    writer.OnKeyedItem("OutputsCount");
                    writer.OnUint64Scalar(info.OutputsCount);
                }

                WritePins("Inputs", inputs, writer, info.Inputs, providers);
                WritePins("Outputs", outputs, writer, info.Outputs, providers);
                info.Provider->GetPlanFormatter().WritePlanDetails(*info.Node, writer, settings.WithLimits);
            }

            TSet<ui64> dependsOn;
            for (auto child : info.Dependencies) {
                GatherDependencies(*child, nodes, dependsOn);
            }

            if (!dependsOn.empty()) {
                writer.OnKeyedItem("DependsOn");
                writer.OnBeginList();
                for (auto childId : dependsOn) {
                    writer.OnListItem();
                    writer.OnUint64Scalar(childId);
                }

                writer.OnEndList();
            }

            writer.OnEndMap();
        }

        writer.OnEndList();
        writer.OnKeyedItem("OperationRoot");
        writer.OnUint64Scalar(lastId);
        WriteProviders("Providers", providers, writer);
        writer.OnKeyedItem("OperationStats");
        writer.OnBeginMap();
        for (auto& x : opStats) {
            writer.OnKeyedItem(x.first);
            writer.OnUint64Scalar(x.second);
        }

        writer.OnEndMap();
        writer.OnEndMap();

        BuildBasicGraph(nodes, order, lastId, basicNodes, basicLinks);

        writer.OnKeyedItem("Basic");
        writer.OnBeginMap();
        writer.OnKeyedItem("nodes");
        writer.OnBeginList();
        for (auto& basicNode : basicNodes) {
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("id");
            writer.OnUint64Scalar(basicNode.Id);
            writer.OnKeyedItem("level");
            writer.OnUint64Scalar(basicNode.Level);
            writer.OnKeyedItem("name");
            writer.OnStringScalar(basicNode.DisplayName);
            writer.OnKeyedItem("type");
            switch (basicNode.Type) {
            case TBasicNode::EType::Operation:
                writer.OnStringScalar("op");
                break;
            case TBasicNode::EType::Input:
                writer.OnStringScalar("in");
                break;
            case TBasicNode::EType::Output:
                writer.OnStringScalar("out");
                break;
            default:
                YQL_ENSURE(false, "Unsupported node type");
            }
            writer.OnEndMap();
        }

        writer.OnEndList();
        writer.OnKeyedItem("links");
        writer.OnBeginList();
        for (auto& basicLink : basicLinks) {
            writer.OnListItem();
            writer.OnBeginMap();
            writer.OnKeyedItem("source");
            writer.OnUint64Scalar(basicLink.Source);
            writer.OnKeyedItem("target");
            writer.OnUint64Scalar(basicLink.Target);
            writer.OnEndMap();
        }

        writer.OnEndList();
        writer.OnEndMap();
        writer.OnEndMap();
    }

    void VisitCallable(const TExprNode::TPtr& node, TNodeMap<TNodeInfo>& nodes, TExprNode::TListType& order) {
        if (nodes.cend() != nodes.find(node.Get())) {
            return;
        }

        auto& translatedId = Types_.NodeToOperationId[node->UniqueId()];
        if (translatedId == 0) {
            translatedId = ++NextId_;
        }

        auto& info = nodes.emplace(node.Get(), TNodeInfo(translatedId, node.Get())).first->second;
        TExprNode::TListType& dependencies = info.Dependencies;
        if (node->Content() == CommitName) {
            dependencies.push_back(node->Child(0));
            auto dataSinkName = node->Child(1)->Child(0)->Content();
            auto datasink = Types_.DataSinkMap.FindPtr(dataSinkName);
            YQL_ENSURE(datasink);
            info.Provider = (*datasink).Get();
            info.IsVisible = dataSinkName != ResultProviderName;
        }
        else if (node->ChildrenSize() >= 2 && node->Child(1)->IsCallable("DataSource")) {
            auto dataSourceName = node->Child(1)->Child(0)->Content();
            auto datasource = Types_.DataSourceMap.FindPtr(dataSourceName);
            YQL_ENSURE(datasource);
            info.Provider = (*datasource).Get();
            info.IsVisible = (*datasource)->GetPlanFormatter().GetDependencies(*node, dependencies, true);
        }
        else if (node->ChildrenSize() >= 2 && node->Child(1)->IsCallable("DataSink")) {
            auto dataSinkName = node->Child(1)->Child(0)->Content();
            auto datasink = Types_.DataSinkMap.FindPtr(dataSinkName);
            YQL_ENSURE(datasink);
            info.Provider = (*datasink).Get();
            info.IsVisible = (*datasink)->GetPlanFormatter().GetDependencies(*node, dependencies, true);
        }
        else if (node->IsCallable("DqStage") ||
            node->IsCallable("DqPhyStage") ||
            node->IsCallable("DqQuery!") ||
            node->ChildrenSize() >= 1 && node->Child(0)->IsCallable("TDqOutput")) {
            auto provider = Types_.DataSinkMap.FindPtr(DqProviderName);
            YQL_ENSURE(provider);
            info.Provider = (*provider).Get();
            info.IsVisible = (*provider)->GetPlanFormatter().GetDependencies(*node, dependencies, true);
        } else {
            info.IsVisible = false;
            for (auto& child : node->Children()) {
                dependencies.push_back(child.Get());
            }
        }

        for (const auto& child : dependencies) {
            VisitNode(child, nodes, order);
        }

        order.push_back(node);
    }

    void VisitNode(const TExprNode::TPtr& node, TNodeMap<TNodeInfo>& nodes,
        TExprNode::TListType& order) {
        switch (node->Type()) {
        case TExprNode::Atom:
        case TExprNode::List:
        case TExprNode::World:
        case TExprNode::Lambda:
        case TExprNode::Argument:
        case TExprNode::Arguments:
            return;
        case TExprNode::Callable:
            VisitCallable(node, nodes, order);
            break;
        }
    }

    void GatherDependencies(const TExprNode& node,
        const TNodeMap<TNodeInfo>& nodes, TSet<ui64>& dependsOn) {
        const auto info = nodes.find(&node);
        if (nodes.cend() == info)
            return;

        if (info->second.IsVisible) {
            dependsOn.insert(info->second.NodeId);
            return;
        }

        for (auto child : info->second.Dependencies) {
            GatherDependencies(*child, nodes, dependsOn);
        }
    }

    void BuildBasicGraph(
        const TNodeMap<TNodeInfo>& nodes, const TExprNode::TListType& order,
        ui32 root, TVector<TBasicNode>& basicNodes, TVector<TBasicLink>& basicLinks) {
        THashMap<TPinKey, ui32, TPinKey::THash> allInputs;
        THashMap<TPinKey, ui32, TPinKey::THash> allOutputs;
        for (auto node : order) {
            auto& info = nodes.find(node.Get())->second;
            if (!info.IsVisible) {
                continue;
            }

            if (info.Provider) {
                for (auto& input : info.Inputs) {
                    if (input.Info.HideInBasicPlan) {
                        continue;
                    }

                    auto inputKey = TPinKey{ input.ProviderId, input.PinId, TBasicNode::EType::Input };
                    if (allInputs.contains(inputKey)) {
                        continue;
                    }

                    auto& translatedId = PinMap_[inputKey];
                    if (translatedId == 0) {
                        translatedId = ++NextId_;
                    }

                    TBasicNode basicNode;
                    basicNode.Level = 0;
                    basicNode.Type = TBasicNode::EType::Input;
                    basicNode.DisplayName = input.Info.DisplayName;
                    basicNode.Id = translatedId;
                    basicNodes.push_back(basicNode);
                    allInputs[inputKey] = basicNode.Id;
                }

                for (auto& output : info.Outputs) {
                    if (output.Info.HideInBasicPlan) {
                        continue;
                    }

                    auto outputKey = TPinKey{ output.ProviderId, output.PinId, TBasicNode::EType::Output };
                    if (allOutputs.contains(outputKey)) {
                        continue;
                    }

                    auto& translatedId = PinMap_[outputKey];
                    if (translatedId == 0) {
                        translatedId = ++NextId_;
                    }

                    TBasicNode basicNode;
                    basicNode.Level = 0;
                    basicNode.Type = TBasicNode::EType::Output;
                    basicNode.DisplayName = output.Info.DisplayName;
                    basicNode.Id = translatedId;
                    basicNodes.push_back(basicNode);
                    allOutputs[outputKey] = basicNode.Id;
                }
            }
        }

        for (auto node : order) {
            auto& info = nodes.find(node.Get())->second;
            if (!info.IsVisible) {
                continue;
            }

            TBasicNode basicNode;
            basicNode.Level = 0;
            basicNode.Type = TBasicNode::EType::Operation;
            TStringBuilder builder;
            builder << info.Provider->GetPlanFormatter().GetOperationDisplayName(*node);
            if (info.InputsCount > 1) {
                builder << ", in " << info.InputsCount;
            }

            if (info.OutputsCount > 1) {
                builder << ", out " << info.OutputsCount;
            }

            basicNode.DisplayName = builder;
            basicNode.Id = info.NodeId;
            basicNodes.push_back(basicNode);

            for (auto& input : info.Inputs) {
                auto inputKey = TPinKey{ input.ProviderId, input.PinId, TBasicNode::EType::Input };
                auto foundInput = allInputs.FindPtr(inputKey);
                if (foundInput) {
                    basicLinks.push_back(TBasicLink(*foundInput, info.NodeId));
                }
            }

            for (auto& output : info.Outputs) {
                auto outputKey = TPinKey{ output.ProviderId, output.PinId, TBasicNode::EType::Output };
                auto foundOutput = allOutputs.FindPtr(outputKey);
                if (foundOutput) {
                    basicLinks.push_back(TBasicLink(info.NodeId, *foundOutput));
                }
            }

            TSet<ui64> dependsOn;
            for (auto child : info.Dependencies) {
                GatherDependencies(*child, nodes, dependsOn);
            }

            for (auto& prevOp : dependsOn) {
                basicLinks.push_back(TBasicLink(prevOp, info.NodeId));
            }
        }

        THashMap<ui32, TLevelContext> basicNodesMap;
        for (auto& node : basicNodes) {
            basicNodesMap.insert({ node.Id, TLevelContext(&node) });
        }

        for (auto& link : basicLinks) {
            basicNodesMap[link.Target].Inputs.push_back(link.Source);
            auto target = basicNodesMap.FindPtr(link.Target);
            YQL_ENSURE(target);
            if (target->Node->Type == TBasicNode::EType::Output) {
                basicNodesMap[link.Source].Outputs.push_back(link.Target);
            }
        }

        if (root) {
            THashSet<ui32> visited;
            FillLevels(basicNodesMap, root, visited);
        }
    }

    void UpdateProviders(TProviderInfoMap& providers, const TExprNode* node, IDataProvider* provider) {
        auto path = provider->GetPlanFormatter().GetProviderPath(*node);
        if (providers.FindPtr(path)) {
            return;
        }

        ui32 providerId = 0;
        if (auto p = ProviderIds_.FindPtr(path)) {
            providerId = *p;
        } else {
            providerId = static_cast<ui32>(ProviderIds_.size() + 1);
            ProviderIds_.insert({path, providerId});
        }

        providers.insert(std::make_pair(path, TProviderInfo(providerId, node, provider)));
    }

    void UpdateProviders(TProviderInfoMap& providers, const TVector<TPinInfo>& pins) {
        for (auto& pin : pins) {
            if (pin.DataSource) {
                auto providerName = pin.DataSource->Child(0)->Content();
                auto providerPtr = Types_.DataSourceMap.FindPtr(providerName);
                YQL_ENSURE(providerPtr);
                UpdateProviders(providers, pin.DataSource, providerPtr->Get());
            }

            if (pin.DataSink) {
                auto providerName = pin.DataSink->Child(0)->Content();
                auto providerPtr = Types_.DataSinkMap.FindPtr(providerName);
                YQL_ENSURE(providerPtr);
                UpdateProviders(providers, pin.DataSink, providerPtr->Get());
            }
        }
    }

    IDataProvider& GetProvider(const TPinInfo& pin, TTypeAnnotationContext& types) {
        if (pin.DataSource) {
            auto providerName = pin.DataSource->Child(0)->Content();
            auto providerPtr = types.DataSourceMap.FindPtr(providerName);
            YQL_ENSURE(providerPtr && *providerPtr);
            return **providerPtr;
        }

        if (pin.DataSink) {
            auto providerName = pin.DataSink->Child(0)->Content();
            auto providerPtr = types.DataSinkMap.FindPtr(providerName);
            YQL_ENSURE(providerPtr && *providerPtr);
            return **providerPtr;
        }

        YQL_ENSURE(false, "Expected either datasource or sink");
    }

    TProviderInfo& FindProvider(TProviderInfoMap& providers, const TPinInfo& pin) const {
        if (pin.DataSource) {
            auto providerName = pin.DataSource->Child(0)->Content();
            auto providerPtr = Types_.DataSourceMap.FindPtr(providerName);
            YQL_ENSURE(providerPtr && *providerPtr);
            auto infoPtr = providers.FindPtr((*providerPtr)->GetPlanFormatter().GetProviderPath(*pin.DataSource));
            YQL_ENSURE(infoPtr);
            return *infoPtr;
        }

        if (pin.DataSink) {
            auto providerName = pin.DataSink->Child(0)->Content();
            auto providerPtr = Types_.DataSinkMap.FindPtr(providerName);
            YQL_ENSURE(providerPtr && *providerPtr);
            auto infoPtr = providers.FindPtr((*providerPtr)->GetPlanFormatter().GetProviderPath(*pin.DataSink));
            YQL_ENSURE(infoPtr);
            return *infoPtr;
        }

        YQL_ENSURE(false, "Expected either datasource or sink");
    }

    void WritePins(const TString& tag, const TVector<TPinInfo>& pins, NYson::TYsonWriter& writer,
        TVector<TPinAttrs>& pinAttrs, TProviderInfoMap& providers) {
        if (!pins.empty()) {
            UpdateProviders(providers, pins);
            writer.OnKeyedItem(tag);
            writer.OnBeginList();
            for (auto pin : pins) {
                TPinAttrs attrs(pin);
                auto& info = FindProvider(providers, pin);
                attrs.ProviderId = info.Id;
                writer.OnListItem();
                writer.OnBeginList();
                writer.OnListItem();
                writer.OnUint64Scalar(info.Id);
                if (pin.Key) {
                    auto p = info.Pin.insert(std::make_pair(pin.Key, static_cast<ui32>(info.Pin.size() + 1)));
                    if (p.second) {
                        info.PinOrder.push_back(pin.Key);
                    }

                    writer.OnListItem();
                    writer.OnUint64Scalar(p.first->second);
                    attrs.PinId = p.first->second;
                }

                writer.OnEndList();
                pinAttrs.push_back(attrs);
            }

            writer.OnEndList();
        }
    }

    void Clear() override {
        Types_.NodeToOperationId.clear();
        PinMap_.clear();
        ProviderIds_.clear();
        NextId_ = 0;
    }

private:
    TTypeAnnotationContext& Types_;
    THashMap<TPinKey, ui32, TPinKey::THash> PinMap_;
    TMap<TString, ui32> ProviderIds_;
    ui32 NextId_ = 0;
};

TAutoPtr<IPlanBuilder> CreatePlanBuilder(TTypeAnnotationContext& types) {
    return new TPlanBuilder(types);
}

}
