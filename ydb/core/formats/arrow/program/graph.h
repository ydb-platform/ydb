#pragma once
#include "abstract.h"

#include <library/cpp/json/writer/json_value.h>

namespace NKikimr::NArrow::NSSA {
class TCalculationProcessor;
}

namespace NKikimr::NArrow::NSSA::NOptimization {

class TGraphNode {
private:
    static inline TAtomicCounter Counter = 0;
    YDB_READONLY(i64, Identifier, Counter.Inc());
    YDB_READONLY_DEF(std::shared_ptr<IResourceProcessor>, Processor);
    class TAddress {
    private:
        const ui32 ColumnId;
        const i64 NodeId;

    public:
        TAddress(const ui32 columnId, const i64 nodeId)
            : ColumnId(columnId)
            , NodeId(nodeId) {
        }

        TAddress AnotherNodeId(const i64 nodeId) const {
            return TAddress(ColumnId, nodeId);
        }

        bool operator<(const TAddress& item) const {
            return std::tie(ColumnId, NodeId) < std::tie(item.ColumnId, item.NodeId);
        }

        NJson::TJsonValue DebugJson() const {
            NJson::TJsonValue result = NJson::JSON_MAP;
            result.InsertValue("c", ColumnId);
            result.InsertValue("n", NodeId);
            return result;
        }
    };
    std::map<TAddress, TGraphNode*> DataFrom;
    std::map<TAddress, TGraphNode*> DataTo;

public:
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        result.InsertValue("id", Identifier);
        auto& inputArr = result.InsertValue("input", NJson::JSON_ARRAY);
        for (auto&& i : DataFrom) {
            inputArr.AppendValue(i.first.DebugJson());
        }
        auto& outputArr = result.InsertValue("output", NJson::JSON_ARRAY);
        for (auto&& i : DataTo) {
            outputArr.AppendValue(i.first.DebugJson());
        }
        return result;
    }

    const std::map<TAddress, TGraphNode*>& GetDataTo() const {
        return DataTo;
    }

    const std::map<TAddress, TGraphNode*>& GetDataFrom() const {
        return DataFrom;
    }

    TGraphNode(const std::shared_ptr<IResourceProcessor>& processor)
        : Processor(processor) {
        AFL_VERIFY(Processor);
    }

    template <class TProcessor>
    std::shared_ptr<TProcessor> GetProcessorAs() const {
        return std::static_pointer_cast<TProcessor>(Processor);
    }

    void AddDataFrom(const ui32 columnId, const std::shared_ptr<TGraphNode>& node) {
        AFL_VERIFY(node);
        AFL_VERIFY(DataFrom.emplace(TAddress(columnId, node->GetIdentifier()), node.get()).second);
    }

    void AddDataFrom(const ui32 columnId, TGraphNode* node) {
        AFL_VERIFY(node);
        AFL_VERIFY(DataFrom.emplace(TAddress(columnId, node->GetIdentifier()), node).second);
    }

    void AddDataTo(const ui32 columnId, TGraphNode* node) {
        AFL_VERIFY(node);
        AFL_VERIFY(DataTo.emplace(TAddress(columnId, node->GetIdentifier()), node).second);
    }

    void AddDataTo(const ui32 columnId, const std::shared_ptr<TGraphNode>& node) {
        AFL_VERIFY(node);
        AFL_VERIFY(DataTo.emplace(TAddress(columnId, node->GetIdentifier()), node.get()).second);
    }

    void RemoveDataFrom(const TAddress& addr) {
        AFL_VERIFY(DataFrom.erase(addr))("addr", addr.DebugJson())("info", DebugJson());
    }

    void RemoveDataTo(const TAddress& addr) {
        AFL_VERIFY(DataTo.erase(addr))("addr", addr.DebugJson())("info", DebugJson());
    }

    std::vector<const TGraphNode*> GetFetchingChain() const;
};

class TGraph {
private:
    std::map<ui64, std::shared_ptr<TGraphNode>> Nodes;
    THashMap<ui32, TGraphNode*> Producers;
    TGraphNode* GetProducerVerified(const ui32 columnId) {
        auto it = Producers.find(columnId);
        AFL_VERIFY(it != Producers.end());
        return it->second;
    }
    TConclusion<bool> OptimizeFilter(TGraphNode* filterNode);
    TConclusion<bool> OptimizeFilterWithCoalesce(TGraphNode* filterNode, TGraphNode* filterArg, const std::shared_ptr<TCalculationProcessor>& calc);
    TConclusion<bool> OptimizeFilterWithAnd(TGraphNode* filterNode, TGraphNode* filterArg, const std::shared_ptr<TCalculationProcessor>& calc);

    void RemoveNode(TGraphNode* node);
    void DetachNode(TGraphNode* node);
    void Connect(TGraphNode* from, TGraphNode* to, const ui32 columnId) {
        from->AddDataTo(columnId, to);
        to->AddDataFrom(columnId, from);
    }

    void AddNode(const std::shared_ptr<IResourceProcessor>& processor);
    NJson::TJsonValue DebugJson() const {
        NJson::TJsonValue result = NJson::JSON_MAP;
        auto& nodesArr = result.InsertValue("nodes", NJson::JSON_ARRAY);
        for (auto&& i : Nodes) {
            nodesArr.AppendValue(i.second->DebugJson());
        }
        return result;
    }

public:
    TGraph(std::vector<std::shared_ptr<IResourceProcessor>>&& processors, const IColumnResolver& resolver);

    TConclusionStatus Collapse();

    TConclusion<std::vector<std::shared_ptr<IResourceProcessor>>> BuildChain();
};
}   // namespace NKikimr::NArrow::NSSA::NOptimization
