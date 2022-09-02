#pragma once

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>

#include <string_view>

namespace NYql {

struct TOptimizeContext {
    TTypeAnnotationContext* Types = nullptr;
    TParentsMap* ParentsMap = nullptr;

    const TExprNode* GetParentIfSingle(const TExprNode& node) const {
        YQL_ENSURE(ParentsMap);

        const auto it = ParentsMap->find(&node);
        YQL_ENSURE(it != ParentsMap->cend());

        auto& parents = it->second;
        YQL_ENSURE(!parents.empty());
        if (parents.size() > 1) {
            return nullptr;
        }

        size_t usageCount = 0;
        for (const auto& child : (*parents.cbegin())->ChildrenList()) {
            if (child.Get() == &node && ++usageCount > 1) {
                return nullptr;
            }
        }

        YQL_ENSURE(usageCount == 1);
        return *parents.cbegin();
    }

    bool IsSingleUsage(const TExprNode& node) const {
        return bool(GetParentIfSingle(node));
    }

    bool IsSingleUsage(const NNodes::TExprBase& node) const {
        return IsSingleUsage(node.Ref());
    }

    bool HasParent(const TExprNode& node) const {
        YQL_ENSURE(ParentsMap);
        return ParentsMap->contains(&node);
    }

    bool IsPersistentNode(const TExprNode& node) const {
        if (Types) {
            for (auto& source: Types->DataSources) {
                if (source->IsPersistent(node)) {
                    return true;
                }
            }

            for (auto& sink: Types->DataSinks) {
                if (sink->IsPersistent(node)) {
                    return true;
                }
            }
        }

        return false;
    }

    bool IsPersistentNode(const NNodes::TExprBase& node) const {
        return IsPersistentNode(node.Ref());
    }
};

using TCallableOptimizerExt = std::function<TExprNode::TPtr (const TExprNode::TPtr&, TExprContext&, TOptimizeContext&)>;
using TCallableOptimizerMap = std::unordered_map<std::string_view, TCallableOptimizerExt>;
using TFinalizingOptimizerExt = std::function<bool (const TExprNode::TPtr&, TNodeOnNodeOwnedMap&, TExprContext&, TOptimizeContext&)>;
using TFinalizingOptimizerMap = std::unordered_map<std::string_view, TFinalizingOptimizerExt>;

struct TCoCallableRules {
    enum {
        SIMPLE_STEP_1,
        SIMPLE_STEP_2,
        SIMPLE_STEP_3,
        SIMPLE_STEPS
    };

    enum {
        FLOW_STEP_1,
        FLOW_STEP_2,
        FLOW_STEPS
    };

    // rules that don't make a flow fork - e.g. False || x -> x
    TCallableOptimizerMap SimpleCallables[SIMPLE_STEPS];
    // rules that make a flow fork - Join pushdown if Join has multiple usage
    TCallableOptimizerMap FlowCallables[FLOW_STEPS];

    TFinalizingOptimizerMap Finalizers;

    // rules to be applied before execution
    TCallableOptimizerMap FinalCallables;

    TCoCallableRules();
    static const TCoCallableRules& Instance();
};

void RegisterCoSimpleCallables1(TCallableOptimizerMap& map);
void RegisterCoSimpleCallables2(TCallableOptimizerMap& map);
void RegisterCoSimpleCallables3(TCallableOptimizerMap& map);
void RegisterCoFlowCallables1(TCallableOptimizerMap& map);
void RegisterCoFlowCallables2(TCallableOptimizerMap& map);
void RegisterCoFinalizers(TFinalizingOptimizerMap& map);
void RegisterCoFinalCallables(TCallableOptimizerMap& map);

}
