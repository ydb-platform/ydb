#include "yql_co_blocks.h"

#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/expr_nodes/yql_expr_nodes.h>

#include <yql/essentials/utils/log/log.h>

namespace NYql {

namespace {

class TBlockVisitor {
public:
    void Visit(const TExprNode& node) {
        auto [it, inserted] = Visited_.emplace(&node, nullptr);
        if (!inserted) {
            it->second = nullptr; // multiple paths
            return;
        }

        const bool isBlock = node.IsCallable("Block");
        if (isBlock) {
            Blocks_.emplace(&node);
            if (!BlockStack_.empty()) {
                it->second = BlockStack_.back();
                YQL_ENSURE(it->second);
            }

            BlockStack_.push_back(&node);
            Visit(node.Head().Tail());
            BlockStack_.pop_back();
        }

        if (node.Type() == TExprNode::Lambda) {
            TVector<const TExprNode*> savedBlocks = std::move(BlockStack_);
            BlockStack_.clear();
            for (ui32 i = 1; i < node.ChildrenSize(); ++i) {
                Visit(*node.Child(i));
            }

            BlockStack_ = std::move(savedBlocks);
        } else {
            for (const auto& child : node.Children()) {
                Visit(*child);
            }
        }
    }

    TNodeMap<const TExprNode*> GetBlockParents() const {
        TNodeMap<const TExprNode*> ret;
        for (const auto b : Blocks_) {
            auto it = Visited_.find(b);
            YQL_ENSURE(it != Visited_.cend());
            if (it->second != nullptr) {
                ret.emplace(b, it->second);
            }
        }

        for (auto& [node,parent] : ret) {
            for (;;) {
                auto parentIt = ret.find(parent);
                if (parentIt == ret.cend()) {
                    break;
                }

                if (parentIt->second == nullptr) {
                    break;
                }

                parent = parentIt->second;
            }
        }

        return ret;
    }

private:
    TNodeMap<const TExprNode*> Visited_;
    TVector<const TExprNode*> BlockStack_;
    TNodeSet Blocks_;
};

}

IGraphTransformer::TStatus OptimizeBlocks(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx,
    TTypeAnnotationContext& typeCtx) {
    output = input;
    TBlockVisitor visitor;
    visitor.Visit(*input);
    auto blockParents = visitor.GetBlockParents();
    if (blockParents.empty()) {
        return IGraphTransformer::TStatus::Ok;
    }

    YQL_CLOG(DEBUG, Core) << "Found " << blockParents.size() << " nested blocks";
    TNodeOnNodeOwnedMap toOptimize;
    for (const auto [node, parent]: blockParents) {
        auto lambda = NNodes::TCoLambda(node->HeadPtr());
        auto parentLambda = NNodes::TCoLambda(parent->HeadPtr());
        toOptimize[lambda.Args().Arg(0).Raw()] = parentLambda.Args().Arg(0).Ptr();
        toOptimize[node] = lambda.Body().Ptr();
    }

    TOptimizeExprSettings settings(&typeCtx);
    settings.CustomInstantTypeTransformer = typeCtx.CustomInstantTypeTransformer.Get();
    return RemapExpr(input, output, toOptimize, ctx, settings);
}

}

