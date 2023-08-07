#pragma once

#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/ast/yql_expr.h>

#include <util/digest/numeric.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>
#include <util/generic/strbuf.h>
#include <util/generic/maybe.h>

namespace NYql {

// Snans (Configure!/Read!/Write!/Commit!)<-Configure! dependencies
class TDependencyUpdaterOld {
public:
    TDependencyUpdaterOld(TStringBuf provider, TStringBuf newConfigureName);
    virtual ~TDependencyUpdaterOld() = default;

    IGraphTransformer::TStatus ReorderGraph(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);

protected:
    virtual TMaybe<ui32> GetReadEpoch(const TExprNode::TPtr& readNode, TExprContext& ctx) const = 0;

    void ScanConfigureDeps(const TExprNode::TPtr& node);
    void ScanNewReadDeps(const TExprNode::TPtr& input, TExprContext& ctx);

private:
    struct TTExprNodePtrHash {
        inline size_t operator()(const TExprNode::TPtr& node) const {
            return NumericHash(node.Get());
        }
    };

    const TStringBuf Provider;
    const TStringBuf NewConfigureName;
    THashMap<TExprNode*, THashSet<TExprNode::TPtr, TTExprNodePtrHash>> NodeToConfigureDeps;
    TExprNode::TListType Configures; // To protect Configure! from die
    TNodeOnNodeOwnedMap NewReadToCommitDeps;
    TNodeSet VisitedNodes;
    TExprNode* LastConfigure = nullptr;
    TExprNode* LastWrite = nullptr;
    TVector<TExprNode*> LastReads;
};

} // namespace NYql
