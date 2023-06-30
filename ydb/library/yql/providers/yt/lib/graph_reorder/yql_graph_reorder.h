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

// Scans (Configure!/Read!/Write!/Commit!)<-Configure! dependencies
class TDependencyUpdater {
public:
    TDependencyUpdater(TStringBuf provider, TStringBuf newConfigureName);
    virtual ~TDependencyUpdater() = default;

    IGraphTransformer::TStatus ReorderGraph(const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx);

protected:
    virtual TMaybe<ui32> GetReadEpoch(const TExprNode::TPtr& readNode) const = 0;
    virtual TString GetWriteTarget(const TExprNode::TPtr& writeNode) const = 0;

    void ScanConfigureDeps(const TExprNode::TPtr& node);
    void ScanNewReadDeps(const TExprNode::TPtr& input, TExprContext& ctx);
    void ScanNewWriteDeps(const TExprNode::TPtr& input);
    static TExprNode::TPtr MakeSync(const TSyncMap& nodes, TExprNode::TChildrenType extra, TPositionHandle pos, TExprContext& ctx);

private:

    const TStringBuf Provider;
    const TStringBuf NewConfigureName;
    THashMap<TExprNode*, TSyncMap> NodeToConfigureDeps;
    TExprNode::TListType Configures; // To protect Configure! from die
    TNodeOnNodeOwnedMap NewReadToCommitDeps;
    TNodeSet VisitedNodes;
    TExprNode* LastConfigure = nullptr;
    TVector<TExprNode*> LastReads;
    THashMap<TExprNode*, TExprNode::TListType> NewWriteDeps;
};

} // namespace NYql
