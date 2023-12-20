#include "rewrite_io_utils.h"

#include <ydb/library/yql/core/expr_nodes/yql_expr_nodes.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>
#include <ydb/library/yql/sql/sql.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NYql {
namespace {

using namespace NNodes;

constexpr const char* QueryGraphNodeSignature = "SavedQueryGraph";

NSQLTranslation::TTranslationSettings CreateViewTranslationSettings(const TString& cluster) {
    NSQLTranslation::TTranslationSettings settings;

    settings.DefaultCluster = cluster;
    settings.ClusterMapping[cluster] = TString(NYql::KikimrProviderName);
    settings.Mode = NSQLTranslation::ESqlMode::LIMITED_VIEW;

    return settings;
}

TExprNode::TPtr CompileQuery(
    const TString& query,
    TExprContext& ctx,
    const TString& cluster
) {
    TAstParseResult queryAst;
    queryAst = NSQLTranslation::SqlToYql(query, CreateViewTranslationSettings(cluster));

    ctx.IssueManager.AddIssues(queryAst.Issues);
    if (!queryAst.IsOk()) {
        return nullptr;
    }

    TExprNode::TPtr queryGraph;
    if (!CompileExpr(*queryAst.Root, queryGraph, ctx, nullptr, nullptr)) {
        return nullptr;
    }

    return queryGraph;
}

bool ContainsNullNode(const TExprNode::TPtr& root) {
    return static_cast<bool>(FindNode(root, [](const TExprNode::TPtr& node) {
        return !node;
    }));
}

void AddChild(const TExprNode::TPtr& parent, const TExprNode::TPtr& newChild) {
    auto childrenToChange = parent->ChildrenList();
    childrenToChange.emplace_back(newChild);
    parent->ChangeChildrenInplace(std::move(childrenToChange));
}

void ChangeChild(const TExprNode::TPtr& parent, ui32 index, const TExprNode::TPtr& newChild) {
    Y_ENSURE(parent->ChildrenSize() > index);

    auto childrenToChange = parent->ChildrenList();
    childrenToChange[index] = newChild;
    parent->ChangeChildrenInplace(std::move(childrenToChange));
}

TExprNode::TPtr FindSavedQueryGraph(const TExprNode::TPtr& carrier) {
    if (carrier->ChildrenSize() == 0) {
        return nullptr;
    }
    auto lastChild = carrier->Children().back();
    return lastChild->IsCallable(QueryGraphNodeSignature) ? lastChild->ChildPtr(0) : TExprNode::TPtr();
}

void SaveQueryGraph(const TExprNode::TPtr& carrier, TExprContext& ctx, const TExprNode::TPtr& payload) {
    AddChild(carrier, ctx.NewCallable(payload->Pos(), QueryGraphNodeSignature, {payload}));
}

void InsertExecutionOrderDependencies(const TExprNode::TPtr& queryGraph, const TExprNode::TPtr& worldBefore) {
    VisitExpr(
        queryGraph,
        nullptr,
        [&worldBefore](const TExprNode::TPtr& node) {
            if (node->ChildrenSize() > 0 && node->Child(0)->IsWorld()) {
                ChangeChild(node, 0, worldBefore);
            }
            return true;
        }
    );
}

bool CheckTopLevelness(const TExprNode::TPtr& candidateRead, const TExprNode::TPtr& queryGraph) {
    THashSet<TExprNode::TPtr> readsInCandidateSubgraph;
    VisitExpr(candidateRead, [&readsInCandidateSubgraph](const TExprNode::TPtr& node) {
        if (node->IsCallable(ReadName)) {
            readsInCandidateSubgraph.emplace(node);
        }
        return true;
    });

    return !FindNode(queryGraph, [&readsInCandidateSubgraph](const TExprNode::TPtr& node) {
        return node->IsCallable(ReadName) && !readsInCandidateSubgraph.contains(node);
    });
}

TExprNode::TPtr FindTopLevelRead(const TExprNode::TPtr& queryGraph) {
    const TExprNode::TPtr* lastReadInTopologicalOrder = nullptr;
    VisitExpr(
        queryGraph,
        nullptr,
        [&lastReadInTopologicalOrder](const TExprNode::TPtr& node) {
            if (node->IsCallable(ReadName)) {
                lastReadInTopologicalOrder = &node;
            }
            return true;
        }
    );

    if (!lastReadInTopologicalOrder) {
        return nullptr;
    }

    Y_ENSURE(CheckTopLevelness(*lastReadInTopologicalOrder, queryGraph),
             "Info for developers: assumption that there is only one top level Read! is wrong\
             for the expression graph of the query stored in the view:\n"
                 << queryGraph->Dump());

    return *lastReadInTopologicalOrder;
}

}

TExprNode::TPtr RewriteReadFromView(
    const TExprNode::TPtr& node,
    TExprContext& ctx,
    const TString& query,
    const TString& cluster
) {
    const auto readNode = node->ChildPtr(0);
    const auto worldBeforeThisRead = readNode->ChildPtr(0);

    TExprNode::TPtr queryGraph = FindSavedQueryGraph(readNode);
    if (!queryGraph) {
        queryGraph = CompileQuery(query, ctx, cluster);
        if (!queryGraph || ContainsNullNode(queryGraph)) {
            ctx.AddError(TIssue(readNode->Pos(ctx),
                         "The query stored in the view contains errors and cannot be compiled."));
            return nullptr;
        }
        YQL_CLOG(TRACE, ProviderKqp) << "Expression graph of the query stored in the view:\n"
                                     << NCommon::ExprToPrettyString(ctx, *queryGraph);

        InsertExecutionOrderDependencies(queryGraph, worldBeforeThisRead);
        SaveQueryGraph(readNode, ctx, queryGraph);
    }

    if (node->IsCallable(RightName)) {
        return queryGraph;
    }

    const auto topLevelRead = FindTopLevelRead(queryGraph);
    if (!topLevelRead) {
        return worldBeforeThisRead;
    }
    return Build<TCoLeft>(ctx, node->Pos()).Input(topLevelRead).Done().Ptr();
}

}