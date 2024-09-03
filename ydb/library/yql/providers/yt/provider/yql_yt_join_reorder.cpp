#include "yql_yt_join_impl.h"
#include "yql_yt_helpers.h"

#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>
#include <ydb/library/yql/core/yql_graph_transformer.h>
#include <ydb/library/yql/dq/opt/dq_opt_log.h>
#include <ydb/library/yql/parser/pg_wrapper/interface/optimizer.h>
#include <ydb/library/yql/providers/common/provider/yql_provider.h>
#include <ydb/library/yql/providers/yt/opt/yql_yt_join.h>
#include <ydb/library/yql/providers/yt/provider/yql_yt_provider_context.h>
#include <ydb/library/yql/utils/log/log.h>

#include <yt/cpp/mapreduce/common/helpers.h>

namespace NYql {

namespace {

void DebugPrint(TYtJoinNode::TPtr node, TExprContext& ctx, int level) {
    auto* op = dynamic_cast<TYtJoinNodeOp*>(node.Get());
    auto printScope = [](const TVector<TString>& scope) -> TString {
        TStringBuilder b;
        for (auto& s : scope) {
            b << s << ",";
        }
        return b;
    };
    TString prefix;
    for (int i = 0; i < level; i++) {
        prefix += ' ';
    }
    if (op) {
        Cerr << prefix
            << "Op: "
            << "Type: " << NCommon::ExprToPrettyString(ctx, *op->JoinKind)
            << "Left: " << NCommon::ExprToPrettyString(ctx, *op->LeftLabel)
            << "Right: " << NCommon::ExprToPrettyString(ctx, *op->RightLabel)
            << "Scope: " << printScope(op->Scope) << "\n"
            << "\n";
        DebugPrint(op->Left, ctx, level+1);
        DebugPrint(op->Right, ctx, level+1);
    } else {
        auto* leaf = dynamic_cast<TYtJoinNodeLeaf*>(node.Get());
        Cerr << prefix
            << "Leaf: "
            << "Section: " << NCommon::ExprToPrettyString(ctx, *leaf->Section.Ptr())
            << "Label: " << NCommon::ExprToPrettyString(ctx, *leaf->Label)
            << "Scope: " << printScope(leaf->Scope) << "\n"
            << "\n";
    }
}

class TYtProviderContext: public TBaseProviderContext {
public:
    TYtProviderContext() { }

    bool HasForceSortedMerge = false;
    bool HasHints = false;
};

class TJoinReorderer {
public:
    TJoinReorderer(
        TYtJoinNodeOp::TPtr op,
        const TYtState::TPtr& state,
        const TString& cluster,
        TExprContext& ctx,
        bool debug = false)
        : Root(op)
        , State(state)
        , Cluster(cluster)
        , Ctx(ctx)
        , Debug(debug)
    {
        Y_UNUSED(State);

        if (Debug) {
            DebugPrint(Root, Ctx, 0);
        }
    }

    TYtJoinNodeOp::TPtr Do() {
        std::shared_ptr<IBaseOptimizerNode> tree;
        std::shared_ptr<IProviderContext> providerCtx;
        BuildOptimizerJoinTree(State, Cluster, tree, providerCtx, Root, Ctx);
        auto ytCtx = std::static_pointer_cast<TYtProviderContext>(providerCtx);

        std::function<void(const TString& str)> log;

        log = [](const TString& str) {
            YQL_CLOG(INFO, ProviderYt) << str;
        };

        std::unique_ptr<IOptimizerNew> opt;

        switch (State->Types->CostBasedOptimizer) {
        case ECostBasedOptimizerType::PG:
            if (ytCtx->HasForceSortedMerge || ytCtx->HasHints) {
                YQL_CLOG(ERROR, ProviderYt) << "PG CBO does not support link settings";
                return Root;
            }
            opt = std::unique_ptr<IOptimizerNew>(MakePgOptimizerNew(*providerCtx, Ctx, log));
            break;
        case ECostBasedOptimizerType::Native:
            if (ytCtx->HasHints) {
                YQL_CLOG(ERROR, ProviderYt) << "Native CBO does not suppor link hints";
                return Root;
            }
            opt = std::unique_ptr<IOptimizerNew>(NDq::MakeNativeOptimizerNew(*providerCtx, 100000));
            break;
        default:
            YQL_CLOG(ERROR, ProviderYt) << "Unknown optimizer type " << ToString(State->Types->CostBasedOptimizer);
            return Root;
        }

        std::shared_ptr<TJoinOptimizerNode> result;

        try {
            result = opt->JoinSearch(std::dynamic_pointer_cast<TJoinOptimizerNode>(tree));
            if (tree == result) { return Root; }
        } catch (...) {
            YQL_CLOG(ERROR, ProviderYt) << "Cannot do join search " << CurrentExceptionMessage();
            return Root;
        }

        std::stringstream ss;
        result->Print(ss);

        YQL_CLOG(INFO, ProviderYt) << "Result: " << ss.str();

        TVector<TString> scope;
        TYtJoinNodeOp::TPtr res = dynamic_cast<TYtJoinNodeOp*>(BuildYtJoinTree(result, Ctx, {}).Get());
        res->CostBasedOptPassed = true;

        YQL_ENSURE(res);
        if (Debug) {
            DebugPrint(res, Ctx, 0);
        }

        return res;
    }

private:
    TYtJoinNodeOp::TPtr Root;
    const TYtState::TPtr& State;
    TString Cluster;
    TExprContext& Ctx;
    bool Debug;
};

class TYtRelOptimizerNode: public TRelOptimizerNode {
public:
    TYtRelOptimizerNode(TString label, std::shared_ptr<TOptimizerStatistics> stats, TYtJoinNodeLeaf* leaf)
        : TRelOptimizerNode(std::move(label), std::move(stats))
        , OriginalLeaf(leaf)
    { }

    TYtJoinNodeLeaf* OriginalLeaf;
};

class TYtJoinOptimizerNode: public TJoinOptimizerNode {
public:
    TYtJoinOptimizerNode(const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& joinConditions,
        const EJoinKind joinType,
        const EJoinAlgoType joinAlgo,
        TYtJoinNodeOp* originalOp)
        : TJoinOptimizerNode(left, right, joinConditions, joinType, joinAlgo,
            originalOp ? originalOp->LinkSettings.LeftHints.contains("any") : false,
            originalOp ? originalOp->LinkSettings.RightHints.contains("any") : false,
            originalOp != nullptr)
        , OriginalOp(originalOp)
    { }

    TYtJoinNodeOp* OriginalOp; // Only for nonReorderable
};

class TOptimizerTreeBuilder
{
public:
    TOptimizerTreeBuilder(TYtState::TPtr state, const TString& cluster, std::shared_ptr<IBaseOptimizerNode>& tree, std::shared_ptr<IProviderContext>& providerCtx, TYtJoinNodeOp::TPtr inputTree, TExprContext& ctx)
        : State(state)
        , Cluster(cluster)
        , Tree(tree)
        , OutProviderCtx(providerCtx)
        , InputTree(inputTree)
        , Ctx(ctx)
    { }

    void Do() {
        ProviderCtx = std::make_shared<TYtProviderContext>();
        Tree = ProcessNode(InputTree);
        OutProviderCtx = ProviderCtx;
    }

private:
    std::shared_ptr<IBaseOptimizerNode> ProcessNode(TYtJoinNode::TPtr node) {
        if (auto* op = dynamic_cast<TYtJoinNodeOp*>(node.Get())) {
            return OnOp(op);
        } else if (auto* leaf = dynamic_cast<TYtJoinNodeLeaf*>(node.Get())) {
            return OnLeaf(leaf);
        } else {
            YQL_ENSURE("Unknown node type");
            return nullptr;
        }
    }

    std::shared_ptr<IBaseOptimizerNode> OnOp(TYtJoinNodeOp* op) {
        auto joinKind = ConvertToJoinKind(TString(op->JoinKind->Content()));
        YQL_ENSURE(op->LeftLabel->ChildrenSize() == op->RightLabel->ChildrenSize());
        std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>> joinConditions;
        for (ui32 i = 0; i < op->LeftLabel->ChildrenSize(); i += 2) {
            auto ltable = op->LeftLabel->Child(i)->Content();
            auto lcolumn = op->LeftLabel->Child(i + 1)->Content();
            auto rtable = op->RightLabel->Child(i)->Content();
            auto rcolumn = op->RightLabel->Child(i + 1)->Content();
            AddRelJoinColumn(TString(ltable), TString(lcolumn));
            AddRelJoinColumn(TString(rtable), TString(rcolumn));
            NDq::TJoinColumn lcol{TString(ltable), TString(lcolumn)};
            NDq::TJoinColumn rcol{TString(rtable), TString(rcolumn)};
            joinConditions.insert({lcol, rcol});
        }
        auto left = ProcessNode(op->Left);
        auto right = ProcessNode(op->Right);
        bool nonReorderable = op->LinkSettings.ForceSortedMerge;
        ProviderCtx->HasForceSortedMerge = ProviderCtx->HasForceSortedMerge || op->LinkSettings.ForceSortedMerge;
        ProviderCtx->HasHints = ProviderCtx->HasHints || !op->LinkSettings.LeftHints.empty() || !op->LinkSettings.RightHints.empty();

        return std::make_shared<TYtJoinOptimizerNode>(
            left, right, joinConditions, joinKind, EJoinAlgoType::GraceJoin, nonReorderable ? op : nullptr
            );
    }

    std::shared_ptr<IBaseOptimizerNode> OnLeaf(TYtJoinNodeLeaf* leaf) {
        TString label = JoinLeafLabel(leaf->Label);

        const TMaybe<ui64> maxChunkCountExtendedStats = State->Configuration->ExtendedStatsMaxChunkCount.Get();
        bool enableExtendedStats = maxChunkCountExtendedStats.Defined();

        TVector<TString> keyList;
        auto joinColumns = RelJoinColumns.find(label);
        if (joinColumns != RelJoinColumns.end()) {
            keyList.assign(joinColumns->second.begin(), joinColumns->second.end());
        }

        TYtSection section{leaf->Section};
        auto stat = std::make_shared<TOptimizerStatistics>();
        stat->ColumnStatistics = TIntrusivePtr<TOptimizerStatistics::TColumnStatMap>(
            new TOptimizerStatistics::TColumnStatMap());
        auto providerStats = std::make_unique<TYtProviderStatistic>();
        TVector<IYtGateway::TPathStatReq> pathStatReqs;
        ui64 totalChunkCount = 0;

        if (Y_UNLIKELY(!section.Settings().Empty()) && Y_UNLIKELY(section.Settings().Item(0).Name() == "Test")) {
            for (const auto& setting : section.Settings()) {
                if (setting.Name() == "Rows") {
                    stat->Nrows += FromString<ui64>(setting.Value().Ref().Content());
                } else if (setting.Name() == "Size") {
                    stat->Cost += FromString<ui64>(setting.Value().Ref().Content());
                }
            }
        } else {
            for (auto path: section.Paths()) {
                auto tableStat = TYtTableBaseInfo::GetStat(path.Table());
                stat->Cost += tableStat->DataSize;
                stat->Nrows += tableStat->RecordsCount;
                totalChunkCount += tableStat->ChunkCount;

                auto pathInfo = MakeIntrusive<TYtPathInfo>(path);
                auto ytPath = BuildYtPathForStatRequest(Cluster, *pathInfo, keyList, *State, Ctx);
                YQL_ENSURE(ytPath);

                if (enableExtendedStats) {
                    pathStatReqs.push_back(
                        IYtGateway::TPathStatReq()
                            .Path(*ytPath)
                            .IsTemp(pathInfo->Table->IsTemp)
                            .IsAnonymous(pathInfo->Table->IsAnonymous)
                            .Epoch(pathInfo->Table->Epoch.GetOrElse(0)));
                }
            }
            auto sorted = section.Ref().GetConstraint<TSortedConstraintNode>();
            if (sorted) {
                TVector<TString> key;
                for (const auto& item : sorted->GetContent()) {
                    for (const auto& path : item.first) {
                        const auto& column = path.front();
                        key.push_back(TString(column));
                    }
                }
                providerStats->SortColumns = key;
            }
        }

        if (!pathStatReqs.empty() &&
            (*maxChunkCountExtendedStats == 0 || totalChunkCount <= *maxChunkCountExtendedStats)) {
            IYtGateway::TPathStatOptions pathStatOptions =
                IYtGateway::TPathStatOptions(State->SessionId)
                    .Cluster(Cluster)
                    .Paths(pathStatReqs)
                    .Config(State->Configuration->Snapshot())
                    .Extended(true);

            IYtGateway::TPathStatResult pathStats = State->Gateway->TryPathStat(std::move(pathStatOptions));
            if (pathStats.Success()) {
                for (const auto& extended : pathStats.Extended) {
                    if (!extended) {
                        continue;
                    }
                    for (const auto& entry : extended->EstimatedUniqueCounts) {
                        stat->ColumnStatistics->Data[entry.first].NumUniqueVals = entry.second;
                    }
                    for (const auto& entry : extended->DataWeight) {
                        providerStats->ColumnStatistics[entry.first] = {
                            .DataWeight = entry.second
                        };
                    }
                }
            }
        }

        stat->Specific = std::unique_ptr<const IProviderStatistics>(providerStats.release());
        return std::make_shared<TYtRelOptimizerNode>(
            std::move(label), std::move(stat), leaf
            );
    }

    void AddRelJoinColumn(const TString& rtable, const TString& rcolumn) {
        auto entry = RelJoinColumns.insert(std::make_pair(rtable, THashSet<TString>{}));
        entry.first->second.insert(rcolumn);
    }

    TYtState::TPtr State;
    const TString Cluster;
    std::shared_ptr<IBaseOptimizerNode>& Tree;
    std::shared_ptr<TYtProviderContext> ProviderCtx;
    std::shared_ptr<IProviderContext>& OutProviderCtx;
    THashMap<TString, THashSet<TString>> RelJoinColumns;
    TYtJoinNodeOp::TPtr InputTree;
    TExprContext& Ctx;
};

TYtJoinNode::TPtr BuildYtJoinTree(std::shared_ptr<IBaseOptimizerNode> node, TVector<TString>& scope, TExprContext& ctx, TPositionHandle pos) {
    if (node->Kind == RelNodeType) {
        auto* leaf = static_cast<TYtRelOptimizerNode*>(node.get())->OriginalLeaf;
        scope.insert(scope.end(), leaf->Scope.begin(), leaf->Scope.end());
        return leaf;
    } else if (node->Kind == JoinNodeType) {
        auto* op = static_cast<TJoinOptimizerNode*>(node.get());
        auto* ytop = dynamic_cast<TYtJoinOptimizerNode*>(op);
        TYtJoinNodeOp::TPtr ret;
        if (ytop && !ytop->IsReorderable) {
            ret = ytop->OriginalOp;
        } else {
            ret = MakeIntrusive<TYtJoinNodeOp>();
            ret->JoinKind = ctx.NewAtom(pos, ConvertToJoinString(op->JoinType));
            TVector<TExprNodePtr> leftLabel, rightLabel;
            leftLabel.reserve(op->JoinConditions.size() * 2);
            rightLabel.reserve(op->JoinConditions.size() * 2);
            for (auto& [left, right] : op->JoinConditions) {
                leftLabel.emplace_back(ctx.NewAtom(pos, left.RelName));
                leftLabel.emplace_back(ctx.NewAtom(pos, left.AttributeName));

                rightLabel.emplace_back(ctx.NewAtom(pos, right.RelName));
                rightLabel.emplace_back(ctx.NewAtom(pos, right.AttributeName));
            }
            ret->LeftLabel = Build<TCoAtomList>(ctx, pos)
                .Add(leftLabel)
                .Done()
                .Ptr();
            ret->RightLabel = Build<TCoAtomList>(ctx, pos)
                .Add(rightLabel)
                .Done()
                .Ptr();
        }
        int index = scope.size();
        ret->Left = BuildYtJoinTree(op->LeftArg, scope, ctx, pos);
        ret->Right = BuildYtJoinTree(op->RightArg, scope, ctx, pos);
        ret->Scope.insert(ret->Scope.end(), scope.begin() + index, scope.end());
        return ret;
    } else {
        YQL_ENSURE(false, "Unknown node type");
    }
}

} // namespace

bool AreSimilarTrees(TYtJoinNode::TPtr node1, TYtJoinNode::TPtr node2) {
    if (node1 == node2) {
        return true;
    }
    if (node1 && !node2) {
        return false;
    }
    if (node2 && !node1) {
        return false;
    }
    if (node1->Scope != node2->Scope) {
        return false;
    }
    auto opLeft = dynamic_cast<TYtJoinNodeOp*>(node1.Get());
    auto opRight = dynamic_cast<TYtJoinNodeOp*>(node2.Get());
    if (opLeft && opRight) {
        return AreSimilarTrees(opLeft->Left, opRight->Left)
            && AreSimilarTrees(opLeft->Right, opRight->Right);
    } else if (!opLeft && !opRight) {
        return true;
    } else {
        return false;
    }
}

void BuildOptimizerJoinTree(TYtState::TPtr state, const TString& cluster, std::shared_ptr<IBaseOptimizerNode>& tree, std::shared_ptr<IProviderContext>& providerCtx, TYtJoinNodeOp::TPtr op, TExprContext& ctx)
{
    TOptimizerTreeBuilder(state, cluster, tree, providerCtx, op, ctx).Do();
}

TYtJoinNode::TPtr BuildYtJoinTree(std::shared_ptr<IBaseOptimizerNode> node, TExprContext& ctx, TPositionHandle pos) {
    TVector<TString> scope;
    return BuildYtJoinTree(node, scope, ctx, pos);
}

TYtJoinNodeOp::TPtr OrderJoins(TYtJoinNodeOp::TPtr op, const TYtState::TPtr& state, const TString& cluster, TExprContext& ctx, bool debug)
{
    if (state->Types->CostBasedOptimizer == ECostBasedOptimizerType::Disable || op->CostBasedOptPassed) {
        return op;
    }

    auto result = TJoinReorderer(op, state, cluster, ctx, debug).Do();
    if (!debug && AreSimilarTrees(result, op)) {
        return op;
    }
    return result;
}

} // namespace NYql
