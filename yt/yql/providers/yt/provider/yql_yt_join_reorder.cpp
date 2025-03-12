
#include "yql_yt_cbo_helpers.h"
#include "yql_yt_provider_context.h"
#include "yql_yt_join_impl.h"
#include "yql_yt_helpers.h"

#include <yql/essentials/core/cbo/cbo_optimizer_new.h>
#include <yql/essentials/core/yql_graph_transformer.h>
#include <yql/essentials/parser/pg_wrapper/interface/optimizer.h>
#include <yql/essentials/providers/common/provider/yql_provider.h>
#include <yt/yql/providers/yt/opt/yql_yt_join.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider_context.h>
#include <yql/essentials/utils/log/log.h>
#include <util/string/vector.h>

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
        TOptimizerLinkSettings linkSettings;
        std::shared_ptr<IProviderContext> providerCtx;
        BuildOptimizerJoinTree(State, Cluster, tree, providerCtx, linkSettings, Root, Ctx);
        auto ytCtx = std::static_pointer_cast<TYtProviderContext>(providerCtx);

        std::function<void(const TString& str)> log;

        log = [](const TString& str) {
            YQL_CLOG(INFO, ProviderYt) << str;
        };

        IOptimizerNew::TPtr opt;

        switch (State->Types->CostBasedOptimizer) {
        case ECostBasedOptimizerType::PG:
            if (linkSettings.HasForceSortedMerge || linkSettings.HasHints) {
                YQL_CLOG(ERROR, ProviderYt) << "PG CBO does not support link settings";
                return Root;
            }
            opt = State->OptimizerFactory_->MakeJoinCostBasedOptimizerPG(*providerCtx, Ctx, {.Logger = log});
            break;
        case ECostBasedOptimizerType::Native:
            if (linkSettings.HasHints) {
                YQL_CLOG(ERROR, ProviderYt) << "Native CBO does not suppor link hints";
                return Root;
            }
            opt = State->OptimizerFactory_->MakeJoinCostBasedOptimizerNative(*providerCtx, Ctx, {.MaxDPhypDPTableSize = 100000});
            break;
        case ECostBasedOptimizerType::Disable:
            YQL_CLOG(DEBUG, ProviderYt) << "CBO disabled";
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
    TYtRelOptimizerNode(TString label, TOptimizerStatistics stats, TYtJoinNodeLeaf* leaf)
        : TRelOptimizerNode(std::move(label), std::move(stats))
        , OriginalLeaf(leaf)
    { }

    TYtJoinNodeLeaf* OriginalLeaf;
};

class TYtJoinOptimizerNode: public TJoinOptimizerNode {
public:
    TYtJoinOptimizerNode(const std::shared_ptr<IBaseOptimizerNode>& left,
        const std::shared_ptr<IBaseOptimizerNode>& right,
        const TVector<NDq::TJoinColumn>& leftKeys,
        const TVector<NDq::TJoinColumn>& rightKeys,
        const EJoinKind joinType,
        const EJoinAlgoType joinAlgo,
        TYtJoinNodeOp* originalOp)
        : TJoinOptimizerNode(left, right, leftKeys, rightKeys, joinType, joinAlgo,
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
    TOptimizerLinkSettings LinkSettings;
    TOptimizerTreeBuilder(TYtState::TPtr state, const TString& cluster, std::shared_ptr<IBaseOptimizerNode>& tree, std::shared_ptr<IProviderContext>& providerCtx, TYtJoinNodeOp::TPtr inputTree, TExprContext& ctx)
        : State(state)
        , Cluster(cluster)
        , Tree(tree)
        , OutProviderCtx(providerCtx)
        , InputTree(inputTree)
        , Ctx(ctx)
    { }

    void Do() {
        Tree = ProcessNode(InputTree, TRelSizeInfo{});
        auto joinMergeForce = State->Configuration->JoinMergeForce.Get();
        TYtProviderContext::TJoinAlgoLimits limits{
            .MapJoinMemLimit = 0,
            .LookupJoinMemLimit = 0,
            .LookupJoinMaxRows = 0
        };

        if (State->Configuration->MapJoinLimit.Get()) {
            limits.MapJoinMemLimit = *State->Configuration->MapJoinLimit.Get();
        }
        if (LinkSettings.HasForceSortedMerge || joinMergeForce && *joinMergeForce) {
            limits.MapJoinMemLimit = 0;
        }
        limits.LookupJoinMaxRows = State->Configuration->LookupJoinMaxRows.Get().GetOrElse(0);
        if (State->Configuration->UseNewPredicateExtraction.Get().GetOrElse(DEFAULT_USE_NEW_PREDICATE_EXTRACTION)) {
            limits.LookupJoinMaxRows = std::min(
                limits.LookupJoinMaxRows,
                State->Configuration->MaxKeyRangeCount.Get().GetOrElse(DEFAULT_MAX_KEY_RANGE_COUNT));
        }
        limits.LookupJoinMemLimit = Min(State->Configuration->LookupJoinLimit.Get().GetOrElse(0),
                                        State->Configuration->EvaluationTableSizeLimit.Get().GetOrElse(Max<ui64>()));
        OutProviderCtx = std::make_shared<TYtProviderContext>(limits, std::move(ProviderRelInfo_));
    }

private:
    TVector<TString> GetJoinColumns(const TVector<TString>& labels) {
        TVector<TString> result;
        for (const auto& label : labels) {
            auto pos = RelJoinColumns.find(label);
            if (pos == RelJoinColumns.end()) {
                YQL_CLOG(ERROR, ProviderYt) << "Did not find any join columns for label " << label;
                return TVector<TString>{};
            }

            std::copy(pos->second.begin(), pos->second.end(), std::back_inserter(result));
        }
        return result;
    }


    std::shared_ptr<IBaseOptimizerNode> ProcessNode(TYtJoinNode::TPtr node, TRelSizeInfo sizeInfo) {
        if (auto* op = dynamic_cast<TYtJoinNodeOp*>(node.Get())) {
            return OnOp(op);
        } else if (auto* leaf = dynamic_cast<TYtJoinNodeLeaf*>(node.Get())) {
            return OnLeaf(leaf, sizeInfo);
        } else {
            YQL_ENSURE("Unknown node type");
            return nullptr;
        }
    }

    std::shared_ptr<IBaseOptimizerNode> OnOp(TYtJoinNodeOp* op) {
        auto joinKind = ConvertToJoinKind(TString(op->JoinKind->Content()));
        YQL_ENSURE(op->LeftLabel->ChildrenSize() == op->RightLabel->ChildrenSize());
        TVector<NDq::TJoinColumn> leftKeys;
        TVector<NDq::TJoinColumn> rightKeys;
        for (ui32 i = 0; i < op->LeftLabel->ChildrenSize(); i += 2) {
            auto ltable = op->LeftLabel->Child(i)->Content();
            auto lcolumn = op->LeftLabel->Child(i + 1)->Content();
            auto rtable = op->RightLabel->Child(i)->Content();
            auto rcolumn = op->RightLabel->Child(i + 1)->Content();
            AddRelJoinColumn(TString(ltable), TString(lcolumn));
            AddRelJoinColumn(TString(rtable), TString(rcolumn));
            NDq::TJoinColumn lcol{TString(ltable), TString(lcolumn)};
            NDq::TJoinColumn rcol{TString(rtable), TString(rcolumn)};
            leftKeys.push_back(lcol);
            rightKeys.push_back(rcol);
        }
        TRelSizeInfo leftSizeInfo;
        TRelSizeInfo rightSizeInfo;
        PopulateJoinStrategySizeInfo(leftSizeInfo, rightSizeInfo, State, Cluster, Ctx, op);

        auto left = ProcessNode(op->Left, leftSizeInfo);
        auto right = ProcessNode(op->Right, rightSizeInfo);

        for (auto& joinColumn : leftKeys) {
            if (MultiLabelIndex_.count(joinColumn.RelName) > 0) {
                joinColumn.OriginalRelName = joinColumn.RelName;
                joinColumn.RelName = JoinStrings(MultiLabels_[MultiLabelIndex_[joinColumn.RelName]], ",");
            }
        }
        for (auto& joinColumn : rightKeys) {
            if (MultiLabelIndex_.count(joinColumn.RelName) > 0) {
                joinColumn.OriginalRelName = joinColumn.RelName;
                joinColumn.RelName = JoinStrings(MultiLabels_[MultiLabelIndex_[joinColumn.RelName]], ",");
            }
        }

        bool nonReorderable = op->LinkSettings.ForceSortedMerge;
        LinkSettings.HasForceSortedMerge = LinkSettings.HasForceSortedMerge || op->LinkSettings.ForceSortedMerge;
        LinkSettings.HasHints = LinkSettings.HasHints || !op->LinkSettings.LeftHints.empty() || !op->LinkSettings.RightHints.empty();

        return std::make_shared<TYtJoinOptimizerNode>(
            left, right, leftKeys, rightKeys, joinKind, EJoinAlgoType::GraceJoin, nonReorderable ? op : nullptr
            );
    }

    std::shared_ptr<IBaseOptimizerNode> OnLeaf(TYtJoinNodeLeaf* leaf, TRelSizeInfo sizeInfo) {
        auto labels = JoinLeafLabels(leaf->Label);
        TString label = JoinStrings(labels, ",");

        const TMaybe<ui64> maxChunkCountExtendedStats = State->Configuration->ExtendedStatsMaxChunkCount.Get();

        TVector<TString> keyList = GetJoinColumns(labels);

        TYtSection section{leaf->Section};
        auto stat = std::make_shared<TOptimizerStatistics>();
        stat->Ncols = std::ssize(keyList);
        stat->ColumnStatistics = TIntrusivePtr<TOptimizerStatistics::TColumnStatMap>(
            new TOptimizerStatistics::TColumnStatMap());
        auto providerStats = std::make_unique<TYtProviderStatistic>();

        if (Y_UNLIKELY(!section.Settings().Empty()) && Y_UNLIKELY(section.Settings().Item(0).Name() == "Test")) {
            for (const auto& setting : section.Settings()) {
                if (setting.Name() == "Rows") {
                    stat->Nrows += FromString<ui64>(setting.Value().Ref().Content());
                }
            }
        } else {
            for (auto path: section.Paths()) {
                auto pathInfo = MakeIntrusive<TYtPathInfo>(path);
                if (pathInfo->HasColumns()) {
                    NYT::TRichYPath ytPath;
                    pathInfo->FillRichYPath(ytPath);
                    stat->Ncols = std::max<int>(stat->Ncols, std::ssize(ytPath.Columns_->Parts_));
                }
                auto tableStat = TYtTableBaseInfo::GetStat(path.Table());
                stat->ByteSize += tableStat->DataSize;
                stat->Nrows += tableStat->RecordsCount;
            }
            if (section.Ref().GetState() >= TExprNode::EState::ConstrComplete) {
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
        }

        TVector<TYtColumnStatistic> columnInfo;

        if (maxChunkCountExtendedStats) {
            TVector<TMaybe<IYtGateway::TPathStatResult::TExtendedResult>> extendedStats;
            extendedStats = GetStatsFromCache(leaf, keyList, *maxChunkCountExtendedStats);
            columnInfo = ExtractColumnInfo(extendedStats);
        }

        TDynBitMap relBitmap;
        relBitmap.Set(std::ssize(ProviderRelInfo_));
        providerStats->RelBitmap = relBitmap;
        providerStats->SizeInfo = sizeInfo;

        ProviderRelInfo_.push_back(TYtProviderRelInfo{
            .Label = label,
            .ColumnInfo = columnInfo,
            .SortColumns = providerStats->SortColumns
        });

        stat->Specific = std::move(providerStats);

        if (labels.size() != 1) {
            MultiLabels_.push_back(labels);
            for (const auto& label : labels) {
                MultiLabelIndex_[label] = std::ssize(MultiLabels_) - 1;
            }
        }
        return std::make_shared<TYtRelOptimizerNode>(std::move(label), std::move(*stat), leaf);
    }

    TVector<TMaybe<IYtGateway::TPathStatResult::TExtendedResult>> GetStatsFromCache(
        TYtJoinNodeLeaf* nodeLeaf, const TVector<TString>& columns, ui64 maxChunkCount) {
        TVector<IYtGateway::TPathStatReq> pathStatReqs;
        TYtSection section{nodeLeaf->Section};
        ui64 totalChunkCount = 0;
        for (auto path: section.Paths()) {
            auto pathInfo = MakeIntrusive<TYtPathInfo>(path);

            totalChunkCount += pathInfo->Table->Stat->ChunkCount;

            auto ytPath = BuildYtPathForStatRequest(Cluster, *pathInfo, columns, *State, Ctx);
            YQL_ENSURE(ytPath);

            pathStatReqs.push_back(
                IYtGateway::TPathStatReq()
                    .Path(*ytPath)
                    .IsTemp(pathInfo->Table->IsTemp)
                    .IsAnonymous(pathInfo->Table->IsAnonymous)
                    .Epoch(pathInfo->Table->Epoch.GetOrElse(0)));

        }
        if (pathStatReqs.empty() || (maxChunkCount != 0 && totalChunkCount > maxChunkCount)) {
            return {};
        }

        IYtGateway::TPathStatOptions pathStatOptions =
            IYtGateway::TPathStatOptions(State->SessionId)
                .Cluster(Cluster)
                .Paths(pathStatReqs)
                .Config(State->Configuration->Snapshot())
                .Extended(true);

        IYtGateway::TPathStatResult pathStats = State->Gateway->TryPathStat(std::move(pathStatOptions));
        if (!pathStats.Success()) {
            YQL_CLOG(WARN, ProviderYt) << "Unable to read path stats that must be already present in cache";
            return {};
        }
        return pathStats.Extended;
    }

    TVector<TYtColumnStatistic> ExtractColumnInfo(TVector<TMaybe<IYtGateway::TPathStatResult::TExtendedResult>> extendedResults)
    {
        THashMap<TString, size_t> columns;
        TVector<TString> columnNames;
        TVector<TMaybe<i64>> dataWeight;
        TVector<TMaybe<ui64>> estimatedUniqueCounts;

        for (const auto& result : extendedResults) {
            if (!result) {
                continue;
            }
            for (const auto& entry : result->DataWeight) {
                auto insResult = columns.insert(std::make_pair(entry.first, columns.size()));
                size_t index = insResult.first->second;
                if (insResult.second) {
                    dataWeight.push_back(0);
                    estimatedUniqueCounts.push_back(Nothing());
                    columnNames.push_back(entry.first);
                }
                dataWeight[index] = (dataWeight[index].GetOrElse(0) + entry.second);
            }
            for (const auto& entry : result->EstimatedUniqueCounts) {
                auto insResult = columns.insert(std::make_pair(entry.first, columns.size()));
                size_t index = insResult.first->second;
                if (insResult.second) {
                    dataWeight.push_back(Nothing());
                    estimatedUniqueCounts.push_back(0);
                    columnNames.push_back(entry.first);
                }
                estimatedUniqueCounts[index] = std::max(estimatedUniqueCounts[index].GetOrElse(0), entry.second);
            }
        }
        TVector<TYtColumnStatistic> result;
        result.reserve(std::ssize(columns));
        for (int i = 0; i < std::ssize(columns); i++) {
            result.push_back(TYtColumnStatistic{
                .ColumnName = columnNames[i],
                .EstimatedUniqueCount = estimatedUniqueCounts[i],
                .DataWeight = dataWeight[i]
            });
        }

        return result;
    }

    void AddRelJoinColumn(const TString& rtable, const TString& rcolumn) {
        auto entry = RelJoinColumns.insert(std::make_pair(rtable, THashSet<TString>{}));
        entry.first->second.insert(rcolumn);
    }

    TYtState::TPtr State;
    const TString Cluster;
    std::shared_ptr<IBaseOptimizerNode>& Tree;
    std::shared_ptr<IProviderContext>& OutProviderCtx;
    THashMap<TString, THashSet<TString>> RelJoinColumns;
    TYtJoinNodeOp::TPtr InputTree;
    TExprContext& Ctx;
    TVector<TYtProviderRelInfo> ProviderRelInfo_;
    TVector<TVector<TString>> MultiLabels_;
    THashMap<TString, int> MultiLabelIndex_;
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
            ret->LinkSettings.JoinAlgo = op->JoinAlgo;
            TVector<TExprNodePtr> leftLabel, rightLabel;
            leftLabel.reserve(op->LeftJoinKeys.size() * 2);
            rightLabel.reserve(op->RightJoinKeys.size() * 2);
            for (auto& left : op->LeftJoinKeys) {
                leftLabel.emplace_back(ctx.NewAtom(pos, left.OriginalRelName ? *left.OriginalRelName : left.RelName));
                leftLabel.emplace_back(ctx.NewAtom(pos, left.AttributeName));
            }
            for (auto& right : op->RightJoinKeys) {
                rightLabel.emplace_back(ctx.NewAtom(pos, right.OriginalRelName ? *right.OriginalRelName : right.RelName));
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

void BuildOptimizerJoinTree(TYtState::TPtr state, const TString& cluster, std::shared_ptr<IBaseOptimizerNode>& tree, std::shared_ptr<IProviderContext>& providerCtx, TOptimizerLinkSettings& linkSettings, TYtJoinNodeOp::TPtr op, TExprContext& ctx)
{
    TOptimizerTreeBuilder builder(state, cluster, tree, providerCtx, op, ctx);
    builder.Do();
    linkSettings = builder.LinkSettings;
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
