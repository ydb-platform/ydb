#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/utils/log/log.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_cbo.h>
#include <ydb/library/yql/providers/dq/common/yql_dq_settings.h>
#include <ydb/library/yql/dq/opt/dq_opt_join_cost_based.h>
#include <typeinfo>

namespace {
using namespace NYql::NDq;
using namespace NKikimr;
using namespace NKikimr::NKqp;

// To use DP CBO, we need to use the column lineage map and map all variables in join condition to
// original aliases and column names in order to correctly use column statistics and shuffle elimination
//
// However, the same alias can appear multiple times in a query, but might already be out of scope
// So we first collect all join conditions and fetch aliases and mappings only for the columns used in join conditions

std::shared_ptr<TJoinOptimizerNode> ConvertJoinTree(std::shared_ptr<TOpCBOTree>& cboTree, TVector<std::shared_ptr<TRelOptimizerNode>>& rels) {
    THashSet<TInfoUnit, TInfoUnit::THashFunction> allJoinColumns;
    std::shared_ptr<TJoinOptimizerNode> result;

    auto lineage = cboTree->TreeRoot->Props.Metadata->ColumnLineage;
    int fakeAliasId = 0;

    for (auto op : cboTree->TreeNodes) {
        auto joinOp = CastOperator<TOpJoin>(op);
        for (const auto& [left, right] : joinOp->JoinKeys) {
            allJoinColumns.insert(left);
            allJoinColumns.insert(right);
        }
    }

    THashMap<IOperator*, std::shared_ptr<IBaseOptimizerNode>> nodeMap;

    // Build rels for CBO. Rel contains a set of aliases and statistics object
    for (auto child : cboTree->Children) {

        TVector<TString> childAliases;
        auto childIUs = child->GetOutputIUs();

        for (auto col : allJoinColumns) {
            if (std::find(childIUs.begin(), childIUs.end(), col) != childIUs.end()) {
                if (auto it = lineage.Mapping.find(col); it != lineage.Mapping.end()) {
                    auto alias = it->second.GetCannonicalAlias();
                    if (std::find(childAliases.begin(), childAliases.end(), alias) == childAliases.end()) {
                        childAliases.push_back(alias);
                    }
                }
            }
        }
        // If there is a real cross join in the plan with no conditions just create a fake alias
        if (childAliases.empty()) {
            childAliases.push_back("#fake_alias" + std::to_string(fakeAliasId++));
        }

        TVector<TInfoUnit> mappedKeyColumns;
        for (const auto& col : child->Props.Metadata->KeyColumns) {
            mappedKeyColumns.push_back(cboTree->TreeRoot->Props.Metadata->MapColumn(col));
        }

        auto stats = BuildOptimizerStatistics(child->Props, true, mappedKeyColumns);
        auto relNode = std::make_shared<TRBORelOptimizerNode>(childAliases, stats, child);
        rels.push_back(relNode);
        nodeMap.insert({child.get(), relNode});
    }

    for (auto node : cboTree->TreeNodes) {
        auto join = CastOperator<TOpJoin>(node);
        auto leftNode = nodeMap.at(join->GetLeftInput().get());
        auto rightNode = nodeMap.at(join->GetRightInput().get());
        TVector<TJoinColumn> leftKeys;
        TVector<TJoinColumn> rightKeys;

        for (auto [leftKey, rightKey] : join->JoinKeys) {
            auto mappedLeftKey = cboTree->TreeRoot->Props.Metadata->MapColumn(leftKey);
            auto mappedRightKey = cboTree->TreeRoot->Props.Metadata->MapColumn(rightKey);

            leftKeys.push_back(TJoinColumn(mappedLeftKey.GetAlias(), mappedLeftKey.GetColumnName()));
            rightKeys.push_back(TJoinColumn(mappedRightKey.GetAlias(), mappedRightKey.GetColumnName()));
        }

        result = std::make_shared<TJoinOptimizerNode>(leftNode,
            rightNode,
            leftKeys,
            rightKeys,
            ConvertToJoinKind(join->JoinKind),
            EJoinAlgoType::Undefined,
            false,
            false,
            false);

        nodeMap.insert({join.get(), result});
    }

    return result;
}

std::shared_ptr<IOperator> ConvertOptimizedTree(std::shared_ptr<IBaseOptimizerNode> tree, const TColumnLineage& lineage, TPositionHandle pos) {
    if (tree->Kind == RelNodeType) {
        auto rel = std::static_pointer_cast<TRBORelOptimizerNode>(tree);
        return rel->Op;
    } else {
        auto join = std::static_pointer_cast<TJoinOptimizerNode>(tree);
        auto leftArg = ConvertOptimizedTree(join->LeftArg, lineage, pos);
        auto rightArg = ConvertOptimizedTree(join->RightArg, lineage, pos);

        Y_ENSURE(join->LeftJoinKeys.size() == join->RightJoinKeys.size());

        TVector<std::pair<TInfoUnit, TInfoUnit>> joinKeys;
        for (size_t i=0; i<join->LeftJoinKeys.size(); i++) {
            auto leftKey = TInfoUnit(join->LeftJoinKeys[i].RelName, join->LeftJoinKeys[i].AttributeName);
            auto rightKey = TInfoUnit(join->RightJoinKeys[i].RelName, join->RightJoinKeys[i].AttributeName);
        
            if (lineage.ReverseMapping.contains(leftKey)) {
                leftKey = lineage.ReverseMapping.at(leftKey);
            }
            if (lineage.ReverseMapping.contains(rightKey)) {
                rightKey = lineage.ReverseMapping.at(rightKey);
            }
            joinKeys.push_back(std::make_pair(leftKey, rightKey));
        }

        auto joinKind = ConvertToJoinString(join->JoinType);

        auto res = std::make_shared<TOpJoin>(leftArg, rightArg, pos, joinKind, joinKeys);
        res->Props.JoinAlgo = join->JoinAlgo;
        return res;
    }
}
}

namespace NKikimr {
namespace NKqp {


/**
 * Run dynamic programming CBO and convert the resulting tree into operator tree
 *
 * In order to support good CBO with pg syntax, where all the variables in the joins
 * are transformed into Pg types, we remap the synthenic variables back into original ones
 * to run the CBO, and then map them back
 */
std::shared_ptr<IOperator> TOptimizeCBOTreeRule::SimpleMatchAndApply(const std::shared_ptr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(props);

    if (input->Kind != EOperator::CBOTree) {
        return input;
    }

    auto & Config = ctx.KqpCtx.Config;
    auto optLevel = Config->CostBasedOptimizationLevel.Get().GetOrElse(Config->GetDefaultCostBasedOptimizationLevel());

    if (optLevel <= 1) {
        return input;
    }

    auto cboTree = CastOperator<TOpCBOTree>(input);

    // Check that all inputs have statistics
    for (auto c : cboTree->Children) {
        if (!c->Props.Statistics.has_value()) {
            ctx.ExprCtx.AddWarning(
                YqlIssue(ctx.ExprCtx.GetPosition(cboTree->Pos), TIssuesIds::CBO_MISSING_TABLE_STATS,
                "Cost Based Optimizer could not be applied to this query: couldn't load statistics"
            )
        );
            return input;
        }
    }

    TVector<std::shared_ptr<TRelOptimizerNode>> rels;
    auto joinTree = ConvertJoinTree(cboTree, rels);

    bool allRowStorage = std::any_of(
        rels.begin(),
        rels.end(),
        [](std::shared_ptr<TRelOptimizerNode>& r) {return r->Stats.StorageType==EStorageType::RowStorage; });

    if (optLevel == 2 && allRowStorage) {
        return input;
    }

    TCBOSettings settings{
        .MaxDPhypDPTableSize = Config->MaxDPHypDPTableSize.Get().GetOrElse(TDqSettings::TDefault::MaxDPHypDPTableSize),
        .ShuffleEliminationJoinNumCutoff = Config->ShuffleEliminationJoinNumCutoff.Get().GetOrElse(TDqSettings::TDefault::ShuffleEliminationJoinNumCutoff)
    };

    // Shuffle elimination is currently disabled
    //bool enableShuffleElimination = ctx.KqpCtx.Config->OptShuffleElimination.Get().GetOrElse(ctx.KqpCtx.Config->GetDefaultEnableShuffleElimination());

    auto providerCtx = TRBOProviderContext(ctx.KqpCtx, optLevel);
    auto opt = std::unique_ptr<IOptimizerNew>(MakeNativeOptimizerNew(providerCtx, settings, ctx.ExprCtx, false, nullptr, nullptr));

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Converted join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    {
        YQL_PROFILE_SCOPE(TRACE, "CBO");
        joinTree = opt->JoinSearch(joinTree, ctx.KqpCtx.GetOptimizerHints());
    }

    if (NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE)) {
        std::stringstream str;
        str << "Optimizied join tree:\n";
        joinTree->Print(str);
        YQL_CLOG(TRACE, CoreDq) << str.str();
    }

    return ConvertOptimizedTree(joinTree, cboTree->Props.Metadata->ColumnLineage, cboTree->Pos);
}

}
}