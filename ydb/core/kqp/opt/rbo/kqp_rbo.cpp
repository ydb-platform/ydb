#include "kqp_rbo.h"
#include <ydb/core/kqp/opt/rbo/analysis/logical_name_constraints.h>

#include <yql/essentials/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

namespace {

void ValidateNoDuplicateOutputIUs(TOpRoot& root) {
    for (const auto& iter : root) {
        THashSet<TInfoUnit, TInfoUnit::THashFunction> seen;
        for (const auto& iu : iter.Current->GetOutputIUs()) {
            Y_ENSURE(!seen.contains(iu), "Duplicate visible column " << iu.GetFullName() << " after " << iter.Current->GetExplainName());
            seen.insert(iu);
        }
    }
}

} // anonymous namespace

bool ISimplifiedRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {

    auto output = SimpleMatchAndApply(input, ctx, props);
    if (input != output) {
        input = output;
        return true;
    } else {
        return false;
    }
}

TRuleBasedStage::TRuleBasedStage(TString&& stageName, TVector<std::unique_ptr<IRule>>&& rules)
    : IRBOStage(std::move(stageName))
    , Rules(std::move(rules)) {
    for (const auto& r : Rules) {
        Props |= r->Props;
    }
}

void ComputeRequiredProps(TOpRoot& root, ui32 props, TRBOContext& ctx, TString stageName) {
    // FIXME: Parents are currently always required, because we need to update them when a rule fires
    root.ComputeParents();
    //if (props & ERuleProperties::RequireParents) {
    //    root.ComputeParents();
    //}
    if (props & (ERuleProperties::RequireTypes | ERuleProperties::RequireStatistics)) {
        if (root.ComputeTypes(ctx) != IGraphTransformer::TStatus::Ok) {
            Y_ENSURE(false, TStringBuilder() << "RBO type annotation failed in stage " << stageName);
        }
    }
    if (props & (ERuleProperties::RequireMetadata | ERuleProperties::RequireStatistics)) {
        root.ComputePlanMetadata(ctx);
    }
    if (props & ERuleProperties::RequireStatistics) {
        root.ComputePlanStatistics(ctx);
    }
    if (props & ERuleProperties::RequireLiveness) {
        ComputePlanLiveness(root);
    }
    if (props & ERuleProperties::RequireNameConstraints) {
        ComputePlanNameConstraints(root);
    }
    if (props & ERuleProperties::RequireAliases) {
        ComputePlanAliases(root);
    }
}

/**
 * Run a rule-based stage
 *
 * Currently we obtain an iterator to the operators, match the rules, and if at least one matched we
 * apply it and start again.
 *
 * TODO: We should have a clear list of properties that are reqiuired by the rules of current stage and
 * ensure they are computed/maintained properly
 *
 * TODO: Add sanity checks that can be tunred on in debug mode to immediately catch transformation problems
 */
void TRuleBasedStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    bool fired = true;
    ui32 numMatches = 0;
    const ui32 maxNumOfMatches = 1000;
    bool needToLog = NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE);

    while (fired && numMatches < maxNumOfMatches) {
        fired = false;

        for (auto iter : root) {
            for (const auto& rule : Rules) {
                auto op = iter.Current;

                if (rule->MatchAndApply(op, ctx, root.PlanProps)) {
                    fired = true;

                    YQL_CLOG(TRACE, CoreDq) << "Applied rule:" << rule->RuleName;

                    // If the original operator had parents, update all parents
                    if (iter.Current->Parents.size()) {
                        for (auto & [parent, parentIdx] : iter.Current->Parents) {
                            parent->Children[parentIdx] = op;
                        }
                    } 
                    // Otherwise, if its not a subplan, it was root, so update root
                    else if (!iter.SubplanIU) {
                        root.SetInput(op);
                    }
                    // Finally, it's a subplan, so update the subplan 
                    else {
                        root.PlanProps.Subplans.Replace(*iter.SubplanIU, op);
                    }

                    if (needToLog && rule->LogRule) {
                        YQL_CLOG(TRACE, CoreDq) << "Plan after applying rule:\n" << root.PlanToString(ctx.ExprCtx);
                    }

                    ComputeRequiredProps(root, Props, ctx, StageName);
                    ++numMatches;
                    break;
                }
            }

            if (fired) {
                break;
            }
        }
    }

    Y_ENSURE(numMatches < maxNumOfMatches);
}

TExprNode::TPtr TRuleBasedOptimizer::Optimize(TOpRoot& root, TRBOContext& rboCtx) {
    bool needToLog = NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE);
    auto& ctx = rboCtx.ExprCtx;

    if (needToLog) {
        YQL_CLOG(TRACE, CoreDq) << "Original plan:\n" << root.PlanToString(ctx);
    }

    for (const auto& stage : Stages) {
        YQL_CLOG(TRACE, CoreDq) << "Running stage: " << stage->StageName;
        ComputeRequiredProps(root, stage->Props, rboCtx, stage->StageName);
        if (needToLog) {
            YQL_CLOG(TRACE, CoreDq) << "Before stage:\n" << root.PlanToString(ctx);
        }
        stage->RunStage(root, rboCtx);
        ValidateNoDuplicateOutputIUs(root);
        if (needToLog) {
            YQL_CLOG(TRACE, CoreDq) << "After stage:\n" << root.PlanToString(ctx);
        }
    }

    YQL_CLOG(TRACE, CoreDq) << "New RBO finished, generating physical plan";

    auto convertProps = ERuleProperties::RequireParents | ERuleProperties::RequireTypes | ERuleProperties::RequireStatistics;
    ComputeRequiredProps(root, convertProps, rboCtx, "Physical plan generaion");
    if (needToLog) {
        YQL_CLOG(TRACE, CoreDq) << "Final plan before generation:\n" << root.PlanToString(ctx, EPrintPlanOptions::PrintFullMetadata | EPrintPlanOptions::PrintBasicStatistics);
    }

    ui64 counter = 0;
    THashMap<IOperator*, ui32> operatorIds;
    rboCtx.ExecutionJson = root.GetExecutionJson(counter, operatorIds);
    rboCtx.ExplainJson = root.GetExplainJson(counter, operatorIds);

    return ConvertToPhysical(root, rboCtx);
}
} // namespace NKqp
} // namespace NKikimr
