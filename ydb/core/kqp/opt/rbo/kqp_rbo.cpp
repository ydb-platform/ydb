#include "kqp_rbo.h"
#include "traces/kqp_rbo_rule_trace.h"
#include "kqp_plan_conversion_utils.h"

#include <ydb/core/kqp/opt/rbo/analysis/logical_name_constraints.h>

#include <yql/essentials/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool HasProperty(ui32 props, ui32 property) {
    return (props & property) == property;
}

} // namespace

bool ISimplifiedRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    if (!QuickMatch(input)) {
        return false;
    }

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

void EnsureRequiredProps(TOpRoot& root, ui32 props, ui32& computedProps, TRBOContext& ctx, const TString& stageName) {
    if (HasProperty(props, ERuleProperties::RequireOutputIUs) && !HasProperty(computedProps, ERuleProperties::RequireOutputIUs)) {
        root.RecomputeOutputIUsSubtree();
        computedProps |= ERuleProperties::RequireOutputIUs;
    }

    if (HasProperty(props, ERuleProperties::RequireParents) && !HasProperty(computedProps, ERuleProperties::RequireParents)) {
        root.ComputeParents();
        computedProps |= ERuleProperties::RequireParents;
    }

    if (HasProperty(props, ERuleProperties::RequireTypes) && !HasProperty(computedProps, ERuleProperties::RequireTypes)) {
        if (root.ComputeTypes(ctx) != IGraphTransformer::TStatus::Ok) {
            Y_ENSURE(false, TStringBuilder() << "RBO type annotation failed in stage " << stageName);
        }
        computedProps |= ERuleProperties::RequireTypes;
    }

    if (HasProperty(props, ERuleProperties::RequireMetadata) && !HasProperty(computedProps, ERuleProperties::RequireMetadata)) {
        root.ComputePlanMetadata(ctx);
        computedProps |= ERuleProperties::RequireMetadata;
    }

    if (HasProperty(props, ERuleProperties::RequireStatistics) && !HasProperty(computedProps, ERuleProperties::RequireStatistics)) {
        root.ComputePlanStatistics(ctx);
        computedProps |= ERuleProperties::RequireStatistics;
    }

    if (HasProperty(props, ERuleProperties::RequireLiveness) && !HasProperty(computedProps, ERuleProperties::RequireLiveness)) {
        ComputePlanLiveness(root);
        computedProps |= ERuleProperties::RequireLiveness;
    }

    if (HasProperty(props, ERuleProperties::RequireNameConstraints) && !HasProperty(computedProps, ERuleProperties::RequireNameConstraints)) {
        ComputePlanNameConstraints(root);
        computedProps |= ERuleProperties::RequireNameConstraints;
    }

    if (HasProperty(props, ERuleProperties::RequireAliases) && !HasProperty(computedProps, ERuleProperties::RequireAliases)) {
        ComputePlanAliases(root);
        computedProps |= ERuleProperties::RequireAliases;
    }
}

void ComputeRequiredProps(TOpRoot& root, ui32 props, TRBOContext& ctx, TString stageName) {
    ui32 computedProps = 0;
    EnsureRequiredProps(root, props, computedProps, ctx, stageName);
}

/**
 * Run a rule-based stage
 *
 * Currently we obtain an iterator to the operators, match the rules, and if at least one matched we
 * apply it and start again.
 *
 * TODO: Add sanity checks that can be tunred on in debug mode to immediately catch transformation problems
 */
void TRuleBasedStage::RunStage(TOpRoot& root, TRBOContext& ctx) {
    bool fired = true;
    ui32 numMatches = 0;
    const ui32 maxNumOfMatches = 1000;
    bool needToLog = NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE);
    ui32 computedProps = 0;

    while (fired && numMatches < maxNumOfMatches) {
        fired = false;

        for (const auto& iter : root) {
            for (const auto& rule : Rules) {
                auto op = iter.Current;
                if (!rule->QuickMatch(op)) {
                    continue;
                }

                EnsureRequiredProps(root, rule->Props, computedProps, ctx, StageName);

                TRuleTraceAttempt traceAttempt(ctx, rule->RuleName);
                const bool ruleApplied = rule->MatchAndApply(op, ctx, root.PlanProps);
                traceAttempt.CloseRule();

                if (!ruleApplied) {
                    traceAttempt.SubmitIfHasInfo(root, StageName);
                    continue;
                }

                if (ruleApplied) {
                    fired = true;

                    YQL_CLOG(TRACE, CoreDq) << "Applied rule:" << rule->RuleName;

                    if (op != iter.Current) {
                        Y_ENSURE(HasProperty(computedProps, ERuleProperties::RequireParents),
                            TStringBuilder() << "Rule " << rule->RuleName << " replaced an operator without requiring parents");

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
                    }

                    if (needToLog && rule->LogRule) {
                        YQL_CLOG(TRACE, CoreDq) << "Plan after applying rule:\n" << root.PlanToString(ctx.ExprCtx);
                    }

                    traceAttempt.SubmitApplied(root, StageName);

                    // The rule has fired, therefore we invalidate ALL the properties, they will be recomputed
                    // as soon as they are needed by next rules.

                    // TODO: In the future, we probably want to be smarter here: have API which tells us
                    // what the rule changed, invalidate partially, recompute incrementally.
                    computedProps = 0;

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

    SubmitInitialPlanTrace(root, rboCtx);

    if (needToLog) {
        YQL_CLOG(TRACE, CoreDq) << "Original plan:\n" << root.PlanToString(ctx);
    }

    for (const auto& stage : Stages) {
        if (rboCtx.NeedToLog()) {
            rboCtx.TraceLog.stage(std::string(stage->StageName.c_str()));
        }
        YQL_CLOG(TRACE, CoreDq) << "Running stage: " << stage->StageName;
        if (stage->NeedsInitialProps()) {
            ComputeRequiredProps(root, stage->Props, rboCtx, stage->StageName);
        }
        if (needToLog) {
            YQL_CLOG(TRACE, CoreDq) << "Before stage:\n" << root.PlanToString(ctx);
        }
        stage->RunStage(root, rboCtx);
        if (needToLog) {
            YQL_CLOG(TRACE, CoreDq) << "After stage:\n" << root.PlanToString(ctx);
        }
    }

    YQL_CLOG(TRACE, CoreDq) << "New RBO finished, generating physical plan";

    auto convertProps = ERuleProperties::RequireParents | ERuleProperties::RequireStatistics
        | ERuleProperties::RequireLiveness;
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
