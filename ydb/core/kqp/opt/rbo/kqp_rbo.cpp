#include "kqp_rbo.h"
#include "traces/kqp_rbo_rule_trace.h"
#include "kqp_plan_conversion_utils.h"

#include <yql/essentials/utils/log/log.h>

namespace NKikimr {
namespace NKqp {

namespace {

bool HasProperty(ui32 props, ui32 property) {
    return (props & property) == property;
}

std::pair<NJson::TJsonValue, NJson::TJsonValue> BuildPlans(const TVector<TIntrusivePtr<TOpRoot>>& roots) {
    ui64 counter = 0;
    ui32 operatorIdx = 0;
    THashMap<IOperator*, ui32> operatorIds;
    TVector<NJson::TJsonValue> executionJsons;
    TVector<NJson::TJsonValue> explainJsons;

    for (auto & rootPtr : roots) {
        executionJsons.push_back(rootPtr->GetExecutionJson(counter, operatorIdx, operatorIds));
        explainJsons.push_back(rootPtr->GetExplainJson(counter, operatorIds));
    }

    if (roots.size()==1) {
        return std::make_pair(executionJsons[0], explainJsons[0]);
    } else {
        NJson::TJsonValue execResult;
        execResult["PlanNodeId"] = counter++;
        execResult["PlanNodeType"] = "ResultSets";
        execResult["Node Type"] = "ResultSets";

        auto execList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
        for (auto & execJson : executionJsons) {
            execList.AppendValue(execJson);
        }
        execResult["Plans"] = execList;

        NJson::TJsonValue explainResult;
        explainResult["PlanNodeId"] = counter++;
        explainResult["PlanNodeType"] = "ResultSets";
        explainResult["Node Type"] = "ResultSets";

        auto explainList = NJson::TJsonValue(NJson::EJsonValueType::JSON_ARRAY);
        for (auto & explainJson : explainJsons) {
            explainList.AppendValue(explainJson);
        }
        explainResult["Plans"] = explainList;

        return std::make_pair(execResult, explainResult);
    }
}
} // namespace

bool ISimplifiedRule::MatchAndApply(TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    if (!QuickMatch(input)) {
        return false;
    }

    const auto* previous = input.get();
    input = SimpleMatchAndApply(input, ctx, props);
    Y_ENSURE(input, "Rule returned a null plan");
    return input.get() != previous;
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
            // A Replicate port's child is the producer every port shares: rules
            // must not rewrite through a port as if it were a pass-through
            // operator. Producer rewrites use the shared slot.
            for (const auto& rule : Rules) {
                if (!rule->QuickMatch(TIntrusivePtr<IOperator>(iter.Current), root.PlanProps)) {
                    continue;
                }

                EnsureRequiredProps(root, rule->Props, computedProps, ctx, StageName);

                TRuleTraceAttempt traceAttempt(ctx, rule->RuleName);
                // Borrowed traversal entries must not be dereferenced after a
                // destructive rewrite. Edit the owning slot, not a copied owner.
                auto* parent = iter.Parent;
                const auto childIndex = iter.ChildIndex;
                const auto subplanIU = iter.SubplanIU;
                auto& op = parent ? parent->MutableChild(childIndex)
                    : subplanIU ? root.PlanProps.Subplans.MutablePlan(*subplanIU)
                    : root.MutableChild(0);
                const bool ruleApplied = rule->MatchAndApply(op, ctx, root.PlanProps);
                Y_ENSURE(op, "Rule left a null plan edge");
                traceAttempt.CloseRule();

                if (!ruleApplied) {
                    traceAttempt.SubmitIfHasInfo(root, StageName);
                    continue;
                }

                if (ruleApplied) {
                    fired = true;

                    YQL_CLOG(TRACE, CoreDq) << "Applied rule:" << rule->RuleName;

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

TExprNode::TPtr TRuleBasedOptimizer::Optimize(const TVector<TIntrusivePtr<TOpRoot>>& roots, TRBOContext& rboCtx) {
    bool needToLog = NYql::NLog::YqlLogger().NeedToLog(NYql::NLog::EComponent::CoreDq, NYql::NLog::ELevel::TRACE);
    auto& ctx = rboCtx.ExprCtx;
    int stageCounter = 0;

    for (auto & rootPtr : roots) {
        auto & root = *rootPtr;
        root.PlanProps.StageGraph.StageCounter = stageCounter;
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

        auto convertProps = ERuleProperties::RequireParents | ERuleProperties::RequireStatistics
            | ERuleProperties::RequireLiveness;
        ComputeRequiredProps(root, convertProps, rboCtx, "Physical plan generaion");
        TUnorderedIUs live;
        for (const auto& item : root) {
            live.UnionWith(GetLiveOut(item.Current));
        }
        root.PlanProps.InfoUnitRegistry.FinalizeDisplayNames(live);
        if (needToLog) {
            YQL_CLOG(TRACE, CoreDq) << "Final plan before generation:\n" << root.PlanToString(ctx, EPrintPlanOptions::PrintFullMetadata | EPrintPlanOptions::PrintBasicStatistics);
        }

        stageCounter = root.PlanProps.StageGraph.StageCounter;
    }

    YQL_CLOG(TRACE, CoreDq) << "New RBO finished, generating physical plan";

    auto [execJson, explainJson] = BuildPlans(roots);
    rboCtx.ExecutionJson = execJson;
    rboCtx.ExplainJson = explainJson;

    return ConvertToPhysical(roots, rboCtx);
}
} // namespace NKqp
} // namespace NKikimr
