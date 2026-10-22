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
    auto* trace = ctx.GetCompileTrace();
    auto traceScope = [&](const char* name) {
        return TCompileTraceScope(trace, TCompileTraceLevels::Detail, name, "NewRBO");
    };

    if (HasProperty(props, ERuleProperties::RequireOutputIUs) && !HasProperty(computedProps, ERuleProperties::RequireOutputIUs)) {
        auto scope = traceScope("Compute output IUs");
        scope.Span().Attribute("ydb.rbo.property", TString("output IUs"));
        root.RecomputeOutputIUsSubtree();
        computedProps |= ERuleProperties::RequireOutputIUs;
    }

    if (HasProperty(props, ERuleProperties::RequireParents) && !HasProperty(computedProps, ERuleProperties::RequireParents)) {
        auto scope = traceScope("Compute parents");
        scope.Span().Attribute("ydb.rbo.property", TString("parents"));
        root.ComputeParents();
        computedProps |= ERuleProperties::RequireParents;
    }

    if (HasProperty(props, ERuleProperties::RequireTypes) && !HasProperty(computedProps, ERuleProperties::RequireTypes)) {
        auto scope = traceScope("Compute types");
        scope.Span().Attribute("ydb.rbo.property", TString("types"));
        if (root.ComputeTypes(ctx) != IGraphTransformer::TStatus::Ok) {
            Y_ENSURE(false, TStringBuilder() << "RBO type annotation failed in stage " << stageName);
        }
        computedProps |= ERuleProperties::RequireTypes;
    }

    if (HasProperty(props, ERuleProperties::RequireMetadata) && !HasProperty(computedProps, ERuleProperties::RequireMetadata)) {
        auto scope = traceScope("Compute metadata");
        scope.Span().Attribute("ydb.rbo.property", TString("metadata"));
        root.ComputePlanMetadata(ctx);
        computedProps |= ERuleProperties::RequireMetadata;
    }

    if (HasProperty(props, ERuleProperties::RequireStatistics) && !HasProperty(computedProps, ERuleProperties::RequireStatistics)) {
        auto scope = traceScope("Compute statistics");
        scope.Span().Attribute("ydb.rbo.property", TString("statistics"));
        root.ComputePlanStatistics(ctx);
        computedProps |= ERuleProperties::RequireStatistics;
    }

    if (HasProperty(props, ERuleProperties::RequireLiveness) && !HasProperty(computedProps, ERuleProperties::RequireLiveness)) {
        auto scope = traceScope("Compute liveness");
        scope.Span().Attribute("ydb.rbo.property", TString("liveness"));
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
    auto* trace = ctx.GetCompileTrace();
    const bool traceRules = trace && trace->IsEnabled(TCompileTraceLevels::Step);
    const bool traceRuleSpans = traceRules && trace->IsEnabled(TCompileTraceLevels::Detail);
    auto& traceStats = ctx.RuleStageTraceStats;
    ctx.TraceRules = traceRuleSpans;
    if (traceRuleSpans) {
        traceStats.Rules.resize(Rules.size());
        for (size_t i = 0; i < Rules.size(); ++i) {
            traceStats.Rules[i].Name = Rules[i]->RuleName;
        }
    }

    while (fired && numMatches < maxNumOfMatches) {
        fired = false;

        for (const auto& iter : root) {
            // A Replicate port's child is the producer every port shares: rules
            // must not rewrite through a port as if it were a pass-through
            // operator. Producer rewrites use the shared slot.
            for (size_t ruleIndex = 0; ruleIndex < Rules.size(); ++ruleIndex) {
                const auto& rule = Rules[ruleIndex];
                auto* ruleStats = traceRuleSpans ? &traceStats.Rules[ruleIndex] : nullptr;
                const ui64 quickStart = ruleStats ? GetCycleCount() : 0;
                const bool quickMatch = rule->QuickMatch(TIntrusivePtr<IOperator>(iter.Current), root.PlanProps);
                if (ruleStats) {
                    ruleStats->QuickMatchCycles += GetCycleCount() - quickStart;
                    ++ruleStats->QuickChecks;
                    ruleStats->QuickRejected += !quickMatch;
                }
                if (traceRules) {
                    ++traceStats.QuickChecks;
                    traceStats.QuickRejected += !quickMatch;
                }
                if (!quickMatch) {
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
                const TInstant ruleStart = traceRules ? trace->Now() : TInstant::Zero();
                std::optional<TCompileTraceDeferredScope> ruleScope;
                if (traceRuleSpans) {
                    ruleScope.emplace(trace, "Rule: " + rule->RuleName, "NewRBO");
                }
                ctx.RuleTraceAttributes.clear();
                const bool ruleApplied = rule->MatchAndApply(op, ctx, root.PlanProps);
                Y_ENSURE(op, "Rule left a null plan edge");
                traceAttempt.CloseRule();
                if (traceRules) {
                    const TInstant ruleEnd = trace->Now();
                    if (ruleScope) {
                        auto span = ruleScope->Finish(ruleApplied);
                        if (span) {
                            span.Attribute("ydb.rbo.rule", rule->RuleName);
                            span.Attribute("ydb.rbo.applied", ruleApplied);
                            for (const auto& [name, value] : ctx.RuleTraceAttributes) {
                                span.Attribute(name, value);
                            }
                            span.End();
                        }
                    }
                    ++traceStats.Attempts;
                    if (ruleStats) {
                        ++ruleStats->Attempts;
                        ruleStats->Applied += ruleApplied;
                        if (!ruleApplied) {
                            ruleStats->UnappliedTime += ruleEnd - ruleStart;
                            ruleStats->UnappliedCpuUs += ruleScope->GetCpuTime();
                        }
                    }
                    if (!ruleApplied) {
                        traceStats.UnappliedTime += ruleEnd - ruleStart;
                    } else {
                        ++traceStats.Applied;
                    }
                }

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
    auto* trace = rboCtx.GetCompileTrace();
    int stageCounter = 0;

    for (auto & rootPtr : roots) {
        auto & root = *rootPtr;
        root.PlanProps.StageGraph.StageCounter = stageCounter;
        SubmitInitialPlanTrace(root, rboCtx);

        if (needToLog) {
            YQL_CLOG(TRACE, CoreDq) << "Original plan:\n" << root.PlanToString(ctx);
        }

        {
            TCompileTraceScope rulesTraceScope(trace, TCompileTraceLevels::Phase, "Rule-based optimization", "NewRBO");
            for (const auto& stage : Stages) {
                TCompileTraceScope traceScope(trace, TCompileTraceLevels::Step, "RBO stage: " + stage->StageName, "NewRBO");
                traceScope.Span().Attribute("ydb.rbo.stage", stage->StageName);
                rboCtx.RuleStageTraceStats = {};
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
                if (const auto& stats = rboCtx.RuleStageTraceStats; stats.QuickChecks) {
                    traceScope.Span()
                        .Attribute("ydb.rbo.quick_checks", static_cast<i64>(stats.QuickChecks))
                        .Attribute("ydb.rbo.quick_rejected", static_cast<i64>(stats.QuickRejected))
                        .Attribute("ydb.rbo.rule_attempts", static_cast<i64>(stats.Attempts))
                        .Attribute("ydb.rbo.rules_applied", static_cast<i64>(stats.Applied))
                        .Attribute("ydb.rbo.unapplied_rules_us", static_cast<i64>(stats.UnappliedTime.MicroSeconds()));
                    // One summary per rule, rather than a span per rejected match.
                    for (const auto& rule : stats.Rules) {
                        traceScope.Span().Event("Rule attempts", {
                            {"ydb.rbo.rule", rule.Name},
                            {"ydb.rbo.quick_checks", static_cast<i64>(rule.QuickChecks)},
                            {"ydb.rbo.quick_rejected", static_cast<i64>(rule.QuickRejected)},
                            {"ydb.rbo.quick_match_us", static_cast<i64>(CyclesToDuration(rule.QuickMatchCycles).MicroSeconds())},
                            {"ydb.rbo.rule_attempts", static_cast<i64>(rule.Attempts)},
                            {"ydb.rbo.rules_applied", static_cast<i64>(rule.Applied)},
                            {"ydb.rbo.unapplied_rules_us", static_cast<i64>(rule.UnappliedTime.MicroSeconds())},
                            {"ydb.rbo.unapplied_rules_cpu_us", static_cast<i64>(rule.UnappliedCpuUs)},
                        });
                    }
                }
            }
        }

        TCompileTraceScope finalizeTraceScope(trace, TCompileTraceLevels::Step, "Finalize RBO plan", "NewRBO");
        auto convertProps = ERuleProperties::RequireParents | ERuleProperties::RequireStatistics
            | ERuleProperties::RequireLiveness | ERuleProperties::RequireTypes;
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

    {
        TCompileTraceScope traceScope(trace, TCompileTraceLevels::Step, "Build plan JSON", "NewRBO");
        auto [execJson, explainJson] = BuildPlans(roots);
        rboCtx.ExecutionJson = execJson;
        rboCtx.ExplainJson = explainJson;
    }

    return ConvertToPhysical(roots, rboCtx);
}
} // namespace NKqp
} // namespace NKikimr
