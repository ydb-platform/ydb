#pragma once

#include "traces/kqp_rbo_trace_log.h"

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>
#include <ydb/core/kqp/tracing/kqp_compile_trace.h>

#include <library/cpp/json/writer/json_value.h>

#include <optional>
#include <utility>
#include <util/generic/hash.h>
#include <util/generic/string.h>

namespace NYql {

struct TExprContext;
struct TTypeAnnotationContext;
class IGraphTransformer;

} // namespace NYql

namespace NMiniKQL {

class IFunctionRegistry;

} // namespace NMiniKQL

namespace NKikimr::NKqp {

class TRBOContext {
public:
    TRBOContext(NOpt::TKqpOptimizeContext& kqpCtx, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx,
        NYql::IGraphTransformer& typeAnnTransformer, const NMiniKQL::IFunctionRegistry& funcRegistry);

    bool NeedToLog() const {
        return TraceLog.NeedToLog();
    }

    TCompileTrace* GetCompileTrace() const;

    template <typename TEnricher>
    void EnrichRuleLog(TEnricher&& enricher) {
        if (auto* tile = TraceLog.CurrentRuleTile()) {
            std::forward<TEnricher>(enricher)(*tile);
        }
    }

    template <typename TEnricher>
    void EnrichRuleLogAfterTree(TEnricher&& enricher) {
        if (TraceLog.CurrentRuleTile()) {
            TraceLog.AddPostBuildEnricher(std::forward<TEnricher>(enricher));
        }
    }

    NOpt::TKqpOptimizeContext& KqpCtx;
    NYql::TExprContext& ExprCtx;
    NYql::TTypeAnnotationContext& TypeCtx;
    NYql::IGraphTransformer& TypeAnnTransformer;
    const NMiniKQL::IFunctionRegistry& FuncRegistry;
    NOpt::TKqpProviderContext CBOCtx;
    std::optional<NJson::TJsonValue> ExecutionJson;
    std::optional<NJson::TJsonValue> ExplainJson;
    bool EmptyPreamble = false;

    TRBOTraceLog TraceLog;

    // Rule attempts of the running rule-based stage, reported in its compilation span.
    struct TRuleStageTraceStats {
        struct TRuleStats {
            TString Name;
            ui64 QuickChecks = 0;
            ui64 QuickRejected = 0;
            ui64 QuickMatchCycles = 0;
            ui64 Attempts = 0;
            ui64 Applied = 0;
            TDuration UnappliedTime;
            ui64 UnappliedCpuUs = 0;
        };

        ui64 QuickChecks = 0;
        ui64 QuickRejected = 0;
        ui64 Attempts = 0;
        ui64 Applied = 0;
        TDuration UnappliedTime;
        TVector<TRuleStats> Rules;
    };
    TRuleStageTraceStats RuleStageTraceStats;

    // Attributes a rule reports for the compilation span of its application,
    // e.g. the size of its input. Collected only while rules are traced.
    TVector<std::pair<const char*, i64>> RuleTraceAttributes;
    bool TraceRules = false;

    void AddRuleTraceAttribute(const char* name, i64 value) {
        if (TraceRules) {
            RuleTraceAttributes.emplace_back(name, value);
        }
    }
};

} // namespace NKikimr::NKqp
