#pragma once

#include "traces/kqp_rbo_trace_log.h"
#include "verification/rule_application.h"

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>

#include <library/cpp/json/writer/json_value.h>

#include <optional>
#include <utility>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {

struct TExprContext;
struct TTypeAnnotationContext;
class IGraphTransformer;

} // namespace NYql

namespace NMiniKQL {

class IFunctionRegistry;

} // namespace NMiniKQL

namespace NKikimr::NKqp {

struct TRBORuleApplicationDebugState {
    void Reset(std::optional<ui64> stopAfter) {
        StopAfter = stopAfter;
        Stopped = false;
        Applications.clear();
    }

    bool OnApplied(const TString& stageName, const TString& ruleName) {
        if (!StopAfter) {
            return false;
        }

        Applications.push_back(TRBORuleApplicationV1{
            Applications.size() + 1,
            stageName,
            ruleName,
        });
        if (Applications.size() != *StopAfter) {
            return false;
        }

        Stopped = true;
        return true;
    }

    std::optional<ui64> StopAfter;
    bool Stopped = false;
    TVector<TRBORuleApplicationV1> Applications;
};

class TRBOContext {
public:
    TRBOContext(NOpt::TKqpOptimizeContext& kqpCtx, NYql::TExprContext& ctx, NYql::TTypeAnnotationContext& typeCtx,
        NYql::IGraphTransformer& typeAnnTransformer, const NMiniKQL::IFunctionRegistry& funcRegistry);

    bool NeedToLog() const {
        return TraceLog.NeedToLog();
    }

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

    // Query-local, opt-in diagnostic state.  Failed rule attempts never call
    // OnApplied, so the contiguous sequence contains committed applications only.
    TRBORuleApplicationDebugState RuleApplicationDebug;

    TRBOTraceLog TraceLog;
};

} // namespace NKikimr::NKqp
