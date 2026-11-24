#pragma once

#include "traces/kqp_rbo_trace_log.h"

#include <ydb/core/kqp/opt/kqp_opt.h>
#include <ydb/core/kqp/opt/logical/kqp_opt_cbo.h>

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

    TRBOTraceLog TraceLog;
};

} // namespace NKikimr::NKqp
