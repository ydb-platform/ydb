#pragma once

#include "kqp_opt.h"

#include <ydb/core/kqp/opt/kqp_operator.h>

namespace NKikimr {
namespace NKqp {

using namespace NOpt;


class IRule {
    public:
    IRule(TString name) : RuleName(name) {}

    virtual std::shared_ptr<IOperator> TestAndApply(std::shared_ptr<IOperator>& input, 
        TExprContext& ctx,
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config) = 0;
    virtual ~IRule() = default;

    TString RuleName;
};

class TRuleBasedOptimizer {
    public:
    TRuleBasedOptimizer(TVector<TVector<std::shared_ptr<IRule>>> stages, 
        const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, 
        TTypeAnnotationContext& typeCtx, 
        const TKikimrConfiguration::TPtr& config) : Stages(stages),
        KqpCtx(kqpCtx),
        TypeCtx(typeCtx),
        Config(config) {}
    
    void Optimize(TOpRoot & root,  TExprContext& ctx);

    TVector<TVector<std::shared_ptr<IRule>>> Stages;
    const TIntrusivePtr<TKqpOptimizeContext>& KqpCtx;
    TTypeAnnotationContext& TypeCtx;
    const TKikimrConfiguration::TPtr& Config;
};

}
}