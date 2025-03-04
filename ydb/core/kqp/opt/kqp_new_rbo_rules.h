#pragma once

#include "kqp_new_rbo.h"


namespace NKikimr {
namespace NKqp {

class TPushFilterRule : public IRule {
    public:
    virtual bool TestAndApply(std::shared_ptr<IOperator> & input, TExprContext& ctx, const TIntrusivePtr<TKqpOptimizeContext>& kqpCtx, TTypeAnnotationContext& typeCtx, const TKikimrConfiguration::TPtr& config) override;
};

extern TVector<std::shared_ptr<IRule>> RuleStage1;

}
}