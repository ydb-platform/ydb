#pragma once

#include "kqp_opt.h"

#include <ydb/core/kqp/opt/kqp_operator.h>

namespace NKikimr {
namespace NKqp {

class TRule {
    public:
    bool Test(const IOperator& input);
    void Apply(IOperator input);
};

class TRuleBasedOptimizer {
    public:
    bool Optimize(IOperator root) { return true; }
    TVector<TVector<TRule>> Stages;
};

}
}