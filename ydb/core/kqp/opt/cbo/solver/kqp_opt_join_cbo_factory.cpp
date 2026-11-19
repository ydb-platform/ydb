#include "kqp_opt_join_cbo_factory.h"

#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_join_cost_based.h>


namespace NKikimr::NKqp {

namespace {
class TKqpOptimizerFactory : public IOptimizerFactory {
public:
    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerNative(IProviderContext& pctx, NYql::TExprContext& ectx, const TCBOSettings& settings) const override {
      return IOptimizerNew::TPtr(MakeNativeOptimizerNew(pctx, settings, ectx, false, nullptr));
    }

    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerPG(IProviderContext& /*pctx*/, NYql::TExprContext& /*ctx*/, const TPGSettings& /*settings*/) const override {
        Y_ABORT("PG optimizer is not supported in the ydb-owned CBO");
    }
};
}

IOptimizerFactory::TPtr MakeCBOOptimizerFactory() {
    return std::make_shared<TKqpOptimizerFactory>();
}

}
