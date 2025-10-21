#include "dq_opt_join_cbo_factory.h"

#include <ydb/library/yql/dq/opt/dq_opt_join_cost_based.h>

#include <yql/essentials/parser/pg_wrapper/interface/optimizer.h>

namespace NYql::NDq {

namespace {
class TDqOptimizerFactory : public IOptimizerFactory {
public:
    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerNative(IProviderContext& pctx, TExprContext& ectx, const TNativeSettings& settings) const override {
      return IOptimizerNew::TPtr(MakeNativeOptimizerNew(pctx, settings.MaxDPhypDPTableSize, ectx, false, nullptr));
    }

    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerPG(IProviderContext& pctx, TExprContext& ctx, const TPGSettings& settings) const override {
        return IOptimizerNew::TPtr(MakePgOptimizerNew(pctx, ctx, settings.Logger));
    }
};
}

IOptimizerFactory::TPtr MakeCBOOptimizerFactory() {
    return std::make_shared<TDqOptimizerFactory>();
}

}
