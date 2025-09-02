#include "cbo_simple.h"

#include <yql/essentials/utils/yql_panic.h>
#include <yql/essentials/parser/pg_wrapper/interface/optimizer.h>

namespace NYql {

namespace {

class TSimpleOptimizerFactory : public IOptimizerFactory {
public:
    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerNative(IProviderContext& pctx, TExprContext& ctx, const TNativeSettings& settings) const override {
        Y_UNUSED(pctx);
        Y_UNUSED(ctx);
        Y_UNUSED(settings);
        YQL_ENSURE(false, "Native CBO is not supported here");
        Y_UNREACHABLE();
    }

    virtual IOptimizerNew::TPtr MakeJoinCostBasedOptimizerPG(IProviderContext& pctx, TExprContext& ctx, const TPGSettings& settings) const override {
        return IOptimizerNew::TPtr(MakePgOptimizerNew(pctx, ctx, settings.Logger));
    }
};

}

IOptimizerFactory::TPtr MakeSimpleCBOOptimizerFactory() {
    return std::make_shared<TSimpleOptimizerFactory>();
}

} // namespace NYql
