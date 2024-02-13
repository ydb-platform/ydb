#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer.h>
#include <ydb/library/yql/core/cbo/cbo_optimizer_new.h>

#include <functional>

namespace NYql {

struct TExprContext;

IOptimizer* MakePgOptimizer(const IOptimizer::TInput& input, const std::function<void(const TString&)>& log = {});

IOptimizerNew* MakePgOptimizerNew(IProviderContext& pctx, TExprContext& ctx, const std::function<void(const TString&)>& log = {});

} // namespace NYql
