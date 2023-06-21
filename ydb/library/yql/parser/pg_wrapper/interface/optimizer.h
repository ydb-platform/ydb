#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer.h>

namespace NYql {

IOptimizer* MakePgOptimizer(const IOptimizer::TInput& input, const std::function<void(const TString&)>& log = {});

} // namespace NYql
