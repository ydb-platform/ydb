#pragma once

#include <ydb/library/yql/core/cbo/cbo_optimizer.h>

#include <functional>

namespace NYql {

IOptimizer* MakePgOptimizer(const std::function<void(const TString&)>& log = {});

} // namespace NYql
