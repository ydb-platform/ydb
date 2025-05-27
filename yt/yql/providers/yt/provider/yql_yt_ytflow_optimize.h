#pragma once

#include "yql_yt_provider.h"

#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_optimization.h>

#include <util/generic/ptr.h>


namespace NYql {

THolder<IYtflowOptimization> CreateYtYtflowOptimization(TYtState* state);

} // namespace NYql
