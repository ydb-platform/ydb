#pragma once

#include "yql_pq_provider.h"

#include <util/generic/ptr.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_optimization.h>

namespace NYql {

THolder<IYtflowOptimization> CreatePqYtflowOptimization(const TPqState::TPtr& state);

}   // namespace NYql
