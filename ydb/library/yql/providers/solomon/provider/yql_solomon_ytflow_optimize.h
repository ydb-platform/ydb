#pragma once

#include "yql_solomon_provider.h"

#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_optimization.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IYtflowOptimization> CreateSolomonYtflowOptimization(const TSolomonState::TPtr& state);

} // namespace NYql
