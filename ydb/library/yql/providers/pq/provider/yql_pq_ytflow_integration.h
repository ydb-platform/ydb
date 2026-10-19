#pragma once

#include "yql_pq_provider.h"

#include <util/generic/ptr.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

namespace NYql {

THolder<IYtflowIntegration> CreatePqYtflowIntegration(const TPqState::TPtr& state);

}   // namespace NYql
