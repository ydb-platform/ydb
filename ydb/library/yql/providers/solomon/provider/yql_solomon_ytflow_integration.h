#pragma once

#include "yql_solomon_provider.h"

#include <util/generic/ptr.h>
#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

namespace NYql {

THolder<IYtflowIntegration> CreateSolomonYtflowIntegration(const TSolomonState::TPtr& state);

}   // namespace NYql
