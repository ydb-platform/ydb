#pragma once

#include "yql_yt_provider.h"

#include <yt/yql/providers/ytflow/integration/interface/yql_ytflow_integration.h>

#include <util/generic/ptr.h>


namespace NYql {

THolder<IYtflowIntegration> CreateYtYtflowIntegration(TYtState* state);

} // namespace NYql
