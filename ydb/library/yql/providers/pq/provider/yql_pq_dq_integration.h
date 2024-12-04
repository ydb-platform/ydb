#pragma once

#include "yql_pq_provider.h"

#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IDqIntegration> CreatePqDqIntegration(const TPqState::TPtr& state);

}
