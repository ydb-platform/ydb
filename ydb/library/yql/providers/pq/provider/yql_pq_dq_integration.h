#pragma once

#include "yql_pq_provider.h"

#include <ydb/library/yql/providers/dq/interface/yql_dq_integration.h> 

#include <util/generic/ptr.h>

namespace NYql {

THolder<IDqIntegration> CreatePqDqIntegration(const TPqState::TPtr& state);

}
