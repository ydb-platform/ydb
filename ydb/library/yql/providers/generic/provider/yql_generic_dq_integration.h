#pragma once

#include "yql_generic_provider.h"

#include <util/generic/ptr.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NYql {

    THolder<IDqIntegration> CreateGenericDqIntegration(TGenericState::TPtr state);

} // namespace NYql
