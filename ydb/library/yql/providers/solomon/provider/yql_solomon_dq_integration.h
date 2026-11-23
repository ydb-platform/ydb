#pragma once

#include "yql_solomon_provider.h"

#include <util/generic/ptr.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NYql {

THolder<IDqIntegration> CreateSolomonDqIntegration(const TSolomonState::TPtr& state);

}
