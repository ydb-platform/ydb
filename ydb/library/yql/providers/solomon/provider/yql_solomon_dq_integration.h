#pragma once

#include "yql_solomon_provider.h"

#include <ydb/library/yql/dq/integration/yql_dq_integration.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IDqIntegration> CreateSolomonDqIntegration(const TSolomonState::TPtr& state);

}
