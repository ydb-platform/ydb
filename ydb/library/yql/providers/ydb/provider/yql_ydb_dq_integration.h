#pragma once

#include "yql_ydb_provider.h"

#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

#include <util/generic/ptr.h>

namespace NYql {

std::unique_ptr<IDqIntegration> CreateYdbDqIntegration(TYdbState::TPtr state);

}
