#pragma once

#include "yql_yt_provider.h"

#include <yql/essentials/core/dq_integration/yql_dq_optimization.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IDqOptimization> CreateYtDqOptimizers(TYtState::TPtr state);

}
