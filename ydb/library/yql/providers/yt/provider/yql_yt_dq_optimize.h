#pragma once

#include "yql_yt_provider.h"

#include <ydb/library/yql/dq/integration/yql_dq_optimization.h>

#include <util/generic/ptr.h>

namespace NYql {

THolder<IDqOptimization> CreateYtDqOptimizers(TYtState::TPtr state);

}
