#pragma once

#include "yql_s3_provider.h"

#include <util/generic/ptr.h>
#include <yql/essentials/core/dq_integration/yql_dq_integration.h>

namespace NYql {

THolder<IDqIntegration> CreateS3DqIntegration(TS3State::TPtr state);

}
