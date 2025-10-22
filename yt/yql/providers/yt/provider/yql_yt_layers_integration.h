#pragma once

#include <yql/essentials/core/layers/layers_integration.h>

namespace NYql {
NLayers::ILayersIntegrationPtr CreateYtLayersIntegration();
}
