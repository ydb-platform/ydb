#pragma once

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

namespace NYql {
    TString DumpGenericClusterConfig(const TGenericClusterConfig& clusterConfig);
}
