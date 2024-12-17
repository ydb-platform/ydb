#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>

namespace NYql {
    TString DumpGenericClusterConfig(const TGenericClusterConfig& clusterConfig);
}
