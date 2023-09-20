#pragma once

#include <ydb/library/yql/providers/common/proto/gateways_config.pb.h>
#include <util/generic/hash.h>

namespace NYql {
    // Converts properties from `CREATE EXTERNAL DATA SOURCE .. WITH ...` into
    // unified representation of a cluster config
    NYql::TGenericClusterConfig GenericClusterConfigFromProperties(
        const TString& clusterName,
        const THashMap<TString, TString>& properties);

    // Validates cluster config. Throws exception in case of error.
    void ValidateGenericClusterConfig(const NYql::TGenericClusterConfig& clusterConfig, const TString& context);
}
