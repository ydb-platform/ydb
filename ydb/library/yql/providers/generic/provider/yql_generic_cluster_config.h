#pragma once

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

// FIXME: simplify this after YQ-3839 is completed
#if __has_include(<yql/essentials/providers/common/proto/connector/common/data_source.pb.h>)
    #include <yql/essentials/providers/common/proto/connector/common/data_source.pb.h> // Y_IGNORE

    namespace NConnectorCommon = NYql::NConnector::NCommon;
#else
    #include <ydb/library/yql/providers/generic/connector/api/common/data_source.pb.h> // Y_IGNORE

    namespace NConnectorCommon = NYql::NConnector::NApi;
#endif

#include <util/generic/hash.h>

namespace NYql {
    // Converts properties from `CREATE EXTERNAL DATA SOURCE .. WITH ...` into
    // unified representation of a cluster config
    NYql::TGenericClusterConfig GenericClusterConfigFromProperties(
        const TString& clusterName,
        const THashMap<TString, TString>& properties);

    // Validates cluster config. Throws exception in case of error.
    void ValidateGenericClusterConfig(const NYql::TGenericClusterConfig& clusterConfig, const TString& context);
} // namespace NYql
