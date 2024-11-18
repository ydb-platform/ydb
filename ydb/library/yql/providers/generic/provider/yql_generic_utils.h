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

namespace NYql {
    TString DumpGenericClusterConfig(const TGenericClusterConfig& clusterConfig);
} // namespace NYql
