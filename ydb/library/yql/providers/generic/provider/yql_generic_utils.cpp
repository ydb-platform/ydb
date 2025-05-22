#include "yql_generic_utils.h"

#include <util/string/builder.h>

namespace NYql {
    TString DumpGenericClusterConfig(const TGenericClusterConfig& clusterConfig) {
        TStringBuilder sb;
        sb << "name = " << clusterConfig.GetName()
           << ", kind = " << NYql::EGenericDataSourceKind_Name(clusterConfig.GetKind())
           << ", database name = " << clusterConfig.GetDatabaseName()
           << ", database id = " << clusterConfig.GetDatabaseId()
           << ", endpoint = " << clusterConfig.GetEndpoint()
           << ", use tls = " << clusterConfig.GetUseSsl()
           << ", protocol = " << NYql::EGenericProtocol_Name(clusterConfig.GetProtocol());

        for (const auto& [key, value] : clusterConfig.GetDataSourceOptions()) {
            sb << ", " << key << " = " << value;
        }

        return sb;
    }
} // namespace NYql
