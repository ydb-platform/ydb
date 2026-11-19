#include "operation_permission.h"

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYql {

    TMaybe<THashSet<TString>> BuildNileOperationPermissions(const TGatewaysConfig* config, TStringBuf userName, const TMaybe<TString>& attributes) {
        if (!config || !config->HasNile() || !attributes || attributes->empty() || !NYT::NodeFromYsonString(*attributes).HasKey("nile_job_uuid")) {
            return Nothing();
        }

        THashSet<TString> allowedClusters;
        for (const auto& rule : config->GetNile().GetAllowed()) {
            if (rule.GetUser() == userName || rule.GetUser() == "*") {
                allowedClusters.insert(rule.GetCluster());
            }
        }
        return allowedClusters;
    }

} // namespace NYql
