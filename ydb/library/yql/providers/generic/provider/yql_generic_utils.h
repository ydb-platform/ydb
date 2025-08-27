#pragma once

#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <yql/essentials/providers/common/proto/gateways_config.pb.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_state.h>
#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>

namespace NYql {
    TString DumpGenericClusterConfig(const TGenericClusterConfig& clusterConfig);

    ///
    /// Fill a select from a TGenSourceSettings
    ///
    void FillSelectFromGenSourceSettings(NConnector::NApi::TSelect& select,
                                         const NNodes::TGenSourceSettings& settings,
                                         TExprContext& ctx,
                                         const TGenericState::TTableMeta* tableMeta);

    ///
    /// Get an unique key for a select request
    ///
    TString GetSelectKey(const NConnector::NApi::TSelect& select);

} // namespace NYql
