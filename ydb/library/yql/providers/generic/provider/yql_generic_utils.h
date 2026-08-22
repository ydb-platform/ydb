#pragma once

#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>
#include <ydb/library/yql/providers/generic/provider/yql_generic_state.h>

namespace NYql {
    TString DumpGenericClusterConfig(const TGenericClusterConfig& clusterConfig);

    ///
    /// Fill a select from a TGenSourceSettings
    ///
    void FillSelectFromGenSourceSettings(NConnector::NApi::TSelect& select,
                                         const NNodes::TGenSourceSettings& settings,
                                         TExprContext& ctx,
                                         const TGenericState::TTableMeta* tableMeta);
} // namespace NYql
