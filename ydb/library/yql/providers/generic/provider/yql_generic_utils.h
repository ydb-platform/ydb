#pragma once

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
    size_t GetSelectKey(const NConnector::NApi::TSelect& select);

    ///
    /// Combine hashes with minimizing collisions
    ///
    void HashCombine(size_t& currentSeed, const size_t& hash);

} // namespace NYql
