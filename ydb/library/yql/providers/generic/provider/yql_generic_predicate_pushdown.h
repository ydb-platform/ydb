#pragma once

#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>

namespace NYql::NConnector::NApi {
    class TPredicate;
} // namespace NYql::NConnector::NApi

namespace NYql {

    bool IsEmptyFilterPredicate(const NNodes::TCoLambda& lambda);
    bool SerializeFilterPredicate(const NNodes::TCoLambda& predicate, NConnector::NApi::TPredicate* proto, TStringBuilder& err);
    TString FormatWhere(const NConnector::NApi::TPredicate& predicate);
} // namespace NYql
