#pragma once

#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>

namespace NYql::NConnector::NApi {
    class TExpression;
    class TPredicate;
} // namespace NYql::NConnector::NApi

namespace NYql {

    bool IsEmptyFilterPredicate(const NNodes::TCoLambda& lambda);
    bool SerializeFilterPredicate(
        const NNodes::TExprBase& predicateBody,
        const NNodes::TCoArgument& predicateArgument,
        NConnector::NApi::TPredicate* proto,
        TStringBuilder& err
    );
    bool SerializeFilterPredicate(
        const NNodes::TCoLambda& predicate,
        NConnector::NApi::TPredicate* proto,
        TStringBuilder& err
    );
    bool SerializeWatermarkExpr(
        const NNodes::TCoLambda& watermarkExpr,
        NConnector::NApi::TExpression* proto,
        TStringBuilder& err
    );
    TString FormatExpression(const NConnector::NApi::TExpression& expression);
    TString FormatPredicate(const NConnector::NApi::TPredicate& predicate);
} // namespace NYql
