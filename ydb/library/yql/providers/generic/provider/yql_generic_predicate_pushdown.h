#pragma once

#include <ydb/library/yql/providers/generic/expr_nodes/yql_generic_expr_nodes.h>

namespace NYql::NConnector::NApi {
    class TExpression;
    class TPredicate;
} // namespace NYql::NConnector::NApi

namespace NYql {

    bool IsEmptyFilterPredicate(const NNodes::TCoLambda& lambda);
    bool SerializeFilterPredicate(
        TExprContext& ctx,
        const NNodes::TExprBase& predicateBody,
        const NNodes::TCoArgument& predicateArgument,
        NConnector::NApi::TPredicate* proto,
        TStringBuilder& err
    );
    bool SerializeFilterPredicate(
        TExprContext& ctx,
        const NNodes::TCoLambda& predicate,
        NConnector::NApi::TPredicate* proto,
        TStringBuilder& err
    );
    bool SerializeWatermarkExpr(
        TExprContext& ctx,
        const NNodes::TCoLambda& watermarkExpr,
        NConnector::NApi::TExpression* proto,
        TStringBuilder& err
    );
    TString FormatExpression(const NConnector::NApi::TExpression& expression);
    TString FormatWhere(const NConnector::NApi::TPredicate& predicate);
} // namespace NYql
