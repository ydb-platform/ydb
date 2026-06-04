#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp::NOpt {

struct TOlapFilterInspection {
    THashSet<TString> Columns;
    bool HasOlapFilter = false;
    bool HasOlapApply = false;
    bool HasOlapProjections = false;
    bool RequiresAllInputColumns = false;
};

TOlapFilterInspection InspectOlapExpression(const NYql::TExprNode::TPtr& node);
TOlapFilterInspection InspectOlapProcessLambda(const NYql::TExprNode::TPtr& lambda);

TString FormatOlapFilter(const NYql::NNodes::TKqpOlapFilter& filter);

} // namespace NKikimr::NKqp::NOpt
