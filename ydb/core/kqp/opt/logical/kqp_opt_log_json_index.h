#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>
#include <yql/essentials/ast/yql_expr.h>

namespace NKikimr::NKqp::NOpt {

struct TJsonIndexSettings {
    TString ColumnName;
    NYql::TKqpReadTableFullTextIndexSettings Settings;
};

std::optional<TJsonIndexSettings> CollectJsonIndexPredicate(const NYql::NNodes::TExprBase& body, const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx);

} // namespace NKikimr::NKqp::NOpt
