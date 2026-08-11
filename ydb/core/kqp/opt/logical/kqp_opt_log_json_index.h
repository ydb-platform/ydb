#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>

#include <util/generic/hash.h>
#include <yql/essentials/ast/yql_expr.h>
#include <yql/essentials/public/issue/yql_issue.h>

#include <expected>

namespace NKikimr::NKqp::NOpt {

struct TJsonIndexSettings {
    TString ColumnName;
    NYql::TKqpReadTableFullTextIndexSettings Settings;
    bool Covered = false;
};

enum class EJsonIndexSelectionMode {
    Explicit,
    Automatic,
};

std::expected<TJsonIndexSettings, NYql::TIssue> CollectJsonIndexPredicate(const NYql::NNodes::TExprBase& body,
    const NYql::NNodes::TExprBase& node, NYql::TExprContext& ctx, const THashSet<TString>& jsonIndexedColumns,
    const TVector<TString>& prefixColumns = {},
    const TVector<std::pair<TString, NYql::TExprNode::TPtr>>& seedPrefixColumns = {},
    EJsonIndexSelectionMode selectionMode = EJsonIndexSelectionMode::Explicit);

}   // namespace NKikimr::NKqp::NOpt
