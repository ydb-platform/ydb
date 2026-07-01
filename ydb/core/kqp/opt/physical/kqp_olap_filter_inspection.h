#pragma once

#include <ydb/core/kqp/common/kqp_yql.h>

#include <yql/essentials/ast/yql_expr.h>

#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/strbuf.h>
#include <util/generic/string.h>

namespace NKikimr::NKqp::NOpt {

struct TOlapFilterInspection {
    THashSet<TString> Columns;
    bool HasOlapFilter = false;
    bool HasOlapApply = false;
    bool HasOlapProjections = false;
    bool RequiresAllInputColumns = false;
};

class TOlapFilterInspector {
public:
    static TOlapFilterInspection Inspect(const NYql::TExprNode::TPtr& node);
    static TOlapFilterInspection InspectLambda(const NYql::TExprNode::TPtr& lambda);
    static NYql::TExprNode::TPtr RenameColumns(
        const NYql::TExprNode::TPtr& node,
        const THashMap<TString, TString>& renameMap,
        NYql::TExprContext& ctx);
    static TString Format(const NYql::NNodes::TKqpOlapFilter& filter);
};

TOlapFilterInspection InspectOlapExpression(const NYql::TExprNode::TPtr& node);
TOlapFilterInspection InspectOlapProcessLambda(const NYql::TExprNode::TPtr& lambda);

TString GetOlapColumnName(TStringBuf columnName, bool stripAliasPrefix);
TString FormatOlapFilter(const NYql::NNodes::TKqpOlapFilter& filter);

} // namespace NKikimr::NKqp::NOpt
