#include "yql_kikimr_query_traits.h"
#include "yql_kikimr_expr_nodes.h"

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/yql_expr_optimize.h>
#include <ydb/library/yql/utils/log/log.h> 

namespace NKikimr {
namespace NKqp {

using namespace NYql;

TQueryTraits CollectQueryTraits(const NNodes::TExprBase& program, TExprContext&) {
    using namespace NNodes;

    TQueryTraits traits;

    VisitExpr(program.Ptr(), [&traits](const TExprNode::TPtr& node) {
        const auto* x = node.Get();

        if (TKiUpdateRow::Match(x) || TKiEraseRow::Match(x) || TKiDeleteTable::Match(x) || TKiEffects::Match(x)) {
            traits.ReadOnly = 0;
        } else if (TKiSelectIndexRange::Match(x)) {
            traits.WithIndex = 1;
        } else if (TCoEquiJoin::Match(x) || TCoJoin::Match(x) || TCoMapJoinCore::Match(x) || TCoJoinDict::Match(x)) {
            traits.WithJoin = 1;
        } else if (TCoSqlIn::Match(x)) {
            traits.WithSqlIn = 1;
        } else if (TCoUdf::Match(x)) {
            traits.WithUdf = 1;
        } else if (auto selectRange = TMaybeNode<TKiSelectRange>(node)) {
            if (selectRange.Cast().Table().Path().Value().ends_with("/indexImplTable"sv)) {
                traits.WithIndex = 1;
            }
        }

        return true;
    });

    return traits;
}

} // namespace NKqp
} // namespace NKikimr
