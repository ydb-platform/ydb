#include "yql_dq_state.h"

#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>
#include <ydb/library/yql/providers/common/transform/yql_visit.h>
#include <ydb/library/yql/core/yql_expr_constraint.h>
#include <ydb/library/yql/ast/yql_constraint.h>

namespace NYql {

using namespace NNodes;

namespace {

class TDqDataSourceConstraintTransformer : public TVisitorTransformerBase {
public:
    TDqDataSourceConstraintTransformer(TDqState::TPtr state)
        : TVisitorTransformerBase(true)
        , State_(std::move(state))
    {
        AddHandler({
            TCoConfigure::CallableName(),
            TDqReadWrap::CallableName(),
            TDqReadWideWrap::CallableName(),
            TDqSource::CallableName(),
            TDqSourceWrap::CallableName(),
            TDqSourceWideWrap::CallableName(),
            TDqSourceWideBlockWrap::CallableName()
        }, Hndl(&TDqDataSourceConstraintTransformer::HandleDefault));
    }

    TStatus HandleDefault(TExprBase, TExprContext&) {
        return TStatus::Ok;
    }
private:
    const TDqState::TPtr State_;
};

}

THolder<IGraphTransformer> CreateDqDataSourceConstraintTransformer(TDqState::TPtr state) {
    return THolder<IGraphTransformer>(new TDqDataSourceConstraintTransformer(std::move(state)));
}

}
