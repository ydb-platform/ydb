#include "yql_dq_datasink_constraints.h"

#include <ydb/library/yql/dq/constraints/dq_constraints.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

#include <yql/essentials/providers/common/transform/yql_visit.h>
#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/ast/yql_constraint.h>

namespace NYql {

using namespace NNodes;

namespace {

template <class... Other>
struct TCopyConstraint;

template <>
struct TCopyConstraint<> {
    static void Do(const TExprNode&, const TExprNode::TPtr&) {}
};

template <class TConstraint, class... Other>
struct TCopyConstraint<TConstraint, Other...> {
    static void Do(const TExprNode& from, const TExprNode::TPtr& to) {
        if (const auto c = from.GetConstraint<TConstraint>())
            to->AddConstraint(c);
        TCopyConstraint<Other...>::Do(from, to);
    }
};

class TDqDataSinkConstraintTransformer : public TVisitorTransformerBase {
public:
    TDqDataSinkConstraintTransformer()
        : TVisitorTransformerBase(true)
    {
        AddHandler({TDqStage::CallableName(), TDqPhyStage::CallableName()}, Hndl(&NDq::ConstraintDqStage));
        AddHandler({TDqOutput::CallableName()}, Hndl(&NDq::ConstraintDqOutput));
        AddHandler({
            TDqCnUnionAll::CallableName(),
            TDqCnBroadcast::CallableName(),
            TDqCnMap::CallableName(),
            TDqCnStreamLookup::CallableName(),
            TDqCnHashShuffle::CallableName(),
            TDqCnResult::CallableName(),
            TDqCnValue::CallableName()
            }, Hndl(&NDq::ConstraintDqConnection));
        AddHandler({TDqCnMerge::CallableName()}, Hndl(&NDq::ConstraintDqCnMerge));
        AddHandler({TDqReplicate::CallableName()}, Hndl(&NDq::ConstraintDqReplicate));
        AddHandler({
            TDqJoin::CallableName(),
            TDqPhyGraceJoin::CallableName(),
            TDqPhyMapJoin::CallableName(),
            TDqPhyCrossJoin::CallableName(),
            TDqPhyJoinDict::CallableName(),
        }, Hndl(&NDq::ConstraintDqJoin));
        AddHandler({
            TDqSink::CallableName(),
            TDqWrite::CallableName(),
            TDqQuery::CallableName(),
            TDqPrecompute::CallableName(),
            TDqPhyPrecompute::CallableName(),
            TDqTransform::CallableName()
        }, Hndl(&TDqDataSinkConstraintTransformer::HandleDefault));
    }

    TStatus HandleDefault(TExprBase, TExprContext&) {
        return TStatus::Ok;
    }
};

} // anonymous namespace

THolder<IGraphTransformer> CreateDqDataSinkConstraintTransformer() {
    return THolder<IGraphTransformer>(new TDqDataSinkConstraintTransformer());
}

} // namespace NYql
