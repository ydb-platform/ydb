#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_expr_nodes.h"
#include "yql_kikimr_settings.h"

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/library/yql/dq/constraints/dq_constraints.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYql {

using namespace NNodes;

namespace {

using TStatus = IGraphTransformer::TStatus;

template <size_t FromChild>
struct TApplyConstraintFromInput {
    template <class TConstraint>
    static void Do(const TExprNode::TPtr& input) {
        if (const auto c = input->Child(FromChild)->GetConstraint<TConstraint>()) {
            input->AddConstraint(c);
        }
    }
};

template <size_t FromChild>
struct TApplyConstraintsFromInput {
    template <class... TConstraint>
    static void Do(const TExprNode::TPtr& input) {
        (TApplyConstraintFromInput<FromChild>::template Do<TConstraint>(input), ...);
    }
};

template <size_t Index>
TStatus CopyAllFrom(const TExprNode::TPtr& node, TExprContext& ctx) {
    Y_UNUSED(ctx);
    node->CopyConstraints(*node->Child(Index));
    return TStatus::Ok;
}

TStatus ConstraintKqpWriteConstraint(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);

    TApplyConstraintFromInput<0>::Do<TStreamingConstraintNode>(input);

    return TStatus::Ok;
}

TStatus ConstraintKqpProgram(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto& lambda = input->ChildRef(TKqpProgram::idx_Lambda);
    const auto argsConstraints = input->Child(TKqpProgram::idx_ArgsConstraints);

    std::vector<TConstraintNode::TListType> constraints;
    constraints.reserve(argsConstraints->ChildrenSize());
    for (const auto& argsConstraint : argsConstraints->Children()) {
        try {
            auto set = ctx.MakeConstraintSet(NYT::NodeFromYsonString(argsConstraint->Content()));
            constraints.push_back(set.GetAllConstraints());
        } catch (...) {
            ctx.AddError(TIssue(ctx.GetPosition(argsConstraint->Pos()), TStringBuilder()
                << "Bad KqpProgram ArgsConstraints yson-value: " << CurrentExceptionMessage()));
            return TStatus::Error;
        }
    }

    return UpdateLambdaConstraints(lambda, ctx, constraints);
}

class TKiSourceConstraintsTransformer final : public TVisitorTransformerBase {
public:
    explicit TKiSourceConstraintsTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx)
        : TVisitorTransformerBase(true)
        , SessionCtx(sessionCtx)
    {
        AddHandler({
            TCoConfigure::CallableName(),
            TKiReadTable::CallableName(),
            TKiReadTableScheme::CallableName(),
            TKiReadTableList::CallableName(),
        }, Hndl(&TKiSourceConstraintsTransformer::HandleDefault));

        if (IsIn({EKikimrQueryType::Query, EKikimrQueryType::Script}, SessionCtx->Query().Type)) {
            AddHandler({TDqSource::CallableName()}, Hndl(&CopyAllFrom<1>));
            AddHandler({
                TDqSourceWrap::CallableName(),
                TDqSourceWideWrap::CallableName(),
                TDqSourceWideBlockWrap::CallableName(),
                TDqReadWrap::CallableName(),
                TDqReadWideWrap::CallableName(),
                TDqReadBlockWideWrap::CallableName(),
                TDqLookupSourceWrap::CallableName(),
            }, Hndl(&CopyAllFrom<0>));
        }
    }

private:
    static TStatus HandleDefault(const TExprNode::TPtr& node, TExprContext& ctx) {
        Y_UNUSED(node, ctx);
        return TStatus::Ok;
    }

    const TIntrusivePtr<TKikimrSessionContext> SessionCtx;
};

} // anonymous namespace

TAutoPtr<IGraphTransformer> CreateKiSourceConstraintsTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx) {
    if (!sessionCtx->Config()._KqpYqlConstraintsTransformerEnabled.Get().GetOrElse(false)) {
        return CreateDefCallableConstraintTransformer();
    }

    return new TKiSourceConstraintsTransformer(sessionCtx);
}

TAutoPtr<IGraphTransformer> CreateKiSinkConstraintsTransformer(TIntrusivePtr<TKikimrSessionContext> sessionCtx) {
    if (!sessionCtx->Config()._KqpYqlConstraintsTransformerEnabled.Get().GetOrElse(false)) {
        return CreateDefCallableConstraintTransformer();
    }

    auto dqTransformer = NDq::CreateDqConstraintsTransformer(/* disableChecks */ true);

    return CreateFunctorTransformer([dq = std::move(dqTransformer)](const TExprNode::TPtr& input, TExprNode::TPtr& output, TExprContext& ctx) -> TStatus {
        output = input;

        if (TKqpProgram::Match(input.Get())) {
            return ConstraintKqpProgram(input, ctx);
        }

        if (TKqpWriteConstraint::Match(input.Get())) {
            return ConstraintKqpWriteConstraint(input, ctx);
        }

        if (dq->CanParse(*input)) {
            return dq->Transform(input, output, ctx);
        }

        return UpdateAllChildLambdasConstraints(*input);
    });
}

} // namespace NYql
