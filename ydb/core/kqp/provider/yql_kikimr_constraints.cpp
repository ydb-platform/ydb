#include "yql_kikimr_provider_impl.h"
#include "yql_kikimr_expr_nodes.h"
#include "yql_kikimr_settings.h"

#include <ydb/core/kqp/expr_nodes/kqp_expr_nodes.h>
#include <ydb/library/yql/dq/constraints/dq_constraints.h>
#include <ydb/library/yql/dq/expr_nodes/dq_expr_nodes.h>
#include <ydb/library/yql/providers/dq/expr_nodes/dqs_expr_nodes.h>

#include <yql/essentials/core/yql_expr_constraint.h>
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/providers/common/transform/yql_visit.h>

#include <library/cpp/yson/node/node_io.h>

namespace NYql {

using namespace NNodes;

namespace {

using TStatus = IGraphTransformer::TStatus;

template <size_t Index>
TStatus CopyAllFrom(const TExprNode::TPtr& node, TExprContext& ctx) {
    Y_UNUSED(ctx);
    node->CopyConstraints(*node->Child(Index));
    return TStatus::Ok;
}

TStatus ConstraintKqpWriteConstraint(const TExprNode::TPtr& input, TExprContext& ctx) {
    Y_UNUSED(ctx);

    if (const auto* c = input->Head().GetConstraint<TStreamingConstraintNode>()) {
        input->AddConstraint(c);
    }

    return TStatus::Ok;
}

TStatus ConstraintKqpStreamingAggregation(const TExprNode::TPtr& input, TExprContext& ctx) {
    if (const auto* streaming = input->Head().GetConstraint<TStreamingConstraintNode>()) {
        THashSet<TStringBuf> keys;
        for (const auto& key : input->Child(TKqpStreamingAggregation::idx_Keys)->Children()) {
            keys.insert(key->Content());
        }
        const auto* filtered = streaming->FilterFields(ctx, [&keys](const TPartOfConstraintBase::TPathType& path) {
            return !path.empty() && keys.contains(path.front());
        });
        input->AddConstraint(filtered->GetSimplifiedForType(*input->GetTypeAnn(), ctx));
    }

    if (const auto status = UpdateAllChildLambdasConstraints(*input); status != TStatus::Ok) {
        return status;
    }

    const auto size = input->Child(1)->ChildrenSize();
    if (!size) {
        return TStatus::Ok;
    }

    bool allKeysInOutput = true;
    if (auto outputColumnsSetting = GetSetting(input->Tail(), "output_columns")) {
        THashSet<TStringBuf> outputColumns;
        for (auto& col : outputColumnsSetting->Child(1)->Children()) {
            YQL_ENSURE(col->IsAtom());
            outputColumns.insert(col->Content());
        }
        allKeysInOutput = AllOf(input->Child(1)->Children(), [&](const auto& key) { return outputColumns.contains(key->Content()); });
    }

    if (allKeysInOutput) {
        std::vector<std::string_view> columns;
        columns.reserve(size);
        for (const auto& child : input->Child(1)->Children()) {
            columns.emplace_back(child->Content());
        }
        input->AddConstraint(ctx.MakeConstraint<TUniqueConstraintNode>(columns));
        input->AddConstraint(ctx.MakeConstraint<TDistinctConstraintNode>(columns));
    }

    if (const auto* c = input->Head().GetConstraint<TEmptyConstraintNode>()) {
        input->AddConstraint(c);
    }

    return TStatus::Ok;
}

TStatus ConstraintKqpProgram(const TExprNode::TPtr& input, TExprContext& ctx) {
    auto& lambda = input->ChildRef(TKqpProgram::idx_Lambda);
    if (input->ChildrenSize() <= TKqpProgram::idx_ArgsConstraints) {
        return UpdateLambdaConstraints(*lambda);
    }

    const auto argsConstraints = input->Child(TKqpProgram::idx_ArgsConstraints);

    std::vector<TConstraintNode::TListType> constraints;
    constraints.reserve(argsConstraints->ChildrenSize());
    for (const auto& argsConstraint : argsConstraints->Children()) {
        try {
            auto set = ctx.MakeConstraintSet(NYT::NodeFromYsonString(argsConstraint->Content()));
            constraints.push_back(set.GetAllConstraints());
        } catch (const yexception& e) {
            ctx.AddError(TIssue(ctx.GetPosition(argsConstraint->Pos()), TStringBuilder()
                << "Bad KqpProgram ArgsConstraints yson-value: " << e.what()));
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
            AddHandler({
                TDqSource::CallableName(),
            }, Hndl(&CopyAllFrom<1>));
            AddHandler({
                TDqSourceWrap::CallableName(),
                TDqLookupSourceWrap::CallableName(),
            }, Hndl(&CopyAllFrom<0>));
            AddHandler({
                TDqSourceWideWrap::CallableName(),
                TDqSourceWideBlockWrap::CallableName(),
            }, Hndl(&TKiSourceConstraintsTransformer::CopyFromWideSourceWrapInput));
            AddHandler({
                TDqReadWrap::CallableName(),
                TDqReadWideWrap::CallableName(),
                TDqReadBlockWideWrap::CallableName(),
            }, Hndl(&CopyAllFrom<0>));
        }
    }

private:
    static TStatus HandleDefault(const TExprNode::TPtr& node, TExprContext& ctx) {
        Y_UNUSED(node, ctx);
        return TStatus::Ok;
    }

    TStatus CopyFromWideSourceWrapInput(const TExprNode::TPtr& node, TExprContext& ctx) {
        TDqSourceWrapBase sourceWrap(node);
        const auto* rowType = sourceWrap
            .RowType().Raw()
            ->GetTypeAnn()
            ->Cast<TTypeExprType>()
            ->GetType()
            ->Cast<TStructExprType>();

        const auto rename = [rowType, &ctx](const TPartOfConstraintBase::TPathType& path) {
            if (path.empty()) {
                return std::vector<TPartOfConstraintBase::TPathType>{path};
            }

            const auto index = rowType->FindItem(path.front());
            if (!index) {
                return std::vector<TPartOfConstraintBase::TPathType>{};
            }

            auto result = path;
            result.front() = ctx.GetIndexAsString(*index);
            return std::vector<TPartOfConstraintBase::TPathType>{std::move(result)};
        };

        const auto& sourceInput = sourceWrap.Input();
        auto constraints = sourceInput.Ref().GetConstraintSet();
        if (const auto* streaming = constraints.RemoveConstraint<TStreamingConstraintNode>()) {
            const auto* renamed = streaming->RenameFields(ctx, rename);
            constraints.AddConstraint(renamed->GetSimplifiedForType(*node->GetTypeAnn(), ctx));
        }
        if (const auto* partOfStreaming = constraints.RemoveConstraint<TPartOfStreamingConstraintNode>()) {
            constraints.AddConstraint(partOfStreaming->RenameFields(ctx, rename));
        }
        node->SetConstraints(constraints);
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

        if (TKqpStreamingAggregation::Match(input.Get())) {
            return ConstraintKqpStreamingAggregation(input, ctx);
        }

        if (dq->CanParse(*input)) {
            return dq->Transform(input, output, ctx);
        }

        return UpdateAllChildLambdasConstraints(*input);
    });
}

} // namespace NYql
