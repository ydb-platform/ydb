#include "type_ann_match_recognize.h"
#include <ydb/library/yql/core/sql_types/match_recognize.h>

namespace NYql::NTypeAnnImpl {

IGraphTransformer::TStatus
MatchRecognizeWrapper(const TExprNode::TPtr &input, TExprNode::TPtr &output, TContext &ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto& source = input->ChildRef(0);
    auto& partitionKeySelector = input->ChildRef(1);
    const auto& partitionColumns = input->ChildRef(2);
    const auto& sortTraits = input->ChildRef(3);
    const auto& params = input->ChildRef(4);
    Y_UNUSED(source, sortTraits);
    auto status = ConvertToLambda(partitionKeySelector, ctx.Expr, 1, 1);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }
    if (!UpdateLambdaAllArgumentsTypes(partitionKeySelector, { GetSeqItemType(source->GetTypeAnn()) }, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    auto partitionKeySelectorType = partitionKeySelector->GetTypeAnn();
    if (!partitionKeySelectorType) {
        return IGraphTransformer::TStatus::Repeat;
    }
    auto partitionKeySelectorItemTypes = partitionKeySelectorType->Cast<TTupleExprType>()->GetItems();

    //merge measure columns, came from params, with partition columns to form output row type
    auto outputTableColumns = params->GetTypeAnn()->Cast<TStructExprType>()->GetItems();
    for (size_t i = 0; i != partitionColumns->ChildrenSize(); ++i) {
        outputTableColumns.push_back(ctx.Expr.MakeType<TItemExprType>(
                partitionColumns->ChildRef(i)->Content(),
                partitionKeySelectorItemTypes[i]
        ));
    }
    const auto outputTableRowType = ctx.Expr.MakeType<TStructExprType>(outputTableColumns);
    input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(outputTableRowType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus
MatchRecognizeParamsWrapper(const TExprNode::TPtr &input, TExprNode::TPtr &output, TContext &ctx) {
    Y_UNUSED(output);
    if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto& measures = input->ChildRef(0);
    input->SetTypeAnn(measures->GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

namespace {


const std::unordered_set<TString> GetPrimaryVars(const TExprNode::TPtr& pattern, TContext &ctx, size_t nestingLevel) {
    std::unordered_set<TString> result;
    for (const auto& term: pattern->Children()) {
        for (const auto& factor: term->Children()) {
            YQL_ENSURE(EnsureArgsCount(*factor, 5, ctx.Expr), "Expect 5 args");
            if (factor->ChildRef(0)->IsAtom()) {
                result.insert(TString(factor->ChildRef(0)->Content()));
            } else {
                YQL_ENSURE(nestingLevel < MaxMatchRecognizePatternNesting, "To big nesting level in the pattern");
                    auto subExprVars = GetPrimaryVars(factor->ChildRef(0), ctx, ++nestingLevel);
                    result.insert(subExprVars.begin(), subExprVars.end());
            }
        }
    }
    return result;
}

const TStructExprType* GetMatchedRowsRangesType(const TExprNode::TPtr& pattern, TContext &ctx) {
    auto vars = GetPrimaryVars(pattern, ctx, 0);
    TVector<const TItemExprType*> items;
    for (const auto& var: vars) {
        const auto& item = ctx.Expr.MakeType<TStructExprType>(TVector<const TItemExprType*>{
                        ctx.Expr.MakeType<TItemExprType>("From", ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)),
                        ctx.Expr.MakeType<TItemExprType>("To", ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64))
                });
        items.push_back(ctx.Expr.MakeType<TItemExprType>(var, ctx.Expr.MakeType<TListExprType>(item)));
    }
    return ctx.Expr.MakeType<TStructExprType>(items);
}

}//namespace {

IGraphTransformer::TStatus
MatchRecognizeMeasuresWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output,
                                 TContext &ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto& inputRowType = input->ChildRef(0);
    const auto& pattern = input->ChildRef(1);
    const auto& names = input->ChildRef(2);
    const size_t FirstLambdaIndex = 3;

    if (!EnsureTupleOfAtoms(*names, ctx.Expr))  {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, FirstLambdaIndex + names->ChildrenSize(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto lambdaInputRowColumns = inputRowType->GetTypeAnn()->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>()->GetItems();
    lambdaInputRowColumns.push_back(ctx.Expr.MakeType<TItemExprType>("_yql_Classifier", ctx.Expr.MakeType<TDataExprType>(EDataSlot::Utf8)));
    lambdaInputRowColumns.push_back(ctx.Expr.MakeType<TItemExprType>("_yql_MatchNumber", ctx.Expr.MakeType<TDataExprType>(EDataSlot::Int64)));
    auto lambdaInputRowType = ctx.Expr.MakeType<TStructExprType>(lambdaInputRowColumns);
    const auto& matchedRowsRanges = GetMatchedRowsRangesType(pattern, ctx);
    YQL_ENSURE(matchedRowsRanges);
    TVector<const TItemExprType*> items;
    for (size_t i = 0; i != names->ChildrenSize(); ++i) {
        auto& lambda = input->ChildRef(FirstLambdaIndex + i);
        auto status = ConvertToLambda(lambda, ctx.Expr, 2, 2);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        if (!UpdateLambdaAllArgumentsTypes(
                lambda,
                {
                        ctx.Expr.MakeType<TListExprType>(lambdaInputRowType),
                        matchedRowsRanges
                },
                ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (auto type = lambda->GetTypeAnn()) {
            items.push_back(ctx.Expr.MakeType<TItemExprType>(names->ChildRef(i)->Content(), type));
        } else {
            return IGraphTransformer::TStatus::Repeat;
        }
    }
    input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(items));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus
MatchRecognizePatternWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output,
                              TContext &ctx) {
    Y_UNUSED(output);
    input->SetTypeAnn(ctx.Expr.MakeType<TVoidExprType>());
    return IGraphTransformer::TStatus::Ok;
}

namespace {

bool IsBoolLikeType(const TTypeAnnotationNode* node) {
    switch (node->GetKind()) {
        case ETypeAnnotationKind::Null:
            return true;
        case ETypeAnnotationKind::Data:
            return node->Cast<TDataExprType>()->GetSlot() == EDataSlot::Bool;
        case ETypeAnnotationKind::Optional: {
            const auto nested = node->Cast<TOptionalExprType>()->GetItemType();
            return (nested->GetKind() == ETypeAnnotationKind::Data) and (nested->Cast<TDataExprType>()->GetSlot() == EDataSlot::Bool);
        }
        default:
            return false;
    }
}
} //namespace

IGraphTransformer::TStatus
MatchRecognizeDefinesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr& output,
                                 TContext &ctx) {
    Y_UNUSED(output);
    if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto& inputRowType = input->ChildRef(0);
    const auto& pattern = input->ChildRef(1);
    const auto& names = input->ChildRef(2);
    const size_t FirstLambdaIndex = 3;

    if (!EnsureTupleOfAtoms(*names, ctx.Expr))  {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, FirstLambdaIndex + names->ChildrenSize(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto matchedRowsRanges = GetMatchedRowsRangesType(pattern, ctx);
    TVector<const TItemExprType*> items;
    for (size_t i = 0; i != names->ChildrenSize(); ++i) {
        auto& lambda = input->ChildRef(FirstLambdaIndex + i);
        auto status = ConvertToLambda(lambda, ctx.Expr, 3, 3);
        if (status.Level != IGraphTransformer::TStatus::Ok) {
            return status;
        }
        if (!UpdateLambdaAllArgumentsTypes(
                lambda,
                {
                    ctx.Expr.MakeType<TListExprType>(inputRowType->GetTypeAnn()->Cast<TTypeExprType>()->GetType()),
                    matchedRowsRanges,
                    ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)
                },
                ctx.Expr)) {
            return IGraphTransformer::TStatus::Error;
        }
        if (auto type = lambda->GetTypeAnn()) {
            if (IsBoolLikeType(type)) {
                items.push_back(ctx.Expr.MakeType<TItemExprType>(names->ChildRef(i)->Content(), type));
            } else {
                ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(lambda->Pos()), "DEFINE expression must be a predicate"));
                return IGraphTransformer::TStatus::Error;
            }
        } else {
            return IGraphTransformer::TStatus::Repeat;
        }
    }
    input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(items));
    return IGraphTransformer::TStatus::Ok;
}

} // namespace NYql::NTypeAnnImpl
