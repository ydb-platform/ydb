#include "type_ann_match_recognize.h"

#include <yql/essentials/core/sql_types/match_recognize.h>
#include <yql/essentials/core/yql_match_recognize.h>

namespace NYql::NTypeAnnImpl {

namespace {

const TStructExprType* GetMatchedRowsRangesType(const TExprNode::TPtr& patternVars, TContext &ctx) {
    const auto itemType = ctx.Expr.MakeType<TStructExprType>(TVector{
            ctx.Expr.MakeType<TItemExprType>("From", ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)),
            ctx.Expr.MakeType<TItemExprType>("To", ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64))
    });

    TVector<const TItemExprType*> items;
    for (const auto& var : patternVars->Children()) {
        items.push_back(ctx.Expr.MakeType<TItemExprType>(
            var->Content(),
            ctx.Expr.MakeType<TListExprType>(itemType)
        ));
    }
    return ctx.Expr.MakeType<TStructExprType>(items);
}

} // anonymous namespace

IGraphTransformer::TStatus MatchRecognizeWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto source = input->Child(0);
    auto& partitionKeySelector = input->ChildRef(1);
    const auto partitionColumns = input->Child(2);
    const auto sortTraits = input->Child(3);
    const auto params = input->Child(4);
    Y_UNUSED(sortTraits);
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
    if (const auto rowsPerMatch = params->Child(1);
        "RowsPerMatch_OneRow" == rowsPerMatch->Content()) {
        for (size_t i = 0; i != partitionColumns->ChildrenSize(); ++i) {
            outputTableColumns.push_back(ctx.Expr.MakeType<TItemExprType>(
                    partitionColumns->Child(i)->Content(),
                    partitionKeySelectorItemTypes[i]
            ));
        }
    } else if ("RowsPerMatch_AllRows" == rowsPerMatch->Content()) {
        const auto& inputTableColumns = GetSeqItemType(source->GetTypeAnn())->Cast<TStructExprType>()->GetItems();
        for (const auto& column : inputTableColumns) {
            outputTableColumns.push_back(ctx.Expr.MakeType<TItemExprType>(column->GetName(), column->GetItemType()));
        }
    } else {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(rowsPerMatch->Pos()), "Unknown RowsPerMatch option"));
        return IGraphTransformer::TStatus::Error;
    }
    const auto outputTableRowType = ctx.Expr.MakeType<TStructExprType>(outputTableColumns);
    input->SetTypeAnn(ctx.Expr.MakeType<TListExprType>(outputTableRowType));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MatchRecognizeMeasuresAggregatesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureMinArgsCount(*input, 4, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto inputRowType = input->Child(0);
    const auto patternVars = input->Child(1);
    const auto names = input->Child(2);
    const auto vars = input->Child(3);
    constexpr size_t FirstLambdaIndex = 4;

    if (!EnsureType(*inputRowType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureTupleOfAtoms(*patternVars, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureTupleOfAtoms(*names, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureTupleOfAtoms(*vars, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureArgsCount(*vars, names->ChildrenSize(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureArgsCount(*input, FirstLambdaIndex + names->ChildrenSize(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    TVector<const TItemExprType*> items;
    for (size_t i = 0; i < names->ChildrenSize(); ++i) {
        auto lambda = input->Child(FirstLambdaIndex + i);
        if (const auto varName = vars->Child(i)->Content()) {
            const auto traits = input->Child(FirstLambdaIndex + i);
            if (!EnsureTupleMinSize(*traits, 2, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!EnsureTupleMaxSize(*traits, 3, ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            if (!EnsureAtom(traits->Head(), ctx.Expr)) {
                return IGraphTransformer::TStatus::Error;
            }
            lambda = traits->Child(1)->Child(NNodes::TCoAggregationTraits::idx_FinishHandler);
        }
        if (auto type = lambda->GetTypeAnn()) {
            if (type->GetKind() != ETypeAnnotationKind::Optional) {
                type = ctx.Expr.MakeType<TOptionalExprType>(type);
            }
            items.push_back(ctx.Expr.MakeType<TItemExprType>(names->Child(i)->Content(), type));
        } else {
            return IGraphTransformer::TStatus::Repeat;
        }
    }
    input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(items));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MatchRecognizeParamsWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto measures = input->Child(0);
    input->SetTypeAnn(measures->GetTypeAnn());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MatchRecognizeMeasuresWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto inputRowType = input->Child(0);
    const auto patternVars = input->Child(1);
    const auto names = input->Child(2);
    constexpr size_t FirstLambdaIndex = 3;

    if (!EnsureType(*inputRowType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureTupleOfAtoms(*patternVars, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureTupleOfAtoms(*names, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, FirstLambdaIndex + names->ChildrenSize(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    auto lambdaInputRowColumns = inputRowType->GetTypeAnn()
            ->Cast<TTypeExprType>()->GetType()->Cast<TStructExprType>()->GetItems();
    using NYql::NMatchRecognize::EMeasureInputDataSpecialColumns;
    lambdaInputRowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(
            MeasureInputDataSpecialColumnName(EMeasureInputDataSpecialColumns::Classifier),
            ctx.Expr.MakeType<TDataExprType>(EDataSlot::Utf8)));
    lambdaInputRowColumns.push_back(ctx.Expr.MakeType<TItemExprType>(
            MeasureInputDataSpecialColumnName(EMeasureInputDataSpecialColumns::MatchNumber),
            ctx.Expr.MakeType<TDataExprType>(EDataSlot::Uint64)));
    auto lambdaInputRowType = ctx.Expr.MakeType<TStructExprType>(lambdaInputRowColumns);
    const auto& matchedRowsRanges = GetMatchedRowsRangesType(patternVars, ctx);
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
            items.push_back(ctx.Expr.MakeType<TItemExprType>(names->Child(i)->Content(), type));
        } else {
            return IGraphTransformer::TStatus::Repeat;
        }
    }
    input->SetTypeAnn(ctx.Expr.MakeType<TStructExprType>(items));
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MatchRecognizePatternWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext& ctx) {
    input->SetTypeAnn(ctx.Expr.MakeType<TVoidExprType>());
    return IGraphTransformer::TStatus::Ok;
}

IGraphTransformer::TStatus MatchRecognizeDefinesWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TContext &ctx) {
    if (!EnsureMinArgsCount(*input, 3, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto inputRowType = input->Child(0);
    const auto patternVars = input->Child(1);
    const auto names = input->Child(2);
    constexpr size_t FirstLambdaIndex = 3;

    if (!EnsureType(*inputRowType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureTupleOfAtoms(*patternVars, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureTupleOfAtoms(*names, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureArgsCount(*input, FirstLambdaIndex + names->ChildrenSize(), ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    const auto matchedRowsRanges = GetMatchedRowsRangesType(patternVars, ctx);
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
            if (IsBoolLike(*type)) {
                items.push_back(ctx.Expr.MakeType<TItemExprType>(names->Child(i)->Content(), type));
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

namespace {

bool ValidateSettings(const TExprNode::TPtr& settings, TExprContext& ctx) {
    if (!EnsureTuple(*settings, ctx)) {
        return false;
    }

    if (!EnsureArgsCount(*settings, 1, ctx)) {
        return false;
    }

    const auto streamingMode = settings->Child(0);
    if (!EnsureTupleOfAtoms(*streamingMode, ctx)) {
        return false;
    }
    if (!EnsureArgsCount(*streamingMode, 2, ctx)) {
        return false;
    }
    if (streamingMode->Child(0)->Content() != "Streaming") {
        ctx.AddError(TIssue(ctx.GetPosition(settings->Pos()), "Expected Streaming setting"));
        return false;
    }
    const auto mode = streamingMode->Child(1)->Content();
    if (mode != "0" and mode != "1") {
        ctx.AddError(TIssue(ctx.GetPosition(settings->Pos()), TStringBuilder() << "Expected 0 or 1, but got: " << mode));
        return false;
    }
    return true;
}

} // anonymous namespace

IGraphTransformer::TStatus MatchRecognizeCoreWrapper(const TExprNode::TPtr& input, TExprNode::TPtr&, TExtContext& ctx) {
    if (not ctx.Types.MatchRecognize) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "MATCH_RECOGNIZE is disabled"));
        return IGraphTransformer::TStatus::Error;
    }
    if (!EnsureArgsCount(*input, 5, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto source = input->Child(0);
    auto& partitionKeySelector = input->ChildRef(1);
    const auto partitionColumns = input->Child(2);
    const auto params = input->Child(3);
    const auto settings = input->Child(4);
    if (not params->IsCallable("MatchRecognizeParams")) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(params->Pos()), "Expected MatchRecognizeParams"));
        return IGraphTransformer::TStatus::Error;
    }

    if (not ValidateSettings(settings, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }

    if (!EnsureFlowType(*source, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto inputRowType = GetSeqItemType(source->GetTypeAnn());
    const auto define = params->Child(4);
    if (not inputRowType->Equals(*define->Child(0)->GetTypeAnn()->Cast<TTypeExprType>()->GetType())) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Expected the same input row type as for DEFINE"));
        return IGraphTransformer::TStatus::Error;
    }

    auto status = ConvertToLambda(partitionKeySelector, ctx.Expr, 1, 1);
    if (status.Level != IGraphTransformer::TStatus::Ok) {
        return status;
    }
    if (!UpdateLambdaAllArgumentsTypes(partitionKeySelector, { inputRowType }, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    auto partitionKeySelectorType = partitionKeySelector->GetTypeAnn();
    if (!partitionKeySelectorType) {
        return IGraphTransformer::TStatus::Repeat;
    }
    if (not EnsureTupleType(partitionKeySelector->Pos(), *partitionKeySelectorType, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    const auto& partitionKeySelectorItemTypes = partitionKeySelectorType->Cast<TTupleExprType>()->GetItems();
    if (not EnsureTupleOfAtoms(*partitionColumns, ctx.Expr)) {
        return IGraphTransformer::TStatus::Error;
    }
    if (partitionColumns->ChildrenSize() != partitionKeySelectorItemTypes.size()) {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(input->Pos()), "Partition columns does not match the size of partition lambda"));
        return IGraphTransformer::TStatus::Error;
    }

    auto outputTableColumns = params->GetTypeAnn()->Cast<TStructExprType>()->GetItems();
    if (const auto rowsPerMatch = params->Child(1);
        "RowsPerMatch_OneRow" == rowsPerMatch->Content()) {
        for (size_t i = 0; i != partitionColumns->ChildrenSize(); ++i) {
            outputTableColumns.push_back(ctx.Expr.MakeType<TItemExprType>(
                    partitionColumns->Child(i)->Content(),
                    partitionKeySelectorItemTypes[i]
            ));
        }
    } else if ("RowsPerMatch_AllRows" == rowsPerMatch->Content()) {
        const auto& inputTableColumns = GetSeqItemType(source->GetTypeAnn())->Cast<TStructExprType>()->GetItems();
        for (const auto& column : inputTableColumns) {
            outputTableColumns.push_back(ctx.Expr.MakeType<TItemExprType>(column->GetName(), column->GetItemType()));
        }
    } else {
        ctx.Expr.AddError(TIssue(ctx.Expr.GetPosition(rowsPerMatch->Pos()), "Unknown RowsPerMatch option"));
        return IGraphTransformer::TStatus::Error;
    }
    const auto outputTableRowType = ctx.Expr.MakeType<TStructExprType>(outputTableColumns);
    input->SetTypeAnn(ctx.Expr.MakeType<TFlowExprType>(outputTableRowType));

    return IGraphTransformer::TStatus::Ok;
}

} // namespace NYql::NTypeAnnImpl
