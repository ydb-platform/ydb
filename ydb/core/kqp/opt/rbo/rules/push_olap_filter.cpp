#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_native_olap.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

namespace NKikimr::NKqp {

namespace {

using namespace NKikimr;
using namespace NKikimr::NKqp;

bool IsSuitableToPushPredicateToColumnTables(IOperator* input) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);

    const auto maybeRead = filter->GetInput().Get();
    if (maybeRead->Kind != EOperator::Source){
        return false;
    }

    const auto read = CastOperator<TOpRead>(maybeRead);
    return read->GetTableStorageType() == NYql::EStorageType::ColumnStorage && !read->OlapProgram;
}

} // anonymous namespace

bool TPushOlapFilterRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->GetChildren().front()->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TPushOlapFilterRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx, TPlanProps&) {
    if (!ctx.KqpCtx.Config->HasOptEnableOlapPushdown()) {
        return input;
    }

    if (!IsSuitableToPushPredicateToColumnTables(input.get())) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput().Get());

    const auto& expression = filter->GetFilterExpression();
    // Both the executable program and its saved statistics predicate must
    // bind entirely to this read's row.
    if (!expression.GetRawInputIUs().IsSubsetOf(read->GetColumns())) return input;
    const auto parts = SplitNativeOlapPredicate(*expression.GetNativeArena(), expression.GetNativeRoot(),
        ctx.KqpCtx.Config->GetEnableOlapScalarApply(), ctx.KqpCtx.Config->GetEnableOlapPushdownRegexp());
    TVector<TExpression> pushed, remaining;
    pushed.reserve(parts.Pushed.size());
    remaining.reserve(parts.Remaining.size());
    for (const auto id : parts.Pushed)
        pushed.push_back(TExpression::FromNative(expression.GetNativeArena(), id, expression.PlanProps, input->Pos));
    for (const auto id : parts.Remaining)
        remaining.push_back(TExpression::FromNative(expression.GetNativeArena(), id, expression.PlanProps, input->Pos));
    if (pushed.empty()) return input;
    const auto predicate = MakeConjunction(pushed);
    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), read->StorageType, read->TableRef,
        TOlapProgram(predicate), read->Limit, read->RangeInfo,
        read->OriginalPredicate ? read->OriginalPredicate : std::optional{filter->GetFilterExpression()},
        read->SortDir, read->Props, read->Pos);
    if (remaining.empty()) return newRead;
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, MakeConjunction(remaining), true);
}
} // namespace NKikimr::NKqp
