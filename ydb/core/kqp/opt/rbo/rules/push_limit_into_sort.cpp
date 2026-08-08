#include "kqp_rules_include.h"

namespace NKikimr {
namespace NKqp {

namespace {

bool IsSafeToDelayAfterTopSort(const TExpression& expression) {
    return !FindNode(
        expression.GetExpressionBody(),
        [](const TExprNode::TPtr& node) {
            return node->HasResult() || node->IsPosAware() ||
                node->HasSideEffects() || !node->IsCseeSafe();
        });
}

bool CanDelayMapAfterTopSort(
    const TIntrusivePtr<TOpMap>& map,
    TPlanProps& props)
{
    if (!map->IsSingleConsumer() || map->GetMapElements().empty() ||
        !map->GetSubplanIUs(props).empty())
    {
        return false;
    }

    const auto& inputIUs = map->GetInput()->GetOutputIUs();
    for (const auto& element : map->GetMapElements()) {
        if (!IsSafeToDelayAfterTopSort(element.GetExpression())) {
            return false;
        }

        for (const auto& iu : element.GetExpression().GetInputIUs(
                 /*includeSubplanVars=*/true,
                 /*includeCorrelatedDeps=*/true))
        {
            if (!ContainsInfoUnit(inputIUs, iu)) {
                return false;
            }
        }
    }

    return true;
}

std::optional<TVector<TSortElement>> RewriteSortElementsBelowMap(
    const TIntrusivePtr<TOpSort>& sort,
    const TIntrusivePtr<TOpMap>& map)
{
    const auto& inputIUs = map->GetInput()->GetOutputIUs();
    const auto renameSources = map->GetRenameSources();
    auto rewritten = sort->GetSortElements();

    for (auto& sortElement : rewritten) {
        const TMapElement* producer = nullptr;
        for (const auto& mapElement : map->GetMapElements()) {
            if (mapElement.GetElementName() != sortElement.SortColumn) {
                continue;
            }
            if (producer) {
                return std::nullopt;
            }
            producer = &mapElement;
        }

        if (producer) {
            if (!producer->IsColumnAccess()) {
                return std::nullopt;
            }
            const auto source = producer->GetColumnAccess();
            if (!ContainsInfoUnit(inputIUs, source)) {
                return std::nullopt;
            }
            sortElement.SortColumn = source;
            continue;
        }

        if (!ContainsInfoUnit(inputIUs, sortElement.SortColumn) ||
            renameSources.contains(sortElement.SortColumn))
        {
            return std::nullopt;
        }
    }

    return rewritten;
}

TIntrusivePtr<IOperator> TryDelayMapAfterTopSort(
    const TIntrusivePtr<TOpSort>& sort,
    const TExpression& limitCond,
    TPlanProps& props)
{
    if (sort->GetSortPhase() != EOpPhase::Undefined ||
        sort->GetInput()->GetKind() != EOperator::Map)
    {
        return {};
    }

    const auto map = CastOperator<TOpMap>(sort->GetInput());
    if (!CanDelayMapAfterTopSort(map, props)) {
        return {};
    }

    const auto rewrittenSortElements =
        RewriteSortElementsBelowMap(sort, map);
    if (!rewrittenSortElements) {
        return {};
    }

    // A distributed TopSort serializes its input before the final limit.  Keep
    // output-only expressions above that limit so discarded rows stay lazy.
    const auto topSort = MakeIntrusive<TOpSort>(
        map->GetInput(),
        sort->Pos,
        *rewrittenSortElements,
        limitCond);
    return MakeIntrusive<TOpMap>(
        topSort,
        map->Pos,
        map->GetMapElements(),
        map->IsOrdered());
}

} // namespace

bool TPushLimitIntoSortRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Limit &&
        input->Children.front()->Kind == EOperator::Sort;
}

TIntrusivePtr<IOperator> TPushLimitIntoSortRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator> &input, TRBOContext &ctx, TPlanProps &props) {
    Y_UNUSED(ctx);

    if (input->Kind != EOperator::Limit) {
        return input;
    }

    auto limit = CastOperator<TOpLimit>(input);
    if (limit->Props.EnsureAtMostOne) {
        return input;
    }

    if (limit->HasOffset()) {
        return input;
    }

    if (limit->GetInput()->Kind != EOperator::Sort) {
        return input;
    }

    auto sort = CastOperator<TOpSort>(limit->GetInput());
    if (!sort->IsSingleConsumer() || sort->LimitCond) {
        return input;
    }

    if (const auto delayedMap =
            TryDelayMapAfterTopSort(sort, limit->LimitCond, props))
    {
        return delayedMap;
    }

    sort->LimitCond = limit->LimitCond;
    return sort;
}
}
}
