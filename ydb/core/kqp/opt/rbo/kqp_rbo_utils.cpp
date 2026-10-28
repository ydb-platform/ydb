#include "kqp_rbo_utils.h"
#include "kqp_operator.h"

namespace NKikimr {
namespace NKqp {

using namespace NYql;

bool ReferencesUnresolvedSubplan(const TExpression& expr, const TPlanProps& props) {
    return expr.GetRawInputIUs().HasAny(props.Subplans.Bindings());
}

bool JoinOutputsLeft(const TString& joinKind) {
    return joinKind != "RightOnly" && joinKind != "RightSemi";
}

bool JoinOutputsRight(const TString& joinKind) {
    return joinKind != "LeftOnly" && joinKind != "LeftSemi";
}

TString GetValidJoinKind(const TString& joinKind) {
    const auto joinKindLowered = to_lower(joinKind);
    if (joinKindLowered == "left") {
        return "Left";
    } else if (joinKindLowered == "inner") {
        return "Inner";
    } else if (joinKindLowered == "cross") {
        return "Cross";
    }
    return joinKind;
}

TOrderedIUs<> GetAggregatePreservedShuffling(const TOpAggregate& aggregate, const TRBOContext& ctx) {
    if (aggregate.GetKeyColumns().Items().empty()) {
        return {};
    }

    const bool enableShuffleElimination = ctx.KqpCtx.Config->OptShuffleElimination.Get()
        .GetOrElse(ctx.KqpCtx.Config->GetDefaultEnableShuffleElimination());
    if (!enableShuffleElimination) {
        return {};
    }

    const auto& input = *aggregate.GetInput();
    if (!input.Props.Metadata || input.Props.Metadata->ShuffledByColumns.Items().empty()) {
        return {};
    }

    // Example: input partitioned by {id} needs no reshuffle for GROUP BY {id, date},
    // because every group has a single id and is already colocated.
    const auto& shuffledBy = input.Props.Metadata->ShuffledByColumns;
    if (!shuffledBy.Unordered().IsSubsetOf(aggregate.GetKeyColumns().Unordered())) {
        return {};
    }

    if (!aggregate.IsDistinctAll()) {
        return shuffledBy;
    }

    // DISTINCT returns the trait results, not the grouping keys. Preserve the
    // hash key order while translating through the intermediate/final aliases.
    TOrderedIUs<> result;
    for (const auto key : shuffledBy.Items()) {
        bool found = false;
        for (const auto& [output, trait] : aggregate.GetAggregationTraits().Items()) {
            if (trait.Input == key && trait.AggFunction == "distinct") {
                result.Append(output);
                found = true;
                break;
            }
        }
        if (!found) {
            return {};
        }
    }
    return result;
}

bool CanEliminateAggregateShuffle(const TOpAggregate& aggregate, const TRBOContext& ctx) {
    return !GetAggregatePreservedShuffling(aggregate, ctx).Items().empty();
}

bool SortMatchesKeyOrder(const TVector<TString>& sortColumns, const TVector<TString>& keyColumns, size_t pointPrefixLen) {
    if (sortColumns.empty() || pointPrefixLen > keyColumns.size()) {
        return false;
    }

    const THashSet<TString> pointKeys(keyColumns.begin(), keyColumns.begin() + pointPrefixLen);
    size_t next = pointPrefixLen;
    for (const auto& sortColumn : sortColumns) {
        if (sortColumn.empty()) {
            return false;
        }
        if (pointKeys.contains(sortColumn)) {
            continue;
        }
        if (next >= keyColumns.size() || keyColumns[next] != sortColumn) {
            return false;
        }
        ++next;
    }
    return true;
}

}
}
