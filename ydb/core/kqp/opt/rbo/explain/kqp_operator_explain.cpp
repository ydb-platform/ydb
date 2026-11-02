#include "../kqp_operator.h"
#include "../kqp_expression.h"
#include "../kqp_olap_expr_inspection.h"
#include "kqp_read_ranges_explain.h"

#include <ydb/core/base/table_index.h>

#include <util/string/vector.h>

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

template <typename TRange>
TString FormatIds(const TRange& ids, const TInfoUnitRegistry* registry = nullptr, bool debug = false) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto id : ids) {
        result << separator << (registry ? (debug ? registry->GetDebugName(id) : registry->GetDisplayName(id)) : ::ToString(id));
        separator = ", ";
    }
    return result;
}

TString FormatSortElements(const TSortIUs& keys, const TInfoUnitRegistry& registry) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto& [id, order] : keys.Items()) {
        result << separator << registry.GetDisplayName(id) << (order.Ascending ? " asc" : " desc")
               << (order.NullsFirst ? " nulls first" : " nulls last");
        separator = ", ";
    }
    return result;
}

} // namespace

NJson::TJsonValue IOperator::ToJson(ui32 explainFlags, const TInfoUnitRegistry&, TExplainContext*)
{
    Y_UNUSED(explainFlags);
    auto res = NJson::TJsonValue(NJson::EJsonValueType::JSON_MAP);
    res["Name"] = GetExplainName();
    return res;
}

TString TOpReplicate::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    const auto& rebindings = GetRebindings();
    TStringBuilder result;
    result << "Replicate #" << Index_;
    if (IsPrimary()) {
        return result << " (identity)";
    }
    result << " {";
    TStringBuf separator;
    for (const auto input : InputIUs_) {
        result << separator << registry.GetDebugName(input) << " -> " << registry.GetDebugName(*rebindings.Find(input));
        separator = ", ";
    }
    return result << "}";
}

TString TOpEmptySource::ToString(TExprContext& ctx, const TInfoUnitRegistry&, TExplainContext*) {
    Y_UNUSED(ctx);
    return "EmptySource";
}

NJson::TJsonValue TOpEmptySource::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto res = IOperator::ToJson(explainFlags, registry, explain);
    if (Input && TCoParameter::Match(Input.Get())) {
        res["Parameter"] = TCoParameter(Input).Name().StringValue();
    }
    if (Input) {
        res["Columns"] = TStringBuilder() << "{" << FormatIds(Columns, &registry) << "}";
    }
    return res;
}

static std::optional<TString> GetUint64Literal(const TExprNode::TPtr& node) {
    if (!node) {
        return std::nullopt;
    }

    if (auto maybeUint64 = TExprBase(node).Maybe<TCoUint64>()) {
        return TString(maybeUint64.Cast().Literal().Cast<TCoAtom>().Value());
    }

    return std::nullopt;
}

NJson::TJsonValue TOpRead::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto res = IOperator::ToJson(explainFlags, registry, explain);

    // Tables are usually named in a path-like fashion, like "/<path>/<name>".
    // In such case we extract the name of a table from a path,
    // fallback to the whole name otherwise.
    auto path = TKqpTable(TableCallable).Path().StringValue();
    auto slash = path.rfind('/');
    res["Table"] = (slash == TString::npos) ? path : path.substr(slash + 1);

    if (slash != TString::npos && TStringBuf(path).SubStr(slash + 1) == NTableIndex::ImplTable) {
        const auto indexSlash = path.rfind('/', slash - 1);
        if (indexSlash != TString::npos) {
            const auto tableSlash = path.rfind('/', indexSlash - 1);
            res["Table"] = path.substr(tableSlash == TString::npos ? 0 : tableSlash + 1);
            res["Index"] = path.substr(indexSlash + 1, slash - indexSlash - 1);
        }
    }

    res["Storage"] = StorageType == NYql::EStorageType::RowStorage ? "Row" : "Column";

    if (SortDir != ESortDir::None) {
        res["SortDirection"] = SortDir == ESortDir::Asc ? "asc" : "desc";
    }
    if (const auto limit = GetUint64Literal(Limit)) {
        res["Limit"] = *limit;
    }

    if (OriginalPredicate && !RangeInfo) {
        res["Predicate"] = OriginalPredicate->ToExplainString(registry);
    }

    // Build ReadColumns: for range scans, list ranged key columns first, then remaining columns.
    NJson::TJsonValue readColumns(NJson::EJsonValueType::JSON_ARRAY);
    THashSet<TString> addedColumns;
    if (RangeInfo) {
        const size_t usedLen = Min(RangeInfo->UsedPrefixLen, RangeInfo->KeyColumns.size());
        TVector<TString> rangedKeys;
        rangedKeys.reserve(usedLen);
        for (size_t i = 0; i < usedLen; ++i) {
            rangedKeys.push_back(SplitAliasedMemberName(RangeInfo->KeyColumns[i]).second);
        }

        const auto description = NOpt::DescribeReadRanges(RangeInfo->LiteralRange, RangeInfo->ComputeNode,
                                                          RangeInfo->KeyColumns, RangeInfo->ExpectedMaxRanges, explain);
        if (description.Ranges) {
            NJson::TJsonValue readRanges(NJson::EJsonValueType::JSON_ARRAY);
            for (const auto& range : *description.Ranges) {
                readRanges.AppendValue(range);
            }
            res["ReadRanges"] = std::move(readRanges);
        } else if (description.Expression) {
            res["ReadRangesExpression"] = *description.Expression;
        }

        NJson::TJsonValue rangeKeys(NJson::EJsonValueType::JSON_ARRAY);
        for (const auto& key : rangedKeys) {
            addedColumns.insert(key);
            readColumns.AppendValue(key);
            rangeKeys.AppendValue(key);
        }
        res["ReadRangesKeys"] = std::move(rangeKeys);

        if (RangeInfo->ExpectedMaxRanges) {
            res["ReadRangesExpectedSize"] = ::ToString(*RangeInfo->ExpectedMaxRanges);
        }
    }
    // Choose a deterministic presentation order, not a logical column order.
    for (const auto id : Columns_) {
        const auto column = registry.Get(id).GetColumnName();
        if (addedColumns.insert(column).second) {
            readColumns.AppendValue(column);
        }
    }
    res["ReadColumns"] = std::move(readColumns);
    // ReadColumns names storage fields; Columns names the produced row.
    res["Columns"] = TStringBuilder() << "{" << FormatIds(Columns_, &registry) << "}";

    return res;
}

TString TOpRead::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto res = TStringBuilder();
    res << "Read (" << TKqpTable(TableCallable).Path().StringValue() << "," << Alias << ", [";

    TStringBuf separator;
    for (const auto id : Columns_) {
        res << separator << registry.GetDebugName(id);
        separator = ", ";
    }
    res << "])";
    const TString storageType = StorageType == NYql::EStorageType::RowStorage ? "Row" : "Column";
    res << " (StorageType: " << storageType << ")";
    if (OlapFilterLambda) {
        THashMap<TString, TString> names;
        for (const auto id : GetRequiredColumns(ctx)) {
            names.emplace(ctx.GetIndexAsString(id), registry.GetDebugName(id));
        }
        res << " OlapFilter: (" << PrintRBOExpression(NOpt::TOlapFilterInspector::RenameColumns(OlapFilterLambda, names, ctx), ctx) << ")";
    }
    if (RangeInfo) {
        const auto description = NOpt::DescribeReadRanges(RangeInfo->LiteralRange, RangeInfo->ComputeNode,
                                                          RangeInfo->KeyColumns, RangeInfo->ExpectedMaxRanges, explain);
        if (description.Ranges) {
            res << " Ranges: [" << JoinSeq(", ", *description.Ranges) << "]";
        } else if (description.Expression) {
            res << " Ranges expression: (" << *description.Expression << ")";
        }
    }
    if (SortDir != ESortDir::None) {
        res << " Sort direction: (" << ((SortDir == ESortDir::Asc) ? "ASC" : "DESC");
        res << ")";
    }
    if (OriginalPredicate.has_value()) {
        res << " Original predicate: (" << OriginalPredicate->ToString() << ")";
    }

    return res;
}

TString TOpMap::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder text;
    text << "Map [";
    TStringBuf separator;
    for (const auto id : MapElements.Keys()) {
        const auto& element = *MapElements.Find(id);
        text << separator << registry.GetDebugName(id) << " := " << element.GetExpression().ToString();
        separator = ", ";
    }
    return text << "]";
}

NJson::TJsonValue TOpMap::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto result = IOperator::ToJson(explainFlags, registry, explain);
    TStringBuilder name;
    name << "Map [";
    TStringBuf separator;
    for (const auto id : MapElements.Keys()) {
        const auto& element = *MapElements.Find(id);
        name << separator << registry.GetDisplayName(id) << " := " << element.GetExpression().ToExplainString(registry);
        separator = ", ";
    }
    result["Name"] = name << "]";
    return result;
}

TString TOpAddDependencies::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder result;
    result << "Correlated [";
    TStringBuf separator;
    for (const auto local : Dependencies.Keys()) {
        result << separator << registry.GetDebugName(local) << " <- outer " << registry.GetDebugName(Dependencies.Find(local)->Outer);
        separator = ", ";
    }
    return result << "]";
}

TString TOpFilter::ToString(TExprContext& ctx, const TInfoUnitRegistry&, TExplainContext*) {
    Y_UNUSED(ctx);
    return TStringBuilder() << "Filter :" << FilterExpr.ToString();
}

NJson::TJsonValue TOpFilter::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto res = IOperator::ToJson(explainFlags, registry, explain);
    res["Predicate"] = FilterExpr.ToExplainString(registry);
    return res;
}

TString GetJoinAlgoName(NKqp::EJoinAlgoType joinAlgo) {
    switch (joinAlgo) {
        case NKqp::EJoinAlgoType::Undefined:
            return "Undefined";
        case NKqp::EJoinAlgoType::LookupJoin:
            return "Lookup";
        case NKqp::EJoinAlgoType::LookupJoinReverse:
            return "ReverseLookup";
        case NKqp::EJoinAlgoType::MapJoin:
            return "Map";
        case NKqp::EJoinAlgoType::GraceJoin:
            return "Shuffle";
        case NKqp::EJoinAlgoType::ReverseBlockJoin:
            return "ReverseBlock";
        case NKqp::EJoinAlgoType::StreamLookupJoin:
            return "StreamLookup";
        case NKqp::EJoinAlgoType::MergeJoin:
            return "Merge";
    }
    Y_ENSURE(false, "Unknown join algo type");
    return "Unknown";
}

TString GetExplainJoinAlgoName(const TPhysicalOpProps& props) {
    Y_ENSURE(props.JoinAlgo.has_value(), "Join algorithm has not been selected");
    Y_ENSURE(props.UseBlockHashJoin.has_value(), "Physical join implementation has not been selected");

    if (*props.UseBlockHashJoin) {
        return "BlockHash";
    }

    const auto joinAlgo = *props.JoinAlgo;
    if (joinAlgo == NKqp::EJoinAlgoType::GraceJoin || joinAlgo == NKqp::EJoinAlgoType::ReverseBlockJoin) {
        return "Grace";
    }
    return GetJoinAlgoName(joinAlgo);
}

TString TOpJoin::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << "Join, Kind: " << JoinKind;
    if (Props.JoinAlgo.has_value()) {
        res << ", Algo: " << GetJoinAlgoName(*Props.JoinAlgo);
    }
    res << " [";
    TStringBuf separator;
    for (const auto& [left, right, equalNulls] : JoinKeys.Items()) {
        res << separator << registry.GetDebugName(left) << (equalNulls ? " IS NOT DISTINCT FROM " : "=") << registry.GetDebugName(right);
        separator = ", ";
    }
    res << "], Filters: [";
    for (size_t i = 0; i < JoinFilters.size(); i++) {
        res << JoinFilters[i].ToString();
        if (i != JoinFilters.size() - 1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

static TString FormatJoinKeys(const TJoinIUs& keys, const TInfoUnitRegistry& registry, bool debug = false) {
    TStringBuilder result;
    TStringBuf separator;
    for (const auto& [left, right, equalNulls] : keys.Items()) {
        result << separator << (debug ? registry.GetDebugName(left) : registry.GetDisplayName(left)) << (equalNulls ? " IS NOT DISTINCT FROM " : " = ") << (debug ? registry.GetDebugName(right) : registry.GetDisplayName(right));
        separator = ", ";
    }
    return result;
}

NJson::TJsonValue TOpJoin::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto res = IOperator::ToJson(explainFlags, registry, explain);
    const auto joinAlgoName = GetExplainJoinAlgoName(Props);

    if (JoinKind == "Cross") {
        res["Name"] = "CrossJoin";
    } else {
        res["Name"] = TStringBuilder() << JoinKind << "Join (" << joinAlgoName << ")";
    }
    res["JoinKind"] = JoinKind;
    res["JoinAlgo"] = joinAlgoName;
    if (!JoinKeys.Items().empty()) {
        res["Condition"] = FormatJoinKeys(JoinKeys, registry);
    }
    if (!JoinFilters.empty()) {
        NJson::TJsonValue filters(NJson::EJsonValueType::JSON_ARRAY);
        for (const auto& filter : JoinFilters) {
            filters.AppendValue(filter.ToExplainString(registry));
        }
        res["Filters"] = filters;
    }

    return res;
}

TString TOpDependentJoin::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    return TStringBuilder() << "DependentJoin, Domain: [" << FormatIds(Dependencies, &registry, true) << "]";
}

TString TOpUnionAll::ToString(TExprContext& ctx, const TInfoUnitRegistry&, TExplainContext*) {
    Y_UNUSED(ctx);
    return "UnionAll";
}

NJson::TJsonValue TOpUnionAll::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto result = IOperator::ToJson(explainFlags, registry, explain);
    result["Ordered"] = Ordered;
    result["Columns"] = TStringBuilder() << "{" << FormatIds(Columns.Keys(), &registry) << "}";
    return result;
}

TString TOpLimit::ToString(TExprContext& ctx, const TInfoUnitRegistry&, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder builder;
    builder << "Limit: " << LimitCond.ToString() << " ";
    if (OffsetCond.has_value()) {
        builder << "Offset: " << OffsetCond->ToString() << " ";
    }
    builder << "Phase: " << ToStringPhase(LimitPhase);
    return builder;
}

NJson::TJsonValue TOpLimit::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto res = IOperator::ToJson(explainFlags, registry, explain);
    res["Limit"] = LimitCond.ToExplainString(registry);
    if (OffsetCond) {
        res["Offset"] = OffsetCond->ToExplainString(registry);
    }
    if (LimitPhase != EOpPhase::Undefined) {
        res["Phase"] = ToStringPhase(LimitPhase);
    }
    return res;
}

TString TOpSort::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder text;
    text << "Sort: [";
    TStringBuf separator;
    for (const auto& [id, order] : SortElements.Items()) {
        text << separator << registry.GetDebugName(id) << (order.Ascending ? " asc" : " desc");
        separator = ", ";
    }
    text << "]";
    if (LimitCond) {
        text << ", Limit: " << LimitCond->ToString();
    }
    return text << " Phase: " << ToStringPhase(SortPhase);
}

static TString FormatLookupKeys(const TOpTableLookup::TLookupKeys& keys, const TInfoUnitRegistry* registry = nullptr, bool debug = false) {
    TStringBuilder text;
    TStringBuf separator;
    for (const auto& [id, column] : keys.Items()) {
        text << separator << FormatIds(std::views::single(id), registry, debug) << " = " << column;
        separator = ", ";
    }
    return text;
}

TString TOpTableLookup::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << GetExplainName() << ": " << TKqpTable(Table).Path().StringValue();
    if (IsJoin()) {
        res << ", kind: " << JoinKind;
    }
    res << ", keys: [";
    TStringBuf separator;
    for (const auto& [id, column] : LookupKeys.Items()) {
        res << separator << registry.GetDebugName(id);
        if (IsJoin()) {
            res << " = " << column;
        }
        separator = ", ";
    }
    res << "], columns: [" << FormatIds(Columns, &registry, true) << "]";
    if (Prefix) {
        res << ", key prefix: [" << JoinSeq(", ", Prefix->Columns) << "]";
        for (const auto& [id, column] : Prefix->Equalities.Items()) {
            res << ", " << column << " = " << registry.GetDebugName(id);
        }
    }
    if (FetchedRowFilter) {
        res << ", filter: " << FetchedRowFilter->ToString();
    }
    for (const auto& [left, right, equalNulls] : ResidualJoinKeys.Items()) {
        res << ", residual: " << registry.GetDebugName(left) << (equalNulls ? " IS NOT DISTINCT FROM " : " = ") << registry.GetDebugName(right);
    }
    return res;
}

NJson::TJsonValue TOpTableLookup::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto result = IOperator::ToJson(explainFlags, registry, explain);
    result["Table"] = TKqpTable(Table).Path().StringValue();
    if (IsJoin()) {
        result["JoinKind"] = JoinKind;
        TStringBuilder condition;
        condition << FormatLookupKeys(LookupKeys, &registry);
        if (Prefix) {
            if (!Prefix->Equalities.Items().empty()) {
                condition << ", " << FormatLookupKeys(Prefix->Equalities, &registry);
            }
            result["LookupKeyPrefix"] = JoinSeq(", ", Prefix->Columns);
        }
        if (!ResidualJoinKeys.Items().empty()) {
            condition << ", " << FormatJoinKeys(ResidualJoinKeys, registry);
        }
        result["Condition"] = condition;
    }
    if (FetchedRowFilter) {
        result["Predicate"] = FetchedRowFilter->ToExplainString(registry);
    }
    return result;
}

TString TOpIndexLookupJoin::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder res;
    res << "IndexLookupJoin, Kind: " << JoinKind << " [" << FormatJoinKeys(JoinKeys, registry, true) << "]";
    return res;
}

NJson::TJsonValue TOpIndexLookupJoin::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto res = IOperator::ToJson(explainFlags, registry, explain);
    res["Name"] = TStringBuilder() << JoinKind << "Join (Lookup)";
    res["JoinKind"] = JoinKind;
    res["JoinAlgo"] = "Lookup";
    if (!JoinKeys.Items().empty()) {
        res["Condition"] = FormatJoinKeys(JoinKeys, registry);
    }
    return res;
}

NJson::TJsonValue TOpSort::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto res = IOperator::ToJson(explainFlags, registry, explain);
    if (IsTopSort()) {
        res["TopSortBy"] = FormatSortElements(SortElements, registry);
        if (LimitCond) {
            res["Limit"] = LimitCond->ToExplainString(registry);
        }
    } else {
        res["SortBy"] = FormatSortElements(SortElements, registry);
    }
    if (SortPhase != EOpPhase::Undefined) {
        res["Phase"] = ToStringPhase(SortPhase);
    }
    return res;
}

TString TOpAggregate::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder text;
    text << "Aggregate [";
    TStringBuf separator;
    for (const auto id : Aggregations.Keys()) {
        const auto& aggregation = *Aggregations.Find(id);
        text << separator << registry.GetDebugName(id) << ": " << aggregation.AggFunction << "("
             << (aggregation.Distinct ? "distinct " : "") << registry.GetDebugName(aggregation.Input) << ")";
        separator = ", ";
    }
    return text << " [" << FormatIds(KeyColumns.Items(), &registry, true) << "]] "
                << (DistinctAll ? " (Distinct all) " : "") << ToStringPhase(AggregationPhase)
                << (PushedToOlap ? " (Pushed to OLAP)" : "");
}

NJson::TJsonValue TOpAggregate::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto result = IOperator::ToJson(explainFlags, registry, explain);
    if (!KeyColumns.Items().empty()) {
        result["GroupBy"] = FormatIds(KeyColumns.Items(), &registry);
    }
    if (!Aggregations.Items().empty()) {
        TStringBuilder aggregation;
        aggregation << "{";
        TStringBuf separator;
        for (const auto id : Aggregations.Keys()) {
            const auto& traits = *Aggregations.Find(id);
            aggregation << separator << registry.GetDisplayName(id) << ": " << traits.AggFunction << "(" << registry.GetDisplayName(traits.Input) << ")";
            separator = ", ";
        }
        aggregation << "}";
        result["Aggregation"] = aggregation;
    }
    result["Phase"] = ToStringPhase(AggregationPhase);
    if (DistinctAll) {
        result["Distinct"] = "All";
    }
    if (PushedToOlap) {
        result["Pushdown"] = "True";
    }
    return result;
}

TString TOpGroupingSets::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);

    TStringBuilder result;
    result << "GroupingSets [";
    for (size_t setIndex = 0; setIndex < GroupingSets.size(); ++setIndex) {
        if (setIndex != 0) {
            result << ", ";
        }
        result << "(" << FormatIds(GroupingSets[setIndex], &registry, true) << ")";
    }
    result << "] Columns [";
    TStringBuf separator;
    for (const auto output : Columns.Keys()) {
        result << separator << registry.GetDebugName(output) << " <- " << registry.GetDebugName(*Columns.Find(output));
        separator = ", ";
    }
    for (const auto& [output, key] : GroupingIndicators.Items()) {
        result << separator << registry.GetDebugName(output) << " <- GROUPING(" << registry.GetDebugName(key) << ")";
        separator = ", ";
    }
    return result << "]";
}

static TString FormatWindowFunctions(const TWindowIUs& functions, const TInfoUnitRegistry* registry = nullptr, bool debug = false) {
    TStringBuilder text;
    TStringBuf separator;
    for (const auto id : functions.Keys()) {
        const auto& function = *functions.Find(id);
        text << separator << FormatIds(std::views::single(id), registry, debug) << ": " << function.Function
             << "(" << FormatIds(function.Arguments.Items(), registry, debug) << ")";
        separator = ", ";
    }
    return text;
}

TString TOpWindow::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext*) {
    Y_UNUSED(ctx);
    TStringBuilder text;
    text << "Window [" << FormatWindowFunctions(WindowFuncs, &registry, true) << "]";
    if (!PartitionKeys.Items().empty()) {
        text << " PartitionBy: [" << FormatIds(PartitionKeys.Items(), &registry, true) << "]";
    }
    if (!SortElements.Items().empty()) {
        text << " OrderBy: [";
        TStringBuf separator;
        for (const auto& [id, order] : SortElements.Items()) {
            text << separator << registry.GetDebugName(id) << (order.Ascending ? " asc" : " desc");
            separator = ", ";
        }
        text << "]";
    }
    text << " Frame: " << ToStringWindowFrameType(Frame.Type) << "[" << ToStringWindowFrameBound(Frame.BeginKind);
    if (Frame.BeginKind == EWindowFrameBound::Preceding || Frame.BeginKind == EWindowFrameBound::Following) {
        text << " " << Frame.BeginValue;
    }
    text << ", " << ToStringWindowFrameBound(Frame.EndKind);
    if (Frame.EndKind == EWindowFrameBound::Preceding || Frame.EndKind == EWindowFrameBound::Following) {
        text << " " << Frame.EndValue;
    }
    return text << "]";
}

NJson::TJsonValue TOpWindow::ToJson(ui32 explainFlags, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    auto result = IOperator::ToJson(explainFlags, registry, explain);
    result["WindowFunctions"] = TStringBuilder() << "{" << FormatWindowFunctions(WindowFuncs, &registry) << "}";
    if (!PartitionKeys.Items().empty()) {
        result["PartitionBy"] = FormatIds(PartitionKeys.Items(), &registry);
    }
    if (!SortElements.Items().empty()) {
        result["OrderBy"] = FormatIds(SortElements.Items() | std::views::keys, &registry);
    }
    result["Frame"] = ToStringWindowFrameType(Frame.Type);
    return result;
}

TString TOpCBOTree::ToString(TExprContext& ctx, const TInfoUnitRegistry& registry, TExplainContext* explain) {
    TExplainContext local;
    if (!explain) explain = &local;
    TStringBuilder res;
    res << "CBO Tree: [";
    for (size_t i=0; i < TreeNodes.size(); i++) {
        res << TreeNodes[i]->ToString(ctx, registry, explain);
        if (i != TreeNodes.size()-1) {
            res << ", ";
        }
    }
    res << "]";
    return res;
}

TString TOpTableEffect::GetExplainName() const {
    switch (EffectType) {
        case EEffectType::InsertRows:
        case EEffectType::InsertRowsIndex:
            return "InsertRows";
        case EEffectType::UpdateRows:
        case EEffectType::UpdateRowsIndex:
            return "UpdateRows";
        case EEffectType::UpsertRows:
        case EEffectType::UpsertRowsIndex:
            return "UpsertRows";
        case EEffectType::DeleteRows:
        case EEffectType::DeleteRowsIndex:
            return "DeleteRows";
        default:
            Y_ENSURE(false, "Uknown table effect type");
    }
}

TString TOpTableEffect::ToString(TExprContext& ctx, const TInfoUnitRegistry&, TExplainContext*) {
    Y_UNUSED(ctx);
    return GetExplainName();
}

TString TOpRoot::ToString(TExprContext& ctx, const TInfoUnitRegistry&, TExplainContext*) {
    Y_UNUSED(ctx);
    return "Root";
}

TString TOpRoot::PlanToString(TExprContext& ctx, ui32 printOptions) {
    TExplainContext explain;
    auto builder = TStringBuilder();
    for (const auto& [binding, subplan] : PlanProps.Subplans) {
        builder << "Subplan binding to " << PlanProps.InfoUnitRegistry.GetDebugName(binding) << ":\n";
        PlanToStringRec(subplan.Plan.Get(), ctx, builder, 0, printOptions, &explain);
    }
    PlanToStringRec(GetInput().Get(), ctx, builder, 0, printOptions, &explain);
    return builder;
}

void TOpRoot::PlanToStringRec(IOperator* op, TExprContext& ctx, TStringBuilder& builder, int tabs, ui32 printOptions, TExplainContext* explain) const {
    TExplainContext local;
    if (!explain) explain = &local;
    TStringBuilder tabString;
    for (int i = 0; i < tabs; i++) {
        tabString << "  ";
    }

    builder << tabString << op->ToString(ctx, PlanProps.InfoUnitRegistry, explain);
    if (op->Props.StageId.has_value()) {
        builder << " StageId: " << *op->Props.StageId;
    }
    builder << "\n";

    if (printOptions & (EPrintPlanOptions::PrintBasicMetadata | EPrintPlanOptions::PrintFullMetadata) && op->Props.Metadata.has_value()) {
        builder << tabString << " ";
        builder << op->Props.Metadata->ToString(printOptions, PlanProps.InfoUnitRegistry);
        if (printOptions & EPrintPlanOptions::PrintFullMetadata) {
            builder << ", Lineage: {";
            for (const auto id : op->GetOutputIUs()) {
                if (const auto* entry = PlanProps.ColumnLineage.Find(id)) {
                    builder << PlanProps.InfoUnitRegistry.GetDebugName(id) << ": <ColName: " << entry->ColumnName
                        << ", Alias: " << entry->SourceAlias
                        << ", Table: " << entry->TableName
                        << ", Relation: " << entry->Relation
                        << ">, ";
                }
            }
            builder << "}";
        }
        builder << "\n";
    }

    if (printOptions & (EPrintPlanOptions::PrintBasicStatistics | EPrintPlanOptions::PrintFullStatistics) && op->Props.Statistics.has_value()) {
        builder << tabString << " ";
        builder << op->Props.Statistics->ToString(printOptions);
        builder << ", Cost: " << (op->Props.Cost.has_value() ? std::to_string(*op->Props.Cost) : "None") << "\n";
    }

    for (auto c : op->GetChildren()) {
        PlanToStringRec(c, ctx, builder, tabs + 1, printOptions, explain);
    }
}

} // namespace NKikimr::NKqp
