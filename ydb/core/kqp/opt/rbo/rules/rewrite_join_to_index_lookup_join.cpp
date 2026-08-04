#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>

namespace NKikimr {
namespace NKqp {

namespace {

using namespace NYql;
using namespace NYql::NNodes;

std::optional<TExpression> BuildFetchedRowFilter(const TOpRead& read, const TIntrusivePtr<TOpFilter>& filter, bool& supported) {
    TVector<TExpression> conjuncts;
    if (read.RangeInfo.has_value()) {
        if (!read.OriginalPredicate.has_value()) {
            supported = false;
            return std::nullopt;
        }
        const auto original = read.OriginalPredicate->SplitConjunct();
        conjuncts.insert(conjuncts.end(), original.begin(), original.end());
    }
    if (filter) {
        const auto filters = filter->FilterExpr.SplitConjunct();
        conjuncts.insert(conjuncts.end(), filters.begin(), filters.end());
    }

    if (conjuncts.empty()) {
        return std::nullopt;
    }

    // The filter is evaluated on a fetched row, so it can only refer to the fetched columns.
    const auto readOutputs = MakeInfoUnitSet(read.OutputIUs);
    for (const auto& conjunct : conjuncts) {
        for (const auto& iu : conjunct.GetInputIUs(/*includeSubplanVars=*/true, /*includeCorrelatedDeps=*/true)) {
            if (!readOutputs.contains(iu)) {
                supported = false;
                return std::nullopt;
            }
        }
    }

    return MakeConjunction(conjuncts);
}

const TTypeAnnotationNode* StripOptional(const TTypeAnnotationNode* type) {
    return type && type->GetKind() == ETypeAnnotationKind::Optional ? type->Cast<TOptionalExprType>()->GetItemType() : type;
}

struct TLookupKey {
    TInfoUnit LeftIU;
    TInfoUnit RightIU;
    TString Column;
};

struct TKeyMatch {
    // Represents a constant prefix keys.
    TVector<TLookupKey> PrefixKeys;
    // Represents a lookup keys.
    TVector<TLookupKey> LookupKeys;
};

std::optional<TKeyMatch> MatchKeyPrefix(const TOpJoin& join, const TOpRead& read, const TVector<TString>& keyColumnNames,
                                        size_t pointPrefixLen) {
    Y_ENSURE(pointPrefixLen < keyColumnNames.size());

    THashMap<TInfoUnit, TString, TInfoUnit::THashFunction> readColumnByIU;
    for (size_t i = 0; i < read.OutputIUs.size(); ++i) {
        readColumnByIU[read.OutputIUs[i]] = read.Columns[i];
    }

    THashMap<TString, TLookupKey> keyByColumn;
    for (const auto& [leftIU, rightIU] : join.JoinKeys) {
        const auto it = readColumnByIU.find(rightIU);
        Y_ENSURE(it != readColumnByIU.end(), "Cannot find a join key in input columns.");
        const auto column = it->second;
        if (!keyByColumn.emplace(column, TLookupKey{leftIU, rightIU, column}).second) {
            return std::nullopt;
        }
    }

    TKeyMatch match;
    size_t matchedJoinKeys = 0;
    for (size_t i = 0; i < keyColumnNames.size(); ++i) {
        const auto it = keyByColumn.find(keyColumnNames[i]);
        if (i < pointPrefixLen) {
            if (it != keyByColumn.end()) {
                match.PrefixKeys.push_back(it->second);
                ++matchedJoinKeys;
            }
            continue;
        }

        if (it == keyByColumn.end()) {
            break;
        }

        match.LookupKeys.push_back(it->second);
        ++matchedJoinKeys;
    }

    // TODO: Support filtering after streamlookup connection, we can apply a filter on join keys which are not.
    if (matchedJoinKeys != keyByColumn.size()) {
        return std::nullopt;
    }

    // Lookup keys cannot be empty.
    if (match.LookupKeys.empty()) {
        return std::nullopt;
    }

    return match;
}

bool KeyTypesMatch(IOperator& leftInput, IOperator& rightInput, const TVector<TLookupKey>& keys) {
    for (const auto& key : keys) {
        const auto* leftType = StripOptional(leftInput.GetIUType(key.LeftIU));
        const auto* rightType = StripOptional(rightInput.GetIUType(key.RightIU));
        // TODO: Add support key with different types.
        if (!leftType || !rightType || leftType != rightType) {
            return false;
        }
    }
    return true;
}

bool KeyTypesMatch(IOperator& leftInput, IOperator& rightInput, const TKeyMatch& keys) {
    return KeyTypesMatch(leftInput, rightInput, keys.LookupKeys) && KeyTypesMatch(leftInput, rightInput, keys.PrefixKeys);
}

bool IsUsablePointPrefix(const TOpRead::TRangeInfo& ranges, const TVector<TString>& keyColumnNames, const TString& joinKind,
                         size_t pointsLimit) {
    if (!ranges.Points || !ranges.PointsItemType || ranges.PointColumns.empty()) {
        return false;
    }

    if (ranges.PointColumns.size() >= keyColumnNames.size()) {
        return false;
    }

    for (size_t i = 0; i < ranges.PointColumns.size(); ++i) {
        if (ranges.PointColumns[i] != keyColumnNames[i]) {
            return false;
        }
    }

    // For left join we cannot support more than 1 point lookup.
    if (joinKind == "Left") {
        pointsLimit = std::min<size_t>(pointsLimit, 1);
    }
    return ranges.ExpectedMaxPoints.Defined() && *ranges.ExpectedMaxPoints <= pointsLimit;
}

} // anonymous namespace

bool TRewriteJoinToIndexLookupJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Join;
}

TIntrusivePtr<IOperator> TRewriteJoinToIndexLookupJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                               TPlanProps& props) {
    Y_UNUSED(props);

    if (!ctx.KqpCtx.Config->GetEnableKqpDataQueryStreamIdxLookupJoin()) {
        return input;
    }
    if (!ctx.KqpCtx.IsDataQuery() && !ctx.KqpCtx.IsGenericQuery()) {
        return input;
    }

    // TODO: Add check for join algo specified by CBO.
    auto join = CastOperator<TOpJoin>(input);
    // TODO: Add support for other join kind.
    const auto joinKind = GetValidJoinKind(join->JoinKind);
    if (joinKind != "Inner" && joinKind != "Left") {
        return input;
    }

    // Not supported for join with join filters.
    if (join->JoinKeys.empty() || !join->JoinFilters.empty()) {
        return input;
    }

    // We transform left side into special form: tuple(left row, key to lookup).
    if (!join->GetLeftInput()->IsSingleConsumer()) {
        return input;
    }

    // We want to find Read or Read -> Filter for the right side.
    auto rightInput = join->GetRightInput();
    TIntrusivePtr<TOpFilter> rightFilter;
    if (rightInput->Kind == EOperator::Filter) {
        if (!rightInput->IsSingleConsumer()) {
            return input;
        }
        rightFilter = CastOperator<TOpFilter>(rightInput);
        rightInput = rightFilter->GetInput();
    }
    if (rightInput->Kind != EOperator::Source || !rightInput->IsSingleConsumer()) {
        return input;
    }
    auto read = CastOperator<TOpRead>(rightInput);

    // Only supports row storage tables.
    if (read->GetTableStorageType() != NYql::EStorageType::RowStorage) {
        return input;
    }

    bool filterSupported = true;
    const auto fetchedRowFilter = BuildFetchedRowFilter(*read, rightFilter, filterSupported);
    if (!filterSupported) {
        return input;
    }

    const auto table = TKqpTable(read->GetTable());
    if (table.PathId().Value().empty()) {
        return input;
    }

    const auto& tableMeta = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, table.Path().Value()).Metadata;
    Y_ENSURE(tableMeta);
    if (!table.SysView().Value().empty() || tableMeta->Kind == EKikimrTableKind::SysView) {
        // Can't lookup in system views: a read of one is not a datashard read even though it is
        // described as a row storage read.
        return input;
    }

    if (tableMeta->KeyColumnNames.empty()) {
        return input;
    }

    size_t pointPrefixLen = 0;
    if (read->RangeInfo && IsUsablePointPrefix(*read->RangeInfo, tableMeta->KeyColumnNames, joinKind, ctx.KqpCtx.Config->GetIdxLookupJoinPointsLimit())) {
        pointPrefixLen = read->RangeInfo->PointColumns.size();
    }

    auto keys = MatchKeyPrefix(*join, *read, tableMeta->KeyColumnNames, pointPrefixLen);
    if (!keys && pointPrefixLen != 0) {
        pointPrefixLen = 0;
        keys = MatchKeyPrefix(*join, *read, tableMeta->KeyColumnNames, 0);
    }

    // Different types for keys are not supported.
    if (!keys || !KeyTypesMatch(*join->GetLeftInput(), *read, *keys)) {
        return input;
    }

    TVector<TInfoUnit> lookupKeys;
    TVector<TString> lookupKeyColumns;
    lookupKeys.reserve(keys->LookupKeys.size());
    lookupKeyColumns.reserve(keys->LookupKeys.size());
    for (const auto& key : keys->LookupKeys) {
        lookupKeys.push_back(key.LeftIU);
        lookupKeyColumns.push_back(key.Column);
    }

    std::optional<TOpTableLookup::TLookupKeyPrefix> prefix;
    if (pointPrefixLen != 0) {
        const auto& ranges = *read->RangeInfo;
        TOpTableLookup::TLookupKeyPrefix keyPrefix;
        keyPrefix.Points = ranges.Points;
        keyPrefix.PointsItemType = ranges.PointsItemType;
        keyPrefix.Columns = ranges.PointColumns;
        for (const auto& key : keys->PrefixKeys) {
            keyPrefix.Equalities.emplace_back(key.Column, key.LeftIU);
        }
        prefix = std::move(keyPrefix);
    }

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Rewriting a " << joinKind << " join into an index lookup join of "
                                 << table.Path().StringValue();

    auto lookup = MakeIntrusive<TOpTableLookup>(join->GetLeftInput(), join->Pos, read->GetTable(), read->Columns, read->OutputIUs,
                                                lookupKeys, lookupKeyColumns, joinKind, fetchedRowFilter, prefix);
    return MakeIntrusive<TOpIndexLookupJoin>(lookup, join->Pos, joinKind, join->JoinKeys);
}

} // namespace NKqp
} // namespace NKikimr
