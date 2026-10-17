#include <ydb/core/kqp/opt/rbo/rules/kqp_rules_include.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_lookup_join.h>

namespace NKikimr {
namespace NKqp {

namespace {

using namespace NYql;
using namespace NYql::NNodes;

std::optional<TExpression> BuildFetchedRowFilter(const TOpRead& read, TOpFilter* filter, bool& supported) {
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
        const auto filters = filter->GetFilterExpression().SplitConjunct();
        conjuncts.insert(conjuncts.end(), filters.begin(), filters.end());
    }

    if (conjuncts.empty()) {
        return std::nullopt;
    }

    // The filter is evaluated on a fetched row, so it can only refer to the fetched columns.
    const auto& readOutputs = read.GetColumns();
    for (const auto& conjunct : conjuncts) {
        if (!conjunct.GetInputIUs(/*includeSubplanVars=*/true, /*includeCorrelatedDeps=*/true).IsSubsetOf(readOutputs)) {
            supported = false;
            return std::nullopt;
        }
    }

    return MakeConjunction(conjuncts);
}

const TTypeAnnotationNode* StripOptional(const TTypeAnnotationNode* type) {
    return type && type->GetKind() == ETypeAnnotationKind::Optional ? type->Cast<TOptionalExprType>()->GetItemType() : type;
}

struct TLookupKey {
    TInfoUnitId LeftIU;
    TInfoUnitId RightIU;
    TString Column;
};

struct TKeyMatch {
    // Represents a constant prefix keys.
    TVector<TLookupKey> PrefixKeys;
    // Represents a lookup keys.
    TVector<TLookupKey> LookupKeys;
    // Represents a join keys which are not present in the right side index.
    TVector<TLookupKey> ResidualKeys;
};

std::optional<TKeyMatch> MatchKeyPrefix(const TOpJoin& join, const TOpRead& read, const TVector<TString>& keyColumnNames,
                                        size_t pointPrefixLen, const TInfoUnitRegistry& registry) {
    Y_ENSURE(pointPrefixLen < keyColumnNames.size());

    THashMap<TString, TLookupKey> keyByColumn;
    for (const auto& [leftIU, rightIU, equalNulls] : join.JoinKeys.Items()) {
        Y_ENSURE(read.GetColumns().Contains(rightIU), "Cannot find a join key in input columns.");
        const auto column = registry.Get(rightIU).GetColumnName();
        if (!keyByColumn.emplace(column, TLookupKey{leftIU, rightIU, column}).second) {
            return std::nullopt;
        }
    }

    TKeyMatch match;
    THashSet<TString> takenKeys;
    const auto end = keyByColumn.end();
    for (size_t i = 0; i < keyColumnNames.size(); ++i) {
        const auto it = keyByColumn.find(keyColumnNames[i]);
        if (i < pointPrefixLen) {
            if (it != end) {
                const auto key = it->second;
                match.PrefixKeys.push_back(key);
                takenKeys.insert(key.Column);
            }
            continue;
        }

        if (it == end) {
            break;
        }

        const auto key = it->second;
        match.LookupKeys.push_back(key);
        takenKeys.insert(key.Column);
    }

    for (const auto& [column, key] : keyByColumn) {
        if (!takenKeys.contains(column)) {
            match.ResidualKeys.push_back(key);
        }
    }

    if (match.LookupKeys.empty()) {
        return std::nullopt;
    }

    return match;
}

bool KeyTypesMatch(const IOperator& leftInput, const IOperator& rightInput, const TVector<TLookupKey>& keys, TExprContext& ctx) {
    for (const auto& key : keys) {
        const auto* leftType = StripOptional(leftInput.GetIUType(key.LeftIU, ctx));
        const auto* rightType = StripOptional(rightInput.GetIUType(key.RightIU, ctx));
        // TODO: Add support key with different types.
        if (!leftType || !rightType || leftType != rightType) {
            return false;
        }
    }
    return true;
}

bool KeyTypesMatch(const IOperator& leftInput, const IOperator& rightInput, const TKeyMatch& keys, TExprContext& ctx) {
    return KeyTypesMatch(leftInput, rightInput, keys.LookupKeys, ctx) && KeyTypesMatch(leftInput, rightInput, keys.PrefixKeys, ctx)
        && KeyTypesMatch(leftInput, rightInput, keys.ResidualKeys, ctx);
}

} // anonymous namespace

bool TRewriteJoinToIndexLookupJoinRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Join;
}

TIntrusivePtr<IOperator> TRewriteJoinToIndexLookupJoinRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& ctx,
                                                                               TPlanProps& props) {
    if (!ctx.KqpCtx.Config->GetEnableKqpDataQueryStreamIdxLookupJoin()) {
        return input;
    }
    if (!ctx.KqpCtx.IsDataQuery() && !ctx.KqpCtx.IsGenericQuery()) {
        return input;
    }

    auto join = CastOperator<TOpJoin>(input);

    if (join->Props.JoinAlgo.has_value() && *join->Props.JoinAlgo != EJoinAlgoType::LookupJoin){
        return input;
    }

    const auto joinKind = GetValidJoinKind(join->JoinKind);
    if (joinKind != "Inner" && joinKind != "Left" && joinKind != "LeftSemi" && joinKind != "LeftOnly") {
        return input;
    }

    if (HasEqualNullsKey(join->JoinKeys)) {
        return input;
    }

    // Not supported for join with join filters.
    if (join->JoinKeys.Items().empty() || !join->JoinFilters.empty()) {
        return input;
    }

    // We transform left side into special form: tuple(left row, key to lookup).
    if (join->GetLeftInput()->Kind == EOperator::Replicate) {
        return input;
    }

    // Read, Filter -> Read, or a read already redirected to a non-covering index.
    const auto rightSide = MatchLookupJoinRightSide(join->GetRightInput());
    if (!rightSide) {
        return input;
    }
    auto* read = rightSide->Read.Get();
    auto* rightFilter = rightSide->Filter.Get();
    TIntrusivePtr<TOpRead> rewrittenRead;

    // Only supports row storage tables.
    if (read->GetTableStorageType() != NYql::EStorageType::RowStorage) {
        return input;
    }

    // Built from the original read, so a predicate pushed into it is re-applied to the fetched rows
    // even when the lookup probes a different table.
    bool filterSupported = true;
    const auto fetchedRowFilter = BuildFetchedRowFilter(*read, rightFilter, filterSupported);
    if (!filterSupported) {
        return input;
    }

    const auto joinKeyColumns = GetLookupJoinKeyColumns(*read, join->JoinKeys.Right(), props.InfoUnitRegistry);
    if (!joinKeyColumns) {
        return input;
    }

    TVector<TString> readColumns;
    for (const auto column : read->GetColumns()) {
        readColumns.push_back(props.InfoUnitRegistry.Get(column).GetColumnName());
    }

    const auto target = ChooseLookupJoinTarget(*read, readColumns, *joinKeyColumns, joinKind == "Inner", ctx.KqpCtx);
    if (!target) {
        return input;
    }
    const auto& tableMeta = target->Metadata;
    if (TKqpTable(read->GetTable()).Path().Value() != tableMeta->Name) {
        auto rightOriginalType = read->Type;
        rewrittenRead = MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), read->GetTableStorageType(),
                                               BuildTableCallable(*tableMeta, read->Pos, ctx.ExprCtx), nullptr, read->Limit,
                                               std::nullopt, std::nullopt, ESortDir::None, read->Props, read->Pos);
        read = rewrittenRead.get();
        read->Type = rightOriginalType;
    }

    const auto table = TKqpTable(read->GetTable());
    const TOpRead::TPointPrefix* pointPrefix = target->PointPrefix;
    size_t pointPrefixLen = pointPrefix ? pointPrefix->Columns.size() : 0;

    auto keys = MatchKeyPrefix(*join, *read, tableMeta->KeyColumnNames, pointPrefixLen, props.InfoUnitRegistry);
    if (!keys && pointPrefixLen != 0) {
        pointPrefixLen = 0;
        keys = MatchKeyPrefix(*join, *read, tableMeta->KeyColumnNames, 0, props.InfoUnitRegistry);
    }

    if (!keys) {
        return input;
    }

    // Different types for keys are not supported.
    if (!KeyTypesMatch(*join->GetLeftInput(), *read, *keys, ctx.ExprCtx)) {
        // This check is missing in CBO, so we need to change join implementation in this case
        join->Props.JoinAlgo = EJoinAlgoType::MapJoin;
        return input;
    }

    TOpTableLookup::TLookupKeys lookupKeys;
    for (const auto& key : keys->LookupKeys) {
        lookupKeys.Append(key.LeftIU, key.Column);
    }

    std::optional<TOpTableLookup::TLookupKeyPrefix> prefix;
    if (pointPrefixLen != 0) {
        TOpTableLookup::TLookupKeyPrefix keyPrefix;
        keyPrefix.Points = pointPrefix->Points;
        keyPrefix.PointsItemType = pointPrefix->PointsItemType;
        keyPrefix.Columns = pointPrefix->Columns;
        for (const auto& key : keys->PrefixKeys) {
            keyPrefix.Equalities.Append(key.LeftIU, key.Column);
        }
        prefix = std::move(keyPrefix);
    }

    TPairedIUs residualJoinKeys;
    for (const auto& key : keys->ResidualKeys) {
        residualJoinKeys.Add(key.LeftIU, key.RightIU);
    }

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Rewriting a " << joinKind << " join into an index lookup join of "
                                 << table.Path().StringValue();

    if (target->MainTable) {
        // The index is not covering: the index lookup finds primary keys of the matching rows, and the main table
        // lookup fetches the rows by them and applies the whole read predicate to them.
        Y_ENSURE(joinKind == "Inner", "A lookup join by a non-covering index is supported for inner joins only");
        const auto& mainKeyColumns = target->MainTable->KeyColumnNames;
        const auto& mainColumns = target->MainTable->Columns;
        const bool mainKeyNotNull = std::all_of(mainKeyColumns.begin(), mainKeyColumns.end(), [&](const TString& column) {
            const auto it = mainColumns.find(column);
            return it != mainColumns.end() && it->second.NotNull;
        });

        // The index lookup produces the main table primary key; each column gets a fresh definition whose
        // storage name is the primary key column, because that is what the lookup returns.
        TUnorderedIUs indexOutputs;
        TOpTableLookup::TLookupKeys mainLookupKeys;
        for (const auto& column : mainKeyColumns) {
            const auto id = props.InfoUnitRegistry.Add(TInfoUnit(read->Alias, column));
            indexOutputs.Add(id);
            mainLookupKeys.Append(id, column);
        }

        auto indexLookup = MakeIntrusive<TOpTableLookup>(join->GetLeftInput(), join->Pos, read->GetTable(), std::move(indexOutputs),
                                                         std::move(lookupKeys), joinKind, std::nullopt, prefix);

        const auto mainTableCallable = BuildTableCallable(*target->MainTable, read->Pos, ctx.ExprCtx);
        if (mainKeyNotNull) {
            // The index lookup feeds the main table lookup directly, and a single lookup join consumes the result.
            // A row without a match in the index comes with a missing key, which is a key of nulls for the lookup:
            // the primary key has no nulls, so such a key is not looked up.
            auto mainLookup = MakeIntrusive<TOpTableLookup>(std::move(indexLookup), join->Pos, mainTableCallable,
                                                            read->GetColumns(), std::move(mainLookupKeys), joinKind,
                                                            fetchedRowFilter, std::nullopt, std::move(residualJoinKeys));
            mainLookup->KeysFromInputLookup = true;
            return MakeIntrusive<TOpIndexLookupJoin>(std::move(mainLookup), join->Pos, joinKind, join->JoinKeys);
        }

        // The primary key can have nulls, which the main table lookup has to allow, so a key of a row without a
        // match in the index cannot be told apart. A lookup join drops such rows before the main table lookup.
        // This join only drops the rows which found nothing in the index; the index lookup already
        // enforced the join condition. It has no keys of its own: the index outputs the primary key,
        // not the columns the join keys name, and a key here would be demanded of the index lookup.
        auto indexLookupJoin = MakeIntrusive<TOpIndexLookupJoin>(std::move(indexLookup), join->Pos, joinKind, TJoinIUs{});

        auto mainLookup = MakeIntrusive<TOpTableLookup>(std::move(indexLookupJoin), join->Pos, mainTableCallable,
                                                        read->GetColumns(), std::move(mainLookupKeys), joinKind,
                                                        fetchedRowFilter, std::nullopt, std::move(residualJoinKeys));
        mainLookup->AllowNullKeys = true;
        return MakeIntrusive<TOpIndexLookupJoin>(std::move(mainLookup), join->Pos, joinKind, join->JoinKeys);
    }

    auto lookup = MakeIntrusive<TOpTableLookup>(join->GetLeftInput(), join->Pos, read->GetTable(), read->GetColumns(),
                                                std::move(lookupKeys), joinKind, fetchedRowFilter, prefix, std::move(residualJoinKeys));
    return MakeIntrusive<TOpIndexLookupJoin>(std::move(lookup), join->Pos, joinKind, join->JoinKeys);
}

} // namespace NKqp
} // namespace NKikimr
