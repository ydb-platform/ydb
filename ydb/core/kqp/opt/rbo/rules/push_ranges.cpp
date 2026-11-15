#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/opt/rbo/kqp_native_range_bounds.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>


#include <utility>
#include <limits>
#include <set>

namespace NKikimr::NKqp {
namespace {

bool IsSelectableIndex(const TIndexDescription& index) {
    return index.Type != TIndexDescription::EType::GlobalAsync
        && index.Type != TIndexDescription::EType::GlobalJson
        && index.Type != TIndexDescription::EType::GlobalJsonCompact
        && index.Type != TIndexDescription::EType::LocalMinMax
        && index.Type != TIndexDescription::EType::LocalBloomFilter
        && index.Type != TIndexDescription::EType::LocalBloomNgramFilter
        && index.State == TIndexDescription::EIndexState::Ready;
}

bool IsSuitableToExtractAndPushRanges(IOperator* input, const NYql::EStorageType applicableTableType) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto maybeRead = filter->GetInput().Get();
    if (maybeRead->Kind != EOperator::Source) {
        return false;
    }

    const auto read = CastOperator<TOpRead>(maybeRead);
    const auto tableType = read->GetTableStorageType();
    return !(read->RangeInfo && read->RangeInfo->NativeRanges) && (tableType == applicableTableType);
}

struct TIndexScore {
    bool SortMatchesAndNoResidual = false;
    bool PointCoversKey = false;
    size_t PointPrefixLen = 0;
    bool UsedCoversKey = false;
    size_t UsedPrefixLen = 0;
    bool SortMatches = false;

    std::tuple<bool, bool, size_t, bool, size_t> AsTuple() const {
        return std::make_tuple(SortMatchesAndNoResidual, PointCoversKey, PointPrefixLen, UsedCoversKey, UsedPrefixLen);
    }

    bool operator<(const TIndexScore& other) const { return AsTuple() < other.AsTuple(); }
};

TIndexScore ScoreKeyOrder(size_t pointPrefix, size_t usedPrefix, bool singleRange, bool noResidual, size_t keyLen,
                          const TVector<TString>& sortColumns, const TVector<TString>& keyColumns, bool covering) {
    TIndexScore score;
    score.PointCoversKey = keyLen != 0 && pointPrefix == keyLen;
    score.PointPrefixLen = score.PointCoversKey ? 0 : pointPrefix;
    score.UsedCoversKey = keyLen != 0 && usedPrefix == keyLen;
    score.UsedPrefixLen = score.UsedCoversKey ? 0 : usedPrefix;

    if (covering && !sortColumns.empty()) {
        const size_t pointPrefixLen = singleRange ? std::min(pointPrefix, keyColumns.size()) : 0;
        score.SortMatches = SortMatchesKeyOrder(sortColumns, keyColumns, pointPrefixLen);
        score.SortMatchesAndNoResidual = score.SortMatches && noResidual;
    }

    return score;
}

bool IsBetterCandidate(const TIndexScore& score, bool covering, const TString& name, const TIndexScore& bestScore, bool bestCovering,
                       const TString& bestName) {
    if (bestScore < score) {
        return true;
    }
    // Index never wins a tie against the main table
    if (score < bestScore || bestName.empty()) {
        return false;
    }
    // Covering index beats non-covering one
    if (covering != bestCovering) {
        return covering;
    }
    if (score.SortMatches != bestScore.SortMatches) {
        return score.SortMatches;
    }
    // Lexicographically smallest index name wins
    return name < bestName;
}

TVector<TString> FindConsumingTopSortColumns(const IOperator* op, TExprContext& ctx, const TInfoUnitRegistry* registry = nullptr) {
    TSubstitutions copies;
    const IOperator* current = op;

    while (current && current->Parents.size() == 1) {
        IOperator* parent = current->Parents.front().first;
        if (!parent) {
            return {};
        }

        if (parent->Kind == EOperator::Map) {
            const auto& elements = CastOperator<TOpMap>(*parent).GetMapElements();
            for (const auto& [output, element] : elements.Items()) {
                if (element.IsColumnAccess()) {
                    copies.Add(output, Substitute(element.GetColumnAccess(), copies));
                }
            }
            current = parent;
            continue;
        }

        if (parent->Kind != EOperator::Sort) {
            return {};
        }

        const auto sort = CastOperator<TOpSort>(parent);
        if (!sort->LimitCond.has_value()) {
            return {};
        }

        TVector<TString> sortColumns;
        const auto& sortElements = sort->GetSortElements().Items();
        sortColumns.reserve(sortElements.size());

        const bool ascending = sortElements.empty() ? true : sortElements.front().second.Ascending;
        for (const auto& [id, order] : sortElements) {
            if (order.Ascending != ascending || order.NullsFirst != ascending) {
                return {};
            }
            const auto source = Substitute(id, copies);
            sortColumns.push_back(registry ? registry->Get(source).GetColumnName() : TString(ctx.GetIndexAsString(source)));
        }
        return sortColumns;
    }

    return {};
}

bool IsCovering(const TOpRead& read, const TKikimrTableMetadata& indexMeta, const TInfoUnitRegistry& registry) {
    for (const auto id : read.GetColumns()) {
        if (!indexMeta.Columns.contains(registry.Get(id).GetColumnName())) {
            return false;
        }
    }
    return true;
}

bool IsUselessIndex(const TVector<TString>& indexKeyColumns, const TVector<TString>& mainKeyColumns) {
    const size_t common = std::min(indexKeyColumns.size(), mainKeyColumns.size());
    for (size_t i = 0; i < common; ++i) {
        if (indexKeyColumns[i] != mainKeyColumns[i]) {
            return false;
        }
    }
    return true;
}

const TKikimrTableDescription* FindTable(const NOpt::TKqpOptimizeContext& kqpCtx, const TString& path) {
    const auto& tables = kqpCtx.Tables->GetTables();
    const auto it = tables.find(std::make_pair(kqpCtx.Cluster, path));
    return it != tables.end() ? &it->second : nullptr;
}

NYql::EStorageType GetStorageType(const TKikimrTableMetadata& meta) {
    switch (meta.Kind) {
        case EKikimrTableKind::Datashard:
            return NYql::EStorageType::RowStorage;
        case EKikimrTableKind::Olap:
            return NYql::EStorageType::ColumnStorage;
        default:
            return NYql::EStorageType::NA;
    }
}

TVector<TString> FilterPhysicalColumns(const TOpFilter& filter, const TOpRead& read, TPlanProps& props) {
    TVector<TString> result;
    THashSet<TString> seen;
    for (const auto& iu : filter.GetFilterIUs(props)) {
        Y_ENSURE(read.GetColumns().Contains(iu));
        const auto physical = props.InfoUnitRegistry.Get(iu).GetColumnName();
        if (seen.insert(physical).second) {
            result.push_back(physical);
        }
    }
    return result;
}

TVector<TString> BuildIndexReadColumns(const TVector<TString>& pkColumns, const TVector<TString>& filterPhysical) {
    TVector<TString> result;
    THashSet<TString> seen;
    for (const auto& c : pkColumns) {
        if (seen.insert(c).second) {
            result.push_back(c);
        }
    }
    for (const auto& c : filterPhysical) {
        if (seen.insert(c).second) {
            result.push_back(c);
        }
    }
    return result;
}

// Distribute Boolean branches only within a bounded amount of work. If the
// expansion is too large, the caller can still extract ordinary top-level
// conjuncts and retain the original Boolean predicate in compute.
std::vector<std::vector<NFrontend::TExprId>> NativeRangeBranches(const NFrontend::TExpressionArena& arena,
    NFrontend::TExprId root, ui64 rangeLimit) {
    using namespace NFrontend;
    constexpr size_t WorkLimit = 100000;
    struct TBranch {
        std::vector<TExprId> Pending, Terms;
    };
    std::vector<TBranch> pending{{{root}, {}}};
    std::vector<std::vector<TExprId>> result;
    size_t work = 0;
    while (!pending.empty()) {
        auto branch = std::move(pending.back());
        pending.pop_back();
        while (!branch.Pending.empty()) {
            if (++work > WorkLimit) return {};
            const auto id = StripNativeOptionalLift(arena, branch.Pending.back());
            branch.Pending.pop_back();
            const auto& node = arena[id];
            if (node.Op == EOpcode::And) {
                branch.Pending.insert(branch.Pending.end(), node.Arguments.rbegin(), node.Arguments.rend());
            } else if (node.Op == EOpcode::Or) {
                Y_ENSURE(node.Arguments.size() == 2);
                if (rangeLimit && result.size() + pending.size() + 2 > rangeLimit) return {};
                work += branch.Pending.size() + branch.Terms.size();
                if (work > WorkLimit) return {};
                auto other = branch;
                other.Pending.push_back(node.Arguments[1]);
                pending.push_back(std::move(other));
                branch.Pending.push_back(node.Arguments[0]);
            } else {
                branch.Terms.push_back(id);
            }
        }
        THashSet<TExprId> seen;
        std::erase_if(branch.Terms, [&](TExprId term) { return !seen.insert(term).second; });
        result.push_back(std::move(branch.Terms));
    }
    return result;
}

TIntrusivePtr<IOperator> PushNativeRanges(const TIntrusivePtr<IOperator>& input, const TKikimrTableMetadata& table,
    TRBOContext& ctx, TPlanProps& props) {
    using namespace NFrontend;
    using O = EOpcode;
    using K = ETypeKind;
    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput());
    const auto& predicate = filter->GetFilterExpression();
    // Bounds stay native for both storage kinds. Only physical conversion
    // chooses the row-source or column-store runtime range representation.
    if (read->Limit || read->OlapProgram ||
        !predicate.GetRawInputIUs().IsSubsetOf(read->GetColumns())) return input;
    const auto arena = predicate.GetNativeArena();
    THashMap<TExprId, bool> rowDependencies;
    bool needsRangeView = false, hasChecked = false;
    const auto usesRow = [&](TExprId root) {
        TVector<std::pair<TExprId, bool>> pending{{root, false}};
        while (!pending.empty()) {
            const auto [id, ready] = pending.back(); pending.pop_back();
            if (rowDependencies.contains(id)) continue;
            const auto& node = (*arena)[id];
            if (!ready) {
                hasChecked |= node.Op == O::Ensure || node.Op == O::Unwrap;
                needsRangeView |= node.Op == O::InResult || node.Op == O::Coalesce ||
                    node.Op == O::Not || node.Op == O::NotEqual ||
                    (node.Op == O::Literal && node.Type.Kind == K::Bool && std::holds_alternative<std::monostate>(node.Value));
                pending.emplace_back(id, true);
                for (const auto arg : node.Arguments) pending.emplace_back(arg, false);
            } else {
                bool dependent = node.Op == O::Column;
                for (const auto arg : node.Arguments) dependent |= rowDependencies.at(arg);
                rowDependencies.emplace(id, dependent);
            }
        }
        return rowDependencies.at(root);
    };
    const bool rowDependent = usesRow(predicate.GetNativeRoot());
    // A closed predicate is evaluated as the old extractor's conditional full
    // range. Row-dependent checks must retain their original lazy evaluation.
    if (hasChecked && rowDependent) return input;
    std::optional<TExpression> rangeView;
    if (needsRangeView) {
        const auto root = PrepareNativeRangePredicate(*arena, predicate.GetNativeRoot());
        if (root != predicate.GetNativeRoot()) rangeView = TExpression::FromNative(arena, root, &props, predicate.Pos());
    }
    const auto& rangePredicate = rangeView ? *rangeView : predicate;
    struct TCondition {
        size_t Term;
        TExprId Value;
        std::optional<TExprId> Validity;
        TType KeyType;
        bool Inclusive;
    };
    struct TNotNull {
        size_t Term;
        TType KeyType;
    };
    struct TConditions {
        std::optional<TCondition> Equal, Lower, Upper;
        std::optional<TNotNull> NotNull;
    };
    std::optional<TExprId> closedCondition;
    const auto& root = (*arena)[rangePredicate.GetNativeRoot()];
    if (!usesRow(rangePredicate.GetNativeRoot()) && root.Repeatable && root.Type.Kind == K::Bool && root.Type.OptionalDepth <= 1) {
        closedCondition = rangePredicate.GetNativeRoot();
        if (root.Type.OptionalDepth) {
            const TExprId alternatives[]{*closedCondition, arena->Literal({K::Bool}, false)};
            closedCondition = arena->Coalesce(alternatives);
        }
    }
    auto branches = closedCondition ? std::vector<std::vector<TExprId>>{{rangePredicate.GetNativeRoot()}}
        : NativeRangeBranches(*arena, rangePredicate.GetNativeRoot(), ctx.KqpCtx.Config->GetExtractPredicateRangesLimit());
    if (branches.empty()) {
        branches.emplace_back();
        for (const auto& term : rangePredicate.SplitConjunct()) branches.back().push_back(term.GetNativeRoot());
    }
    std::vector<THashMap<TString, TConditions>> branchKeys(branches.size());
    std::vector<std::vector<std::pair<size_t, TExprId>>> branchConditions(branches.size());
    THashMap<TExprId, TExprId> closedTerms;
    for (size_t branch = 0; branch < branches.size(); ++branch) for (size_t i = 0; i < branches[branch].size(); ++i) {
        const auto id = StripNativeOptionalLift(*arena, branches[branch][i]);
        auto op = (*arena)[id].Op;
        const auto type = (*arena)[id].Type;
        if (type.Kind == K::Bool && type.OptionalDepth <= 1 && (*arena)[id].Repeatable && !usesRow(id)) {
            auto [entry, added] = closedTerms.emplace(id, id);
            if (added && type.OptionalDepth) {
                const TExprId alternatives[]{id, arena->Literal({K::Bool}, false)};
                entry->second = arena->Coalesce(alternatives);
            }
            branchConditions[branch].emplace_back(i, entry->second);
            continue;
        }
        const bool nullPoint = op == O::IsNull, notNull = op == O::IsNotNull;
        if (!nullPoint && !notNull && op != O::Equal && op != O::Less && op != O::LessEqual && op != O::Greater && op != O::GreaterEqual) continue;
        auto column = notNull ? StripNativeOptionalLift(*arena, (*arena)[id].Arguments[0])
            : StripNativeRangeKeyPromotion(*arena, (*arena)[id].Arguments[0], op);
        auto value = nullPoint || notNull ? column : (*arena)[id].Arguments[1];
        if ((*arena)[column].Op != O::Column) {
            if (nullPoint || notNull) continue;
            std::swap(column, value);
            column = StripNativeRangeKeyPromotion(*arena, column, op);
            switch (op) {
                case O::Less: op = O::Greater; break;
                case O::LessEqual: op = O::GreaterEqual; break;
                case O::Greater: op = O::Less; break;
                case O::GreaterEqual: op = O::LessEqual; break;
                default: break;
            }
        }
        if ((*arena)[column].Op != O::Column) continue;
        const auto keyType = (*arena)[column].Type;
        // Key promotions, scalar bound conversion and validity share the
        // native adapter; the rule selects keys and indexes as before.
        if (!keyType.IsIntegral() && !keyType.IsString() && keyType.Kind != K::Bool && keyType.Kind != K::Date &&
            keyType.Kind != K::Datetime && keyType.Kind != K::Timestamp && keyType.Kind != K::Interval &&
            keyType.Kind != K::Date32 && keyType.Kind != K::Datetime64 && keyType.Kind != K::Timestamp64 &&
            keyType.Kind != K::Interval64 && keyType.Kind != K::Uuid && !HasUnorderedNativeKeys(keyType)) continue;
        auto& conditions = branchKeys[branch][props.InfoUnitRegistry.Get((*arena)[column].ColumnId).GetColumnName()];
        if (notNull) {
            if (!conditions.NotNull) conditions.NotNull = TNotNull{i, keyType};
            continue;
        }
        // IS NULL is a finite NULL point, unlike comparison with NULL (UNKNOWN).
        const auto bound = nullPoint
            ? (keyType.OptionalDepth ? std::optional{TNativeRangeBound{arena->Literal(keyType, {}), {}, true}} : std::nullopt)
            : BuildNativeRangeBound(TExpression::FromNative(arena, value, &props), keyType, op);
        if (!bound) continue;
        auto& slot = nullPoint || op == O::Equal ? conditions.Equal : (op == O::Greater || op == O::GreaterEqual ? conditions.Lower : conditions.Upper);
        if (!slot) slot = TCondition{i, bound->Value, bound->Validity, keyType, bound->Inclusive};
    }
    // Inspect the predicate once, then build each candidate from its physical
    // key order. Choosing an index does not require another typing/lowering pass.
    struct TCandidate {
        TOpRead::TRangeInfo Info;
        THashSet<size_t> Consumed;
        TVector<size_t> SourceBranches;
        bool AllConsumed = true;
    };
    const auto addCondition = [&](std::optional<TExpression>& condition, std::optional<TExprId> valid) {
        if (valid) {
            const auto root = condition ? arena->Binary(O::And, condition->GetNativeRoot(), *valid) : *valid;
            condition = TExpression::FromNative(arena, root, &props, read->Pos);
        }
    };
    const auto build = [&](const TKikimrTableMetadata& metadata) {
        TCandidate result;
        auto& info = result.Info;
        info.KeyColumns = metadata.KeyColumnNames;
        info.MainTable = table.Name;
        info.ExpectedMaxRanges = branches.size();
        info.UsedPrefixLen = info.PointPrefixLen = metadata.KeyColumnNames.size();
        info.NativeRanges.emplace();
        if (closedCondition && !metadata.KeyColumnNames.empty()) {
            // The previous extractor assigns the first key to a conditional
            // full range. Retain that admission/scoring contract even though
            // the interval has no finite bound and no point prefix.
            info.UsedPrefixLen = 1;
            info.PointPrefixLen = 0;
            auto& range = info.NativeRanges->emplace_back();
            range.Condition = TExpression::FromNative(arena, *closedCondition, &props, read->Pos);
            return result;
        }
        std::set<std::vector<TExprId>> rangeTerms;
        for (size_t branch = 0; branch < branches.size(); ++branch) {
            auto& range = info.NativeRanges->emplace_back();
            THashSet<size_t> consumed;
            size_t usedPrefix = 0, pointPrefix = 0;
            for (const auto& [term, condition] : branchConditions[branch]) {
                addCondition(range.Condition, condition);
                consumed.insert(term);
            }
            const auto append = [&](TOpRead::TKeyBound& bound, const TCondition& condition) {
                bound.Values.push_back(TExpression::FromNative(arena, condition.Value, &props, read->Pos));
                bound.Inclusive = condition.Inclusive;
                if (consumed.insert(condition.Term).second) addCondition(range.Condition, condition.Validity);
            };
            for (const auto& key : metadata.KeyColumnNames) {
                const auto found = branchKeys[branch].find(key);
                if (found == branchKeys[branch].end()) break;
                const auto& conditions = found->second;
                if (conditions.Equal) {
                    append(range.Lower, *conditions.Equal);
                    append(range.Upper, *conditions.Equal);
                    if (conditions.NotNull) {
                        consumed.insert(conditions.NotNull->Term);
                        const auto& value = (*arena)[conditions.Equal->Value];
                        if (value.Op == O::Literal && std::holds_alternative<std::monostate>(value.Value))
                            addCondition(range.Condition, arena->Literal({K::Bool}, false));
                    }
                    ++usedPrefix;
                    ++pointPrefix;
                    continue;
                }
                if (!conditions.Lower && !conditions.Upper && !conditions.NotNull) break;
                if (conditions.Lower) append(range.Lower, *conditions.Lower);
                if (conditions.Lower && !conditions.Upper && HasUnorderedNativeKeys(conditions.Lower->KeyType)) {
                    const auto type = conditions.Lower->KeyType.Required();
                    const TScalar nan = type.Kind == K::Decimal ? TScalar{TDecimal{TDecimal::NaN()}}
                        : TScalar{std::numeric_limits<double>::quiet_NaN()};
                    range.Upper.Values.push_back(TExpression::FromNative(arena, arena->Literal(type, nan), &props, read->Pos));
                    range.Upper.Inclusive = false;
                }
                if (conditions.Upper) {
                    append(range.Upper, *conditions.Upper);
                    // Exclude SQL NULL from an upper-only interval, including
                    // every suffix of a composite key.
                    if (!conditions.Lower && conditions.Upper->KeyType.OptionalDepth) {
                        const auto null = arena->Literal(conditions.Upper->KeyType, {});
                        range.Lower.Values.push_back(TExpression::FromNative(arena, null, &props, read->Pos));
                        range.Lower.Inclusive = false;
                    }
                }
                if (conditions.NotNull) {
                    consumed.insert(conditions.NotNull->Term);
                    if (!conditions.Lower && !conditions.Upper && conditions.NotNull->KeyType.OptionalDepth) {
                        const auto null = arena->Literal(conditions.NotNull->KeyType, {});
                        range.Lower.Values.push_back(TExpression::FromNative(arena, null, &props, read->Pos));
                        range.Lower.Inclusive = false;
                    }
                    // An IS NOT NULL interval includes unordered non-NULL keys
                    // (NaN); only actual comparisons install the NaN upper cut.
                }
                ++usedPrefix;
                break;
            }
            // A closed conjunct supplies a conditional full range on the first
            // key when this branch has no finite key bound, as in the old extractor.
            if (!usedPrefix && !branchConditions[branch].empty() && !metadata.KeyColumnNames.empty()) usedPrefix = 1;
            // Omitting an unconstrained OR branch would discard matching rows.
            info.UsedPrefixLen = std::min(info.UsedPrefixLen, usedPrefix);
            info.PointPrefixLen = std::min(info.PointPrefixLen, pointPrefix);
            if (!info.UsedPrefixLen) {
                result.AllConsumed = false;
                return result;
            }
            result.AllConsumed &= consumed.size() == branches[branch].size();
            // Distinct residual branches can select the same key interval.
            // Its consumed source terms identify the bounds and validity
            // conditions without formatting or re-evaluating scalar values.
            if (branches.size() > 1) {
                std::vector<TExprId> terms;
                terms.reserve(consumed.size());
                for (const auto term : consumed) terms.push_back(branches[branch][term]);
                std::sort(terms.begin(), terms.end());
                if (!rangeTerms.insert(std::move(terms)).second) {
                    info.NativeRanges->pop_back();
                    continue;
                }
            }
            result.SourceBranches.push_back(branch);
            if (branches.size() == 1) result.Consumed = std::move(consumed);
        }
        info.ExpectedMaxRanges = info.NativeRanges->size();
        return result;
    };
    const auto sortColumns = read->StorageType == NYql::EStorageType::RowStorage ?
        FindConsumingTopSortColumns(input.Get(), ctx.ExprCtx, &props.InfoUnitRegistry) : TVector<TString>{};
    const auto score = [&](const TCandidate& candidate, bool covering) {
        const auto& info = candidate.Info;
        return ScoreKeyOrder(info.PointPrefixLen, info.UsedPrefixLen, info.ExpectedMaxRanges == size_t{1}, candidate.AllConsumed,
            info.KeyColumns.size(), sortColumns, info.KeyColumns, covering);
    };
    const bool allPointPrefixes = ctx.KqpCtx.Config->GetEnableLookupJoinPointPrefixes();
    TVector<TOpRead::TPointPrefix> pointPrefixes;
    const auto addPointPrefix = [&](const TCandidate& candidate, const TKikimrTableMetadata& metadata) {
        const auto length = candidate.Info.PointPrefixLen;
        const auto limit = ctx.KqpCtx.Config->GetIdxLookupJoinPointsLimit();
        if (read->StorageType != NYql::EStorageType::RowStorage || !limit || !length || length >= metadata.KeyColumnNames.size()) return;
        TOpRead::TPointPrefix prefix;
        prefix.Table = metadata.Name;
        prefix.Columns.assign(metadata.KeyColumnNames.begin(), metadata.KeyColumnNames.begin() + length);
        auto& points = prefix.NativePoints.emplace();
        for (const auto& column : prefix.Columns) points.Types.push_back(branchKeys.front().at(column).Equal->KeyType);
        THashSet<TString> seen;
        for (size_t branch = 0; branch < candidate.Info.NativeRanges->size(); ++branch) {
            const auto& range = candidate.Info.NativeRanges->at(branch);
            TStringBuilder key;
            for (size_t i = 0; i < length; ++i) {
                const auto& value = range.Lower.Values.at(i);
                auto root = value.GetNativeRoot();
                while ((*arena)[root].Op == O::Convert || (*arena)[root].Op == O::Unwrap) {
                    const auto& conversion = (*arena)[root];
                    key << unsigned(conversion.Op) << conversion.Type.ToString() << ':';
                    root = conversion.Arguments[0];
                }
                const auto& node = (*arena)[root];
                const auto cell = node.Op == O::Parameter ? "p" + node.Name : node.Op == O::Literal
                    ? "v" + FormatValue({node.Type, node.Value}) : "e" + std::to_string(root);
                key << cell.size() << ':' << cell;
            }
            if (!seen.insert(key).second) continue;
            auto& point = points.Rows.emplace_back();
            point.Values.assign(range.Lower.Values.begin(), range.Lower.Values.begin() + length);
            for (const auto& column : prefix.Columns)
                addCondition(point.Condition, branchKeys[candidate.SourceBranches[branch]].at(column).Equal->Validity);
            if (points.Rows.size() > limit) return;
        }
        prefix.ExpectedMaxPoints = points.Rows.size();
        pointPrefixes.push_back(std::move(prefix));
    };
    auto chosen = build(table);
    if (allPointPrefixes) addPointPrefix(chosen, table);
    auto bestScore = score(chosen, true);
    TString bestName;
    bool covering = false;
    TIntrusivePtr<TKikimrTableMetadata> chosenIndex;
    TVector<TString> filterColumns;
    if (!ctx.KqpCtx.Config->IsAutoIndexSelectionDisabled() && !bestScore.PointCoversKey) {
        filterColumns = FilterPhysicalColumns(*filter, *read, props);
        for (const auto& index : table.Indexes) {
            if (!IsSelectableIndex(index)) continue;
            const auto metadata = table.GetIndexMetadata(index.Name).first;
            if (!metadata || IsUselessIndex(metadata->KeyColumnNames, table.KeyColumnNames) || !FindTable(ctx.KqpCtx, metadata->Name)) continue;
            auto candidate = build(*metadata);
            if (!candidate.Info.UsedPrefixLen) continue;
            if (allPointPrefixes) addPointPrefix(candidate, *metadata);
            const bool candidateCovering = IsCovering(*read, *metadata, props.InfoUnitRegistry);
            if (!candidateCovering && !std::all_of(filterColumns.begin(), filterColumns.end(),
                    [&](const auto& column) { return metadata->Columns.contains(column); })) continue;
            const auto candidateScore = score(candidate, candidateCovering);
            if (!IsBetterCandidate(candidateScore, candidateCovering, index.Name, bestScore, covering, bestName)) continue;
            chosen = std::move(candidate);
            chosenIndex = metadata;
            bestScore = candidateScore;
            bestName = index.Name;
            covering = candidateCovering;
        }
    }
    if (!chosen.Info.UsedPrefixLen) return input;
    if (!allPointPrefixes) addPointPrefix(chosen, chosenIndex ? *chosenIndex : table);
    chosen.Info.PointPrefixes = std::move(pointPrefixes);
    TVector<TExpression> remaining;
    if (!chosen.AllConsumed) {
        if (branches.size() == 1) {
            for (size_t i = 0; i < branches.front().size(); ++i) if (!chosen.Consumed.contains(i))
                remaining.push_back(TExpression::FromNative(arena, branches.front()[i], &props, predicate.Pos()));
        } else {
            // Branch-specific residuals cannot be conjoined outside the union.
            remaining.push_back(predicate);
        }
    }
    const auto original = read->OriginalPredicate ? *read->OriginalPredicate : predicate;
    if (chosenIndex && !covering) {
        auto sourcePrefixes = chosen.Info.PointPrefixes;
        TUnorderedIUs columns;
        THashMap<TString, TInfoUnitId> ids;
        for (const auto& column : BuildIndexReadColumns(table.KeyColumnNames, filterColumns)) {
            const auto id = props.InfoUnitRegistry.Add(TInfoUnit(read->Alias, column));
            columns.Add(id);
            ids.emplace(column, id);
        }
        TSubstitutions renames;
        for (const auto id : predicate.GetRawInputIUs()) renames.Add(id, ids.at(props.InfoUnitRegistry.Get(id).GetColumnName()));
        TIntrusivePtr<IOperator> indexRead = MakeIntrusive<TOpRead>(read->Alias, std::move(columns), GetStorageType(*chosenIndex),
            MakeRboTable(*chosenIndex), nullptr, std::nullopt, std::move(chosen.Info),
            predicate.ApplyRenames(renames), ESortDir::None, read->Props, read->Pos);
        if (!remaining.empty()) indexRead = MakeIntrusive<TOpFilter>(indexRead, filter->Pos, filter->Props,
            MakeConjunction(remaining).ApplyRenames(renames), true);
        TOpTableLookup::TLookupKeys lookupKeys;
        for (const auto& key : table.KeyColumnNames) lookupKeys.Append(ids.at(key), key);
        auto lookup = MakeIntrusive<TOpTableLookup>(indexRead, read->Pos, read->TableRef, read->GetColumns(), std::move(lookupKeys));
        if (allPointPrefixes && read->StorageType == NYql::EStorageType::RowStorage) {
            lookup->SourceRead = TOpTableLookup::TSourceRead{original, std::move(sourcePrefixes)};
        }
        return lookup;
    }
    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), chosenIndex ? GetStorageType(*chosenIndex) : read->StorageType,
        chosenIndex ? MakeRboTable(*chosenIndex) : read->TableRef,
        read->OlapProgram, read->Limit, std::move(chosen.Info), original,
        chosenIndex ? ESortDir::None : read->SortDir, read->Props, read->Pos);
    if (remaining.empty()) return newRead;
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, MakeConjunction(remaining), true);
}


} // namespace

bool TPushRangesRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->GetChildren().front()->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TPushRangesRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    auto& kqpCtx = rboCtx.KqpCtx;
    auto& ctx = rboCtx.ExprCtx;

    auto predicateExtractSetting = kqpCtx.Config->GetOptPredicateExtract();
    if (predicateExtractSetting == EOptionalFlag::Disabled) {
        return input;
    }

    if (!IsSuitableToExtractAndPushRanges(input.get(), ApplicableTableType)) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput().Get());
    const auto tablePath = read->GetTable().Path;

    // Check for table.
    const auto tableDesc = kqpCtx.Tables->EnsureTableExists(kqpCtx.Cluster, tablePath, read->Pos, ctx);
    if (!tableDesc || !tableDesc->Metadata) {
        return input;
    }

    const auto tableKind = tableDesc->Metadata->Kind;
    if (tableKind != EKikimrTableKind::Olap && tableKind != EKikimrTableKind::Datashard) {
        return input;
    }

    return PushNativeRanges(input, *tableDesc->Metadata, rboCtx, props);
}

} // namespace NKikimr::NKqp
