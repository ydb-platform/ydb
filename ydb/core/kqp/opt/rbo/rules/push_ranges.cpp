#include <ydb/core/kqp/opt/cbo/solver/kqp_opt_predicate_selectivity.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_statistics.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/opt/rbo/rules/kqp_rbo_index_selection.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>

#include <limits>

namespace NKikimr::NKqp {

namespace {

using namespace NYql::NNodes;
using namespace NKikimr;

bool IsValidForRange(const NYql::TExprNode::TPtr& node) {
    TExprBase expr(node);
    if (auto sqlin = expr.Maybe<TCoSqlIn>()) {
        auto collection = sqlin.Cast().Collection().Ptr();
        bool result = true;
        VisitExpr(collection, [&](const TExprNode::TPtr& node) {
            if (node->IsCallable() && (node->Content().StartsWith("Dq") || node->Content().StartsWith("Kql") || node->Content().StartsWith("Kqp"))) {
                result = false;
                return false;
            }
            return true;
        });
        return result;
    }
    return true;
}

TExprNode::TPtr TypeAnnotateLambda(TExprNode::TPtr lambda, const TStructExprType* structType, TRBOContext& ctx) {
    if (!UpdateLambdaAllArgumentsTypes(lambda, {structType}, ctx.ExprCtx)) {
        return nullptr;
    }

    ctx.TypeAnnTransformer.Rewind();
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = ctx.TypeAnnTransformer.Transform(lambda, lambda, ctx.ExprCtx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    return status == IGraphTransformer::TStatus::Ok ? lambda : nullptr;
}

// The extractor builds a key range for Exists, which requires an optional key.
TExprNode::TPtr FoldExistsOverNonOptional(TExprNode::TPtr lambda, const TStructExprType* structType, TRBOContext& ctx) {
    TOptimizeExprSettings settings(&ctx.TypeCtx);
    // So we try to fold Exists.
    const auto status = OptimizeExpr(lambda, lambda, [&](const TExprNode::TPtr& node, TExprContext& exprCtx) -> TExprNode::TPtr {
        return node->IsCallable("Exists") ? OptimizeExists(node, exprCtx, ctx.TypeCtx) : node;
    }, ctx.ExprCtx, settings);
    if (status == IGraphTransformer::TStatus::Error) {
        return nullptr;
    }

    return TypeAnnotateLambda(lambda, structType, ctx);
}

// The range extractor expects a non optional predicate typed over the read row type.
TExprNode::TPtr GetLambdaForRangeExtractor(TExprNode::TPtr node, const TTypeAnnotationNode* inputType, TRBOContext& rboCtx) {
    if (!inputType) {
        return node;
    }

    auto& ctx = rboCtx.ExprCtx;
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();

    auto lambda = TypeAnnotateLambda(ctx.DeepCopyLambda(*node), structType, rboCtx);
    if (lambda) {
        lambda = FoldExistsOverNonOptional(lambda, structType, rboCtx);
    }
    if (!lambda || !lambda->GetTypeAnn()->IsOptionalOrNull()) {
        return lambda;
    }

    // Wrap over coalesce.
    // clang-format off
    auto newBody = Build<TCoCoalesce>(ctx, node->Pos())
        .Predicate(TCoLambda(lambda).Body())
        .Value<TCoBool>()
            .Literal().Build("false")
        .Build()
    .Done();
    // clang-format on

    return TypeAnnotateLambda(ctx.NewLambda(node->Pos(), lambda->HeadPtr(), newBody.Ptr()), structType, rboCtx);
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
    return !read->GetRanges() && (tableType == applicableTableType);
}

TPredicateExtractorSettings PrepareExtractorSettings(TKqpOptimizeContext& kqpCtx) {
    TPredicateExtractorSettings settings;
    settings.MergeAdjacentPointRanges = true;
    settings.HaveNextValueCallable = true;
    settings.BuildLiteralRange = true;
    settings.IsValidForRange = IsValidForRange;

    if (kqpCtx.Config->GetExtractPredicateRangesLimit() != 0) {
        settings.MaxRanges = kqpCtx.Config->GetExtractPredicateRangesLimit();
    } else {
        settings.MaxRanges = Nothing();
    }

    if (kqpCtx.QueryCtx->RuntimeParameterSizeLimitSatisfied && kqpCtx.QueryCtx->RuntimeParameterSizeLimit > 0) {
        settings.ExternalParameterMaxSize = kqpCtx.QueryCtx->RuntimeParameterSizeLimit;
    }
    return settings;
}

// Returns a bound value of a literal range in the form the read source takes as is: a literal, a null or a parameter
// of the key column type. The extractor converts the value to the key column type: it retypes an integer literal
// itself and wraps a Utf8 literal for a String key in ToString, which is folded here because the literal text stays
// the same. Null if the value has to be computed.
TExprNode::TPtr GetLiteralBoundValue(const TExprNode::TPtr& value, const TTypeAnnotationNode& keyType, TExprContext& ctx) {
    const TTypeAnnotationNode* type = value->GetTypeAnn();
    if (!type) {
        return nullptr;
    }

    if (value->IsCallable("Just")) {
        return GetLiteralBoundValue(value->HeadPtr(), keyType, ctx);
    }

    if (!IsSameAnnotation(*RemoveOptionalType(type), *RemoveOptionalType(&keyType))) {
        return nullptr;
    }

    if (TCoDataCtor::Match(value.Get()) || TCoPgConst::Match(value.Get()) || TCoNothing::Match(value.Get())
        || TCoParameter::Match(value.Get())) {
        return value;
    }

    if (!value->IsCallable("ToString") || !value->Head().IsCallable("Utf8")) {
        return nullptr;
    }

    auto folded = ctx.NewCallable(value->Pos(), "String", {TCoDataCtor(value->HeadPtr()).Literal().Ptr()});
    folded->SetTypeAnn(type);
    return folded;
}

// Returns the ranges as a KqlKeyRange, which the read source takes as is, when the extractor found them to be a single
// range known at compile time and every bound value is a literal, a null or a parameter. Null otherwise.
TExprNode::TPtr BuildLiteralRange(const IPredicateRangeExtractor::TBuildResult& result, const TVector<TString>& keyColumns,
                                  const TStructExprType& schemeType, TPositionHandle pos, TExprContext& ctx) {
    if (!result.LiteralRange) {
        return nullptr;
    }

    using TBound = IPredicateRangeExtractor::TBuildResult::TLiteralRange::TLiteralRangeBound;
    auto buildBound = [&](const TBound& bound) -> TMaybeNode<TKqlKeyTuple> {
        Y_ENSURE(bound.Columns.size() <= keyColumns.size());
        TExprNode::TListType values;
        for (size_t i = 0; i < bound.Columns.size(); ++i) {
            const auto* keyType = schemeType.FindItemType(keyColumns[i]);
            auto value = keyType ? GetLiteralBoundValue(bound.Columns[i], *keyType, ctx) : nullptr;
            if (!value) {
                return {};
            }
            values.push_back(std::move(value));
        }

        if (bound.Inclusive) {
            return Build<TKqlKeyInc>(ctx, pos).Add(values).Done().Cast<TKqlKeyTuple>();
        }
        return Build<TKqlKeyExc>(ctx, pos).Add(values).Done().Cast<TKqlKeyTuple>();
    };

    const auto from = buildBound(result.LiteralRange->Left);
    const auto to = buildBound(result.LiteralRange->Right);
    if (!from || !to) {
        return nullptr;
    }

    // clang-format off
    return Build<TKqlKeyRange>(ctx, pos)
        .From(from.Cast())
        .To(to.Cast())
    .Done().Ptr();
    // clang-format on
}

// The extractor needs the complete table schema, including keys absent from the Read.
// Selected columns use their ID atoms. Other schema entries are extractor-only labels,
// not plan bindings; a nonnumeric prefix keeps them disjoint from every ID.
THashMap<TString, TString> BuildExtractorNames(const TOpRead& read, const TStructExprType& schema,
                                             const TInfoUnitRegistry& registry, TExprContext& ctx) {
    THashMap<TString, TString> result;
    for (const auto* item : schema.GetItems()) {
        result[TString(item->GetName())] = TStringBuilder() << "column:" << item->GetName();
    }
    for (const auto id : read.GetColumns()) {
        result[registry.Get(id).GetColumnName()] = ctx.GetIndexAsString(id);
    }
    return result;
}

TVector<TString> ResolveExposedKeyColumns(const THashMap<TString, TString>& names, const TVector<TString>& physicalKeyColumns) {
    TVector<TString> keyColumns;
    keyColumns.reserve(physicalKeyColumns.size());
    for (const auto& key : physicalKeyColumns) {
        keyColumns.push_back(names.at(key));
    }
    return keyColumns;
}

const TStructExprType* PrepareSchemeType(const THashMap<TString, TString>& names, const TStructExprType* schemeType,
                                       const TOpRead& read, const TInfoUnitRegistry& registry, TExprContext& ctx) {
    TVector<const TItemExprType*> newItemTypes;
    for (const auto itemType : schemeType->GetItems()) {
        newItemTypes.push_back(ctx.MakeType<TItemExprType>(names.at(TString(itemType->GetName())), itemType->GetItemType()));
    }
    // Several Read IDs may fetch the same storage field. Key matching chooses
    // one, but every ID referenced by the predicate must still have a type.
    for (const auto id : read.GetColumns()) {
        const auto physical = registry.Get(id).GetColumnName();
        const auto name = ctx.GetIndexAsString(id);
        if (name != names.at(physical)) {
            newItemTypes.push_back(ctx.MakeType<TItemExprType>(name, schemeType->FindItemType(physical)));
        }
    }
    return ctx.MakeType<TStructExprType>(newItemTypes);
}

std::optional<TOpRead::TPointPrefix> ExtractPointPrefix(size_t pointPrefixLen, const TExprNode::TPtr& lambda,
                                                        const TStructExprType* schemeType, const THashSet<TString>& possibleKeys,
                                                        const TVector<TString>& exposedKeyColumns, const TKikimrTableMetadata& tableMeta,
                                                        const TPredicateExtractorSettings& baseSettings, TRBOContext& rboCtx) {
    const auto& physicalKeyColumns = tableMeta.KeyColumnNames;
    Y_ENSURE(exposedKeyColumns.size() == physicalKeyColumns.size());
    // A lookup join needs key columns after the prefix to look up by.
    if (pointPrefixLen == 0 || pointPrefixLen >= exposedKeyColumns.size()) {
        return std::nullopt;
    }

    auto& ctx = rboCtx.ExprCtx;

    auto settings = baseSettings;
    settings.MergeAdjacentPointRanges = false;
    settings.HaveNextValueCallable = false;
    settings.MaxRanges = Nothing();

    const TVector<TString> exposedPointColumns(exposedKeyColumns.begin(), exposedKeyColumns.begin() + pointPrefixLen);
    TVector<TString> physicalPointColumns(physicalKeyColumns.begin(), physicalKeyColumns.begin() + pointPrefixLen);

    THashSet<TString> keys = possibleKeys;
    auto extractor = MakePredicateRangeExtractor(settings);
    if (!extractor->Prepare(lambda, *schemeType, keys, ctx, rboCtx.TypeCtx)) {
        return std::nullopt;
    }

    const auto result = extractor->BuildComputeNode(exposedPointColumns, ctx, rboCtx.TypeCtx);
    if (!result.ComputeNode || result.PointPrefixLen != pointPrefixLen) {
        return std::nullopt;
    }

    TVector<const TItemExprType*> items;
    items.reserve(pointPrefixLen);
    for (size_t i = 0; i < pointPrefixLen; ++i) {
        const auto* columnType = schemeType->FindItemType(exposedPointColumns[i]);
        if (!columnType) {
            return std::nullopt;
        }
        items.push_back(ctx.MakeType<TItemExprType>(physicalPointColumns[i], columnType));
    }

    TOpRead::TPointPrefix prefix;
    prefix.Table = tableMeta.Name;
    prefix.Points = BuildPointsList(result, physicalPointColumns, ctx);
    prefix.PointsItemType = ctx.MakeType<TStructExprType>(items);
    prefix.Columns = std::move(physicalPointColumns);
    prefix.ExpectedMaxPoints = result.ExpectedMaxRanges ? TMaybe<size_t>(*result.ExpectedMaxRanges) : TMaybe<size_t>();

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Extracted points of " << prefix.Table << ": "
                                 << KqpExprToPrettyString(*prefix.Points, ctx);
    return prefix;
}

// Whether the ranges cover the whole filter predicate, so the extractor pruned it to `true`.
bool IsPrunedToTrue(const TExprNode::TPtr& prunedLambda) {
    const auto literal = TCoLambda(prunedLambda).Body().Maybe<TCoBool>();
    return literal.IsValid() && literal.Cast().Literal().Value() == "true";
}

bool HasNoResidualPredicate(const TExprNode::TPtr& prunedLambda) {
    if (!prunedLambda) {
        return false;
    }
    const auto body = TCoLambda(prunedLambda).Body();
    if (const auto cond = body.Maybe<TCoConditionalValueBase>()) {
        const auto boolLit = cond.Cast().Predicate().Maybe<TCoBool>();
        return boolLit.IsValid() && boolLit.Cast().Literal().Value() == "true" && cond.Cast().Value().Maybe<TCoArgument>().IsValid();
    }
    return body.Maybe<TCoArgument>().IsValid();
}

TVector<TString> FindConsumingTopSortColumns(const IOperator* op, TExprContext& ctx) {
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
            sortColumns.push_back(TString(ctx.GetIndexAsString(Substitute(id, copies))));
        }
        return sortColumns;
    }

    return {};
}

bool IsSelectableIndex(const TIndexDescription& index) {
    return index.Type != TIndexDescription::EType::GlobalAsync
        && index.Type != TIndexDescription::EType::GlobalJson
        && index.Type != TIndexDescription::EType::GlobalJsonCompact
        && index.Type != TIndexDescription::EType::LocalMinMax
        && index.Type != TIndexDescription::EType::LocalBloomFilter
        && index.Type != TIndexDescription::EType::LocalBloomNgramFilter
        && index.State == TIndexDescription::EIndexState::Ready;
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

TExprNode::TPtr BuildTableCallable(const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) {
    // clang-format off
    return Build<TKqpTable>(ctx, pos)
        .Path().Build(meta.Name)
        .PathId().Build(meta.PathId.ToString())
        .SysView().Build(meta.SysView)
        .Version().Build(meta.SchemaVersion)
    .Done().Ptr();
    // clang-format on
}

} // anonymous namespace

bool TPushRangesRule::QuickMatch(const TIntrusivePtr<IOperator>& input) const {
    return input->Kind == EOperator::Filter &&
        input->GetChildren().front()->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TPushRangesRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    auto& kqpCtx = rboCtx.KqpCtx;
    auto& ctx = rboCtx.ExprCtx;
    auto& typeCtx = rboCtx.TypeCtx;

    auto predicateExtractSetting = kqpCtx.Config->GetOptPredicateExtract();
    if (predicateExtractSetting == EOptionalFlag::Disabled) {
        return input;
    }

    if (!IsSuitableToExtractAndPushRanges(input.get(), ApplicableTableType)) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput().Get());
    const auto tablePath = TExprBase(read->GetTable()).Cast<TKqpTable>().Path().StringValue();

    // Check for table.
    const auto tableDesc = kqpCtx.Tables->EnsureTableExists(kqpCtx.Cluster, tablePath, read->Pos, ctx);
    if (!tableDesc || !tableDesc->Metadata) {
        return input;
    }

    const auto tableKind = tableDesc->Metadata->Kind;
    if (tableKind != EKikimrTableKind::Olap && tableKind != EKikimrTableKind::Datashard) {
        return input;
    }

    const auto extractorLambda = GetLambdaForRangeExtractor(filter->GetFilterExpression().Node, read->Type, rboCtx);
    if (!extractorLambda) {
        return input;
    }

    auto lambda = TCoLambda(extractorLambda);
    auto originalLambda = ctx.DeepCopyLambda(*lambda.Ptr());
    // Predicate extract lib requires constraints.
    auto arg = lambda.Args().Arg(0).Ptr();
    arg->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());

    THashSet<TString> possibleKeys;
    auto settings = PrepareExtractorSettings(kqpCtx);
    auto extractor = MakePredicateRangeExtractor(settings);
    const auto extractorNames = BuildExtractorNames(*read, *tableDesc->SchemeNode, props.InfoUnitRegistry, ctx);
    const auto schemeType = PrepareSchemeType(extractorNames, tableDesc->SchemeNode, *read, props.InfoUnitRegistry, ctx);
    const bool prepareSuccess = extractor->Prepare(lambda.Ptr(), *schemeType, possibleKeys, ctx, typeCtx);
    YQL_ENSURE(prepareSuccess);

    const auto& mainMeta = *tableDesc->Metadata;
    const auto mainKeyColumns = ResolveExposedKeyColumns(extractorNames, mainMeta.KeyColumnNames);
    const auto sortColumns =
        read->GetTableStorageType() == NYql::EStorageType::RowStorage ? FindConsumingTopSortColumns(input.get(), ctx) : TVector<TString>();

    // The candidates are matched on the normalized predicate, the extractor builds the ranges of the chosen one only.
    const auto predicateTree = TPredicateTreeBuilder(nullptr).Build(lambda.Body());
    const auto normalizedPredicate = TPredicateTreeBuilder::Normalize(predicateTree);
    if (normalizedPredicate) {
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Normalized predicate of a read of " << tablePath << ":\n"
                                     << normalizedPredicate->ToString();
    }

    // Point prefixes are computed for every table the read can be redirected to, so a lookup join can
    // choose the one which fits its keys best. Otherwise only the table chosen for the read gets one.
    // Point lookups are only applicable to row storage tables.
    const bool allPointPrefixes = kqpCtx.Config->GetEnableLookupJoinPointPrefixes();
    TVector<TOpRead::TPointPrefix> pointPrefixes;
    auto addPointPrefix = [&](const TKikimrTableMetadata& tableMeta, size_t pointPrefixLen, const TVector<TString>& exposedKeyColumns) {
        if (read->GetTableStorageType() != NYql::EStorageType::RowStorage) {
            return;
        }
        if (auto prefix = ExtractPointPrefix(pointPrefixLen, lambda.Ptr(), schemeType, possibleKeys, exposedKeyColumns,
                                             tableMeta, settings, rboCtx)) {
            pointPrefixes.push_back(std::move(*prefix));
        }
    };

    auto mainMatch = MatchKey(normalizedPredicate, mainKeyColumns, settings.MaxRanges);
    if (allPointPrefixes) {
        addPointPrefix(mainMeta, mainMatch.PointPrefixLen, mainKeyColumns);
    }

    auto mainScore = ScoreKeyOrder(mainMatch, mainKeyColumns.size(), sortColumns, mainKeyColumns, true);
    TVector<TReadCandidate> candidates;
    candidates.push_back(TReadCandidate{
        .KeyColumns = mainKeyColumns,
        .Match = std::move(mainMatch),
        .Covering = true,
        .Score = std::move(mainScore),
    });

    if (!kqpCtx.Config->IsAutoIndexSelectionDisabled() && !candidates.front().Score.PointCoversKey) {
        const auto filterPhysical = FilterPhysicalColumns(*filter, *read, props);
        for (const auto& index : mainMeta.Indexes) {
            if (!IsSelectableIndex(index)) {
                continue;
            }

            const auto indexMeta = mainMeta.GetIndexMetadata(index.Name).first;
            if (!indexMeta || IsUselessIndex(indexMeta->KeyColumnNames, mainMeta.KeyColumnNames)) {
                continue;
            }

            if (!FindTable(kqpCtx, indexMeta->Name)) {
                continue;
            }

            auto indexKeyColumns = ResolveExposedKeyColumns(extractorNames, indexMeta->KeyColumnNames);
            auto indexMatch = MatchKey(normalizedPredicate, indexKeyColumns, settings.MaxRanges);
            if (indexMatch.UsedPrefixLen == 0) {
                continue;
            }

            // A lookup join fetches the rows it finds in a non-covering index from the main table and applies
            // the whole predicate to them, so the index needs neither to cover the read nor the predicate.
            if (allPointPrefixes) {
                addPointPrefix(*indexMeta, indexMatch.PointPrefixLen, indexKeyColumns);
            }

            const bool covering = IsCovering(*read, *indexMeta, props.InfoUnitRegistry);
            if (!covering) {
                if (read->Limit) {
                    continue;
                }
                const bool evaluable = std::all_of(filterPhysical.begin(), filterPhysical.end(),
                                                   [&](const TString& col) { return indexMeta->Columns.contains(col); });
                if (!evaluable) {
                    continue;
                }
            }

            auto score = ScoreKeyOrder(indexMatch, indexKeyColumns.size(), sortColumns, indexKeyColumns, covering);
            candidates.push_back(TReadCandidate{
                .IndexMeta = indexMeta,
                .IndexName = index.Name,
                .KeyColumns = std::move(indexKeyColumns),
                .Match = std::move(indexMatch),
                .Covering = covering,
                .Score = std::move(score),
            });
        }
    }

    auto accessPaths = BuildAccessPaths(candidates, mainMeta);
    const bool costBased = IsCostBasedIndexSelectionEnabled(kqpCtx, read->GetTableStorageType()) && candidates.size() > 1
        && EstimateAccessPathCosts(accessPaths, candidates, *read, normalizedPredicate, mainMeta.RecordsCount, rboCtx, props);

    TVector<bool> rejected(candidates.size(), false);
    size_t winnerIdx = 0;
    IPredicateRangeExtractor::TBuildResult chosen;
    while (true) {
        // The score stays the choice when the cost cannot be estimated.
        winnerIdx = costBased ? ChooseCheapestAccessPath(accessPaths) : ChooseCandidateByScore(candidates, rejected);
        const auto& candidate = candidates[winnerIdx];
        chosen = extractor->BuildComputeNode(candidate.KeyColumns, ctx, typeCtx);
        if (winnerIdx == 0 || (chosen.ComputeNode && chosen.UsedPrefixLen >= candidate.Match.UsedPrefixLen)) {
            break;
        }
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Index " << candidate.IndexName << " uses " << chosen.UsedPrefixLen
                                     << " key columns instead of " << candidate.Match.UsedPrefixLen << ", rejected";
        rejected[winnerIdx] = true;
        accessPaths[winnerIdx].Cost = std::numeric_limits<double>::infinity();
    }

    // The chosen path reports what the extractor built.
    auto& chosenPath = accessPaths[winnerIdx];
    chosenPath.Chosen = true;
    chosenPath.UsedPrefixLen = chosen.UsedPrefixLen;
    chosenPath.PointPrefixLen = chosen.PointPrefixLen;
    chosenPath.ExpectedMaxRanges = chosen.ExpectedMaxRanges ? TMaybe<size_t>(*chosen.ExpectedMaxRanges) : TMaybe<size_t>();
    chosenPath.HasResidual = !HasNoResidualPredicate(chosen.PrunedLambda);
    LogAccessPaths(accessPaths);
    const auto& winner = candidates[winnerIdx];

    if (winner.IndexMeta && !winner.Covering) {
        const auto& lookupIndexMeta = winner.IndexMeta;
        const auto& lookupResult = chosen;
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Selected non-covering index " << lookupIndexMeta->Name
                                     << " for a read of " << tablePath;

        TOpRead::TRangeInfo rangeInfo{
            .ComputeNode = lookupResult.ComputeNode,
            .KeyColumns = lookupIndexMeta->KeyColumnNames,
            .UsedPrefixLen = lookupResult.UsedPrefixLen,
            .PointPrefixLen = lookupResult.PointPrefixLen,
            .ExpectedMaxRanges = lookupResult.ExpectedMaxRanges ? TMaybe<size_t>(*lookupResult.ExpectedMaxRanges) : TMaybe<size_t>(),
        };
        if (GetStorageType(*lookupIndexMeta) == NYql::EStorageType::RowStorage) {
            rangeInfo.LiteralRange = BuildLiteralRange(lookupResult, winner.KeyColumns, *schemeType, read->Pos, ctx);
        }

        const auto lookupReadColumns = BuildIndexReadColumns(mainMeta.KeyColumnNames, FilterPhysicalColumns(*filter, *read, props));
        TUnorderedIUs indexColumns;
        THashMap<TString, TInfoUnitId> indexIds;
        for (const auto& col : lookupReadColumns) {
            const auto id = props.InfoUnitRegistry.Add(TInfoUnit(read->Alias, col));
            indexColumns.Add(id);
            indexIds.emplace(col, id);
        }

        auto indexRead = MakeIntrusive<TOpRead>(read->Alias, std::move(indexColumns), GetStorageType(*lookupIndexMeta),
                                                BuildTableCallable(*lookupIndexMeta, read->Pos, ctx), nullptr, nullptr,
                                                std::move(rangeInfo), std::nullopt, ESortDir::None, read->Props, read->Pos);

        TSubstitutions substitutions;
        for (const auto id : filter->GetFilterIUs(props)) {
            substitutions.Add(id, indexIds.at(props.InfoUnitRegistry.Get(id).GetColumnName()));
        }
        TIntrusivePtr<IOperator> indexFilter = std::move(indexRead);
        if (!IsPrunedToTrue(lookupResult.PrunedLambda)) {
            auto indexPredicate = TExpression(lookupResult.PrunedLambda, &ctx, &props).ApplyRenames(substitutions);
            indexFilter = MakeIntrusive<TOpFilter>(std::move(indexFilter), filter->Pos, filter->Props, std::move(indexPredicate), true);
        }

        TOpTableLookup::TLookupKeys lookupKeys;
        for (const auto& pk : mainMeta.KeyColumnNames) {
            lookupKeys.Append(indexIds.at(pk), pk);
        }

        auto lookup = MakeIntrusive<TOpTableLookup>(std::move(indexFilter), read->Pos, read->GetTable(), read->GetColumns(),
                                                    std::move(lookupKeys));
        // Keep the read predicate and the point prefixes, so a lookup join can still probe the main table
        // or an index instead of this subtree.
        if (allPointPrefixes && read->GetTableStorageType() == NYql::EStorageType::RowStorage) {
            lookup->SourceRead = TOpTableLookup::TSourceRead{
                .Predicate = TExpression(originalLambda, &ctx, &props),
                .PointPrefixes = std::move(pointPrefixes),
            };
        }
        return lookup;
    }

    if (!chosen.ComputeNode) {
        return input;
    }
    const auto& chosenKeyColumns = winner.KeyColumns;
    const auto& chosenIndexMeta = winner.IndexMeta;

    if (chosenIndexMeta) {
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Selected index " << chosenIndexMeta->Name << " for a read of " << tablePath;
    }
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Extracted ranges: " << KqpExprToPrettyString(*chosen.ComputeNode, ctx);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Pruned lambda: " << KqpExprToPrettyString(*chosen.PrunedLambda, ctx);

    if (!allPointPrefixes) {
        addPointPrefix(chosenIndexMeta ? *chosenIndexMeta : mainMeta, chosen.PointPrefixLen, chosenKeyColumns);
    }

    TOpRead::TRangeInfo rangeInfo{
        .ComputeNode = chosen.ComputeNode,
        .KeyColumns = chosenIndexMeta ? chosenIndexMeta->KeyColumnNames : mainMeta.KeyColumnNames,
        .UsedPrefixLen = chosen.UsedPrefixLen,
        .PointPrefixLen = chosen.PointPrefixLen,
        .ExpectedMaxRanges = chosen.ExpectedMaxRanges ? TMaybe<size_t>(*chosen.ExpectedMaxRanges) : TMaybe<size_t>(),
        .PointPrefixes = std::move(pointPrefixes),
        .MainTable = mainMeta.Name,
    };
    const auto storageType = chosenIndexMeta ? GetStorageType(*chosenIndexMeta) : read->GetTableStorageType();

    // Only the row storage read source takes a literal range.
    if (storageType == NYql::EStorageType::RowStorage) {
        rangeInfo.LiteralRange = BuildLiteralRange(chosen, chosenKeyColumns, *schemeType, read->Pos, ctx);
        if (rangeInfo.LiteralRange) {
            YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Literal range: " << KqpExprToPrettyString(*rangeInfo.LiteralRange, ctx);
        }
    }

    const auto tableCallable = chosenIndexMeta ? BuildTableCallable(*chosenIndexMeta, read->Pos, ctx) : read->TableCallable;
    const auto sortDir = chosenIndexMeta ? ESortDir::None : read->SortDir;
    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), storageType, tableCallable, read->OlapFilterLambda,
                                          read->Limit, std::move(rangeInfo), TExpression(originalLambda, &ctx, &props), sortDir, read->Props, read->Pos);
    // An always true filter would stand between the read and its consumers, e.g. an OLAP aggregate.
    if (IsPrunedToTrue(chosen.PrunedLambda)) {
        return newRead;
    }
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, TExpression(chosen.PrunedLambda, &ctx, &props), true);
}
} // namespace NKikimr::NKqp
