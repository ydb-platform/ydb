#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>
#include <yql/essentials/core/yql_opt_utils.h>

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
    settings.BuildLiteralRange = false;
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

struct TPointPrefix {
    TExprNode::TPtr Points;
    const TStructExprType* PointsItemType = nullptr;
    TVector<TString> Columns;
    TMaybe<size_t> ExpectedMaxPoints;
};

TPointPrefix ExtractPointPrefix(size_t pointPrefixLen, const TExprNode::TPtr& lambda, const TStructExprType* schemeType,
                               const THashSet<TString>& possibleKeys, const TVector<TString>& exposedKeyColumns,
                               const TVector<TString>& physicalKeyColumns, const TPredicateExtractorSettings& baseSettings,
                               TRBOContext& rboCtx) {
    Y_ENSURE(exposedKeyColumns.size() == physicalKeyColumns.size());
    pointPrefixLen = std::min(pointPrefixLen, exposedKeyColumns.size());
    if (pointPrefixLen == 0) {
        return {};
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
        return {};
    }

    const auto result = extractor->BuildComputeNode(exposedPointColumns, ctx, rboCtx.TypeCtx);
    if (!result.ComputeNode || result.PointPrefixLen != pointPrefixLen) {
        return {};
    }

    TVector<const TItemExprType*> items;
    items.reserve(pointPrefixLen);
    for (size_t i = 0; i < pointPrefixLen; ++i) {
        const auto* columnType = schemeType->FindItemType(exposedPointColumns[i]);
        if (!columnType) {
            return {};
        }
        items.push_back(ctx.MakeType<TItemExprType>(physicalPointColumns[i], columnType));
    }

    TPointPrefix prefix;
    prefix.Points = BuildPointsList(result, physicalPointColumns, ctx);
    prefix.PointsItemType = ctx.MakeType<TStructExprType>(items);
    prefix.Columns = std::move(physicalPointColumns);
    prefix.ExpectedMaxPoints = result.ExpectedMaxRanges ? TMaybe<size_t>(*result.ExpectedMaxRanges) : TMaybe<size_t>();

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Extracted points: " << KqpExprToPrettyString(*prefix.Points, ctx);
    return prefix;
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

TIndexScore ScoreKeyOrder(const IPredicateRangeExtractor::TBuildResult& result, size_t keyLen, const TVector<TString>& sortColumns,
                          const TVector<TString>& keyColumns, bool covering) {
    TIndexScore score;
    score.PointCoversKey = keyLen != 0 && result.PointPrefixLen == keyLen;
    score.PointPrefixLen = score.PointCoversKey ? 0 : result.PointPrefixLen;
    score.UsedCoversKey = keyLen != 0 && result.UsedPrefixLen == keyLen;
    score.UsedPrefixLen = score.UsedCoversKey ? 0 : result.UsedPrefixLen;

    if (covering && !sortColumns.empty()) {
        const size_t pointPrefixLen =
            (result.ExpectedMaxRanges && *result.ExpectedMaxRanges == 1) ? std::min(result.PointPrefixLen, keyColumns.size()) : 0;
        score.SortMatches = SortMatchesKeyOrder(sortColumns, keyColumns, pointPrefixLen);
        score.SortMatchesAndNoResidual = score.SortMatches && HasNoResidualPredicate(result.PrunedLambda);
    }

    return score;
}

// Ties are broken deterministically, independently of the order indexes are declared
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
    const auto mainResult = extractor->BuildComputeNode(mainKeyColumns, ctx, typeCtx);
    const auto sortColumns =
        read->GetTableStorageType() == NYql::EStorageType::RowStorage ? FindConsumingTopSortColumns(input.get(), ctx) : TVector<TString>();

    TIntrusivePtr<TKikimrTableMetadata> chosenIndexMeta;
    IPredicateRangeExtractor::TBuildResult winnerResult;
    TVector<TString> winnerKeyColumns;

    TIntrusivePtr<TKikimrTableMetadata> lookupIndexMeta;
    IPredicateRangeExtractor::TBuildResult lookupResult;
    TVector<TString> lookupReadColumns;

    auto bestScore = ScoreKeyOrder(mainResult, mainKeyColumns.size(), sortColumns, mainKeyColumns, true);
    TString bestIndexName;
    bool bestCovering = false;

    if (!kqpCtx.Config->IsAutoIndexSelectionDisabled() && !bestScore.PointCoversKey) {
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

            auto indexKeyColumns = ResolveExposedKeyColumns(extractorNames, indexMeta->KeyColumnNames);
            auto indexResult = extractor->BuildComputeNode(indexKeyColumns, ctx, typeCtx);
            if (!indexResult.ComputeNode) {
                continue;
            }

            const auto score = ScoreKeyOrder(indexResult, indexKeyColumns.size(), sortColumns, indexKeyColumns, covering);
            if (!IsBetterCandidate(score, covering, index.Name, bestScore, bestCovering, bestIndexName)) {
                continue;
            }

            bestScore = score;
            bestIndexName = index.Name;
            bestCovering = covering;
            if (covering) {
                chosenIndexMeta = indexMeta;
                winnerResult = std::move(indexResult);
                winnerKeyColumns = std::move(indexKeyColumns);
                lookupIndexMeta.Reset();
            } else {
                lookupIndexMeta = indexMeta;
                lookupResult = std::move(indexResult);
                lookupReadColumns = BuildIndexReadColumns(mainMeta.KeyColumnNames, filterPhysical);
                chosenIndexMeta.Reset();
            }
        }
    }

    if (lookupIndexMeta) {
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Selected non-covering index " << lookupIndexMeta->Name
                                     << " for a read of " << tablePath;

        TOpRead::TRangeInfo rangeInfo{
            .ComputeNode = lookupResult.ComputeNode,
            .KeyColumns = lookupIndexMeta->KeyColumnNames,
            .UsedPrefixLen = lookupResult.UsedPrefixLen,
            .PointPrefixLen = lookupResult.PointPrefixLen,
            .ExpectedMaxRanges = lookupResult.ExpectedMaxRanges ? TMaybe<size_t>(*lookupResult.ExpectedMaxRanges) : TMaybe<size_t>(),
        };

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
        auto indexPredicate = TExpression(lookupResult.PrunedLambda, &ctx, &props).ApplyRenames(substitutions);
        auto indexFilter = MakeIntrusive<TOpFilter>(std::move(indexRead), filter->Pos, filter->Props, std::move(indexPredicate), true);

        TOpTableLookup::TLookupKeys lookupKeys;
        for (const auto& pk : mainMeta.KeyColumnNames) {
            lookupKeys.Append(indexIds.at(pk), pk);
        }

        return MakeIntrusive<TOpTableLookup>(std::move(indexFilter), read->Pos, read->GetTable(), read->GetColumns(), std::move(lookupKeys));
    }

    const auto& chosen = chosenIndexMeta ? winnerResult : mainResult;
    if (!chosen.ComputeNode) {
        return input;
    }
    const auto& chosenKeyColumns = chosenIndexMeta ? winnerKeyColumns : mainKeyColumns;

    if (chosenIndexMeta) {
        YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Selected index " << chosenIndexMeta->Name << " for a read of " << tablePath;
    }
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Extracted ranges: " << KqpExprToPrettyString(*chosen.ComputeNode, ctx);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Pruned lambda: " << KqpExprToPrettyString(*chosen.PrunedLambda, ctx);

    TOpRead::TRangeInfo rangeInfo{
        .ComputeNode = chosen.ComputeNode,
        .KeyColumns = chosenIndexMeta ? chosenIndexMeta->KeyColumnNames : mainMeta.KeyColumnNames,
        .UsedPrefixLen = chosen.UsedPrefixLen,
        .PointPrefixLen = chosen.PointPrefixLen,
        .ExpectedMaxRanges = chosen.ExpectedMaxRanges ? TMaybe<size_t>(*chosen.ExpectedMaxRanges) : TMaybe<size_t>(),
    };
    const auto storageType = chosenIndexMeta ? GetStorageType(*chosenIndexMeta) : read->GetTableStorageType();

    // Point lookup is only applicable to row storage tables.
    if (storageType == NYql::EStorageType::RowStorage && chosen.PointPrefixLen > 0) {
        const auto& chosenPhysicalKeyColumns = chosenIndexMeta ? chosenIndexMeta->KeyColumnNames : mainMeta.KeyColumnNames;
        auto prefix = ExtractPointPrefix(chosen.PointPrefixLen, lambda.Ptr(), schemeType, possibleKeys, chosenKeyColumns,
                                         chosenPhysicalKeyColumns, settings, rboCtx);
        if (prefix.Points) {
            rangeInfo.Points = std::move(prefix.Points);
            rangeInfo.PointsItemType = prefix.PointsItemType;
            rangeInfo.PointColumns = std::move(prefix.Columns);
            rangeInfo.ExpectedMaxPoints = prefix.ExpectedMaxPoints;
        }
    }

    const auto tableCallable = chosenIndexMeta ? BuildTableCallable(*chosenIndexMeta, read->Pos, ctx) : read->TableCallable;
    const auto sortDir = chosenIndexMeta ? ESortDir::None : read->SortDir;
    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->GetColumns(), storageType, tableCallable, read->OlapFilterLambda,
                                          read->Limit, std::move(rangeInfo), TExpression(originalLambda, &ctx, &props), sortDir, read->Props, read->Pos);
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, TExpression(chosen.PrunedLambda, &ctx, &props), true);
}
} // namespace NKikimr::NKqp
