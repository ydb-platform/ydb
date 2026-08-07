#include <ydb/core/kqp/opt/rbo/kqp_rbo_rules.h>
#include <ydb/core/kqp/opt/rbo/kqp_rbo_utils.h>
#include <ydb/core/kqp/provider/yql_kikimr_settings.h>

#include <yql/essentials/core/extract_predicate/extract_predicate.h>
#include <yql/essentials/core/peephole_opt/yql_opt_peephole_physical.h>
#include <yql/essentials/core/yql_expr_optimize.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

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

bool IsLambdaOptionalType(TExprNode::TPtr node, const TTypeAnnotationNode* structType, TRBOContext& ctx) {
    Y_ENSURE(node);
    auto lambda = ctx.ExprCtx.DeepCopyLambda(*node);
    if (!UpdateLambdaAllArgumentsTypes(lambda, {structType}, ctx.ExprCtx)) {
        return false;
    }

    ctx.TypeAnnTransformer.Rewind();
    IGraphTransformer::TStatus status(IGraphTransformer::TStatus::Ok);
    do {
        status = ctx.TypeAnnTransformer.Transform(lambda, lambda, ctx.ExprCtx);
    } while (status == IGraphTransformer::TStatus::Repeat);

    const TTypeAnnotationNode* lambdaType = lambda->GetTypeAnn();
    if (!lambdaType) {
        return false;
    }

    return lambdaType->IsOptionalOrNull();
}

TExprNode::TPtr GetLambdaForRangeExtractor(TExprNode::TPtr node, const TTypeAnnotationNode* inputType, TRBOContext& rboCtx) {
    if (!inputType) {
        return node;
    }

    auto& ctx = rboCtx.ExprCtx;
    auto structType = inputType->Cast<TListExprType>()->GetItemType()->Cast<TStructExprType>();
    if (!IsLambdaOptionalType(node, structType, rboCtx)) {
        return node;
    }

    auto lambda = TCoLambda(node);
    // clang-format off
    auto newBody = Build<TCoCoalesce>(ctx, node->Pos())
        .Predicate(lambda.Body())
        .Value<TCoBool>()
            .Literal().Build("false")
        .Build()
    .Done();
    // clang-format on

    // clang-format off
    auto newLambda = Build<TCoLambda>(ctx, node->Pos())
        .Args({"arg"})
        .Body<TExprApplier>()
            .Apply(newBody)
            .With(lambda.Args().Arg(0), "arg")
        .Build()
    .Done();
    // clang-format on

    TVector<const TTypeAnnotationNode*> argTypes{structType};
    // clang-format off
    auto predicateClosure = Build<TKqpPredicateClosure>(ctx, node->Pos())
        .Lambda(newLambda)
        .ArgsType(ExpandType(node->Pos(), *ctx.MakeType<TTupleExprType>(argTypes), ctx))
    .Done();
    // clang-format on

    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Range exctractor, before peephole: " << KqpExprToPrettyString(predicateClosure, ctx);

    TExprNode::TPtr afterPeephole;
    bool hasNonDeterministicFunctions;
    if (const auto status = PeepHoleOptimizeNode(predicateClosure.Ptr(), afterPeephole, ctx, rboCtx.TypeCtx, nullptr,
                                                 hasNonDeterministicFunctions);
        status != IGraphTransformer::TStatus::Ok) {
        YQL_CLOG(ERROR, ProviderKqp) << "[NEW RBO] Peephole failed with status: " << status << Endl;
        afterPeephole = nullptr;
    }
    Y_ENSURE(afterPeephole);
    YQL_CLOG(TRACE, ProviderKqp) << "[NEW RBO] Range exctractor, after peephole: " << KqpExprToPrettyString(TExprBase(afterPeephole), ctx);

    return TExprBase(afterPeephole).Cast<TKqpPredicateClosure>().Lambda().Ptr();
}

bool IsSuitableToExtractAndPushRanges(const TIntrusivePtr<IOperator>& input, const NYql::EStorageType applicableTableType) {
    if (input->Kind != EOperator::Filter) {
        return false;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto maybeRead = filter->GetInput();
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

// Map a physical table column name to the name the read actually exposes. Projection elimination
// can rename read outputs (most commonly by stripping the alias), so Columns[i] (physical) is
// aligned with OutputIUs[i] (exposed). Columns not selected by the read are absent from the map.
THashMap<TString, TString> BuildPhysicalToExposedName(const TOpRead& read) {
    THashMap<TString, TString> result;
    const size_t count = std::min(read.Columns.size(), read.OutputIUs.size());
    for (size_t i = 0; i < count; ++i) {
        result[read.Columns[i]] = read.OutputIUs[i].GetFullName();
    }
    return result;
}

// Resolve the name a physical column should carry so it stays consistent with the read's exposed
// outputs and the lambda the predicate extractor is typed against.
TString ResolveExposedName(const TString& physicalName, const TOpRead& read,
                           const THashMap<TString, TString>& physicalToExposed, bool exposesQualified) {
    if (const auto it = physicalToExposed.find(physicalName); it != physicalToExposed.end()) {
        return it->second;
    }
    if (exposesQualified && !read.Alias.empty()) {
        return read.Alias + "." + physicalName;
    }
    return physicalName;
}

bool ExposesQualifiedNames(const TOpRead& read) {
    return std::any_of(read.OutputIUs.begin(), read.OutputIUs.end(), [](const TInfoUnit& iu) { return !iu.GetAlias().empty(); });
}

TVector<TString> ResolveExposedKeyColumns(const TOpRead& read, const TVector<TString>& physicalKeyColumns) {
    const auto physicalToExposed = BuildPhysicalToExposedName(read);
    const bool exposesQualified = ExposesQualifiedNames(read);

    TVector<TString> keyColumns;
    keyColumns.reserve(physicalKeyColumns.size());
    for (const auto& key : physicalKeyColumns) {
        keyColumns.emplace_back(ResolveExposedName(key, read, physicalToExposed, exposesQualified));
    }
    return keyColumns;
}

const TStructExprType* PrepareSchemeType(const TOpRead& read, const TStructExprType* schemeType, TExprContext& ctx) {
    const auto physicalToExposed = BuildPhysicalToExposedName(read);
    const bool exposesQualified = ExposesQualifiedNames(read);

    TVector<const TItemExprType*> newItemTypes;
    bool changed = false;
    for (const auto itemType : schemeType->GetItems()) {
        const TString physicalName(itemType->GetName());
        const auto newName = ResolveExposedName(physicalName, read, physicalToExposed, exposesQualified);
        changed |= newName != physicalName;
        newItemTypes.push_back(ctx.MakeType<TItemExprType>(newName, itemType->GetItemType()));
    }

    return changed ? ctx.MakeType<TStructExprType>(newItemTypes) : schemeType;
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

TVector<TString> FindConsumingTopSortColumns(const TIntrusivePtr<IOperator>& op) {
    THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renames;
    const IOperator* current = op.Get();

    while (current && current->Parents.size() == 1) {
        IOperator* parent = current->Parents.front().first;
        if (!parent) {
            return {};
        }

        if (parent->Kind == EOperator::Map) {
            for (const auto& [exposed, source] : CastOperator<TOpMap>(TIntrusivePtr<IOperator>(parent))->GetRenames()) {
                renames[exposed] = source;
            }
            current = parent;
            continue;
        }

        if (parent->Kind != EOperator::Sort) {
            return {};
        }

        const auto sort = CastOperator<TOpSort>(TIntrusivePtr<IOperator>(parent));
        if (!sort->LimitCond.has_value()) {
            return {};
        }

        TVector<TString> sortColumns;
        const auto& sortElements = sort->SortElements;
        sortColumns.reserve(sortElements.size());

        const bool ascending = sortElements.empty() ? true : sortElements.front().Ascending;
        for (const auto& sortElement : sortElements) {
            if (sortElement.Ascending != ascending || sortElement.NullsFirst != ascending) {
                return {};
            }
            const auto it = renames.find(sortElement.SortColumn);
            sortColumns.push_back((it != renames.end() ? it->second : sortElement.SortColumn).GetFullName());
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

bool IsCovering(const TOpRead& read, const TKikimrTableMetadata& indexMeta) {
    for (const auto& column : read.Columns) {
        if (!indexMeta.Columns.contains(column)) {
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
    THashMap<TString, TString> exposedToPhysical;
    const size_t count = std::min(read.Columns.size(), read.OutputIUs.size());
    for (size_t i = 0; i < count; ++i) {
        exposedToPhysical[read.OutputIUs[i].GetFullName()] = read.Columns[i];
    }

    TVector<TString> result;
    THashSet<TString> seen;
    for (const auto& iu : filter.GetFilterIUs(props)) {
        const auto it = exposedToPhysical.find(iu.GetFullName());
        const TString physical = it != exposedToPhysical.end() ? it->second : iu.GetColumnName();
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
        input->Children.front()->Kind == EOperator::Source;
}

TIntrusivePtr<IOperator> TPushRangesRule::SimpleMatchAndApply(const TIntrusivePtr<IOperator>& input, TRBOContext& rboCtx, TPlanProps& props) {
    Y_UNUSED(props);
    auto& kqpCtx = rboCtx.KqpCtx;
    auto& ctx = rboCtx.ExprCtx;
    auto& typeCtx = rboCtx.TypeCtx;

    auto predicateExtractSetting = kqpCtx.Config->GetOptPredicateExtract();
    if (predicateExtractSetting == EOptionalFlag::Disabled) {
        return input;
    }

    if (!IsSuitableToExtractAndPushRanges(input, ApplicableTableType)) {
        return input;
    }

    const auto filter = CastOperator<TOpFilter>(input);
    const auto read = CastOperator<TOpRead>(filter->GetInput());
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

    auto lambda = TCoLambda(GetLambdaForRangeExtractor(filter->FilterExpr.Node, read->Type, rboCtx));
    auto originalLambda = ctx.DeepCopyLambda(*lambda.Ptr());
    // Predicate extract lib requires constraints.
    auto arg = lambda.Args().Arg(0).Ptr();
    arg->AddConstraint(ctx.MakeConstraint<TEmptyConstraintNode>());

    THashSet<TString> possibleKeys;
    auto settings = PrepareExtractorSettings(kqpCtx);
    auto extractor = MakePredicateRangeExtractor(settings);
    const auto schemeType = PrepareSchemeType(*read, tableDesc->SchemeNode, ctx);
    const bool prepareSuccess = extractor->Prepare(lambda.Ptr(), *schemeType, possibleKeys, ctx, typeCtx);
    YQL_ENSURE(prepareSuccess);

    const auto& mainMeta = *tableDesc->Metadata;
    const auto mainKeyColumns = ResolveExposedKeyColumns(*read, mainMeta.KeyColumnNames);
    const auto mainResult = extractor->BuildComputeNode(mainKeyColumns, ctx, typeCtx);
    const auto sortColumns =
        read->StorageType == NYql::EStorageType::RowStorage ? FindConsumingTopSortColumns(input) : TVector<TString>();

    TIntrusivePtr<TKikimrTableMetadata> chosenIndexMeta;
    IPredicateRangeExtractor::TBuildResult winnerResult;
    TVector<TString> winnerKeyColumns;

    TIntrusivePtr<TKikimrTableMetadata> lookupIndexMeta;
    IPredicateRangeExtractor::TBuildResult lookupResult;
    TVector<TString> lookupKeyColumns;
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

            const bool covering = IsCovering(*read, *indexMeta);
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

            auto indexKeyColumns = ResolveExposedKeyColumns(*read, indexMeta->KeyColumnNames);
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
                lookupKeyColumns = std::move(indexKeyColumns);
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
            .KeyColumns = lookupKeyColumns,
            .UsedPrefixLen = lookupResult.UsedPrefixLen,
            .PointPrefixLen = lookupResult.PointPrefixLen,
            .ExpectedMaxRanges = lookupResult.ExpectedMaxRanges ? TMaybe<size_t>(*lookupResult.ExpectedMaxRanges) : TMaybe<size_t>(),
        };

        TVector<TInfoUnit> indexOutputIUs;
        indexOutputIUs.reserve(lookupReadColumns.size());
        for (const auto& col : lookupReadColumns) {
            indexOutputIUs.emplace_back(TString(), col);
        }

        auto indexRead = MakeIntrusive<TOpRead>(read->Alias, lookupReadColumns, indexOutputIUs, GetStorageType(*lookupIndexMeta),
                                                BuildTableCallable(*lookupIndexMeta, read->Pos, ctx), nullptr, nullptr,
                                                std::move(rangeInfo), std::nullopt, ESortDir::None, read->Props, read->Pos);

        auto indexFilter = MakeIntrusive<TOpFilter>(indexRead, filter->Pos, filter->Props,
                                                    TExpression(lookupResult.PrunedLambda, &ctx, &props));
        THashMap<TInfoUnit, TInfoUnit, TInfoUnit::THashFunction> renameMap;
        const size_t renameCount = std::min(read->Columns.size(), read->OutputIUs.size());
        for (size_t i = 0; i < renameCount; ++i) {
            renameMap[read->OutputIUs[i]] = TInfoUnit(TString(), read->Columns[i]);
        }
        indexFilter->RenameUsedIUs(renameMap, ctx);

        TVector<TInfoUnit> lookupKeys;
        for (const auto& pk : mainMeta.KeyColumnNames) {
            lookupKeys.emplace_back(TString(), pk);
        }

        return MakeIntrusive<TOpTableLookup>(indexFilter, read->Pos, read->TableCallable, read->Columns,
                                             read->GetOutputIUs(), lookupKeys);
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
        .KeyColumns = chosenKeyColumns,
        .UsedPrefixLen = chosen.UsedPrefixLen,
        .PointPrefixLen = chosen.PointPrefixLen,
        .ExpectedMaxRanges = chosen.ExpectedMaxRanges ? TMaybe<size_t>(*chosen.ExpectedMaxRanges) : TMaybe<size_t>(),
    };
    const auto storageType = chosenIndexMeta ? GetStorageType(*chosenIndexMeta) : read->StorageType;

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
    auto newRead = MakeIntrusive<TOpRead>(read->Alias, read->Columns, read->GetOutputIUs(), storageType, tableCallable, read->OlapFilterLambda,
                                          read->Limit, std::move(rangeInfo), TExpression(originalLambda, &ctx, &props), sortDir, read->Props, read->Pos);
    return MakeIntrusive<TOpFilter>(newRead, filter->Pos, filter->Props, TExpression(chosen.PrunedLambda, &ctx, &props));
}
} // namespace NKikimr::NKqp
