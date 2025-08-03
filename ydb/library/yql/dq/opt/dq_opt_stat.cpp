#include "dq_opt_stat.h"

#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/core/yql_cost_function.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/core/yql_expr_type_annotation.h>

#include "util/string/join.h"

namespace NYql::NDq {

using namespace NNodes;

namespace {
    /***
     * We maintain a white list of callables that we consider part of constant expressions
     * All other callables will not be evaluated
     */
    THashSet<TString> ConstantFoldingWhiteList = {
        "Concat", "Just", "Optional", "SafeCast", "AsList",
        "+", "-", "*", "/", "%"};

    THashSet<TString> PgConstantFoldingWhiteList = {
        "PgResolvedOp", "PgResolvedCall", "PgCast", "PgConst", "PgArray", "PgType"};

    TVector<TString> UdfBlackList = {
        "RandomNumber",
        "Random",
        "RandomUuid",
        "Now",
        "CurrentUtcDate",
        "CurrentUtcDatetime",
        "CurrentUtcTimestamp"
    };

    bool IsConstantUdf(const TExprNode::TPtr& input, bool withParams = false) {
        if (!TCoApply::Match(input.Get())) {
            return false;
        }

        if (input->ChildrenSize()!=2) {
            return false;
        }
        if (input->Child(0)->IsCallable("Udf")) {
            auto udf = TCoUdf(input->Child(0));
            auto udfName = udf.MethodName().StringValue();

            for (auto blck : UdfBlackList) {
                if (udfName.find(blck) != TString::npos) {
                    return false;
                }
            }

            if (withParams) {
                return IsConstantExprWithParams(input->Child(1));
            }
            else {
                return IsConstantExpr(input->Child(1));
            }
        }
        return false;
    }

    TString RemoveAliases(TString attributeName) {
        if (auto idx = attributeName.find('.'); idx != TString::npos) {
            return attributeName.substr(idx+1);
        }
        return attributeName;
    }

    TString ExtractAlias(TString attributeName) {
        if (auto idx = attributeName.find('.'); idx != TString::npos) {
            auto substr = attributeName.substr(0, idx);
            if (auto idx2 = substr.find('.'); idx != TString::npos) {
                substr = substr.substr(idx2+1);
            }
            return substr;
        }
        return TString();
    }

    TVector<TString> InferLabels(std::shared_ptr<TOptimizerStatistics>& stats, TCoAtomList joinColumns) {
        if(stats->Labels) {
            return *stats->Labels;
        }

        if (!joinColumns.Size()) {
            return TVector<TString>();
        }

        auto fullColumnName = joinColumns.Item(0).StringValue();
        for (size_t i = 0; i < fullColumnName.size(); i++) {
            if (fullColumnName[i]=='.') {
                fullColumnName = fullColumnName.substr(0, i);
            }
            else if (i == fullColumnName.size() - 1) {
                return TVector<TString>();
            }
        }

        auto res = TVector<TString>();
        res.push_back(fullColumnName);
        return res;
    }

    std::shared_ptr<TOptimizerStatistics> ApplyRowsHints(
        std::shared_ptr<TOptimizerStatistics>& inputStats,
        TVector<TString>& labels,
        TCardinalityHints hints
    ) {

            if (labels.size() != 1) {
                return inputStats;
            }

            for (auto h : hints.Hints) {
                if (h.JoinLabels.size() == 1 && h.JoinLabels == labels) {
                    auto outputStats = std::make_shared<TOptimizerStatistics>(
                        inputStats->Type,
                        h.ApplyHint(inputStats->Nrows),
                        inputStats->Ncols,
                        inputStats->ByteSize,
                        inputStats->Cost,
                        inputStats->KeyColumns,
                        inputStats->ColumnStatistics,
                        inputStats->StorageType);
                    outputStats->Labels = inputStats->Labels;
                    return outputStats;
                }
            }
            return inputStats;
    }

    std::shared_ptr<TOptimizerStatistics> ApplyBytesHints(
        std::shared_ptr<TOptimizerStatistics>& inputStats,
        TVector<TString>& labels,
        TCardinalityHints hints
    ) {

            if (labels.size() != 1) {
                return inputStats;
            }

            for (auto h : hints.Hints) {
                if (h.JoinLabels.size() == 1 && h.JoinLabels == labels) {
                    auto outputStats = std::make_shared<TOptimizerStatistics>(
                        inputStats->Type,
                        inputStats->Nrows,
                        inputStats->Ncols,
                        h.ApplyHint(inputStats->ByteSize),
                        inputStats->Cost,
                        inputStats->KeyColumns,
                        inputStats->ColumnStatistics,
                        inputStats->StorageType);
                    outputStats->Labels = inputStats->Labels;
                    return outputStats;
                }
            }
            return inputStats;
    }

    TVector<TString> UnionLabels(TVector<TString>& leftLabels, TVector<TString>& rightLabels) {
        auto res = TVector<TString>();
        res.insert(res.begin(), leftLabels.begin(), leftLabels.end());
        res.insert(res.end(), rightLabels.begin(), rightLabels.end());
        return res;
    }

    TCardinalityHints::TCardinalityHint* FindCardHint(TVector<TString>& labels, TCardinalityHints& hints) {
        THashSet<TString> labelsSet;
        labelsSet.insert(labels.begin(), labels.end());

        for (auto & h: hints.Hints ) {
            THashSet<TString> hintLabels;
            hintLabels.insert(h.JoinLabels.begin(), h.JoinLabels.end());
            if (labelsSet == hintLabels) {
                return &h;
            }
        }

        return nullptr;
    }

    TCardinalityHints::TCardinalityHint* FindBytesHint(TVector<TString>& labels, TCardinalityHints& hints) {
        THashSet<TString> labelsSet;
        labelsSet.insert(labels.begin(), labels.end());

        for (auto & h: hints.Hints ) {
            THashSet<TString> hintLabels;
            hintLabels.insert(h.JoinLabels.begin(), h.JoinLabels.end());
            if (labelsSet == hintLabels) {
                return &h;
            }
        }

        return nullptr;
    }

}

bool NeedCalc(NNodes::TExprBase node) {
    auto type = node.Ref().GetTypeAnn();
    if (type->IsSingleton()) {
        return false;
    }

    if (type->GetKind() == ETypeAnnotationKind::Optional) {
        if (node.Maybe<TCoNothing>()) {
            return false;
        }
        if (auto maybeJust = node.Maybe<TCoJust>()) {
            return NeedCalc(maybeJust.Cast().Input());
        }
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::Tuple) {
        if (auto maybeTuple = node.Maybe<TExprList>()) {
            return AnyOf(maybeTuple.Cast(), [](const auto& item) { return NeedCalc(item); });
        }
        return true;
    }

    if (type->GetKind() == ETypeAnnotationKind::List) {
        if (node.Maybe<TCoList>()) {
            YQL_ENSURE(node.Ref().ChildrenSize() == 1, "Should be rewritten to AsList");
            return false;
        }
        if (auto maybeAsList = node.Maybe<TCoAsList>()) {
            return AnyOf(maybeAsList.Cast().Args(), [](const auto& item) { return NeedCalc(NNodes::TExprBase(item)); });
        }
        return true;
    }

    YQL_ENSURE(type->GetKind() == ETypeAnnotationKind::Data,
                "Object of type " << *type << " should not be considered for calculation");

    return !node.Maybe<TCoDataCtor>();
}

bool IsConstantExprPg(const TExprNode::TPtr& input) {
    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        if (input->IsCallable("PgConst")) {
            return true;
        }
    }

    if (TMaybeNode<TCoAtom>(input)) {
        return true;
    }

    if (input->IsCallable(PgConstantFoldingWhiteList) || input->IsList()) {
        for (size_t i = 0; i < input->ChildrenSize(); i++) {
            auto callableInput = input->Child(i);
            if (callableInput->IsLambda() && !IsConstantExprPg(callableInput->Child(1))) {
                return false;
            }
            if (!callableInput->IsCallable("PgType") && !IsConstantExprPg(callableInput)) {
                return false;
            }
        }
        return true;
    }

    return false;
}

bool IsSuitableToFoldFlatMap(const TExprNode::TPtr& input) {
    if (!TCoFlatMap::Match(input.Get())) {
        return false;
    }

    if (auto maybeApply = TMaybeNode<TCoApply>(input->Child(0))) {
        auto apply = maybeApply.Cast();
        return IsConstantUdf(apply.Callable().Ptr());
    }

    return false;
}

/***
 * Check if the expression is a constant expression
 * Its type annotation need to specify that its a data type, and then we check:
 *   - If its a literal, its a constant expression
 *   - If its a callable in the while list and all children are constant expressions, then its a constant expression
 *   - If one of the child is a type expression, it also passes the check
 */
bool IsConstantExpr(const TExprNode::TPtr& input, bool foldUdfs) {
    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        return IsConstantExprPg(input);
    }

    if (!IsDataOrOptionalOfData(input->GetTypeAnn())) {
        return false;
    }

    if (!NeedCalc(TExprBase(input))) {
        return true;
    }

    else if (input->IsCallable(ConstantFoldingWhiteList)) {
        for (size_t i = 0; i < input->ChildrenSize(); i++) {
            auto callableInput = input->Child(i);
            if (callableInput->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type && !IsConstantExpr(callableInput)) {
                return false;
            }
        }
        return true;
    }

    else if (foldUdfs && ((TCoApply::Match(input.Get()) && IsConstantUdf(input)) || IsSuitableToFoldFlatMap(input))) {
        return true;
    }

    return false;
}

bool IsConstantExprWithParams(const TExprNode::TPtr& input) {
    if (input->IsCallable("Parameter")) {
        return true;
    }

    if (input->GetTypeAnn()->GetKind() == ETypeAnnotationKind::Pg) {
        return IsConstantExprPg(input);
    }

    if (!IsDataOrOptionalOfData(input->GetTypeAnn())) {
        return false;
    }

    if (!NeedCalc(TExprBase(input))) {
        return true;
    }

    else if (input->IsCallable(ConstantFoldingWhiteList)) {
        for (size_t i = 0; i < input->ChildrenSize(); i++) {
            auto callableInput = input->Child(i);
            if (callableInput->GetTypeAnn()->GetKind() != ETypeAnnotationKind::Type && !IsConstantExprWithParams(callableInput)) {
                return false;
            }
        }
        return true;
    }

    else if (TCoApply::Match(input.Get()) && IsConstantUdf(input, true)) {
        return true;
    }

    return false;
}

/**
 * Compute statistics for map join
 * FIX: Currently we treat all join the same from the cost perspective, need to refine cost function
 */
void InferStatisticsForMapJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TOptimizerHints hints) {

    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TCoMapJoinCore>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightDict();

    auto leftStats = typeCtx->GetStats(leftArg.Raw());
    auto rightStats = typeCtx->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    auto leftLabels = InferLabels(leftStats, join.LeftKeysColumnNames());
    auto rightLabels = InferLabels(rightStats, join.RightKeysColumnNames());

    leftStats = ApplyRowsHints(leftStats, leftLabels, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightLabels, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftLabels, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightLabels, *hints.BytesHints);

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (size_t i=0; i<join.LeftKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.LeftKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.LeftKeysColumnNames().Item(i).StringValue());
        leftJoinKeys.push_back(TJoinColumn(alias, attrName));
    }
    for (size_t i=0; i<join.RightKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.RightKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.RightKeysColumnNames().Item(i).StringValue());
        rightJoinKeys.push_back(TJoinColumn(alias, attrName));
    }

    auto unionOfLabels = UnionLabels(leftLabels, rightLabels);
    auto resStats = std::make_shared<TOptimizerStatistics>(
        ctx.ComputeJoinStatsV2(
            *leftStats,
            *rightStats,
            leftJoinKeys,
            rightJoinKeys,
            EJoinAlgoType::MapJoin,
            ConvertToJoinKind(join.JoinKind().StringValue()),
            FindCardHint(unionOfLabels, *hints.CardinalityHints),
            false,
            false,
            FindBytesHint(unionOfLabels, *hints.BytesHints)
        )
    );
    resStats->Labels = std::make_shared<TVector<TString>>();
    resStats->Labels->insert(resStats->Labels->begin(), unionOfLabels.begin(), unionOfLabels.end());
    typeCtx->SetStats(join.Raw(), resStats);
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for MapJoin: " << resStats->ToString();
}

/**
 * Compute statistics for grace join
 * FIX: Currently we treat all join the same from the cost perspective, need to refine cost function
 */
void InferStatisticsForGraceJoin(
    const TExprNode::TPtr& input,
    TTypeAnnotationContext* typeCtx,
    const IProviderContext& ctx,
    TOptimizerHints hints,
    TShufflingOrderingsByJoinLabels* shufflingOrderingsByJoinLabels
) {
    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TCoGraceJoinCore>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightInput();

    auto leftStats = typeCtx->GetStats(leftArg.Raw());
    auto rightStats = typeCtx->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    auto leftLabels = InferLabels(leftStats, join.LeftKeysColumnNames());
    auto rightLabels = InferLabels(rightStats, join.RightKeysColumnNames());

    leftStats = ApplyRowsHints(leftStats, leftLabels, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightLabels, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftLabels, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightLabels, *hints.BytesHints);

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (size_t i=0; i<join.LeftKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.LeftKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.LeftKeysColumnNames().Item(i).StringValue());
        leftJoinKeys.push_back(TJoinColumn(alias, attrName));
    }
    for (size_t i=0; i<join.RightKeysColumnNames().Size(); i++) {
        auto alias = ExtractAlias(join.RightKeysColumnNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.RightKeysColumnNames().Item(i).StringValue());
        rightJoinKeys.push_back(TJoinColumn(alias, attrName));
    }

    auto unionOfLabels = UnionLabels(leftLabels, rightLabels);

    auto joinAlgo = EJoinAlgoType::GraceJoin;
    for (size_t i=0; i<join.Flags().Size(); i++) {
        if (join.Flags().Item(i).StringValue() == "Broadcast") {
            joinAlgo = EJoinAlgoType::MapJoin;
            break;
        }
    }

    auto resStats = std::make_shared<TOptimizerStatistics>(
            ctx.ComputeJoinStatsV2(
                *leftStats,
                *rightStats,
                leftJoinKeys,
                rightJoinKeys,
                joinAlgo,
                ConvertToJoinKind(join.JoinKind().StringValue()),
                FindCardHint(unionOfLabels, *hints.CardinalityHints),
                join.LeftInput().Maybe<TDqCnHashShuffle>().IsValid(),
                join.RightInput().Maybe<TDqCnHashShuffle>().IsValid(),
                FindBytesHint(unionOfLabels, *hints.BytesHints)
            )
        );

    resStats->Labels = std::make_shared<TVector<TString>>();
    resStats->Labels->insert(resStats->Labels->begin(), unionOfLabels.begin(), unionOfLabels.end());

    if (shufflingOrderingsByJoinLabels) {
        auto maybeShufflingOrdering = shufflingOrderingsByJoinLabels->GetShufflingOrderigsByJoinLabels(unionOfLabels);
        if (maybeShufflingOrdering) {
            resStats->LogicalOrderings = *maybeShufflingOrdering;
        }
    }

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for GraceJoin with labels: " << "[" << JoinSeq(", ", unionOfLabels) << "]" << ", stats: " << resStats->ToString();
    typeCtx->SetStats(join.Raw(), std::move(resStats));
}

/**
 * Infer statistics for DqJoin
 * DqJoin is an intermediary join representantation in Dq
 */
void InferStatisticsForDqJoinBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx, const IProviderContext& ctx, TOptimizerHints hints) {
    if (auto stats = typeCtx->GetStats(TExprBase(input).Raw())) {
        return;
    }

    auto inputNode = TExprBase(input);
    auto join = inputNode.Cast<TDqJoinBase>();

    auto leftArg = join.LeftInput();
    auto rightArg = join.RightInput();

    auto leftStats = typeCtx->GetStats(leftArg.Raw());
    auto rightStats = typeCtx->GetStats(rightArg.Raw());

    if (!leftStats || !rightStats) {
        return;
    }

    EJoinAlgoType joinAlgo = EJoinAlgoType::Undefined;
    if (auto dqJoin = TMaybeNode<TDqJoin>(input)) {
        joinAlgo = FromString<EJoinAlgoType>(dqJoin.Cast().JoinAlgo().StringValue());
        if (joinAlgo == EJoinAlgoType::Undefined && join.JoinType().StringValue() != "Cross" /* we don't set any join algo to cross join */) {
            return;
        }
    }

    auto leftLabels = InferLabels(leftStats, join.LeftJoinKeyNames());
    auto rightLabels = InferLabels(rightStats, join.RightJoinKeyNames());

    leftStats = ApplyRowsHints(leftStats, leftLabels, *hints.CardinalityHints);
    rightStats = ApplyRowsHints(rightStats, rightLabels, *hints.CardinalityHints);

    leftStats = ApplyBytesHints(leftStats, leftLabels, *hints.BytesHints);
    rightStats = ApplyBytesHints(rightStats, rightLabels, *hints.BytesHints);

    TVector<TJoinColumn> leftJoinKeys;
    TVector<TJoinColumn> rightJoinKeys;

    for (size_t i=0; i<join.LeftJoinKeyNames().Size(); i++) {
        auto alias = ExtractAlias(join.LeftJoinKeyNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.LeftJoinKeyNames().Item(i).StringValue());
        leftJoinKeys.push_back(TJoinColumn(alias, attrName));
    }
    for (size_t i=0; i<join.RightJoinKeyNames().Size(); i++) {
        auto alias = ExtractAlias(join.RightJoinKeyNames().Item(i).StringValue());
        auto attrName = RemoveAliases(join.RightJoinKeyNames().Item(i).StringValue());
        rightJoinKeys.push_back(TJoinColumn(alias, attrName));
    }

    auto unionOfLabels = UnionLabels(leftLabels, rightLabels);

    auto resStats = std::make_shared<TOptimizerStatistics>(
            ctx.ComputeJoinStatsV2(
                *leftStats,
                *rightStats,
                leftJoinKeys,
                rightJoinKeys,
                joinAlgo,
                ConvertToJoinKind(join.JoinType().StringValue()),
                FindCardHint(unionOfLabels, *hints.CardinalityHints),
                false,
                false,
                FindBytesHint(unionOfLabels, *hints.BytesHints)
            )
        );

    resStats->Labels = std::make_shared<TVector<TString>>();
    resStats->Labels->insert(resStats->Labels->begin(), unionOfLabels.begin(), unionOfLabels.end());

    if (auto maybeMapJoin = TMaybeNode<TDqPhyMapJoin>(inputNode.Raw())) {
        resStats->SortingOrderings = leftStats->SortingOrderings;
    }

    typeCtx->SetStats(join.Raw(), resStats);
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for DqJoin: " << resStats->ToString();
}

/**
 * Infer statistics for DqSource
 *
 * We just pass up the statistics from the Settings of the DqSource
 */
void InferStatisticsForDqSource(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto dqSource = inputNode.Cast<TDqSource>();
    auto inputStats = typeCtx->GetStats(dqSource.Settings().Raw());
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats(input.Get(), inputStats);
}

/**
 * For Flatmap we check the input and fetch the statistcs and cost from below
 * Then we analyze the filter predicate and compute it's selectivity and apply it
 * to the result.
 *
 * If this flatmap's lambda is a join, we propagate the join result as the output of FlatMap
 */
void InferStatisticsForFlatMap(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto flatmap = inputNode.Cast<TCoFlatMapBase>();
    auto flatmapInput = flatmap.Input();
    auto inputStats = typeCtx->GetStats(flatmapInput.Raw());

    if (! inputStats ) {
        return;
    }

    if (IsPredicateFlatMap(flatmap.Lambda().Body().Ref())) {
        // Selectivity is the fraction of tuples that are selected by this predicate
        // Currently we just set the number to 10% before we have statistics and parse
        // the predicate

        double selectivity = TPredicateSelectivityComputer(inputStats).Compute(flatmap.Lambda().Body());

        auto outputStats = TOptimizerStatistics(
            inputStats->Type,
            inputStats->Nrows * selectivity,
            inputStats->Ncols,
            inputStats->ByteSize * selectivity,
            inputStats->Cost,
            inputStats->KeyColumns,
            inputStats->ColumnStatistics,
            inputStats->StorageType
        );

        outputStats.SortingOrderings = inputStats->SortingOrderings;
        outputStats.ShuffledByColumns = inputStats->ShuffledByColumns;
        outputStats.LogicalOrderings = inputStats->LogicalOrderings;
        outputStats.SourceTableName = inputStats->SourceTableName;
        outputStats.Aliases = inputStats->Aliases;
        outputStats.Labels = inputStats->Labels;
        outputStats.Selectivity *= (inputStats->Selectivity * selectivity);

        typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(std::move(outputStats)) );
    }
    else if (flatmap.Lambda().Body().Maybe<TCoMapJoinCore>() ||
            flatmap.Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoMapJoinCore>() ||
            flatmap.Lambda().Body().Maybe<TCoJoinDict>() ||
            flatmap.Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoJoinDict>() ||
            flatmap.Lambda().Body().Maybe<TCoGraceJoinCore>() ||
            flatmap.Lambda().Body().Maybe<TCoMap>().Input().Maybe<TCoGraceJoinCore>()){

        typeCtx->SetStats(input.Get(), typeCtx->GetStats(flatmap.Lambda().Body().Raw()));
    }
    else {
        typeCtx->SetStats(input.Get(), typeCtx->GetStats(flatmapInput.Raw()));
    }
}

/**
 * For Filter we check the input and fetch the statistcs and cost from below
 * Then we analyze the filter predicate and compute it's selectivity and apply it
 * to the result, just like in FlatMap, except we check for a specific pattern:
 * If the filter's lambda is an Exists callable with a Member callable, we set the
 * selectivity to 1 to be consistent with SkipNullMembers in the logical plan
 */
void InferStatisticsForFilter(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto filter = inputNode.Cast<TCoFilterBase>();
    auto filterInput = filter.Input();
    auto inputStats = typeCtx->GetStats(filterInput.Raw());

    if (!inputStats){
        return;
    }

    // Selectivity is the fraction of tuples that are selected by this predicate
    // Currently we just set the number to 10% before we have statistics and parse
    // the predicate
    auto filterBody = filter.Lambda().Body();
    double selectivity = TPredicateSelectivityComputer(inputStats).Compute(filterBody);

    auto outputStats = TOptimizerStatistics(
        inputStats->Type,
        inputStats->Nrows * selectivity,
        inputStats->Ncols,
        inputStats->ByteSize * selectivity,
        inputStats->Cost,
        inputStats->KeyColumns,
        inputStats->ColumnStatistics,
        inputStats->StorageType
    );
    outputStats.SortingOrderings = inputStats->SortingOrderings;
    outputStats.ShuffledByColumns = inputStats->ShuffledByColumns;
    outputStats.TableAliases = inputStats->TableAliases;
    outputStats.Aliases = inputStats->Aliases;
    outputStats.SourceTableName = inputStats->SourceTableName;

    outputStats.Selectivity *= (selectivity * inputStats->Selectivity);
    outputStats.Labels = inputStats->Labels;

    typeCtx->SetStats(input.Get(), std::make_shared<TOptimizerStatistics>(std::move(outputStats)) );
}

/**
 * Infer statistics and costs for SkipNullMembers
 * We don't have a good idea at this time how many nulls will be discarded, so we just return the
 * input statistics.
 */
void InferStatisticsForSkipNullMembers(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto skipNullMembers = inputNode.Cast<TCoSkipNullMembers>();
    auto skipNullMembersInput = skipNullMembers.Input();

    auto inputStats = typeCtx->GetStats(skipNullMembersInput.Raw());
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
}

void InferStatisticsForExtendBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto unionAll = inputNode.Cast<TCoExtendBase>();

    auto stats = std::make_shared<TOptimizerStatistics>();
    for (const auto& input: input->Children()) {
        if (auto inputStats = typeCtx->GetStats(input.Get())) {
            stats->Nrows += inputStats->Nrows;
            stats->ByteSize += inputStats->ByteSize;
            stats->Cost += inputStats->Cost;
        }
    }

    if (typeCtx->OrderingsFSM) {
        stats->LogicalOrderings = typeCtx->OrderingsFSM->CreateState();
    }
    typeCtx->SetStats( input.Get(), std::move(stats) );
}

/**
 * Infer statistics and costs for AggregateCombine
 * We just return the input statistics.
*/
void InferStatisticsForAggregateBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto agg = inputNode.Cast<TCoAggregateBase>();
    auto aggInput = agg.Input();

    auto inputStats = typeCtx->GetStats(aggInput.Raw());
    if (!inputStats) {
        return;
    }

    auto aggStats = std::make_shared<TOptimizerStatistics>(*inputStats);

    aggStats->TableAliases = inputStats->TableAliases;
    aggStats->Aliases = inputStats->Aliases;

    auto orderingInfo = GetAggregationBaseShuffleOrderingInfo(agg, typeCtx->OrderingsFSM, inputStats->TableAliases.Get());
    aggStats->ShufflingOrderingIdx = orderingInfo.OrderingIdx;
    if (typeCtx->OrderingsFSM) {
        aggStats->LogicalOrderings = typeCtx->OrderingsFSM->CreateState(orderingInfo.OrderingIdx);
    }

    TVector<TString> strKeys;
    strKeys.reserve(agg.Keys().Size());
    for (const auto& key: agg.Keys()) {
        strKeys.push_back(key.StringValue());
    }
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for AggregateBase with keys: " << JoinSeq(", ", strKeys) << ", with stats: " << aggStats->ToString();
    typeCtx->SetStats(input.Get(), std::move(aggStats));
}

/**
 * Infer statistics and costs for AggregateMergeFinalize
 * Just return input stats
*/
void InferStatisticsForAggregateMergeFinalize(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    auto inputNode = TExprBase(input);
    auto agg = inputNode.Cast<TCoAggregateMergeFinalize>();
    auto aggInput = agg.Input();

    auto inputStats = typeCtx->GetStats(aggInput.Raw() );
    if (!inputStats) {
        return;
    }

    typeCtx->SetStats( input.Get(), inputStats );
}

void InferStatisticsForAsList(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    double nRows = input->ChildrenSize();
    int nAttrs = 5;
    if (input->ChildrenSize() && input->Child(0)->IsCallable("AsStruct")) {
        nAttrs = input->Child(0)->ChildrenSize();
    }
    auto outputStats = std::make_shared<TOptimizerStatistics>(
        EStatisticsType::BaseTable, nRows, nAttrs, nRows*nAttrs, 0.0);
    outputStats->StorageType = EStorageType::RowStorage;
    typeCtx->SetStats(input.Get(), outputStats);
}

/***
 * Infer statistics for a list of structs
 */
bool InferStatisticsForListParam(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto param = TCoParameter(input);
    if (param.Name().Maybe<TCoAtom>()) {
        auto atom = param.Name().Cast<TCoAtom>();
        if (atom.Value().StartsWith("%kqp%tx_result_binding")) {
            return false;
        }
    }

    if (auto maybeListType = param.Type().Maybe<TCoListType>()) {
        auto itemType = maybeListType.Cast().ItemType();
        if (auto maybeStructType = itemType.Maybe<TCoStructType>()) {
            int nRows = 100;
            int nAttrs = maybeStructType.Cast().Ptr()->ChildrenSize();
            auto resStats = std::make_shared<TOptimizerStatistics>(EStatisticsType::BaseTable, nRows, nAttrs, nRows*nAttrs, 0.0);
            resStats->StorageType = EStorageType::RowStorage;
            typeCtx->SetStats(input.Get(), resStats);
        }
    }
    return true;
}

/***
 * For callables that include lambdas, we want to propagate the statistics from lambda's input to its argument, so
 * that the operators inside lambda receive the correct statistics
*/
void PropagateStatisticsToLambdaArgument(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {

    if (input->ChildrenSize()<2) {
        return;
    }

    auto callableInput = input->ChildRef(0);

    // Iterate over all children except for the input
    // Check if the child is a lambda and propagate the statistics into it
    for (size_t i=1; i<input->ChildrenSize(); i++) {
        auto maybeLambda = TExprBase(input->ChildRef(i));
        if (!maybeLambda.Maybe<TCoLambda>()) {
            continue;
        }

        auto lambda = maybeLambda.Cast<TCoLambda>();
        if (!lambda.Args().Size()){
            continue;
        }

        // If the input to the callable is a list, then lambda also takes a list of arguments
        // So we need to propagate corresponding arguments

        if (callableInput->IsList()){
            for(size_t j=0; j<callableInput->ChildrenSize(); j++){
                auto inputStats = typeCtx->GetStats(callableInput->Child(j) );
                if (inputStats){
                    typeCtx->SetStats( lambda.Args().Arg(j).Raw(), inputStats );
                }
            }

        }
        else {
            auto inputStats = typeCtx->GetStats(callableInput.Get());
            if (!inputStats) {
                return;
            }

            // We have a special case of Olap tables, where statistics is computed before lambda, but
            // is finalized after visiting labda (which may contain a filter)
            if (typeCtx->GetStats(input.Get())){
                inputStats = typeCtx->GetStats(input.Get());
            }

            typeCtx->SetStats( lambda.Args().Arg(0).Raw(), inputStats );
        }
    }
}

/**
 * After processing the lambda for the stage we set the stage output to the result of the lambda
*/
void InferStatisticsForStage(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto stage = inputNode.Cast<TDqStageBase>();

    auto lambdaStats = typeCtx->GetStats( stage.Program().Body().Raw());
    if (lambdaStats){
        typeCtx->SetStats( stage.Raw(), lambdaStats );
    }
}

void InferStatisticsForDqMerge(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto merge = inputNode.Cast<TDqCnMerge>();

    auto inputStats = typeCtx->GetStats(merge.Output().Raw());
    if (!inputStats) {
        return;
    }

    auto newStats = std::make_shared<TOptimizerStatistics>(*inputStats);

    TVector<TJoinColumn> sorting;
    sorting.reserve(merge.SortColumns().Size());
    for (const auto& c : merge.SortColumns()) {
        auto column = c.Column().StringValue();
        auto sortDir = c.SortDirection().StringValue();

        if (sortDir != "Asc") {
            sorting.clear();
            break;
        }

        auto alias = ExtractAlias(column);
        auto columnNoAlias = RemoveAliases(column);
        sorting.emplace_back(std::move(alias), std::move(columnNoAlias));
    }

    YQL_CLOG(TRACE, CoreDq) << "DqCnMerge input stats: " << inputStats->ToString();
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for Merge: " << newStats->ToString();

    typeCtx->SetStats(merge.Raw(), newStats);
}

void InferStatisticsForUnionAll(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto unionAll = inputNode.Cast<TCoUnionAll>();

    auto stats = std::make_shared<TOptimizerStatistics>();
    for (const auto& input: input->Children()) {
        if (auto inputStats = typeCtx->GetStats(input.Get())) {
            stats->Nrows += inputStats->Nrows;
            stats->ByteSize += inputStats->ByteSize;
            stats->Cost += inputStats->Cost;
        }
    }

    typeCtx->SetStats(inputNode.Raw(), std::move(stats));
}

/**
 * Just update the sorted order with alias
 */
void InferStatisticsForDqPhyCrossJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto cross = inputNode.Cast<TDqPhyCrossJoin>();

    auto inputStats = typeCtx->GetStats(cross.LeftInput().Raw());
    if (!inputStats) {
        return;
    }

    auto outputStats = RemoveSorting(inputStats);
    typeCtx->SetStats(cross.Raw(), outputStats);
}

std::shared_ptr<TOptimizerStatistics> RemoveSorting(const std::shared_ptr<TOptimizerStatistics>& stats) {
    if (stats->SortingOrderings.HasState()) {
        auto newStats = *stats;
        newStats.SortingOrderings.RemoveState();
        return std::make_shared<TOptimizerStatistics>(std::move(newStats));
    } else {
        return stats;
    }
}

std::shared_ptr<TOptimizerStatistics> RemoveSorting(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input) {
    if (
        TDqCnHashShuffle::Match(input.Get()) ||
        TDqCnBroadcast::Match(input.Get()) ||
        TDqCnUnionAll::Match(input.Get())
    ) {
        return RemoveSorting(stats);
    } else {
        return stats;
    }
}

std::shared_ptr<TOptimizerStatistics> RemoveShuffling(const std::shared_ptr<TOptimizerStatistics>& stats) {
    if (stats->LogicalOrderings.HasState()) {
        auto newStats = *stats;
        newStats.LogicalOrderings.RemoveState();
        return std::make_shared<TOptimizerStatistics>(std::move(newStats));
    } else {
        return stats;
    }
}

std::shared_ptr<TOptimizerStatistics> RemoveShuffling(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input) {
    if (
        TDqCnMerge::Match(input.Get()) ||
        TDqCnBroadcast::Match(input.Get()) ||
        TDqCnUnionAll::Match(input.Get())
    ) {
        return RemoveShuffling(stats);
    } else {
        return stats;
    }
}

std::shared_ptr<TOptimizerStatistics> RemoveOrderings(const std::shared_ptr<TOptimizerStatistics>& stats, const TExprNode::TPtr& input) {
    return RemoveSorting(RemoveShuffling(stats, input), input);
}

void InferStatisticsForAsStruct(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto asStruct = inputNode.Cast<TCoAsStruct>();

    TTableAliasMap aliases;
    std::shared_ptr<TOptimizerStatistics> inputStats;
    TString structString;
    for (const auto& field: input->Children()) {
        if (field->ChildrenSize() != 2) {
            continue;
        }

        auto maybeAtom = field->Child(0);
        if (!maybeAtom->IsAtom()) {
            continue;
        }

        auto renameTo = TString(maybeAtom->Content());
        structString.append(renameTo).append(";");
        if (renameTo.empty()) {
            continue;
        }

        auto maybeMember = field->Child(1);
        if (!TCoMember::Match(maybeMember)) {
            continue;
        }
        auto member = TExprBase(maybeMember).Cast<TCoMember>();

        auto renameFrom = member.Name().StringValue();
        if (renameFrom.empty()) {
            continue;
        }

        auto memberStats = typeCtx->GetStats(member.Struct().Raw());
        if (inputStats == nullptr) {
            inputStats = memberStats;
        }

        if (memberStats && memberStats->TableAliases) {
            aliases.Merge(*memberStats->TableAliases);
        }

        aliases.AddRename(renameFrom, renameTo);
    }

    std::shared_ptr<TOptimizerStatistics> stats;
    if (inputStats == nullptr) {
        stats = std::make_shared<TOptimizerStatistics>();
    } else {
        stats = std::make_shared<TOptimizerStatistics>(*inputStats);
    }

    stats->TableAliases = MakeIntrusive<TTableAliasMap>(std::move(aliases));
    YQL_CLOG(TRACE, CoreDq) << "Propogate TableAliases for Struct[" << structString << "]: " << stats->TableAliases->ToString();
    typeCtx->SetStats(inputNode.Raw(), std::move(stats));
}

void InferStatisticsForTopBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto topBase = inputNode.Cast<TCoTopBase>();

    auto inputStats = typeCtx->GetStats(topBase.Input().Raw());
    if (!inputStats) {
        return;
    }

    auto topStats = std::make_shared<TOptimizerStatistics>(*inputStats);
    auto orderingInfo = GetTopBaseSortingOrderingInfo(topBase, typeCtx->SortingsFSM, topStats->TableAliases.Get());
    if (
        typeCtx->SortingsFSM &&
        !topStats->SortingOrderings.ContainsSorting(orderingInfo.OrderingIdx) &&
        inputNode.Maybe<TCoTopSort>() // TopBase can be Top, which doesn't sort the input
    ) {
        topStats->SortingOrderings = typeCtx->SortingsFSM->CreateState(orderingInfo.OrderingIdx);
    }
    topStats->SortingOrderingIdx = orderingInfo.OrderingIdx;

    TString propagatedAliases;
    if (topStats->TableAliases) {
        propagatedAliases = topStats->TableAliases ? topStats->TableAliases->ToString() : "empty";
    }
    YQL_CLOG(TRACE, CoreDq) << "Input of the TopBase: " << inputStats->ToString();
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for TopBase: " << topStats->ToString() << ", propagated aliases: " << propagatedAliases;
    typeCtx->SetStats(inputNode.Raw(), std::move(topStats));
}

void InferStatisticsForSortBase(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto sortBase = inputNode.Cast<TCoSortBase>();

    auto inputStats = typeCtx->GetStats(sortBase.Input().Raw());
    if (!inputStats) {
        return;
    }
    auto topStats = std::make_shared<TOptimizerStatistics>(*inputStats);
    auto orderingInfo = GetSortBaseSortingOrderingInfo(sortBase, typeCtx->SortingsFSM, topStats->TableAliases.Get());
    if (typeCtx->SortingsFSM && !topStats->SortingOrderings.ContainsSorting(orderingInfo.OrderingIdx)) {
        topStats->SortingOrderings = typeCtx->SortingsFSM->CreateState(orderingInfo.OrderingIdx);
    }
    topStats->SortingOrderingIdx = orderingInfo.OrderingIdx;

    TString propagatedAliases;
    if (topStats->TableAliases) {
        propagatedAliases = topStats->TableAliases ? topStats->TableAliases->ToString() : "empty";
    }
    YQL_CLOG(TRACE, CoreDq) << "Input of the SortBase: " << inputStats->ToString();
    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for SortBase: " << topStats->ToString() << ", propagated aliases: " << propagatedAliases;
    typeCtx->SetStats(inputNode.Raw(), std::move(topStats));
}

template <typename TAggregationCallable>
void InferStatisticsForAggregationCallable(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto inputNode = TExprBase(input);
    auto aggr = inputNode.Cast<TAggregationCallable>();

    auto inputStats = typeCtx->GetStats(aggr.Input().Raw());
    if (!inputStats) {
        return;
    }

    TOptimizerStatistics aggStats = *inputStats;

    auto& shufflingsFSM = typeCtx->OrderingsFSM;
    if (shufflingsFSM) {
        auto shuffling = TShuffling(GetKeySelectorOrdering(aggr.KeySelectorLambda()));
        std::int64_t orderingIdx = shufflingsFSM->FDStorage.FindShuffling(shuffling, inputStats->TableAliases.Get());
        if (!inputStats->LogicalOrderings.ContainsShuffle(orderingIdx)) {
            aggStats.LogicalOrderings = shufflingsFSM->CreateState(orderingIdx);
        }
        aggStats.ShufflingOrderingIdx = orderingIdx;
    }

    YQL_CLOG(TRACE, CoreDq) << "Infer statistics for " << input->Content() << " with stats: " << aggStats.ToString();
    typeCtx->SetStats(aggr.Raw(), std::make_shared<TOptimizerStatistics>(std::move(aggStats)));
}

template void InferStatisticsForAggregationCallable<TCoShuffleByKeys>(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx);

void InferStatisticsForEquiJoin(const TExprNode::TPtr& input, TTypeAnnotationContext* typeCtx) {
    auto equiJoin = TExprBase(input).Cast<TCoEquiJoin>();

    TTableAliasMap tableAliases;
    for (size_t i = 0; i < equiJoin.ArgCount() - 2; ++i) {
        auto input = equiJoin.Arg(i).Cast<TCoEquiJoinInput>();

        auto scope = input.Scope();
        if (!scope.Maybe<TCoAtom>()){
            continue;
        }

        TString label = scope.Cast<TCoAtom>().StringValue();
        auto joinArg = input.List();
        auto inputStats = typeCtx->GetStats(joinArg.Raw());
        if (inputStats == nullptr) {
            continue;
        }

        if (inputStats->Aliases) {
            inputStats->Aliases->insert(std::move(label));
        } else
        if (inputStats->TableAliases) {
            tableAliases.Merge(*inputStats->TableAliases);
        }
    }

    auto joinSettings = equiJoin.Arg(equiJoin.ArgCount() - 1);
    for (const auto& option : joinSettings.Ref().Children()) {
        if (option->Head().IsAtom("rename")) {
            TCoAtom fromName{option->Child(1)};
            YQL_ENSURE(!fromName.Value().empty());
            TCoAtom toName{option->Child(2)};
            if (!toName.Value().empty()) {
                tableAliases.AddRename(fromName.StringValue(), toName.StringValue());
            }
        }
    }

    if (tableAliases.Empty()) {
        return;
    }

    YQL_CLOG(TRACE, CoreDq) << "Propogate TableAliases for EquiJoin: " << tableAliases.ToString();

    if (auto equiJoinStats = typeCtx->GetStats(equiJoin.Raw())) {
        equiJoinStats->TableAliases = MakeIntrusive<TTableAliasMap>(std::move(tableAliases));
    } else {
        equiJoinStats = std::make_shared<TOptimizerStatistics>();
        equiJoinStats->TableAliases = MakeIntrusive<TTableAliasMap>(std::move(tableAliases));
        typeCtx->SetStats(equiJoin.Raw(), std::move(equiJoinStats));
    }
}

TOrderingInfo GetAggregationBaseShuffleOrderingInfo(
    const NNodes::TCoAggregateBase& aggregationBase,
    const TSimpleSharedPtr<TOrderingsStateMachine>& shufflingsFSM,
    TTableAliasMap* tableAlias
) {
    TVector<TJoinColumn> ordering;
    ordering.reserve(aggregationBase.Keys().Size());
    for (const auto& key: aggregationBase.Keys()) {
        TString aggregationKey = key.StringValue();
        ordering.emplace_back(TJoinColumn::FromString(aggregationKey));
    }

    std::int64_t orderingIdx = -1;
    if (shufflingsFSM) {
        auto shuffling = TShuffling(ordering);
        orderingIdx = shufflingsFSM->FDStorage.FindShuffling(shuffling, tableAlias);
    }

    return TOrderingInfo{
        .OrderingIdx = orderingIdx,
        .Ordering = std::move(ordering)
    };
}

TVector<TJoinColumn> GetKeySelectorOrdering(
    const NNodes::TCoLambda& keySelector
) {
    TVector<TJoinColumn> ordering;
    if (auto body = keySelector.Body().template Maybe<TCoMember>()) {
        ordering.push_back(TJoinColumn::FromString(body.Cast().Name().StringValue()));
    } else if (auto body = keySelector.Body().template Maybe<TExprList>()) {
        for (size_t i = 0; i < body.Cast().Size(); ++i) {
            auto item = body.Cast().Item(i);

            auto collectMember = [&ordering](auto&& self, const TExprBase& item) -> void {
                if (auto member = item.Maybe<TCoMember>()) {
                    ordering.push_back(TJoinColumn::FromString(member.Cast().Name().StringValue()));
                }

                if (auto coalesce = item.Maybe<TCoCoalesce>()) {
                    self(self, TExprBase(coalesce.Cast().Predicate()));
                }
            };

            collectMember(collectMember, item);
        }
    }

    return ordering;
}

template <typename TSortCallable>
TOrderingInfo GetSortingOrderingInfoImpl(
    const TSortCallable& sortCallable,
     const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM,
    TTableAliasMap* tableAlias
) {
    const auto& keySelector = sortCallable.KeySelectorLambda();
    TVector<TJoinColumn> sorting = GetKeySelectorOrdering(keySelector);

    auto getDirection = [] (TExprBase expr) {
        if (!expr.Maybe<TCoBool>()) {
            return TOrdering::TItem::EDirection::ENone;
        }

        if (!FromString<bool>(expr.Cast<TCoBool>().Literal().Value())) {
            return TOrdering::TItem::EDirection::EDescending;
        }

        return TOrdering::TItem::EDirection::EAscending;
    };

    std::vector<TOrdering::TItem::EDirection> directions;
    const auto& sortDirections = sortCallable.SortDirections();
    if (auto maybeList = sortDirections.template Maybe<TExprList>()) {
        for (const auto& expr : maybeList.Cast()) {
            directions.push_back(getDirection(expr));
        }
    } else if (auto maybeBool = sortDirections.template Maybe<TCoBool>()){
        directions.push_back(getDirection(TExprBase(maybeBool.Cast())));
    }

    if (directions.empty()) {
        return TOrderingInfo();
    }

    std::int64_t orderingIdx = -1;
    if (sortingsFSM) {
        orderingIdx = sortingsFSM->FDStorage.FindSorting(TSorting(sorting, directions), tableAlias);
    }

    return TOrderingInfo{
        .OrderingIdx = orderingIdx,
        .Directions = std::move(directions),
        .Ordering = std::move(sorting)
    };
}

TOrderingInfo GetSortBaseSortingOrderingInfo(
    const NNodes::TCoSortBase& sort,
    const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM,
    TTableAliasMap* tableAlias
) {
    return GetSortingOrderingInfoImpl(sort, sortingsFSM, tableAlias);
}

TOrderingInfo GetTopBaseSortingOrderingInfo(
    const NNodes::TCoTopBase& topBase,
    const TSimpleSharedPtr<TOrderingsStateMachine>& sortingsFSM,
    TTableAliasMap* tableAlias
) {
    return GetSortingOrderingInfoImpl(topBase, sortingsFSM, tableAlias);
}

} // namespace NYql::NDq {
